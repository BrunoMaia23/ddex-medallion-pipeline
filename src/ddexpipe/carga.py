"""Carga no banco transacional (aqui um SQLite no lugar do Oracle).

Uma transação por arquivo e um SAVEPOINT por gravação. O ledger é gravado depois do COMMIT, e a
chave única (message_id, isrc) cobre o caso de o processo cair entre os dois.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Callable

from pyspark.sql import SparkSession

from . import escrita
from .config import Config
from .spark import t

DDL = [
    """CREATE TABLE IF NOT EXISTS gravacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL, isrc TEXT NOT NULL, titulo VARCHAR(100) NOT NULL,
        duracao_seg INTEGER NOT NULL, data_referencia TEXT NOT NULL, titular_id TEXT NOT NULL,
        share REAL, criticas TEXT,
        UNIQUE (message_id, isrc))""",
    """CREATE TABLE IF NOT EXISTS participante (
        gravacao_id INTEGER NOT NULL REFERENCES gravacao(id), nome TEXT, classe TEXT)""",
    """CREATE TABLE IF NOT EXISTS controle_arquivo (
        file_id TEXT PRIMARY KEY, message_id TEXT NOT NULL, carregado_em TEXT NOT NULL)""",
]


def conectar(cfg: Config) -> sqlite3.Connection:
    cfg.banco.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(cfg.banco, isolation_level=None)  # transações controladas à mão
    for sql in DDL:
        con.execute(sql)
    return con


def carregar(spark: SparkSession, cfg: Config, falhar: Callable[[dict], bool] | None = None) -> dict:
    """Carrega os arquivos em REGRAS_OK. `falhar` simula erro em gravações escolhidas (testes)."""
    arquivos = [linha.file_id for linha in spark.sql(
        f"SELECT file_id FROM {t('bronze.arquivo')} WHERE estado = 'REGRAS_OK' ORDER BY nome").collect()]
    if not arquivos:
        return {"arquivos": 0, "gravacoes": 0, "recuperadas": 0, "erros": 0}
    lista = ", ".join(f"'{f}'" for f in arquivos)
    ledger = {(linha.file_id, linha.isrc) for linha in spark.sql(
        f"SELECT file_id, isrc FROM {t('gold.ledger_carga')} WHERE file_id IN ({lista})").collect()}
    gravacoes = escrita.como_dicts(spark.sql(
        f"SELECT * FROM {t('gold.gravacao')} WHERE file_id IN ({lista}) ORDER BY file_id, recurso_ref"))
    participantes: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for p in escrita.como_dicts(spark.sql(
            f"SELECT file_id, isrc, nome, classe FROM {t('gold.participante')} WHERE file_id IN ({lista})")):
        participantes[(p["file_id"], p["isrc"])].append(p)
    mensagens = {linha.file_id: linha.message_id for linha in spark.sql(
        f"SELECT file_id, message_id FROM {t('silver.mensagem')} WHERE file_id IN ({lista})").collect()}

    por_arquivo: dict[str, list[dict]] = defaultdict(list)
    for g in gravacoes:
        por_arquivo[g["file_id"]].append(g)

    resumo = {"arquivos": 0, "gravacoes": 0, "recuperadas": 0, "erros": 0}
    estados: dict[str, str] = {}
    con = conectar(cfg)
    try:
        for file_id in arquivos:
            confirmadas, erros = [], 0
            con.execute("BEGIN")
            for g in por_arquivo.get(file_id, []):
                if (file_id, g["isrc"]) in ledger:
                    continue
                con.execute("SAVEPOINT gravacao")
                try:
                    if falhar and falhar(g):
                        raise sqlite3.OperationalError("falha simulada")
                    cursor = con.execute(
                        "INSERT INTO gravacao (message_id, isrc, titulo, duracao_seg, data_referencia, "
                        "titular_id, share, criticas) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [g["message_id"], g["isrc"], g["titulo"], g["duracao_seg"], g["data_referencia"],
                         g["titular_id"], g["share"], g["criticas"]])
                    id_banco = cursor.lastrowid
                    con.executemany("INSERT INTO participante VALUES (?, ?, ?)",
                                    [(id_banco, p["nome"], p["classe"]) for p in participantes[(file_id, g["isrc"])]])
                    resumo["gravacoes"] += 1
                except sqlite3.IntegrityError:
                    # já está no banco: a execução anterior caiu entre o COMMIT e o ledger
                    con.execute("ROLLBACK TO gravacao")
                    id_banco = con.execute("SELECT id FROM gravacao WHERE message_id = ? AND isrc = ?",
                                           [g["message_id"], g["isrc"]]).fetchone()[0]
                    resumo["recuperadas"] += 1
                except sqlite3.Error:
                    con.execute("ROLLBACK TO gravacao")
                    con.execute("RELEASE gravacao")
                    erros += 1
                    continue
                con.execute("RELEASE gravacao")
                confirmadas.append({"file_id": file_id, "message_id": g["message_id"], "isrc": g["isrc"],
                                    "id_banco": id_banco})
            if not erros:
                con.execute("INSERT OR IGNORE INTO controle_arquivo VALUES (?, ?, datetime('now'))",
                            [file_id, mensagens.get(file_id, "")])
            con.execute("COMMIT")  # o que deu certo fica, mesmo com erro em outra gravação
            escrita.anexar(spark, cfg, "gold.ledger_carga", confirmadas)  # ledger só depois do commit
            ledger.update((c["file_id"], c["isrc"]) for c in confirmadas)
            resumo["erros"] += erros
            if not erros:
                estados[file_id] = "CARREGADO"
                resumo["arquivos"] += 1
    finally:
        con.close()
    escrita.mudar_estados(spark, cfg, estados)
    return resumo
