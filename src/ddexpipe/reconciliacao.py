"""Reconciliação entre raw, índice, silver, gold, ledger, banco e retorno. Devolve a lista de furos."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pyspark.sql import SparkSession

from . import carga, escrita
from .config import Config
from .spark import t


def reconciliar(spark: SparkSession, cfg: Config) -> list[str]:
    furos: list[str] = []
    arquivos = escrita.como_dicts(spark.sql(
        f"SELECT file_id, nome, caminho_raw, estado FROM {t('bronze.arquivo')}"))

    for a in arquivos:
        raw = Path(a["caminho_raw"])
        if not raw.exists():
            furos.append(f"raw ausente: {a['nome']}")
        elif hashlib.sha256(raw.read_bytes()).hexdigest() != a["file_id"]:
            furos.append(f"raw alterado depois de recebido: {a['nome']}")
        if a["estado"] not in ("CARREGADO", "QUARENTENA"):
            furos.append(f"arquivo parado em {a['estado']}: {a['nome']}")

    sem_destino = spark.sql(f"""
        SELECT s.file_id, s.recurso_ref FROM {t('silver.gravacao')} s
        LEFT ANTI JOIN (SELECT file_id, recurso_ref FROM {t('gold.gravacao')}
                        UNION ALL SELECT file_id, recurso_ref FROM {t('gold.rejeicao')}) d
          ON s.file_id = d.file_id AND s.recurso_ref = d.recurso_ref""").count()
    if sem_destino:
        furos.append(f"{sem_destino} gravações do silver sem destino no gold")

    carregados = f"(SELECT file_id FROM {t('bronze.arquivo')} WHERE estado = 'CARREGADO')"
    fora_do_ledger = spark.sql(f"""
        SELECT g.file_id, g.isrc FROM {t('gold.gravacao')} g
        LEFT ANTI JOIN {t('gold.ledger_carga')} c ON g.file_id = c.file_id AND g.isrc = c.isrc
        WHERE g.file_id IN {carregados}""").count()
    if fora_do_ledger:
        furos.append(f"{fora_do_ledger} gravações de arquivos CARREGADO fora do ledger")
    ids_ledger = {linha.id_banco for linha in spark.table(t("gold.ledger_carga")).select("id_banco").collect()}
    con = carga.conectar(cfg)
    try:
        ids_banco = {linha[0] for linha in con.execute("SELECT id FROM gravacao")}
    finally:
        con.close()
    if ids_ledger != ids_banco:
        furos.append(f"ledger e banco divergem: {len(ids_ledger - ids_banco)} só no ledger, "
                     f"{len(ids_banco - ids_ledger)} só no banco")

    esperados = {(r.lote, r.message_id): r.n for r in spark.sql(f"""
        SELECT m.lote, m.message_id, count(x.recurso_ref) AS n
        FROM {t('silver.mensagem')} m
        LEFT JOIN (SELECT file_id, recurso_ref FROM {t('silver.gravacao')}
                   UNION ALL SELECT file_id, recurso_ref FROM {t('quarentena.recurso')}) x ON x.file_id = m.file_id
        WHERE m.file_id IN {carregados}
        GROUP BY m.lote, m.message_id""").collect()}
    for (lote, message_id), n in sorted(esperados.items()):
        xml = cfg.saida / lote / f"{message_id}.xml"
        manifesto = cfg.saida / lote / f"{message_id}.manifest.json"
        if not xml.exists() or not manifesto.exists():
            furos.append(f"retorno ausente: {message_id}")
            continue
        dados = json.loads(manifesto.read_text(encoding="utf-8"))
        if dados["sha256"] != hashlib.sha256(xml.read_bytes()).hexdigest():
            furos.append(f"retorno alterado depois de gerado: {message_id}")
        if dados["itens"] != n:
            furos.append(f"retorno de {message_id} com {dados['itens']} status para {n} gravações")
    return furos
