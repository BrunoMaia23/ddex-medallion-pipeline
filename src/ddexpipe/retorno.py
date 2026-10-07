"""Arquivo de retorno ao parceiro, com um status por gravação e um manifesto com o hash.

Só sai para mensagem já carregada. Se o conteúdo não mudou, gerar de novo não regrava nada.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from xml.sax.saxutils import escape

from pyspark.sql import SparkSession

from . import escrita
from .config import Config
from .spark import t

ACEITA, CONFLITO, REJEITADA = "Accepted", "PotentialConflict", "Rejected"


def gerar(spark: SparkSession, cfg: Config) -> dict:
    carregados = f"(SELECT file_id FROM {t('bronze.arquivo')} WHERE estado = 'CARREGADO')"
    mensagens = escrita.como_dicts(spark.sql(
        f"SELECT lote, file_id, message_id FROM {t('silver.mensagem')} WHERE file_id IN {carregados} "
        "ORDER BY lote, message_id"))
    itens: dict[str, list[dict]] = defaultdict(list)
    for g in escrita.como_dicts(spark.sql(
            f"SELECT file_id, recurso_ref, isrc, criticas FROM {t('gold.gravacao')} WHERE file_id IN {carregados}")):
        status = CONFLITO if g["criticas"] and "ISRC_JA_CADASTRADO" in g["criticas"] else ACEITA
        itens[g["file_id"]].append({"recurso_ref": g["recurso_ref"], "isrc": g["isrc"], "status": status, "motivo": None})
    for tabela in ("gold.rejeicao", "quarentena.recurso"):
        for r in escrita.como_dicts(spark.sql(
                f"SELECT file_id, recurso_ref, isrc, motivo FROM {t(tabela)} WHERE file_id IN {carregados}")):
            itens[r["file_id"]].append({"recurso_ref": r["recurso_ref"], "isrc": r["isrc"],
                                        "status": REJEITADA, "motivo": r["motivo"]})

    resumo = {"mensagens": 0, "novos_arquivos": 0}
    por_lote: dict[str, list[dict]] = defaultdict(list)
    for m in mensagens:
        lista = sorted(itens[m["file_id"]], key=lambda i: _ordem(i["recurso_ref"]))
        conteudo = _xml(m["message_id"], lista).encode("utf-8")
        if _escrever_se_mudou(cfg.saida / m["lote"], m["message_id"], conteudo, lista):
            resumo["novos_arquivos"] += 1
        resumo["mensagens"] += 1
        por_lote[m["lote"]] += [{"lote": m["lote"], "file_id": m["file_id"], "message_id": m["message_id"], **i}
                                for i in lista]
    for lote, registros in por_lote.items():
        escrita.substituir_lote(spark, cfg, "gold.retorno_item", lote, registros)
    return resumo


def _ordem(recurso_ref: str) -> tuple[int, str]:
    numero = "".join(c for c in recurso_ref or "" if c.isdigit())
    return (int(numero) if numero else 0, recurso_ref or "")


def _xml(message_id: str, itens: list[dict]) -> str:
    linhas = ['<?xml version="1.0" encoding="UTF-8"?>', "<RightsClaimStatusUpdate>",
              f"  <RelatedMessageId>{escape(message_id)}</RelatedMessageId>"]
    for i in itens:
        motivo = f"<Reason>{escape(i['motivo'])}</Reason>" if i["motivo"] else ""
        isrc = escape(i["isrc"] or "")
        linhas.append(f"  <ClaimStatus><ResourceReference>{escape(i['recurso_ref'])}</ResourceReference>"
                      f"<ISRC>{isrc}</ISRC><Status>{i['status']}</Status>{motivo}</ClaimStatus>")
    linhas.append("</RightsClaimStatusUpdate>")
    return "\n".join(linhas) + "\n"


def _escrever_se_mudou(pasta: Path, message_id: str, conteudo: bytes, itens: list[dict]) -> bool:
    destino = pasta / f"{message_id}.xml"
    if destino.exists() and destino.read_bytes() == conteudo:
        return False
    pasta.mkdir(parents=True, exist_ok=True)
    manifesto = {
        "arquivo": destino.name,
        "sha256": hashlib.sha256(conteudo).hexdigest(),
        "itens": len(itens),
        "por_status": {s: sum(1 for i in itens if i["status"] == s) for s in (ACEITA, CONFLITO, REJEITADA)},
    }
    for caminho, dados in ((destino, conteudo),
                           (pasta / f"{message_id}.manifest.json",
                            json.dumps(manifesto, ensure_ascii=False, indent=2).encode("utf-8"))):
        tmp = caminho.with_suffix(caminho.suffix + ".tmp")
        tmp.write_bytes(dados)
        os.replace(tmp, caminho)
    return True
