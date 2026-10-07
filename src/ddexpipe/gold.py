"""Gold: join com a referência no Spark e decisão por gravação em regras.py."""
from __future__ import annotations

from collections import Counter

from pyspark.sql import SparkSession

from . import escrita, regras
from .config import Config
from .spark import t

PARTES_TESTE = {"PA-9999"}


def processar(spark: SparkSession, cfg: Config) -> dict:
    resumo = {"carregaveis": 0, "rejeicoes": Counter(), "criticas": Counter()}
    for lote in escrita.lotes_com_estado(spark, "PARSEADO"):
        gravacoes = escrita.como_dicts(spark.sql(f"""
            SELECT g.*, r.isrc IS NOT NULL AS ja_cadastrado
            FROM {t('silver.gravacao')} g
            LEFT JOIN {t('ref.isrc_cadastrado')} r ON g.isrc = r.isrc
            WHERE g.lote = '{lote}'
            ORDER BY g.file_id, g.recurso_ref"""))
        participantes = escrita.como_dicts(spark.sql(
            f"SELECT file_id, recurso_ref, nome, papel FROM {t('silver.participante')} WHERE lote = '{lote}'"))
        por_recurso: dict[tuple[str, str], list[dict]] = {}
        for p in participantes:
            por_recurso.setdefault((p["file_id"], p["recurso_ref"]), []).append(p)

        saida_gravacao, saida_participante, saida_rejeicao = [], [], []
        estados = {linha.file_id: "REGRAS_OK" for linha in spark.sql(
            f"SELECT file_id FROM {t('bronze.arquivo')} WHERE lote = '{lote}' AND estado = 'PARSEADO'").collect()}
        for g in gravacoes:
            chave = {"lote": lote, "file_id": g["file_id"], "message_id": g["message_id"], "isrc": g["isrc"]}
            decisao = regras.avaliar(g, PARTES_TESTE, g["ja_cadastrado"])
            novo = g["file_id"] in estados
            if not decisao.carregar:
                saida_rejeicao.append({**chave, "recurso_ref": g["recurso_ref"], "motivo": decisao.motivo})
                if novo:
                    resumo["rejeicoes"][decisao.motivo] += 1
                continue
            saida_gravacao.append({
                **chave, "recurso_ref": g["recurso_ref"], "titulo": g["titulo"],
                "duracao_seg": regras.duracao_em_segundos(g["duracao_iso"]),
                "data_referencia": regras.data_de_referencia(g["data_lancamento"], g["data_criacao"]),
                "titular_id": g["titular_id"], "share": g["share"],
                "criticas": ",".join(decisao.criticas) or None,
            })
            saida_participante += [{**chave, "nome": p["nome"], "classe": regras.classe(p["papel"])}
                                   for p in por_recurso.get((g["file_id"], g["recurso_ref"]), [])]
            if novo:
                resumo["carregaveis"] += 1
                resumo["criticas"].update(decisao.criticas)

        escrita.substituir_lote(spark, cfg, "gold.gravacao", lote, saida_gravacao)
        escrita.substituir_lote(spark, cfg, "gold.participante", lote, saida_participante)
        escrita.substituir_lote(spark, cfg, "gold.rejeicao", lote, saida_rejeicao)
        escrita.mudar_estados(spark, cfg, estados)  # por último
    return resumo
