"""Portão de qualidade (por gravação) e auditoria do silver, que só mede e não bloqueia."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from . import regras
from .config import Config
from .spark import t

LIMITE_TITULO = 100  # tamanho da coluna de título no banco de destino


def validar_recurso(gravacao: dict) -> str | None:
    """Motivo de quarentena do registro, ou None se ele pode seguir."""
    titulo = gravacao.get("titulo")
    if not titulo:
        return "TITULO_AUSENTE"
    if len(titulo) > LIMITE_TITULO:
        return "TITULO_LONGO_DEMAIS"
    if not regras.isrc_valido(gravacao.get("isrc")):
        return "ISRC_INVALIDO"
    if regras.duracao_em_segundos(gravacao.get("duracao_iso")) is None:
        return "DURACAO_INVALIDA"
    if not gravacao.get("titular_id"):
        return "SEM_TITULAR"
    return None


def auditar(spark: SparkSession, cfg: Config) -> dict:
    """Mede o silver e grava o resultado em <base>/auditoria. Não altera dados."""
    linha = spark.sql(f"""
        SELECT count(*) AS gravacoes,
               count(DISTINCT lote) AS lotes,
               sum(CASE WHEN data_lancamento IS NULL THEN 1 ELSE 0 END) AS sem_data_lancamento,
               sum(CASE WHEN data_criacao IS NULL THEN 1 ELSE 0 END) AS sem_data_criacao,
               sum(CASE WHEN isrc RLIKE '^[A-Z]{{2}}[A-Z0-9]{{3}}[0-9]{{7}}$' THEN 0 ELSE 1 END) AS isrc_fora_do_padrao,
               count(isrc) - count(DISTINCT isrc) AS isrc_repetido
        FROM {t('silver.gravacao')}""").collect()[0].asDict()
    quarentena = {r.motivo: r.n for r in spark.sql(
        f"SELECT motivo, count(*) AS n FROM {t('quarentena.recurso')} GROUP BY motivo").collect()}
    relatorio = {"gerado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 "silver": linha, "quarentena_recurso": quarentena}
    cfg.auditoria.mkdir(parents=True, exist_ok=True)
    destino = cfg.auditoria / f"auditoria_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json"
    destino.write_text(json.dumps(relatorio, ensure_ascii=False, indent=2), encoding="utf-8")
    return relatorio
