"""Snapshot dos ISRCs que já existem no banco de destino, lido de referencia_isrc.csv."""
from __future__ import annotations

import csv

from pyspark.sql import SparkSession

from . import escrita
from .config import Config


def atualizar(spark: SparkSession, cfg: Config) -> int:
    arquivo = cfg.entrada / "referencia_isrc.csv"
    if not arquivo.exists():
        isrcs: list[str] = []
    else:
        with open(arquivo, encoding="utf-8", newline="") as f:
            isrcs = sorted({linha["isrc"] for linha in csv.DictReader(f) if linha.get("isrc")})
    escrita.substituir_tudo(spark, cfg, "ref.isrc_cadastrado", [{"isrc": i} for i in isrcs])
    return len(isrcs)
