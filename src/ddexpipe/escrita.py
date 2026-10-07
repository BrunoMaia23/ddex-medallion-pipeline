"""Escrita nas tabelas Iceberg.

Os registros viram um NDJSON temporário que a própria JVM lê (spark.read.json), sem worker Python.
"""
from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from .config import Config
from .spark import t


@contextmanager
def _dataframe(spark: SparkSession, cfg: Config, registros: list[dict], schema):
    cfg.temporarios.mkdir(parents=True, exist_ok=True)
    caminho: Path = cfg.temporarios / f"{uuid.uuid4().hex}.json"
    caminho.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in registros), encoding="utf-8")
    try:
        df = spark.read.schema(schema).json(caminho.resolve().as_uri())
        for campo in df.schema.fields:  # timestamps vazios ficam com a hora da escrita
            if isinstance(campo.dataType, TimestampType):
                df = df.withColumn(campo.name, F.coalesce(F.col(campo.name), F.current_timestamp()))
        yield df
    finally:
        caminho.unlink(missing_ok=True)


def anexar(spark: SparkSession, cfg: Config, tabela: str, registros: list[dict]) -> None:
    if not registros:
        return
    with _dataframe(spark, cfg, registros, spark.table(t(tabela)).schema) as df:
        df.writeTo(t(tabela)).append()


def substituir_lote(spark: SparkSession, cfg: Config, tabela: str, lote: str, registros: list[dict]) -> None:
    """Reconstrói a partição de um lote inteiro numa única operação atômica."""
    if any(r["lote"] != lote for r in registros):
        raise ValueError("todos os registros precisam ser do mesmo lote")
    if not registros:  # overwritePartitions sem linhas não apaga a partição
        spark.sql(f"DELETE FROM {t(tabela)} WHERE lote = '{lote}'")
        return
    with _dataframe(spark, cfg, registros, spark.table(t(tabela)).schema) as df:
        df.writeTo(t(tabela)).overwritePartitions()


def substituir_tudo(spark: SparkSession, cfg: Config, tabela: str, registros: list[dict]) -> None:
    """Troca o conteúdo inteiro de uma tabela sem partição (ex.: o snapshot de referência)."""
    if not registros:
        spark.sql(f"DELETE FROM {t(tabela)}")
        return
    with _dataframe(spark, cfg, registros, spark.table(t(tabela)).schema) as df:
        df.writeTo(t(tabela)).overwritePartitions()


def mudar_estados(spark: SparkSession, cfg: Config, estados: dict[str, str]) -> None:
    """MERGE do estado dos arquivos no índice do bronze. Cada etapa chama por último."""
    if not estados:
        return
    registros = [{"file_id": f, "estado": e} for f, e in sorted(estados.items())]
    with _dataframe(spark, cfg, registros, "file_id STRING, estado STRING") as df:
        df.createOrReplaceTempView("_novos_estados")
        try:
            spark.sql(f"""
                MERGE INTO {t('bronze.arquivo')} a USING _novos_estados n ON a.file_id = n.file_id
                WHEN MATCHED THEN UPDATE SET a.estado = n.estado, a.atualizado_em = current_timestamp()""")
        finally:
            spark.catalog.dropTempView("_novos_estados")


def lotes_com_estado(spark: SparkSession, estado: str) -> list[str]:
    linhas = spark.sql(f"SELECT DISTINCT lote FROM {t('bronze.arquivo')} WHERE estado = '{estado}' ORDER BY lote")
    return [linha.lote for linha in linhas.collect()]


def contar(spark: SparkSession, tabela: str, onde: str = "true") -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {t(tabela)} WHERE {onde}").collect()[0].n


def como_dicts(df: DataFrame) -> list[dict]:
    return [linha.asDict() for linha in df.collect()]
