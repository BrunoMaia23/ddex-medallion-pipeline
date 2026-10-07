"""Sessão Spark com o catálogo Iceberg (tipo hadoop, sem metastore) e as tabelas do lake."""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from .config import Config

VERSAO_ICEBERG = "1.11.0"
PACOTE_ICEBERG = f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{VERSAO_ICEBERG}"
CATALOGO = "lake"

# nome -> (colunas, partição). Silver, quarentena e gold são particionadas por lote.
TABELAS: dict[str, tuple[str, str | None]] = {
    "bronze.arquivo": ("file_id STRING, nome STRING, lote STRING, tamanho BIGINT, caminho_raw STRING, "
                       "estado STRING, recebido_em TIMESTAMP, atualizado_em TIMESTAMP", None),
    "silver.mensagem": ("lote STRING, file_id STRING, message_id STRING, remetente STRING, criada_em STRING", "lote"),
    "silver.gravacao": ("lote STRING, file_id STRING, message_id STRING, recurso_ref STRING, isrc STRING, "
                        "titulo STRING, duracao_iso STRING, data_lancamento STRING, data_criacao STRING, "
                        "titular_id STRING, titular_nome STRING, share DOUBLE", "lote"),
    "silver.participante": ("lote STRING, file_id STRING, message_id STRING, recurso_ref STRING, "
                            "nome STRING, papel STRING", "lote"),
    "quarentena.arquivo": ("lote STRING, file_id STRING, nome STRING, motivo STRING, detalhe STRING", "lote"),
    "quarentena.recurso": ("lote STRING, file_id STRING, message_id STRING, recurso_ref STRING, isrc STRING, "
                           "motivo STRING", "lote"),
    "ref.isrc_cadastrado": ("isrc STRING", None),
    "gold.gravacao": ("lote STRING, file_id STRING, message_id STRING, recurso_ref STRING, isrc STRING, "
                      "titulo STRING, duracao_seg INT, data_referencia STRING, titular_id STRING, "
                      "share DOUBLE, criticas STRING", "lote"),
    "gold.participante": ("lote STRING, file_id STRING, message_id STRING, isrc STRING, nome STRING, "
                          "classe STRING", "lote"),
    "gold.rejeicao": ("lote STRING, file_id STRING, message_id STRING, recurso_ref STRING, isrc STRING, "
                      "motivo STRING", "lote"),
    "gold.ledger_carga": ("file_id STRING, message_id STRING, isrc STRING, id_banco BIGINT, "
                          "carregado_em TIMESTAMP", None),
    "gold.retorno_item": ("lote STRING, file_id STRING, message_id STRING, recurso_ref STRING, isrc STRING, "
                          "status STRING, motivo STRING", "lote"),
}


def t(nome: str) -> str:
    """Nome completo da tabela no catálogo: t('gold.gravacao') -> 'lake.gold.gravacao'."""
    return f"{CATALOGO}.{nome}"


def sessao(cfg: Config) -> SparkSession:
    # sem isso o Spark procura "python3", que não existe no Windows
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    cfg.warehouse.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.master("local[2]").appName("ddexpipe")
        .config("spark.jars.packages", PACOTE_ICEBERG)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOGO}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOGO}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOGO}.warehouse", cfg.warehouse.resolve().as_uri())
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    _silenciar_limpeza_no_windows(spark)
    criar_tabelas(spark)
    return spark


def _silenciar_limpeza_no_windows(spark: SparkSession) -> None:
    """No Windows o Spark tenta apagar o jar em uso ao sair e loga um erro que não importa."""
    if os.name != "nt":
        return
    jvm = spark.sparkContext._jvm
    jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
        "org.apache.spark.util.ShutdownHookManager", jvm.org.apache.logging.log4j.Level.OFF)


def criar_tabelas(spark: SparkSession) -> None:
    for namespace in sorted({nome.split(".")[0] for nome in TABELAS}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOGO}.{namespace}")
    for nome, (colunas, particao) in TABELAS.items():
        particionamento = f" PARTITIONED BY ({particao})" if particao else ""
        spark.sql(f"CREATE TABLE IF NOT EXISTS {t(nome)} ({colunas}) USING iceberg{particionamento}")


def apagar_tabelas(spark: SparkSession) -> None:
    """Apaga as tabelas e os arquivos delas. Usado nos testes."""
    for nome in TABELAS:
        spark.sql(f"DROP TABLE IF EXISTS {t(nome)} PURGE")


def aquecer() -> None:
    """Baixa o runtime do Iceberg no build da imagem Docker."""
    with tempfile.TemporaryDirectory() as pasta:
        sessao(Config(Path(pasta))).stop()
