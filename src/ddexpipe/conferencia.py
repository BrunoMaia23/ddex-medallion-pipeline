"""Conferência célula a célula contra a saída do legado.

Os lados são casados por (message_id, isrc). Cada célula diferente precisa cair numa família
conhecida. Diferença sem família, ou gravação que só existe de um lado, reprova.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import Config
from .spark import t

COLUNAS = ("titulo", "duracao_seg", "data_referencia", "titular_id")


@dataclass(frozen=True)
class Familia:
    codigo: str
    descricao: str
    coluna: str
    reconhece: Callable[[str, str, dict], bool]  # (valor_novo, valor_legado, contexto) -> bool


FAMILIAS = (
    Familia("TITULO_MAIUSCULO_CORTADO",
            "O legado grava o título em maiúsculas e corta em 40 caracteres.",
            "titulo", lambda novo, legado, _: legado == novo.upper()[:40]),
    Familia("DURACAO_EM_MINUTOS",
            "O legado arredonda a duração para baixo, em minutos inteiros.",
            "duracao_seg", lambda novo, legado, _: int(legado) == int(novo) // 60 * 60),
    Familia("LEGADO_PREFERE_DATA_DE_CRIACAO",
            "Com data de lançamento e de criação, o legado usa a de criação; o novo usa a de lançamento.",
            "data_referencia",
            lambda novo, legado, ctx: legado == ctx.get("data_criacao") and novo == ctx.get("data_lancamento")),
)


@dataclass
class Relatorio:
    linhas_em_comum: int = 0
    so_de_um_lado: int = 0
    celulas: int = 0
    iguais: int = 0
    explicadas: Counter = field(default_factory=Counter)   # células diferentes, por família
    nao_explicadas: list[tuple] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.nao_explicadas


def classificar(coluna: str, novo: str | None, legado: str | None, contexto: dict) -> str | None:
    """Família que explica a diferença numa célula, ou None se nenhuma explica."""
    for familia in FAMILIAS:
        if familia.coluna != coluna or novo is None or legado is None:
            continue
        try:
            if familia.reconhece(novo, legado, contexto):
                return familia.codigo
        except (TypeError, ValueError):
            continue
    return None


def conferir(spark: SparkSession, cfg: Config, legado_csv: Path | str | None = None) -> Relatorio:
    caminho = Path(legado_csv or cfg.entrada / "legado.csv").resolve()
    legado = (spark.read.option("header", True)
              .schema("message_id STRING, isrc STRING, titulo STRING, duracao_seg STRING, "
                      "data_referencia STRING, titular_id STRING")
              .csv(caminho.as_uri())
              .withColumn("no_legado", F.lit(True)))
    novo = spark.sql(f"""
        SELECT g.message_id, g.isrc, g.titulo, CAST(g.duracao_seg AS STRING) AS duracao_seg,
               g.data_referencia, g.titular_id, s.data_lancamento, s.data_criacao, true AS no_novo
        FROM {t('gold.gravacao')} g
        JOIN {t('silver.gravacao')} s ON g.file_id = s.file_id AND g.recurso_ref = s.recurso_ref""")
    junto = novo.alias("n").join(legado.alias("l"), ["message_id", "isrc"], "full_outer")

    comum = junto.where("n.no_novo AND l.no_legado")
    iguais = [F.sum(F.when(F.col(f"n.{c}").eqNullSafe(F.col(f"l.{c}")), 1).otherwise(0)).alias(c)
              for c in COLUNAS]
    agregado = comum.agg(F.count(F.lit(1)).alias("linhas"), *iguais).collect()[0]

    rel = Relatorio(linhas_em_comum=agregado["linhas"] or 0)
    rel.celulas = rel.linhas_em_comum * len(COLUNAS)
    rel.iguais = sum(agregado[c] or 0 for c in COLUNAS)

    # só as linhas com diferença vão para o driver
    alguma_diferente = " OR ".join(f"NOT (n.{c} <=> l.{c})" for c in COLUNAS)
    colunas = ["message_id", "isrc", "n.data_lancamento", "n.data_criacao"]
    colunas += [F.col(f"n.{c}").alias(f"novo_{c}") for c in COLUNAS]
    colunas += [F.col(f"l.{c}").alias(f"legado_{c}") for c in COLUNAS]
    for linha in comum.where(alguma_diferente).select(*colunas).collect():
        contexto = {"data_lancamento": linha["data_lancamento"], "data_criacao": linha["data_criacao"]}
        for c in COLUNAS:
            n, l = linha[f"novo_{c}"], linha[f"legado_{c}"]
            if n == l:
                continue
            familia = classificar(c, n, l, contexto)
            if familia:
                rel.explicadas[familia] += 1
            else:
                rel.nao_explicadas.append(((linha["message_id"], linha["isrc"]), c, n, l))

    for linha in junto.where("n.no_novo IS NULL OR l.no_legado IS NULL").select(
            "message_id", "isrc", F.col("n.no_novo").alias("no_novo")).collect():
        rel.so_de_um_lado += 1
        lado = "só no novo" if linha["no_novo"] else "só no legado"
        rel.nao_explicadas.append(((linha["message_id"], linha["isrc"]), "presenca", lado, ""))
    rel.nao_explicadas.sort(key=lambda d: (d[0], d[1]))
    return rel


def descricoes() -> dict[str, str]:
    return {f.codigo: f.descricao for f in FAMILIAS}
