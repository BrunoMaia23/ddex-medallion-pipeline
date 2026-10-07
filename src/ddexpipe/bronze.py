"""Bronze: cópia do XML como chegou e índice dos arquivos recebidos.

O id do arquivo é o SHA-256 do conteúdo, então um reenvio com outro nome não entra de novo.
Estados no índice: RECEBIDO -> PARSEADO | QUARENTENA -> REGRAS_OK -> CARREGADO.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from pyspark.sql import SparkSession

from . import escrita
from .config import Config
from .spark import t


def ingerir(spark: SparkSession, cfg: Config) -> dict[str, int]:
    """Guarda no bronze os XML novos de todas as pastas de lote da entrada."""
    conhecidos = {linha.file_id for linha in spark.table(t("bronze.arquivo")).select("file_id").collect()}
    novos: list[dict] = []
    repetidos = 0
    for pasta in sorted(p for p in cfg.entrada.glob("lote_*") if p.is_dir()):
        for xml in sorted(pasta.glob("*.xml")):
            dados = xml.read_bytes()
            file_id = hashlib.sha256(dados).hexdigest()
            if file_id in conhecidos:
                repetidos += 1
                continue
            destino = cfg.raw / pasta.name / f"{file_id}.xml"
            if not destino.exists():  # grava num .tmp e renomeia
                destino.parent.mkdir(parents=True, exist_ok=True)
                tmp = destino.with_suffix(".tmp")
                tmp.write_bytes(dados)
                os.replace(tmp, destino)
            novos.append({"file_id": file_id, "nome": xml.name, "lote": pasta.name, "tamanho": len(dados),
                          "caminho_raw": str(destino), "estado": "RECEBIDO"})
            conhecidos.add(file_id)
    escrita.anexar(spark, cfg, "bronze.arquivo", novos)
    return {"novos": len(novos), "repetidos": repetidos}
