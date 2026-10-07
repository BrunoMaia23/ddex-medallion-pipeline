"""Pastas de trabalho, ambiente e trava de produção.

O padrão é teste. Produção precisa ser pedida e confirmada com a frase em DDEXPIPE_CONFIRMA_PRODUCAO,
e nesta versão continua desligada no código.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PRODUCAO_HABILITADA = False
FRASE_CONFIRMACAO = "SIM-ESTOU-GRAVANDO-EM-PRODUCAO"
AMBIENTES = ("teste", "producao")


class TravaDeProducao(RuntimeError):
    """Tentativa de apontar para produção sem todas as confirmações."""


@dataclass(frozen=True)
class Config:
    base: Path
    ambiente: str = "teste"

    @property
    def entrada(self) -> Path:
        return self.base / "entrada"

    @property
    def warehouse(self) -> Path:
        return self.base / "lake"

    @property
    def raw(self) -> Path:
        return self.base / "raw"

    @property
    def banco(self) -> Path:
        return self.base / "banco" / f"{self.ambiente}.sqlite"

    @property
    def saida(self) -> Path:
        return self.base / "saida_parceiro"

    @property
    def auditoria(self) -> Path:
        return self.base / "auditoria"

    @property
    def temporarios(self) -> Path:
        return self.base / "_tmp"


def carregar(base: Path | str, ambiente: str | None = None) -> Config:
    ambiente = (ambiente or os.environ.get("DDEXPIPE_AMBIENTE") or "teste").lower()
    if ambiente not in AMBIENTES:
        raise ValueError(f"ambiente desconhecido: {ambiente!r} (use {' ou '.join(AMBIENTES)})")
    if ambiente == "producao":
        if os.environ.get("DDEXPIPE_CONFIRMA_PRODUCAO") != FRASE_CONFIRMACAO:
            raise TravaDeProducao("produção exige DDEXPIPE_CONFIRMA_PRODUCAO com a frase de confirmação")
        if not PRODUCAO_HABILITADA:
            raise TravaDeProducao("produção está desligada nesta versão (modo sombra)")
    return Config(Path(base), ambiente)
