"""Regras de negócio em funções puras (sem Spark nem banco), simplificadas para este repositório."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date

# ISRC: 2 letras do país, 3 caracteres do registrante, 2 dígitos do ano, 5 dígitos da gravação.
ISRC = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}\d{7}$")
DURACAO = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")
# Só AAAA-MM-DD: a partir do Python 3.11 o fromisoformat também aceita 20240101 e 2024-W01-1.
DATA_ISO = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}")

CLASSES = {
    "Composer": "AUTOR",
    "Lyricist": "AUTOR",
    "ComposerLyricist": "AUTOR",
    "MainArtist": "INTERPRETE",
    "FeaturedArtist": "INTERPRETE",
    "Producer": "PRODUTOR",
}


def isrc_valido(isrc: str | None) -> bool:
    return bool(isrc and ISRC.match(isrc))


def duracao_em_segundos(iso: str | None) -> int | None:
    """Duração ISO 8601 (PT3M12S, PT1H2M3.5S) em segundos inteiros; None se inválida."""
    if not iso:
        return None
    m = DURACAO.match(iso)
    if not m or not any(m.groups()):
        return None
    horas, minutos, segundos = (float(x) if x else 0.0 for x in m.groups())
    return int(horas * 3600 + minutos * 60 + segundos + 0.5)


def data_valida(texto: str | None) -> str | None:
    """A data ISO (AAAA-MM-DD) se for uma data real; senão None."""
    if not texto or not DATA_ISO.match(texto):
        return None
    try:
        return date.fromisoformat(texto[:10]).isoformat()
    except ValueError:
        return None


def data_de_referencia(lancamento: str | None, criacao: str | None) -> str | None:
    """Cascata: data de lançamento; se não houver uma válida, data de criação."""
    return data_valida(lancamento) or data_valida(criacao)


def classe(papel: str | None) -> str:
    return CLASSES.get(papel or "", "OUTROS")


@dataclass(frozen=True)
class Decisao:
    carregar: bool
    motivo: str | None = None                      # por que NÃO carrega
    criticas: tuple[str, ...] = field(default=())   # carrega, mas sinalizado


def avaliar(gravacao: dict, partes_teste: set[str], ja_cadastrado: bool) -> Decisao:
    """Destino de uma gravação que já passou pelo portão de qualidade."""
    if gravacao.get("titular_id") in partes_teste:  # vem antes de qualquer outra regra
        return Decisao(False, "RECURSO_DE_TESTE")
    if data_de_referencia(gravacao.get("data_lancamento"), gravacao.get("data_criacao")) is None:
        return Decisao(False, "SEM_DATA")
    return Decisao(True, criticas=("ISRC_JA_CADASTRADO",) if ja_cadastrado else ())
