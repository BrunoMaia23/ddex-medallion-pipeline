"""Silver: leitura dos XML com iterparse e portão de qualidade por gravação.

O lote é sempre regravado inteiro e o estado dos arquivos só muda depois de todas as tabelas.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import Counter

from pyspark.sql import SparkSession

from . import escrita, qualidade
from .config import Config
from .spark import t

RAIZ_SUPORTADA = "DeclarationOfSoundRecordingRightsClaimMessage"


class TipoNaoSuportado(Exception):
    """Mensagem bem formada, mas de um tipo que este fluxo não trata (ex.: revogação)."""


def _texto(el: ET.Element, caminho: str) -> str | None:
    achado = el.find(caminho)
    if achado is None or achado.text is None:
        return None
    return achado.text.strip() or None


def ler_arquivo(caminho: str) -> tuple[dict, list[dict], list[dict]]:
    """Devolve (mensagem, gravações, participantes). Levanta TipoNaoSuportado, ET.ParseError ou ValueError."""
    mensagem: dict | None = None
    gravacoes: list[dict] = []
    participantes: list[dict] = []
    eventos = ET.iterparse(caminho, events=("start", "end"))
    _, raiz = next(eventos)
    if raiz.tag != RAIZ_SUPORTADA:
        raise TipoNaoSuportado(raiz.tag)
    for evento, el in eventos:
        if evento != "end":
            continue
        if el.tag == "MessageHeader":
            mensagem = {"message_id": _texto(el, "MessageId"), "remetente": _texto(el, "MessageSender/PartyId"),
                        "criada_em": _texto(el, "MessageCreatedDateTime")}
        elif el.tag == "SoundRecording":
            ref = _texto(el, "ResourceReference")
            gravacoes.append({
                "recurso_ref": ref,
                "isrc": _texto(el, "SoundRecordingId/ISRC"),
                "titulo": _texto(el, "ReferenceTitle/TitleText"),
                "duracao_iso": _texto(el, "Duration"),
                "data_lancamento": _texto(el, "OriginalReleaseDate"),
                "data_criacao": _texto(el, "CreationDate"),
                "titular_id": _texto(el, "RightsController/PartyId"),
                "titular_nome": _texto(el, "RightsController/PartyName"),
                "share": float(_texto(el, "RightsController/RightSharePercentage") or 0),
            })
            participantes += [{"recurso_ref": ref, "nome": _texto(c, "PartyName"), "papel": _texto(c, "Role")}
                              for c in el.findall("Contributor")]
            el.clear()  # mantém a memória baixa em arquivo grande
    if not mensagem or not mensagem["message_id"]:
        raise ValueError("mensagem sem MessageHeader/MessageId")
    return mensagem, gravacoes, participantes


def processar(spark: SparkSession, cfg: Config) -> dict:
    """Lê todos os lotes que têm arquivo RECEBIDO."""
    resumo = {"arquivos": 0, "gravacoes": 0, "quarentena_arquivo": Counter(), "quarentena_recurso": Counter()}
    for lote in escrita.lotes_com_estado(spark, "RECEBIDO"):
        arquivos = spark.sql(f"SELECT file_id, nome, caminho_raw, estado FROM {t('bronze.arquivo')} "
                             f"WHERE lote = '{lote}' ORDER BY nome").collect()
        tabelas: dict[str, list[dict]] = {nome: [] for nome in (
            "silver.mensagem", "silver.gravacao", "silver.participante", "quarentena.arquivo", "quarentena.recurso")}
        estados: dict[str, str] = {}
        for a in arquivos:
            base = {"lote": lote, "file_id": a.file_id}
            try:
                mensagem, gravacoes, participantes = ler_arquivo(a.caminho_raw)
            except TipoNaoSuportado as exc:
                motivo, detalhe = "TIPO_NAO_SUPORTADO", str(exc)
            except (ET.ParseError, ValueError) as exc:
                motivo, detalhe = "XML_INVALIDO", str(exc)
            else:
                motivo = detalhe = None
            if motivo:
                tabelas["quarentena.arquivo"].append({**base, "nome": a.nome, "motivo": motivo, "detalhe": detalhe[:300]})
                estados[a.file_id] = "QUARENTENA"
                if a.estado == "RECEBIDO":
                    resumo["quarentena_arquivo"][motivo] += 1
                continue

            mid = mensagem["message_id"]
            tabelas["silver.mensagem"].append({**base, **mensagem})
            validos = set()
            for g in gravacoes:
                motivo = qualidade.validar_recurso(g)
                if motivo:
                    tabelas["quarentena.recurso"].append({**base, "message_id": mid, "recurso_ref": g["recurso_ref"],
                                                          "isrc": g["isrc"], "motivo": motivo})
                    if a.estado == "RECEBIDO":
                        resumo["quarentena_recurso"][motivo] += 1
                else:
                    tabelas["silver.gravacao"].append({**base, "message_id": mid, **g})
                    validos.add(g["recurso_ref"])
            tabelas["silver.participante"] += [{**base, "message_id": mid, **p}
                                               for p in participantes if p["recurso_ref"] in validos]
            if a.estado == "RECEBIDO":
                estados[a.file_id] = "PARSEADO"
                resumo["arquivos"] += 1
                resumo["gravacoes"] += len(validos)

        for tabela, registros in tabelas.items():
            escrita.substituir_lote(spark, cfg, tabela, lote, registros)
        escrita.mudar_estados(spark, cfg, estados)  # por último
    return resumo
