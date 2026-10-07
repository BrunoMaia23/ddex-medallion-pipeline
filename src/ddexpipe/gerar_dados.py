"""Dados fictícios no formato DDEX: lotes de XML, ISRCs de referência e a saída do "legado".

Tudo inventado, com semente fixa. Cada lote vem com alguns problemas de propósito: XML corrompido,
revogação, reenvio com outro nome, campos inválidos, gravação sem data e parte de teste.
"""
from __future__ import annotations

import csv
import random
from pathlib import Path
from xml.sax.saxutils import escape

from . import qualidade, regras
from .leitura import RAIZ_SUPORTADA

PAISES = ["US", "GB", "FR", "DE", "BR", "JP", "MX"]
NOMES = ["Ana Lima", "Carla Dias", "Diego Rocha", "Elisa Prado", "Felipe Nunes", "Gabriela Reis",
         "Heitor Melo", "Isabela Torres", "Lucas Viana", "Marina Sales", "Nicolas Freitas",
         "Olivia Campos", "Paulo Teixeira", "Renata Alves", "Sofia Martins", "Tiago Barros"]
PALAVRAS = ["Luz", "Mar", "Cidade", "Noite", "Caminho", "Vento", "Horizonte", "Saudade", "Aurora",
            "Silêncio", "Estrada", "Lua", "Chuva", "Fogo", "Rio", "Sol", "Memória", "Distância"]
TITULARES = [("PA-1001", "Selo Aurora"), ("PA-1002", "Editora Horizonte"),
             ("PA-1003", "Música Atlântica"), ("PA-1004", "Selo Brisa")]
PARTE_TESTE = ("PA-9999", "Parte de Teste")
PAPEIS = ["Composer", "Lyricist", "MainArtist", "FeaturedArtist", "Producer", "Arranger"]
REGISTRANTES = "ABCDEFGHJKLMNPQRSTUVWXYZ"


def gerar(destino: Path | str, lotes: int = 2, mensagens: int = 12, semente: int = 42) -> dict:
    destino = Path(destino)
    rng = random.Random(semente)
    sequencia = iter(range(1, 10**6))
    legado: list[dict] = []
    validos: list[str] = []
    total_gravacoes = 0
    primeiro_arquivo: Path | None = None

    for n_lote in range(1, lotes + 1):
        pasta = destino / f"lote_{n_lote:02d}"
        pasta.mkdir(parents=True, exist_ok=True)
        for n_msg in range(1, mensagens + 1):
            message_id = f"MSG-{n_lote:02d}-{n_msg:04d}"
            gravacoes = [_gravacao(rng, next(sequencia)) for _ in range(rng.randint(3, 8))]
            total_gravacoes += len(gravacoes)
            arquivo = pasta / f"declaracao_{n_lote:02d}_{n_msg:04d}.xml"
            arquivo.write_text(_xml(RAIZ_SUPORTADA, message_id, gravacoes), encoding="utf-8")
            primeiro_arquivo = primeiro_arquivo or arquivo
            for g in gravacoes:
                _registrar_no_legado(legado, validos, message_id, g)

        texto = _xml(RAIZ_SUPORTADA, f"MSG-{n_lote:02d}-CORROMPIDA", [_gravacao(rng, next(sequencia))])
        (pasta / f"declaracao_{n_lote:02d}_corrompida.xml").write_text(texto[: len(texto) // 2], encoding="utf-8")
        if n_lote == 1:
            revogacao = _xml("RevokeSoundRecordingRightsClaimMessage", "MSG-01-REVOGACAO", [])
            (pasta / "revogacao_01.xml").write_text(revogacao, encoding="utf-8")
        elif primeiro_arquivo:
            # reenvio: mesmo conteúdo, outro nome
            (pasta / "reenvio_de_um_arquivo_antigo.xml").write_bytes(primeiro_arquivo.read_bytes())

    with open(destino / "legado.csv", "w", encoding="utf-8", newline="") as f:
        campos = ["message_id", "isrc", "titulo", "duracao_seg", "data_referencia", "titular_id"]
        escritor = csv.DictWriter(f, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(legado)

    conhecidos = rng.sample(validos, max(1, len(validos) // 12))
    with open(destino / "referencia_isrc.csv", "w", encoding="utf-8", newline="") as f:
        f.write("isrc\n" + "".join(f"{isrc}\n" for isrc in sorted(conhecidos)))

    return {"lotes": lotes, "mensagens": lotes * mensagens, "gravacoes": total_gravacoes,
            "linhas_legado": len(legado)}


def _gravacao(rng: random.Random, n: int) -> dict:
    titular = PARTE_TESTE if rng.random() < 0.05 else rng.choice(TITULARES)
    lancamento, criacao = _datas(rng)
    return {
        "recurso_ref": f"A{n}",
        "isrc": _isrc(rng, n),
        "titulo": _titulo(rng),
        "duracao_iso": _duracao(rng),
        "data_lancamento": lancamento,
        "data_criacao": criacao,
        "titular_id": titular[0],
        "titular_nome": titular[1],
        "share": rng.choice([100, 100, 100, 50, 75]),
        "participantes": [(rng.choice(NOMES), rng.choice(PAPEIS)) for _ in range(rng.randint(1, 4))],
    }


def _isrc(rng: random.Random, n: int) -> str:
    if rng.random() < 0.04:
        return rng.choice(["BR-ABC-24-00001", "US12", "XX1234567890X"])
    return f"{rng.choice(PAISES)}{rng.choice(REGISTRANTES)}{rng.randint(10, 99)}{rng.randint(15, 26):02d}{n:05d}"


def _titulo(rng: random.Random) -> str:
    if rng.random() < 0.02:
        return ""
    palavras = rng.sample(PALAVRAS, 6)
    if rng.random() < 0.25:  # alguns títulos longos, que o legado corta
        return f"{palavras[0]} e {palavras[1]} (Ao Vivo em {palavras[2]} com {palavras[3]} e {palavras[4]})"
    return " ".join(palavras[: rng.randint(1, 3)])


def _duracao(rng: random.Random) -> str:
    if rng.random() < 0.03:
        return rng.choice(["3:12", "PT", "duracao"])
    segundos = rng.randint(95, 420)
    fracao = f".{rng.randint(100, 900)}" if rng.random() < 0.2 else ""
    return f"PT{segundos // 60}M{segundos % 60}{fracao}S"


def _datas(rng: random.Random) -> tuple[str | None, str | None]:
    lancamento = f"20{rng.randint(15, 25)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
    criacao = f"20{rng.randint(10, 24)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
    sorteio = rng.random()
    if sorteio < 0.05:
        return None, None
    if sorteio < 0.20:
        return None, criacao
    if sorteio < 0.30:
        return lancamento, None
    return lancamento, criacao


def _xml(raiz: str, message_id: str, gravacoes: list[dict]) -> str:
    def tag(nome: str, valor) -> str:
        return f"<{nome}>{escape(str(valor))}</{nome}>" if valor not in (None, "") else ""

    linhas = ['<?xml version="1.0" encoding="UTF-8"?>', f"<{raiz}>",
              "  <MessageHeader>", f"    {tag('MessageId', message_id)}",
              "    <MessageSender><PartyId>PA-0001</PartyId></MessageSender>",
              "    <MessageCreatedDateTime>2026-01-05T04:00:00</MessageCreatedDateTime>",
              "  </MessageHeader>", "  <ResourceList>"]
    for g in gravacoes:
        contribuidores = "".join(f"<Contributor>{tag('PartyName', nome)}{tag('Role', papel)}</Contributor>"
                                 for nome, papel in g["participantes"])
        linhas += [
            "    <SoundRecording>",
            f"      {tag('ResourceReference', g['recurso_ref'])}",
            f"      <SoundRecordingId>{tag('ISRC', g['isrc'])}</SoundRecordingId>",
            f"      <ReferenceTitle>{tag('TitleText', g['titulo'])}</ReferenceTitle>",
            f"      {tag('Duration', g['duracao_iso'])}",
            f"      {tag('OriginalReleaseDate', g['data_lancamento'])}",
            f"      {tag('CreationDate', g['data_criacao'])}",
            f"      <RightsController>{tag('PartyId', g['titular_id'])}{tag('PartyName', g['titular_nome'])}"
            f"{tag('RightSharePercentage', g['share'])}</RightsController>",
            f"      {contribuidores}",
            "    </SoundRecording>",
        ]
    linhas += ["  </ResourceList>", f"</{raiz}>"]
    return "\n".join(linhas) + "\n"


def _registrar_no_legado(legado: list[dict], validos: list[str], message_id: str, g: dict) -> None:
    """O que o legado (fictício) teria gravado, com as manias dele."""
    if qualidade.validar_recurso(g) is not None:   # o legado também recusava registro inválido
        return
    if not regras.avaliar(g, partes_teste={PARTE_TESTE[0]}, ja_cadastrado=False).carregar:
        return
    validos.append(g["isrc"])
    legado.append({
        "message_id": message_id,
        "isrc": g["isrc"],
        "titulo": g["titulo"].upper()[:40],                                   # maiúsculas, cortado em 40
        "duracao_seg": regras.duracao_em_segundos(g["duracao_iso"]) // 60 * 60,  # minutos inteiros
        "data_referencia": regras.data_valida(g["data_criacao"]) or regras.data_valida(g["data_lancamento"]),
        "titular_id": g["titular_id"],
    })
