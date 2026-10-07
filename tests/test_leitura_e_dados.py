import xml.etree.ElementTree as ET

import pytest

from ddexpipe import conferencia, gerar_dados, leitura


@pytest.fixture
def entrada(tmp_path):
    gerar_dados.gerar(tmp_path, lotes=2, mensagens=3, semente=3)
    return tmp_path


def test_dados_sao_deterministicos(tmp_path):
    a = gerar_dados.gerar(tmp_path / "a", lotes=1, mensagens=4, semente=9)
    b = gerar_dados.gerar(tmp_path / "b", lotes=1, mensagens=4, semente=9)
    assert a == b
    assert (tmp_path / "a" / "legado.csv").read_bytes() == (tmp_path / "b" / "legado.csv").read_bytes()


def test_lote_traz_os_casos_dificeis(entrada):
    nomes = {p.name for p in (entrada / "lote_01").glob("*.xml")}
    assert "revogacao_01.xml" in nomes
    assert "declaracao_01_corrompida.xml" in nomes
    assert (entrada / "lote_02" / "reenvio_de_um_arquivo_antigo.xml").exists()


def test_leitura_em_streaming(entrada):
    mensagem, gravacoes, participantes = leitura.ler_arquivo(str(entrada / "lote_01" / "declaracao_01_0001.xml"))
    assert mensagem["message_id"] == "MSG-01-0001"
    assert gravacoes and all(g["recurso_ref"] for g in gravacoes)
    assert {p["recurso_ref"] for p in participantes} <= {g["recurso_ref"] for g in gravacoes}


def test_tipo_nao_suportado(entrada):
    with pytest.raises(leitura.TipoNaoSuportado):
        leitura.ler_arquivo(str(entrada / "lote_01" / "revogacao_01.xml"))


def test_xml_corrompido(entrada):
    with pytest.raises(ET.ParseError):
        leitura.ler_arquivo(str(entrada / "lote_01" / "declaracao_01_corrompida.xml"))


@pytest.mark.parametrize("coluna, novo, legado, contexto, familia", [
    ("titulo", "Noite e Mar (Ao Vivo em Lua com Sol e Rio)", "NOITE E MAR (AO VIVO EM LUA COM SOL E RI", {},
     "TITULO_MAIUSCULO_CORTADO"),
    ("duracao_seg", "193", "180", {}, "DURACAO_EM_MINUTOS"),
    ("data_referencia", "2024-05-10", "2019-01-02",
     {"data_lancamento": "2024-05-10", "data_criacao": "2019-01-02"}, "LEGADO_PREFERE_DATA_DE_CRIACAO"),
    ("titular_id", "PA-1001", "PA-1002", {}, None),            # nenhuma família explica: reprova
    ("duracao_seg", "193", "120", {}, None),
])
def test_familias_de_diferenca(coluna, novo, legado, contexto, familia):
    assert conferencia.classificar(coluna, novo, legado, contexto) == familia
