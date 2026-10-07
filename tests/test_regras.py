import pytest

from ddexpipe import regras


@pytest.mark.parametrize("isrc, esperado", [
    ("USA121500001", True),
    ("BRX992600042", True),
    ("BR-ABC-24-00001", False),   # com hífens
    ("US12", False),
    ("usa121500001", False),      # minúsculas
    (None, False),
])
def test_isrc(isrc, esperado):
    assert regras.isrc_valido(isrc) is esperado


@pytest.mark.parametrize("iso, segundos", [
    ("PT3M12S", 192),
    ("PT1H2M3S", 3723),
    ("PT3M12.600S", 193),         # arredonda meio para cima
    ("PT45S", 45),
    ("PT", None),
    ("3:12", None),
    (None, None),
])
def test_duracao(iso, segundos):
    assert regras.duracao_em_segundos(iso) == segundos


def test_data_em_cascata():
    assert regras.data_de_referencia("2024-05-10", "2020-01-01") == "2024-05-10"
    assert regras.data_de_referencia(None, "2020-01-01") == "2020-01-01"
    assert regras.data_de_referencia("2024-02-31", "2020-01-01") == "2020-01-01"  # data impossível cai para a próxima
    assert regras.data_de_referencia(None, None) is None


def test_data_so_no_formato_aaaa_mm_dd():
    assert regras.data_valida("2024-05-10T12:00:00") == "2024-05-10"
    assert regras.data_valida("20240510") is None    # o Python 3.11+ aceitaria
    assert regras.data_valida("2024-W19-5") is None  # idem (data por semana)


def test_classes_de_participante():
    assert regras.classe("Composer") == "AUTOR"
    assert regras.classe("MainArtist") == "INTERPRETE"
    assert regras.classe("Arranger") == "OUTROS"
    assert regras.classe(None) == "OUTROS"


BASE = {"isrc": "USA121500001", "data_lancamento": "2024-01-01", "data_criacao": None, "titular_id": "PA-1001"}


def test_avaliar_carrega_e_sinaliza_ja_cadastrado():
    assert regras.avaliar(BASE, {"PA-9999"}, ja_cadastrado=False) == regras.Decisao(True)
    decisao = regras.avaliar(BASE, {"PA-9999"}, ja_cadastrado=True)
    assert decisao.carregar and decisao.criticas == ("ISRC_JA_CADASTRADO",)


def test_avaliar_ordem_dos_motivos():
    teste_sem_data = {**BASE, "titular_id": "PA-9999", "data_lancamento": None}
    assert regras.avaliar(teste_sem_data, {"PA-9999"}, False).motivo == "RECURSO_DE_TESTE"  # teste vem primeiro
    assert regras.avaliar({**BASE, "data_lancamento": None}, set(), False).motivo == "SEM_DATA"
