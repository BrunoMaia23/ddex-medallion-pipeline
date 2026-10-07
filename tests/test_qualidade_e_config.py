import pytest

from ddexpipe import config, qualidade

VALIDA = {"titulo": "Noite", "isrc": "USA121500001", "duracao_iso": "PT3M", "titular_id": "PA-1001"}


@pytest.mark.parametrize("mudanca, motivo", [
    ({}, None),
    ({"titulo": ""}, "TITULO_AUSENTE"),
    ({"titulo": "x" * 101}, "TITULO_LONGO_DEMAIS"),
    ({"isrc": "US12"}, "ISRC_INVALIDO"),
    ({"duracao_iso": "3:00"}, "DURACAO_INVALIDA"),
    ({"titular_id": None}, "SEM_TITULAR"),
])
def test_portao_de_qualidade(mudanca, motivo):
    assert qualidade.validar_recurso({**VALIDA, **mudanca}) == motivo


def test_ambiente_padrao_e_teste(tmp_path, monkeypatch):
    monkeypatch.delenv("DDEXPIPE_AMBIENTE", raising=False)
    assert config.carregar(tmp_path).ambiente == "teste"


def test_producao_sem_confirmacao_e_bloqueada(tmp_path, monkeypatch):
    monkeypatch.delenv("DDEXPIPE_CONFIRMA_PRODUCAO", raising=False)
    with pytest.raises(config.TravaDeProducao, match="frase de confirmação"):
        config.carregar(tmp_path, "producao")


def test_producao_confirmada_continua_desligada_no_codigo(tmp_path, monkeypatch):
    monkeypatch.setenv("DDEXPIPE_CONFIRMA_PRODUCAO", config.FRASE_CONFIRMACAO)
    with pytest.raises(config.TravaDeProducao, match="desligada"):
        config.carregar(tmp_path, "producao")


def test_ambiente_desconhecido(tmp_path):
    with pytest.raises(ValueError):
        config.carregar(tmp_path, "homologacao-errada")
