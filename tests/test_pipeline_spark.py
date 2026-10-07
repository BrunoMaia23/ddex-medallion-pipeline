"""Testes de ponta a ponta com Spark + Iceberg de verdade (sessão local)."""
import csv
import re

import pytest

from ddexpipe import bronze, carga, conferencia, escrita, gold, leitura, reconciliacao, referencias, retorno
from ddexpipe.spark import t

pytestmark = pytest.mark.spark


def ate_o_gold(spark, cfg):
    bronze.ingerir(spark, cfg)
    leitura.processar(spark, cfg)
    referencias.atualizar(spark, cfg)
    gold.processar(spark, cfg)


def tudo(spark, cfg):
    ate_o_gold(spark, cfg)
    carga.carregar(spark, cfg)
    retorno.gerar(spark, cfg)


def totais(spark, cfg) -> dict:
    con = carga.conectar(cfg)
    try:
        banco = con.execute("SELECT count(*), count(DISTINCT message_id || isrc) FROM gravacao").fetchone()
    finally:
        con.close()
    tabelas = ("bronze.arquivo", "silver.gravacao", "silver.participante", "quarentena.arquivo",
               "quarentena.recurso", "gold.gravacao", "gold.participante", "gold.rejeicao",
               "gold.ledger_carga", "gold.retorno_item")
    return {**{nome: escrita.contar(spark, nome) for nome in tabelas},
            "banco": banco[0], "banco_distintos": banco[1], "retornos": len(list(cfg.saida.rglob("*.xml")))}


def estados(spark) -> dict:
    return {r.estado: r.n for r in spark.sql(
        f"SELECT estado, count(*) AS n FROM {t('bronze.arquivo')} GROUP BY estado").collect()}


def test_pipeline_completo_e_idempotente(pipeline):
    spark, cfg = pipeline
    tudo(spark, cfg)
    primeira = totais(spark, cfg)
    assert primeira["gold.gravacao"] > 0
    assert primeira["banco"] == primeira["banco_distintos"] == primeira["gold.gravacao"]
    assert set(estados(spark)) == {"CARREGADO", "QUARENTENA"}
    assert reconciliacao.reconciliar(spark, cfg) == []
    assert conferencia.conferir(spark, cfg).ok

    tudo(spark, cfg)
    assert totais(spark, cfg) == primeira


def test_reenvio_reconhecido_pelo_hash(pipeline):
    spark, cfg = pipeline
    primeira = bronze.ingerir(spark, cfg)
    assert primeira["repetidos"] == 1          # o "reenvio" do lote 2 é cópia de um arquivo do lote 1
    assert bronze.ingerir(spark, cfg) == {"novos": 0, "repetidos": primeira["novos"] + 1}


def test_quarentena_com_motivo(pipeline):
    spark, cfg = pipeline
    ate_o_gold(spark, cfg)
    arquivos = {r.motivo: r.n for r in spark.sql(
        f"SELECT motivo, count(*) AS n FROM {t('quarentena.arquivo')} GROUP BY motivo").collect()}
    assert arquivos == {"XML_INVALIDO": 2, "TIPO_NAO_SUPORTADO": 1}
    motivos = {r.motivo for r in spark.sql(f"SELECT DISTINCT motivo FROM {t('quarentena.recurso')}").collect()}
    assert motivos and motivos <= {"TITULO_AUSENTE", "TITULO_LONGO_DEMAIS", "ISRC_INVALIDO",
                                    "DURACAO_INVALIDA", "SEM_TITULAR"}


def test_lote_reconstruido_inteiro_nao_duplica(pipeline):
    spark, cfg = pipeline
    ate_o_gold(spark, cfg)
    antes = totais(spark, cfg)
    ids = [r.file_id for r in spark.sql(
        f"SELECT file_id FROM {t('bronze.arquivo')} WHERE lote = 'lote_01' AND estado = 'REGRAS_OK'").collect()]
    escrita.mudar_estados(spark, cfg, {ids[0]: "RECEBIDO"})   # um único arquivo volta para o começo
    leitura.processar(spark, cfg)                           # o lote inteiro é relido e regravado
    gold.processar(spark, cfg)
    assert totais(spark, cfg) == antes


def test_carga_com_falha_e_retomada(pipeline):
    spark, cfg = pipeline
    ate_o_gold(spark, cfg)
    alvo = spark.sql(f"SELECT file_id, isrc FROM {t('gold.gravacao')} ORDER BY file_id, isrc LIMIT 1").collect()[0]

    r = carga.carregar(spark, cfg, falhar=lambda g: g["isrc"] == alvo.isrc)
    assert r["erros"] == 1
    estado_alvo = spark.sql(f"SELECT estado FROM {t('bronze.arquivo')} WHERE file_id = '{alvo.file_id}'").collect()[0]
    assert estado_alvo.estado == "REGRAS_OK"                 # não marca como carregado com registro faltando
    assert escrita.contar(spark, "gold.ledger_carga", f"isrc = '{alvo.isrc}'") == 0

    r = carga.carregar(spark, cfg)                           # a retomada carrega só o que faltou
    assert (r["gravacoes"], r["erros"]) == (1, 0)
    retorno.gerar(spark, cfg)
    assert totais(spark, cfg)["banco"] == escrita.contar(spark, "gold.gravacao")
    assert reconciliacao.reconciliar(spark, cfg) == []


def test_queda_entre_commit_e_ledger(pipeline):
    spark, cfg = pipeline
    tudo(spark, cfg)
    antes = totais(spark, cfg)
    alvo = spark.sql(f"SELECT file_id, count(*) AS n FROM {t('gold.ledger_carga')} "
                     "GROUP BY file_id ORDER BY file_id LIMIT 1").collect()[0]
    # simula a queda: o banco confirmou, mas o ledger nunca foi gravado
    spark.sql(f"DELETE FROM {t('gold.ledger_carga')} WHERE file_id = '{alvo.file_id}'")
    escrita.mudar_estados(spark, cfg, {alvo.file_id: "REGRAS_OK"})

    r = carga.carregar(spark, cfg)
    assert (r["gravacoes"], r["recuperadas"]) == (0, alvo.n)   # nada duplicado, ledger refeito
    assert totais(spark, cfg) == antes
    assert reconciliacao.reconciliar(spark, cfg) == []


def test_retorno_tem_um_status_por_gravacao(pipeline):
    spark, cfg = pipeline
    tudo(spark, cfg)
    for xml in cfg.saida.rglob("*.xml"):
        status = re.findall(r"<Status>(\w+)</Status>", xml.read_text(encoding="utf-8"))
        assert status and set(status) <= {"Accepted", "PotentialConflict", "Rejected"}
    assert retorno.gerar(spark, cfg)["novos_arquivos"] == 0    # gerar de novo não muda nada


def mexer_no_legado(cfg, mudanca):
    legado = cfg.entrada / "legado.csv"
    with open(legado, encoding="utf-8", newline="") as f:
        linhas = list(csv.DictReader(f))
    campos = list(linhas[0])
    mudanca(linhas)
    with open(legado, "w", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=campos)
        escritor.writeheader()
        escritor.writerows(linhas)
    return linhas


def test_conferencia_reprova_diferenca_nova(pipeline):
    spark, cfg = pipeline
    tudo(spark, cfg)
    mexer_no_legado(cfg, lambda linhas: linhas[0].update(titular_id="PA-0000"))  # nenhuma família explica
    rel = conferencia.conferir(spark, cfg)
    assert not rel.ok
    assert [d[1] for d in rel.nao_explicadas] == ["titular_id"]


def test_conferencia_reprova_gravacao_de_um_lado_so(pipeline):
    spark, cfg = pipeline
    tudo(spark, cfg)
    removidas = []
    mexer_no_legado(cfg, lambda linhas: removidas.append(linhas.pop()))
    rel = conferencia.conferir(spark, cfg)
    assert not rel.ok and rel.so_de_um_lado == 1
    chave = (removidas[0]["message_id"], removidas[0]["isrc"])
    assert rel.nao_explicadas == [(chave, "presenca", "só no novo", "")]


def test_reconciliacao_detecta_raw_alterado(pipeline):
    spark, cfg = pipeline
    tudo(spark, cfg)
    raw = next(cfg.raw.rglob("*.xml"))
    raw.write_bytes(raw.read_bytes() + b"<!-- alterado -->")
    assert any("raw alterado" in furo for furo in reconciliacao.reconciliar(spark, cfg))
