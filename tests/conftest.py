import os
import shutil

import pytest


def _java_disponivel() -> bool:
    return bool(shutil.which("java") or os.environ.get("JAVA_HOME"))


@pytest.fixture(scope="session")
def _spark_e_cfg(tmp_path_factory):
    """Uma única sessão Spark (uma JVM) para todos os testes de pipeline."""
    if not _java_disponivel():
        pytest.skip("os testes de pipeline precisam de Java 17 (Spark)")
    from ddexpipe import spark as sp
    from ddexpipe.config import Config

    cfg = Config(tmp_path_factory.mktemp("ddexpipe"))
    spark = sp.sessao(cfg)
    yield spark, cfg
    spark.stop()


@pytest.fixture
def pipeline(_spark_e_cfg):
    """Lake vazio, pastas limpas e dados fictícios novos a cada teste."""
    from ddexpipe import gerar_dados
    from ddexpipe import spark as sp

    spark, cfg = _spark_e_cfg
    sp.apagar_tabelas(spark)
    sp.criar_tabelas(spark)
    for pasta in (cfg.entrada, cfg.raw, cfg.banco.parent, cfg.saida, cfg.auditoria, cfg.temporarios):
        shutil.rmtree(pasta, ignore_errors=True)
    gerar_dados.gerar(cfg.entrada, lotes=2, mensagens=5, semente=7)
    return spark, cfg
