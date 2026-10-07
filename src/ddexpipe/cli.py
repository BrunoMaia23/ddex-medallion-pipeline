"""Pipeline DDEX em camadas, um subcomando por etapa."""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from . import (bronze, carga, conferencia, config, escrita, gerar_dados, gold, leitura, qualidade,
               reconciliacao, referencias, retorno)
from . import spark as sp

MARCADOR = ".ddexpipe-demo"


def _fmt(contagem: dict) -> str:
    return ", ".join(f"{k} {v}" for k, v in sorted(contagem.items())) or "nenhuma"


def etapa_ingerir(spark, cfg) -> int:
    r = bronze.ingerir(spark, cfg)
    print(f"[bronze]     {r['novos']} arquivos novos, {r['repetidos']} já recebidos (mesmo hash)")
    return 0


def etapa_ler(spark, cfg) -> int:
    r = leitura.processar(spark, cfg)
    print(f"[silver]     {r['arquivos']} arquivos lidos, {r['gravacoes']} gravações | "
          f"quarentena de arquivo: {_fmt(r['quarentena_arquivo'])} | "
          f"quarentena de registro: {_fmt(r['quarentena_recurso'])}")
    return 0


def etapa_auditar(spark, cfg) -> int:
    r = qualidade.auditar(spark, cfg)["silver"]
    print(f"[auditoria]  {r['gravacoes']} gravações no silver, {r['isrc_fora_do_padrao']} ISRC fora do padrão, "
          f"{r['isrc_repetido']} ISRC repetido (não bloqueia; relatório em auditoria/)")
    return 0


def etapa_referencias(spark, cfg) -> int:
    print(f"[referência] snapshot com {referencias.atualizar(spark, cfg)} ISRCs já cadastrados")
    return 0


def etapa_regras(spark, cfg) -> int:
    r = gold.processar(spark, cfg)
    print(f"[gold]       {r['carregaveis']} gravações carregáveis | rejeitadas: {_fmt(r['rejeicoes'])} | "
          f"críticas: {_fmt(r['criticas'])}")
    return 0


def etapa_carregar(spark, cfg) -> int:
    r = carga.carregar(spark, cfg)
    print(f"[carga]      {r['arquivos']} arquivos, {r['gravacoes']} gravações inseridas, "
          f"{r['recuperadas']} já estavam no banco, {r['erros']} com erro")
    return 1 if r["erros"] else 0


def etapa_retorno(spark, cfg) -> int:
    r = retorno.gerar(spark, cfg)
    print(f"[retorno]    {r['mensagens']} mensagens com retorno, {r['novos_arquivos']} arquivos novos na saída")
    return 0


def etapa_reconciliar(spark, cfg) -> int:
    furos = reconciliacao.reconciliar(spark, cfg)
    for furo in furos:
        print(f"   FURO: {furo}")
    print("[reconciliação] OK: as contas fecham entre todas as camadas." if not furos
          else f"[reconciliação] {len(furos)} furos encontrados.")
    return 1 if furos else 0


def etapa_conferir(spark, cfg) -> int:
    rel = conferencia.conferir(spark, cfg)
    print(f"[conferência] {rel.linhas_em_comum} gravações nos dois lados -> {rel.celulas} células: "
          f"{rel.iguais} iguais, {sum(rel.explicadas.values())} diferentes e explicadas")
    print(f"              {rel.so_de_um_lado} gravações de um lado só | "
          f"{len(rel.nao_explicadas)} diferenças NÃO explicadas")
    descricoes = conferencia.descricoes()
    for codigo, qtd in rel.explicadas.most_common():
        print(f"   {qtd:>5}  {codigo}: {descricoes[codigo]}")
    for chave, coluna, novo, legado in rel.nao_explicadas[:10]:
        print(f"   NÃO EXPLICADA {chave} {coluna}: novo={novo!r} legado={legado!r}")
    print("[conferência] OK: toda diferença tem explicação." if rel.ok
          else "[conferência] REPROVADA: existe diferença sem explicação.")
    return 0 if rel.ok else 1


ETAPAS = {
    "ingerir": etapa_ingerir,
    "ler": etapa_ler,
    "auditar": etapa_auditar,
    "referencias": etapa_referencias,
    "regras": etapa_regras,
    "carregar": etapa_carregar,
    "retorno": etapa_retorno,
    "reconciliar": etapa_reconciliar,
    "conferir": etapa_conferir,
}


def rodar_tudo(spark, cfg) -> int:
    codigo = 0
    for etapa in ETAPAS.values():
        codigo |= etapa(spark, cfg)
    return codigo


def demo(base: Path, lotes: int, mensagens: int, semente: int) -> int:
    if base.exists():
        if not (base / MARCADOR).exists():
            print(f"A pasta {base} já existe e não foi criada pela demo; escolha outra com --base.")
            return 2
        shutil.rmtree(base)
    base.mkdir(parents=True)
    (base / MARCADOR).write_text("pasta criada pela demo do ddexpipe\n", encoding="utf-8")
    cfg = config.carregar(base)
    r = gerar_dados.gerar(cfg.entrada, lotes, mensagens, semente)
    print(f"[dados]      {r['lotes']} lotes, {r['mensagens']} mensagens, {r['gravacoes']} gravações fictícias")
    spark = sp.sessao(cfg)
    try:
        print("\n== 1ª execução")
        primeira = rodar_tudo(spark, cfg)
        totais = _totais(spark, cfg)
        print("\n== 2ª execução (nada pode mudar)")
        segunda = rodar_tudo(spark, cfg)
        iguais = _totais(spark, cfg) == totais
        print(f"\n[idempotência] totais idênticos depois da 2ª execução: {'sim' if iguais else 'NÃO'} -> {totais}")
        return primeira | segunda | (0 if iguais else 1)
    finally:
        spark.stop()


def _totais(spark, cfg) -> dict:
    con = carga.conectar(cfg)
    try:
        banco = con.execute("SELECT count(*) FROM gravacao").fetchone()[0]
    finally:
        con.close()
    return {"bronze": escrita.contar(spark, "bronze.arquivo"), "silver": escrita.contar(spark, "silver.gravacao"),
            "gold": escrita.contar(spark, "gold.gravacao"), "rejeicoes": escrita.contar(spark, "gold.rejeicao"),
            "banco": banco, "retornos": len(list(cfg.saida.rglob("*.xml")))}


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):  # acentos legíveis mesmo com a saída redirecionada
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(prog="ddexpipe", description=__doc__)
    sub = parser.add_subparsers(dest="comando", required=True)

    p = sub.add_parser("demo", help="gera dados fictícios e roda o pipeline inteiro duas vezes")
    p.add_argument("--base", type=Path, default=Path("demo"))
    p.add_argument("--lotes", type=int, default=2)
    p.add_argument("--mensagens", type=int, default=12)
    p.add_argument("--semente", type=int, default=42)

    p = sub.add_parser("gerar", help="só gera os dados fictícios em <base>/entrada")
    p.add_argument("--base", type=Path, default=Path("dados"))
    p.add_argument("--lotes", type=int, default=2)
    p.add_argument("--mensagens", type=int, default=12)
    p.add_argument("--semente", type=int, default=42)

    for nome in (*ETAPAS, "rodar"):
        p = sub.add_parser(nome, help="todas as etapas, em ordem" if nome == "rodar" else f"etapa: {nome}")
        p.add_argument("--base", type=Path, default=Path("dados"))
        p.add_argument("--ambiente", default=None, help="teste (padrão) ou producao")

    args = parser.parse_args(argv)
    if args.comando == "demo":
        return demo(args.base, args.lotes, args.mensagens, args.semente)
    if args.comando == "gerar":
        print(gerar_dados.gerar(config.carregar(args.base).entrada, args.lotes, args.mensagens, args.semente))
        return 0
    cfg = config.carregar(args.base, args.ambiente)
    spark = sp.sessao(cfg)
    try:
        return rodar_tudo(spark, cfg) if args.comando == "rodar" else ETAPAS[args.comando](spark, cfg)
    finally:
        spark.stop()
