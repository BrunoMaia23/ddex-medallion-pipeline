"""DAG de exemplo para o Airflow 2 ou 3: uma tarefa por etapa da CLI, em sequência.

Não roda nos testes; o CI só confere a sintaxe.
"""
from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator

COMANDO = "python -m ddexpipe {etapa} --base {{{{ var.value.ddexpipe_base }}}}"

ETAPAS = [
    ("ingestao", "ingerir"),            # parceiro (SFTP) -> bronze
    ("leitura_xml", "ler"),             # bronze -> silver + quarentena
    ("auditoria", "auditar"),           # mede o silver, não bloqueia
    ("referencias", "referencias"),     # snapshot do cadastro
    ("regras", "regras"),               # silver -> gold
    ("carga", "carregar"),              # gold -> banco transacional, com ledger
    ("retorno", "retorno"),             # arquivo de status para o parceiro
    ("reconciliacao", "reconciliar"),   # confere as contas entre as camadas
    ("conferencia", "conferir"),        # célula a célula contra o legado (modo sombra)
]

with DAG(
    dag_id="ddex_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10),
                  "execution_timeout": timedelta(hours=2)},
    tags=["ddex", "medalhao"],
) as dag:
    tarefas = [BashOperator(task_id=task_id, bash_command=COMANDO.format(etapa=etapa))
               for task_id, etapa in ETAPAS]
    for anterior, seguinte in zip(tarefas, tarefas[1:]):
        anterior >> seguinte
