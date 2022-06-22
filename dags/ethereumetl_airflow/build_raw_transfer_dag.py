from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG, models
from airflow.operators.sensors import ExternalTaskSensor

from ethereumetl_airflow.data_types import TransferClient
from ethereumetl_airflow.operators.fixed_spark_submit_operator import FixedSparkSubmitOperator


def build_raw_transfer_dag(
        dag_id: str,
        client: TransferClient,
        spark_config: Dict[str, any] = None,
        parse_start_date: datetime = datetime(2018, 7, 1),
        schedule_interval: str = '0 0 * * *'
) -> DAG:
    default_dag_args = {
        'depends_on_past': True,
        'start_date': parse_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }

    dag = models.DAG(
        dag_id,
        catchup=False,
        schedule_interval=schedule_interval,
        default_args=default_dag_args
    )

    def create_transfer_task(table: str, application_args: List[str]) -> None:
        sensor = ExternalTaskSensor(
            task_id=f'wait_for_raw_{table}',
            external_dag_id='ethereum_load_dag',
            external_task_id=f'enrich_{table}',
            execution_delta=timedelta(minutes=90),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )

        application_args += [
            '--table-name',
            table,
            '--dt',
            '{{ds}}'
        ]

        task = FixedSparkSubmitOperator(
            task_id=f'transfer_raw_{table}',
            name=f'transfer_raw_{table}',
            java_class=spark_config['java_class'],
            application=spark_config['application'],
            conf=spark_config['conf'],
            jars=spark_config['jars'],
            application_args=application_args,
            dag=dag
        )

        sensor >> task

    for raw in client.raws:
        create_transfer_task(raw, application_args=client.application_args)

    return dag
