from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG, models
from airflow.operators.sensors import ExternalTaskSensor

from ethereumetl_airflow.data_types import TransferABI, TransferClient
from ethereumetl_airflow.operators.fixed_spark_submit_operator import FixedSparkSubmitOperator


def build_transfer_dag(
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

    def create_transfer_tasks(abi: TransferABI, application_args: List[str]) -> None:
        sensor = ExternalTaskSensor(
            task_id=f'wait_for_{abi.task_name}',
            external_dag_id=abi.upstream_dag_name,
            external_task_id=abi.task_name,
            execution_delta=timedelta(minutes=30),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )

        application_args += [
            '--group-name',
            abi.group_name,
            '--contract-name',
            abi.contract_name,
            '--abi-type',
            abi.abi_type,
            '--abi-name',
            abi.abi_name,
            '--dt',
            '{{ds}}'
        ]

        task = FixedSparkSubmitOperator(
            task_id=f'transfer_{abi.task_name}',
            name=f'transfer_{abi.task_name}',
            java_class=spark_config['java_class'],
            application=spark_config['application'],
            conf=spark_config['conf'],
            jars=spark_config['jars'],
            application_args=application_args,
            dag=dag
        )

        sensor >> task

    for abi in client.abis:
        create_transfer_tasks(abi, application_args=client.application_args)

    return dag
