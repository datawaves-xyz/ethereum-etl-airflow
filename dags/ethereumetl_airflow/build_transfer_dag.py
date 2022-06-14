from datetime import datetime, timedelta
from typing import List

from airflow import DAG, models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor

from ethereumetl_airflow.data_types import TransferABI


def build_transfer_dag(
        dag_id: str,
        abis: List[TransferABI],
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

    def create_transfer_tasks(abi: TransferABI) -> None:
        sensor = ExternalTaskSensor(
            task_id=f'wait_for_{abi.task_name}',
            external_dag_id=abi.dag_name,
            external_task_id=abi.task_name,
            execution_delta=timedelta(minutes=30),
            priority_weight=0,
            mode='reschedule',
            poke_interval=5 * 60,
            timeout=60 * 60 * 12,
            dag=dag
        )

        # TODO
        task = BashOperator(
            task_id=f'transfer_{abi.task_name}',
            bash_command=f"echo {abi.dag_name}.{abi.task_name}",
            dag=dag
        )

        sensor >> task

    for abi in abis:
        create_transfer_tasks(abi)

    return dag
