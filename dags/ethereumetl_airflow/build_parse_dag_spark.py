import json
import logging
import os
from datetime import datetime, timedelta
from glob import glob
from typing import cast, List, Dict

from airflow import models, DAG
from airflow.operators.sensors import ExternalTaskSensor
from bdbt.abi.abi_type import ABI

from ethereumetl_airflow.common import read_json_file
from ethereumetl_airflow.operators.fixed_spark_submit_operator import FixedSparkSubmitOperator

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

dags_folder = os.environ.get('DAGS_FOLDER', '/opt/airflow/dags/repo/dags')


class ContractDefinition(TypedDict, total=False):
    abi: ABI
    contract_address: str
    contract_name: str
    dataset_name: str


def build_parse_dag(
        dag_id: str,
        dataset_folder: str,
        spark_config: Dict[str, any] = None,
        s3_config: Dict[str, any] = None,
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
        default_args=default_dag_args)

    def create_parse_tasks(contract: ContractDefinition,
                           _logs_sensor: ExternalTaskSensor,
                           _traces_sensor: ExternalTaskSensor) -> None:
        for element in contract['abi']:
            etype = element['type']

            if etype != 'event' and etype != 'function' or len(element.get('inputs', [])) == 0:
                continue

            database_name = contract['dataset_name']
            table_name = f'{contract["contract_name"]}_{"evt" if etype == "event" else "call"}_{element["name"]}'

            application_args = [
                '--group-name',
                contract['dataset_name'],
                '--contract-name',
                contract['contract_name'],
                '--abi-type',
                etype,
                '--abi-json',
                json.dumps(element),
                '--abi-name',
                element['name'],
                '--s3-access-key',
                s3_config['access_key'],
                '--s3-secret-key',
                s3_config['secret_key'],
                '--s3-bucket',
                s3_config['bucket'],
                '--s3-region',
                s3_config['region'],
                '--dt',
                '{{ds}}'
            ]

            if 'contract_address' in contract:
                application_args.extend([
                    '--contract-address',
                    contract['contract_address']
                ])

            task = FixedSparkSubmitOperator(
                task_id=f'{database_name}.{table_name}',
                name=f'{database_name}.{table_name}',
                java_class=spark_config['java_class'],
                application=spark_config['application'],
                conf=spark_config['conf'],
                jars=spark_config['jars'],
                application_args=application_args,
                dag=dag
            )

            (logs_sensor if etype == 'event' else traces_sensor) >> task

    logs_sensor = ExternalTaskSensor(
        task_id=f'wait_for_ethereum_enrich_logs',
        external_dag_id='ethereum_load_dag',
        external_task_id=f'enrich_logs',
        execution_delta=timedelta(hours=1),
        priority_weight=0,
        mode='reschedule',
        poke_interval=5 * 60,
        timeout=60 * 60 * 12,
        dag=dag
    )

    traces_sensor = ExternalTaskSensor(
        task_id=f'wait_for_ethereum_enrich_traces',
        external_dag_id='ethereum_load_dag',
        external_task_id=f'enrich_traces',
        execution_delta=timedelta(hours=1),
        priority_weight=0,
        mode='reschedule',
        poke_interval=5 * 60,
        timeout=60 * 60 * 12,
        dag=dag
    )

    json_files = get_list_of_files(dataset_folder, '*.json')
    logging.info(json_files)

    for json_file in json_files:
        table_definition = cast(ContractDefinition, read_json_file(json_file))
        create_parse_tasks(table_definition, logs_sensor, traces_sensor)

    return dag


def get_list_of_files(dataset_folder: str, filter: str = '*.json') -> List[str]:
    logging.info('get_list_of_files')
    logging.info(dataset_folder)
    logging.info(os.path.join(dataset_folder, filter))
    return [f for f in glob(os.path.join(dataset_folder, filter))]
