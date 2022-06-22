import json
import logging
import os
from datetime import datetime

from ethereumetl_airflow.build_raw_transfer_dag import build_raw_transfer_dag
from ethereumetl_airflow.data_types import TransferConfig
from ethereumetl_airflow.variables import read_var

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/opt/airflow/dags/repo/dags')
transfer_config_path = os.path.join(DAGS_FOLDER, 'resources/stages/transfer/config.json')

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

var_prefix = 'ethereum_'

config = TransferConfig.from_dict(
    json.loads(read_var(var_name='transfer_config', var_prefix=var_prefix, required=True)))

spark_config = \
    json.loads(read_var(var_name='raw_transfer_spark_config', var_prefix=var_prefix, required=True))

logging.info(config)

for client in config.clients:
    globals()[client.raw_dag_name] = build_raw_transfer_dag(
        dag_id=client.raw_dag_name,
        client=client,
        spark_config=spark_config,
        parse_start_date=datetime.strptime('2022-04-04', '%Y-%m-%d'),
        schedule_interval='30 3 * * *'
    )
