import json
import logging
import os
from datetime import datetime

from ethereumetl_airflow.build_transfer_dag import build_transfer_dag
from ethereumetl_airflow.data_types import TransferConfig

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/opt/airflow/dags/repo/dags')
transfer_config_path = os.path.join(DAGS_FOLDER, 'resources/stages/transfer/config.json')

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

var_prefix = 'ethereum_'

with open(transfer_config_path) as f:
    config = TransferConfig.from_dict(json.loads(f.read()))

    for client in config.clients:
        globals()[client.dag_name()] = build_transfer_dag(
            dag_id=client.dag_name() + '_dag',
            abis=client.abis,
            parse_start_date=datetime.strptime('2022-04-04', '%Y-%m-%d'),
            schedule_interval='30 3 * * *'
        )
