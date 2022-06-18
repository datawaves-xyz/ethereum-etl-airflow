import json
import logging
import os
from datetime import datetime
from glob import glob

from ethereumetl_airflow.build_parse_dag_spark import build_parse_dag
from ethereumetl_airflow.variables import read_var

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/opt/airflow/dags/repo/dags')
table_definitions_folder = os.path.join(DAGS_FOLDER, 'resources/stages/parse/spark/contract_definitions/*')

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

var_prefix = 'ethereum_'

spark_config = \
    json.loads(read_var(var_name='parse_spark_config', var_prefix=var_prefix, required=True))

s3_config = \
    json.loads(read_var(var_name='s3_config', var_prefix=None, required=True))

for folder in glob(table_definitions_folder):
    dataset = folder.split('/')[-1]

    dag_id = f'ethereum_parse_{dataset}_dag'
    logging.info(folder)
    logging.info(dataset)
    globals()[dag_id] = build_parse_dag(
        dag_id=dag_id,
        dataset_folder=folder,
        spark_config=spark_config,
        s3_config=s3_config,
        parse_start_date=datetime.strptime('2022-04-04', '%Y-%m-%d'),
        schedule_interval='0 3 * * *'
    )
