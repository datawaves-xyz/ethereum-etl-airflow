from __future__ import print_function

from ethereumetl_airflow.build_export_dag import build_export_dag
from ethereumetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_export_dag(
    dag_id='ethereum_export_dag',
    **read_export_dag_vars(
        var_prefix='ethereum_',
        export_schedule_interval='30 1 * * *',
        export_start_date='2022-02-11',
        export_max_workers=10,
        export_batch_size=10,
        export_retries=5,
    )
)
