from datetime import datetime

from airflow.models import Variable


def read_spark_vars(output_bucket, **kwargs):
    spark_prefix = 'spark_'
    return {
        'conf': {
            'spark.kubernetes.namespace': read_var('k8s_namespace', spark_prefix, True, **kwargs),
            'spark.kubernetes.authenticate.driver.serviceAccountName':
                read_var('driver_service_account_name', spark_prefix, True, **kwargs),
            'spark.kubernetes.container.image': read_var('image', spark_prefix, True, **kwargs),
            'spark.kubernetes.file.upload.path': 's3a://{bucket}/airflow/spark-application'.format(
                bucket=output_bucket),
            'spark.hive.metastore.uris': read_var('metastore_uris', spark_prefix, True, **kwargs),
            'spark.sql.catalogImplementation': 'hive',
            # https://stackoverflow.com/questions/50006526/overwrite-only-some-partitions-in-a-partitioned-spark-dataset
            'spark.sql.sources.partitionOverwriteMode': 'dynamic',
            'spark.hadoop.fs.s3a.access.key': read_var('s3a_access_key', spark_prefix, True, **kwargs),
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.fast.upload': True,
            'spark.hadoop.fs.s3a.secret.key': read_var('s3a_secret_key', spark_prefix, True, **kwargs),
            'spark.hadoop.hive.metastore.client.socket.timeout': 600,
            'spark.executor.memory': '4g',
            'spark.sql.warehouse.dir': read_var('warehouse_dir', spark_prefix, True, **kwargs),
            'spark.kubernetes.driver.podTemplateFile': read_var('driver_pod_template_file', spark_prefix, True,
                                                                **kwargs),
            'spark.kubernetes.executor.podTemplateFile': read_var('executor_pod_template_file', spark_prefix, True,
                                                                  **kwargs),
            'spark.kubernetes.driverEnv.HADOOP_USER_NAME': 150,
            'spark.kubernetes.executorEnv.HADOOP_USER_NAME': 150
        },
        'jars': read_var('jars', spark_prefix, True, **kwargs),
        'py_files': read_var('py_files', spark_prefix, True, **kwargs)
    }


def read_export_dag_vars(var_prefix, **kwargs):
    export_start_date = read_var('export_start_date', var_prefix, True, **kwargs)
    export_start_date = datetime.strptime(export_start_date, '%Y-%m-%d')

    provider_uris = read_var('provider_uris', var_prefix, True, **kwargs)
    provider_uris = [uri.strip() for uri in provider_uris.split(',')]

    provider_uris_archival = read_var('provider_uris_archival', var_prefix, False, **kwargs)
    if provider_uris_archival is None:
        provider_uris_archival = provider_uris
    else:
        provider_uris_archival = [uri.strip() for uri in provider_uris_archival.split(',')]

    cloud_provider = read_var('cloud_provider', var_prefix, False, **kwargs)
    if cloud_provider is None:
        cloud_provider = 'gcp'

    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'cloud_provider': cloud_provider,
        'export_start_date': export_start_date,
        'export_schedule_interval': read_var('export_schedule_interval', var_prefix, True, **kwargs),
        'provider_uris': provider_uris,
        'provider_uris_archival': provider_uris_archival,
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'export_max_active_runs': parse_int(read_var('export_max_active_runs', var_prefix, False, **kwargs)),
        'export_max_workers': parse_int(read_var('export_max_workers', var_prefix, True, **kwargs)),
        'export_batch_size': parse_int(read_var('export_batch_size', var_prefix, True, **kwargs)),
        'export_retries': parse_int(read_var('export_retries', var_prefix, True, **kwargs)),
        # toggles
        'export_daofork_traces_option': parse_bool(
            read_var('export_daofork_traces_option', var_prefix, False, **kwargs)),
        'export_genesis_traces_option': parse_bool(
            read_var('export_genesis_traces_option', var_prefix, False, **kwargs)),
        'export_blocks_and_transactions_toggle': parse_bool(
            read_var('export_blocks_and_transactions_toggle', var_prefix, False, **kwargs)),
        'export_receipts_and_logs_toggle': parse_bool(
            read_var('export_receipts_and_logs_toggle', var_prefix, False, **kwargs)),
        'extract_contracts_toggle': parse_bool(
            read_var('extract_contracts_toggle', var_prefix, False, **kwargs)),
        'extract_tokens_toggle': parse_bool(
            read_var('extract_tokens_toggle', var_prefix, False, **kwargs)),
        'extract_token_transfers_toggle': parse_bool(
            read_var('extract_token_transfers_toggle', var_prefix, False, **kwargs)),
        'export_traces_toggle': parse_bool(
            read_var('export_traces_toggle', var_prefix, False, **kwargs)),
        'export_prices_usd_toggle': parse_bool(
            read_var('export_prices_usd_toggle', var_prefix, False, **kwargs))
    }

    return vars


def read_load_dag_vars(var_prefix, **kwargs):
    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'destination_dataset_project_id': read_var('destination_dataset_project_id', var_prefix, True, **kwargs),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'schedule_interval': read_var('schedule_interval', var_prefix, True, **kwargs),
        'load_all_partitions': parse_bool(read_var('load_all_partitions', var_prefix, True, **kwargs))
    }

    load_start_date = read_var('load_start_date', vars, False, **kwargs)
    if load_start_date is not None:
        load_start_date = datetime.strptime(load_start_date, '%Y-%m-%d')
        vars['load_start_date'] = load_start_date

    return vars


def read_amend_dag_vars(var_prefix, **kwargs):
    vars = {
        'destination_dataset_project_id': read_var('destination_dataset_project_id', var_prefix, True, **kwargs),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'schedule_interval': read_var('schedule_interval', var_prefix, True, **kwargs),
    }

    return vars


def read_parse_dag_vars(var_prefix, dataset, **kwargs):
    per_dataset_var_prefix = var_prefix + dataset + '_'
    vars = {
        'parse_destination_dataset_project_id': read_var('parse_destination_dataset_project_id', var_prefix, True,
                                                         **kwargs),
        'schedule_interval': read_var('schedule_interval', var_prefix, True, **kwargs),
        'parse_all_partitions': parse_bool(read_var('parse_all_partitions', per_dataset_var_prefix, False),
                                           default=None),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
    }

    parse_start_date = read_var('parse_start_date', vars, False, **kwargs)
    if parse_start_date is not None:
        parse_start_date = datetime.strptime(parse_start_date, '%Y-%m-%d')
        vars['parse_start_date'] = parse_start_date

    return vars


def read_load_dag_redshift_vars(var_prefix, **kwargs):
    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'aws_access_key_id': read_var('aws_access_key_id', var_prefix, True, **kwargs),
        'aws_secret_access_key': read_var('aws_secret_access_key', var_prefix, True, **kwargs),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'schedule_interval': read_var('schedule_interval', var_prefix, True, **kwargs),
    }

    load_start_date = read_var('load_start_date', vars, False, **kwargs)
    if load_start_date is not None:
        load_start_date = datetime.strptime(load_start_date, '%Y-%m-%d')
        vars['load_start_date'] = load_start_date

    return vars


def read_load_dag_spark_vars(var_prefix, **kwargs):
    output_bucket = read_var('output_bucket', var_prefix, True, **kwargs)

    vars = {
        'output_bucket': output_bucket,
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'schedule_interval': read_var('schedule_interval', var_prefix, True, **kwargs),
        'spark_conf': read_spark_vars(output_bucket)
    }

    load_start_date = read_var('load_start_date', vars, False, **kwargs)
    if load_start_date is not None:
        load_start_date = datetime.strptime(load_start_date, '%Y-%m-%d')
        vars['load_start_date'] = load_start_date

    return vars


def read_verify_streaming_dag_vars(var_prefix, **kwargs):
    vars = {
        'destination_dataset_project_id': read_var('destination_dataset_project_id', var_prefix, True, **kwargs),
        'parse_destination_dataset_project_id': read_var('parse_destination_dataset_project_id', var_prefix, True,
                                                         **kwargs),
        'verify_partitioned_tables': parse_bool(read_var('verify_partitioned_tables', var_prefix, False, **kwargs),
                                                default=False),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
    }

    max_lag_in_minutes = read_var('max_lag_in_minutes', var_prefix, False, **kwargs)
    if max_lag_in_minutes is not None:
        vars['max_lag_in_minutes'] = max_lag_in_minutes

    return vars


def read_var(var_name, var_prefix=None, required=False, **kwargs):
    full_var_name = f'{var_prefix}{var_name}' if var_prefix is not None else var_name
    var = Variable.get(full_var_name, '')
    var = var if var != '' else None
    if var is None:
        var = kwargs.get(var_name)
    if required and var is None:
        raise ValueError(f'{full_var_name} variable is required')
    return var


def parse_bool(bool_string, default=True):
    if isinstance(bool_string, bool):
        return bool_string
    if bool_string is None or len(bool_string) == 0:
        return default
    else:
        return bool_string.lower() in ['true', 'yes']


def parse_int(val):
    if val is None:
        return None
    return int(val)
