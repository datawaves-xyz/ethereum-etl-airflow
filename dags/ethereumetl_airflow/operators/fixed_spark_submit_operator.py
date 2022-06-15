from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from ethereumetl_airflow.operators.fixed_spark_submit_hook import FixedSparkSubmitHook


class FixedSparkSubmitOperator(SparkSubmitOperator):
    def execute(self, context):
        """
        Call the SparkSubmitHook to run the provided spark job
        """
        self._hook = FixedSparkSubmitHook(
            conf=self._conf,
            conn_id=self._conn_id,
            files=self._files,
            py_files=self._py_files,
            archives=self._archives,
            driver_class_path=self._driver_class_path,
            jars=self._jars,
            java_class=self._java_class,
            packages=self._packages,
            exclude_packages=self._exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self._keytab,
            principal=self._principal,
            proxy_user=self._proxy_user,
            name=self._name,
            num_executors=self._num_executors,
            status_poll_interval=self._status_poll_interval,
            application_args=self._application_args,
            env_vars=self._env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary
        )
        self._hook.submit(self._application)
