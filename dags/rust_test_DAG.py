from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from utils import rust_airflow_utils

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        dag_id='Rust_on_Airflow_Chadstack',
        default_args=default_args,
        schedule_interval=None,
        start_date=datetime.now() - timedelta(days=1),
        tags=['rust', 'chadstack'],
        catchup=False
) as dag:

    download_rust_binary_from_s3 = \
            PythonOperator(
                task_id="download_rust_binary_from_s3",
                python_callable=rust_airflow_utils.download_rust_executable_from_uri,
                trigger_rule="all_success",
                provide_context=True,
                op_kwargs={
                    "bucket": "confessions-of-a-data-guy",
                    "key": "rustWithAirflow"
                }
            )
    
    execute_rust_binary = \
        PythonOperator(
                task_id="execute_rust_binary",
                python_callable=rust_airflow_utils.execute_rust_binary,
                trigger_rule="all_success",
                provide_context=True,
                op_kwargs={
                    "uri": "s3://confessions-of-a-data-guy/file.txt",
                }
            )

    download_rust_binary_from_s3 >> execute_rust_binary