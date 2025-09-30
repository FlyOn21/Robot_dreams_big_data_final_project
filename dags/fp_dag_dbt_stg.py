import sys

sys.path.append('/')

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from spark.utils.check_connection import check_connect_data

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# env_data = {
#     "POSTGRES_URL": "jdbc:postgresql://dbt_hw_fp_host:5432/dbt_hw_fp",
#     "POSTGRES_USER": "dbt_user",
#     "POSTGRES_PASSWORD": "12345678",
#     "PYTHONPATH": "/opt/airflow/dags:$PYTHONPATH",
#     "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk",
# }

with DAG(
        dag_id='fp_dag_dbt_stg',
        default_args=default_args,
        description='DBT models for final project',
        schedule=None,
        catchup=False,
        tags=['final_project', 'dbt', "stg"],
) as dag:

    check_connect_data_task_dbt = PythonOperator(
        task_id='check_connect_data_task_dbt',
        python_callable=check_connect_data
    )

    dbt_deps_stg = BashOperator(
        task_id='dbt_deps_stg',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            rm -rf /dbt_fp/fp_big_data/dbt_packages
            echo "Installing dbt dependencies..."
            dbt deps --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )
    dbt_run_stg = BashOperator(
        task_id='dbt_run_stg',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running dbt models..."
            dbt run --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    dbt_test_stg = BashOperator(
        task_id='dbt_test_stg',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running dbt tests..."
            dbt test --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    end_dbt_stg = EmptyOperator(
        task_id='end_dbt_stg',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )


    check_connect_data_task_dbt >> dbt_deps_stg >> dbt_run_stg >> dbt_test_stg >> end_dbt_stg

