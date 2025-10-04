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

    dbt_run_ucr_codes = BashOperator(
        task_id='dbt_run_ucr_codes',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running stg_ucr_codes..."
            dbt run --select stg_ucr_codes --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    dbt_run_crimes = BashOperator(
        task_id='dbt_run_crimes',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running stg_crimes..."
            dbt run --select stg_crimes --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    dbt_run_arrests = BashOperator(
        task_id='dbt_run_arrests',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running stg_arrests..."
            dbt run --select stg_arrests --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    dbt_run_arrests_quarantine = BashOperator(
        task_id='dbt_run_arrests_quarantine',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running stg_arrests_quarantine..."
            dbt run --select stg_arrests_quarantine --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
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


    check_connect_data_task_dbt >> dbt_deps_stg >> dbt_run_ucr_codes >> dbt_run_crimes >> [dbt_run_arrests, dbt_run_arrests_quarantine] >> dbt_test_stg >> end_dbt_stg

