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
        dag_id='fp_dag_dbt_core',
        default_args=default_args,
        description='DBT Core (Gold Layer) models for final project',
        schedule=None,
        catchup=False,
        tags=['final_project', 'dbt', 'core', 'gold'],
) as dag:
    check_connect_data_task_dbt = PythonOperator(
        task_id='check_connect_data_task_dbt',
        python_callable=check_connect_data
    )

    dbt_deps_core = BashOperator(
        task_id='dbt_deps_core',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            rm -rf /dbt_fp/fp_big_data/dbt_packages
            echo "Installing dbt dependencies..."
            dbt deps --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    #DIM

    dbt_run_dim_crime_type = BashOperator(
        task_id='dbt_run_dim_crime_type',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running dim_crime_type..."
            dbt run --select dim_crime_type --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    dbt_run_dim_location = BashOperator(
        task_id='dbt_run_dim_location',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running dim_location..."
            dbt run --select dim_location --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    #FACT

    dbt_run_fact_crimes = BashOperator(
        task_id='dbt_run_fact_crimes',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running fact_crimes..."
            dbt run --select fact_crimes --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    dbt_run_fact_arrests = BashOperator(
        task_id='dbt_run_fact_arrests',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running fact_arrests..."
            dbt run --select fact_arrests --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    # AGGREGATED


    dbt_run_agg_daily_crimes = BashOperator(
        task_id='dbt_run_agg_daily_crimes',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running agg_daily_crimes..."
            dbt run --select agg_daily_crimes --full-refresh --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    # TEST

    dbt_test_core = BashOperator(
        task_id='dbt_test_core',
        bash_command=r"""
            set -e
            cd /dbt_fp/fp_big_data
            echo "Running dbt tests for core models..."
            dbt test --select core.* --target dev --profiles-dir /home/airflow/.dbt --project-dir /dbt_fp/fp_big_data
            """
    )

    end_dbt_core = EmptyOperator(
        task_id='end_dbt_core',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    check_connect_data_task_dbt >> dbt_deps_core

    dbt_deps_core >> [dbt_run_dim_crime_type, dbt_run_dim_location]

    [dbt_run_dim_crime_type, dbt_run_dim_location] >> dbt_run_fact_crimes

    [dbt_run_dim_crime_type, dbt_run_fact_crimes] >> dbt_run_fact_arrests

    dbt_run_fact_crimes >> dbt_run_agg_daily_crimes

    [dbt_run_fact_arrests, dbt_run_agg_daily_crimes] >> dbt_test_core

    dbt_test_core >> end_dbt_core