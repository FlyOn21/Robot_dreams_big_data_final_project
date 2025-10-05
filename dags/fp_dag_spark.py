import sys

sys.path.append('/')

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from spark.bronze.bronze_utils.spark_postgres_validate_result import validate_results
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

env_data = {
            "POSTGRES_URL": "jdbc:postgresql://dbt_hw_fp_host:5432/dbt_hw_fp",
            "POSTGRES_USER": "dbt_user",
            "POSTGRES_PASSWORD": "12345678",
            "PYTHONPATH": "/opt/airflow/dags:$PYTHONPATH",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk",
        }

with DAG(
        dag_id='fp_dag_spark',
        default_args=default_args,
        description='Spark jobs for final project',
        schedule=None,
        catchup=False,
        tags=['final_project', 'spark', 'bronze', "silver"],
) as dag:
    check_connect_data_task = PythonOperator(
        task_id='check_connect_data',
        python_callable=check_connect_data
    )

    bronze_chicago_ucr_codes_job = BashOperator(
        task_id="bronze_chicago_ucr_codes_job",
        bash_command=r"""
          set -e
          export PYTHONPATH="/:$PYTHONPATH"
          cd /spark/bronze
          python3 bronze_chicago_ucr_codes_job.py
        """,
        env=env_data
    )

    bronze_chicago_arrests_job = BashOperator(
        task_id="bronze_chicago_arrests_job",
        bash_command=r"""
          set -e
          export PYTHONPATH="/:$PYTHONPATH"
          cd /spark/bronze
          python3 bronze_chicago_arrests_job.py
        """,
        env=env_data
    )

    bronze_chicago_crimes_job = BashOperator(
        task_id="bronze_chicago_crimes_job",
        bash_command=r"""
          set -e
          export PYTHONPATH="/:$PYTHONPATH"
          cd /spark/bronze
          python3 bronze_chicago_crimes_job.py
        """,
        env=env_data
    )

    point_wait_jobs_bronze = EmptyOperator(
        task_id='point_wait_jobs_bronze',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    validate_results_task = PythonOperator(
        task_id='validate_results',
        python_callable=validate_results
    )

    crimes_silver = BashOperator(
        task_id="crimes_silver_job",
        bash_command=r"""
          set -e
          export PYTHONPATH="/:$PYTHONPATH"
          cd /spark/silver
          python3 crimes_silver_transformer.py
        """,
        env=env_data
    )

    arrests_silver = BashOperator(
        task_id="arrests_silver_job",
        bash_command=r"""
              set -e
              export PYTHONPATH="/:$PYTHONPATH"
              cd /spark/silver
              python3 arrests_silver_transformer.py
            """,
        env=env_data
    )

    ucr_code_silver = BashOperator(
        task_id="ucr_code_silver_job",
        bash_command=r"""
                  set -e
                  export PYTHONPATH="/:$PYTHONPATH"
                  cd /spark/silver
                  python3 ucr_code_silver_transformer.py
                """,
        env=env_data
    )

    point_wait_jobs_silver = EmptyOperator(
        task_id='point_wait_jobs_silver',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )


    jobs_bronze = [bronze_chicago_arrests_job, bronze_chicago_crimes_job, bronze_chicago_ucr_codes_job]

    jobs_silver = [crimes_silver, arrests_silver, ucr_code_silver]

    check_connect_data_task >> jobs_bronze >> point_wait_jobs_bronze >> validate_results_task >> jobs_silver >> point_wait_jobs_silver >> end

