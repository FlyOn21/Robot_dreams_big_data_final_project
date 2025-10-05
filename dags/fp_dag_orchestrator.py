import sys

sys.path.append('/')

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
import time

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


def wait_5_minutes():
    print("Waiting 5 minutes before triggering next DAG...")
    time.sleep(300)
    print("Wait completed, proceeding to next DAG")


with DAG(
        dag_id='fp_dag_orchestrator',
        default_args=default_args,
        description='Orchestrator DAG to run Spark -> DBT Staging -> DBT Core with 5min delays',
        schedule=None,
        catchup=False,
        tags=['final_project', 'orchestrator', 'master'],
) as dag:
    start = EmptyOperator(
        task_id='start_master_dag',
    )

    # Step 1: Trigger Spark DAG
    trigger_spark_dag = TriggerDagRunOperator(
        task_id='trigger_fp_dag_spark_master',
        trigger_dag_id='fp_dag_spark',
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}',
        reset_dag_run=True,
    )

    wait_after_spark = PythonOperator(
        task_id='wait_5min_after_spark',
        python_callable=wait_5_minutes
    )

    # Step 2: Trigger DBT Staging DAG
    trigger_dbt_stg_dag = TriggerDagRunOperator(
        task_id='trigger_fp_dag_dbt_stg_master',
        trigger_dag_id='fp_dag_dbt_stg',
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}',
        reset_dag_run=True,
    )

    wait_after_dbt_stg = PythonOperator(
        task_id='wait_5min_after_dbt_stg',
        python_callable=wait_5_minutes
    )

    # Step 3: Trigger DBT Core DAG
    trigger_dbt_core_dag = TriggerDagRunOperator(
        task_id='trigger_fp_dag_dbt_core_master',
        trigger_dag_id='fp_dag_dbt_core',
        wait_for_completion=True,
        poke_interval=30,
        execution_date='{{ ds }}',
        reset_dag_run=True,
    )

    end = EmptyOperator(
        task_id='end_master_dag',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    start >> trigger_spark_dag >> wait_after_spark >> trigger_dbt_stg_dag >> wait_after_dbt_stg >> trigger_dbt_core_dag >> end