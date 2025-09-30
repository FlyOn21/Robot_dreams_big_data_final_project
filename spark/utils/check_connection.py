import logging
import os

import psycopg2


def check_connect_data():
    """Check connection to data sources and PostgreSQL."""

    logger = logging.getLogger('fp_dag_spark_bronze.check_connect_data')
    try:
        conn = psycopg2.connect(
            host='dbt_hw_fp_host',
            port=5432,
            database='dbt_hw_fp',
            user='dbt_user',
            password='12345678'
        )
        conn.close()
        logger.info("PostgreSQL connection successful")
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        raise

    logger.info("All connections and data checks passed!")
    return True
