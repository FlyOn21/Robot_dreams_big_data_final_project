import logging

import psycopg2


def validate_results():
    """Validate processed data in PostgreSQL."""

    logger = logging.getLogger('fp_dag_spark_bronze.validate_results')

    conn = None
    try:
        conn = psycopg2.connect(
            host='dbt_hw_fp_host',
            port=5432,
            database='dbt_hw_fp',
            user='dbt_user',
            password='12345678'
        )
        cursor = conn.cursor()

        tables = ['bronze_chicago_arrests', 'bronze_chicago_crimes', 'bronze_chicago_ucr_codes']

        table_queries = {
            'bronze_chicago_arrests': 'SELECT COUNT(*) FROM public.bronze_chicago_arrests',
            'bronze_chicago_crimes': 'SELECT COUNT(*) FROM public.bronze_chicago_crimes',
            'bronze_chicago_ucr_codes': 'SELECT COUNT(*) FROM public.bronze_chicago_ucr_codes'
        }

        for table in tables:
            try:
                if table not in table_queries:
                    raise ValueError(f"Unknown table: {table}")
                cursor.execute(table_queries[table])
                count = cursor.fetchone()[0]
                logger.info(f"{table}: {count:,} records")

                if count == 0:
                    raise ValueError(f"No data in {table}")
            except psycopg2.ProgrammingError as e:
                if "does not exist" not in str(e):
                    raise e
                logger.error(f"Table {table} does not exist")
                raise ValueError(f"Table {table} does not exist") from e

        logger.info("Data validation successful!")

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise
    finally:
        if conn:
            conn.close()

    return True
