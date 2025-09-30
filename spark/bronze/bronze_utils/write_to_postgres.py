import os


def write_to_postgres_bronze(df, table_name):
    """Write DataFrame to PostgreSQL"""
    postgres_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://127.0.0.1:15433/dbt_hw_fp")
    postgres_user = os.getenv("POSTGRES_USER", "dbt_user")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "12345678")

    (df.write
     .format("jdbc")
     .option("url", postgres_url)
     .option("dbtable", f"public.{table_name}")
     .option("user", postgres_user)
     .option("password", postgres_password)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .save())
