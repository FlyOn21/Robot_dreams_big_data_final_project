"""
Base transformer class for Silver layer processing
"""
import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

import psycopg2
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, desc, row_number, to_timestamp, unix_timestamp


class SilverTransformer(ABC):
    """
    Abstract base class for Silver layer transformations.
    Provides common functionality for reading from Bronze and writing to Silver.
    Handles deduplication by hash_row and load_timestamp.
    """

    def __init__(self, app_name: str):
        """
        Initialize the transformer
        """
        self.app_name = app_name
        self.spark: SparkSession | None = None
        self.logger = logging.getLogger(self.__class__.__name__)

        # PostgreSQL configuration
        self.pg_host = os.getenv("POSTGRES_HOST", "dbt_hw_fp_host")
        self.pg_port = os.getenv("POSTGRES_PORT", "5432")
        self.pg_db = os.getenv("POSTGRES_DB", "dbt_hw_fp")
        self.pg_user = os.getenv("POSTGRES_USER", "dbt_user")
        self.pg_password = os.getenv("POSTGRES_PASSWORD", "12345678")
        self.jdbc_url = f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

        # Spark configuration
        self.spark_jars = os.getenv("SPARK_JARS", "/jars/postgresql-42.7.0.jar")
        self.shuffle_partitions = os.getenv("SPARK_SHUFFLE_PARTITIONS", "400")

    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        self.logger.info(f"Creating Spark session: {self.app_name}")

        self.spark = (
            SparkSession.builder
            .appName(self.app_name)
            .config("spark.driver.memory", "4g")  # Increase as needed
            .config("spark.executor.memory", "4g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")

            .config("spark.sql.shuffle.partitions", self.shuffle_partitions)
            .config("spark.jars", self.spark_jars)
            .config("spark.driver.extraClassPath", self.spark_jars)
            .config("spark.executor.extraClassPath", self.spark_jars)

            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

        return self.spark

    def read_from_bronze(self, table_name: str) -> DataFrame:
        """
        Read data from PostgreSQL bronze table

        Args:
            table_name: Name of the bronze table

        Returns:
            DataFrame with bronze data
        """
        self.logger.info(f"Reading from bronze.{table_name}")

        df = (
            self.spark.read
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", f"public.{table_name}")
            .option("user", self.pg_user)
            .option("password", self.pg_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        record_count = df.count()
        self.logger.info(f"Read {record_count} records from bronze.{table_name}")

        return df

    def deduplicate_by_hash_and_timestamp(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicates based on hash_row, keeping the record with the latest load_timestamp.
        Optimized for memory efficiency.
        """
        self.logger.info("Deduplicating by hash_row and load_timestamp")

        if "hash_row" not in df.columns or "load_timestamp" not in df.columns:
            self.logger.warning("hash_row or load_timestamp not found, skipping deduplication")
            return df

        self.logger.info("Starting deduplication process")

        df = df.repartition(400, "hash_row")

        window_spec = Window.partitionBy("hash_row").orderBy(desc("load_timestamp"))

        df_deduped = (
            df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        self.logger.info("Deduplication completed")
        return df_deduped

    def write_to_silver(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Write data to PostgreSQL silver table

        Args:
            df: DataFrame to write
            table_name: Name of the silver table
            mode: Write mode (overwrite, append, etc.)
        """
        # Ensure silver schema exists
        self._ensure_schema_exists("silver")

        record_count = df.count()
        self.logger.info(f"Writing {record_count} records to silver.{table_name} (mode: {mode})")

        (
            df.write
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", f"silver.{table_name}")
            .option("user", self.pg_user)
            .option("password", self.pg_password)
            .option("driver", "org.postgresql.Driver")
            .option("isolationLevel", "READ_UNCOMMITTED")
            .mode(mode)
            .save()
        )

        self.logger.info(f"Successfully wrote {record_count} records to silver.{table_name}")

    def _ensure_schema_exists(self, schema_name: str):
        """
        Check if schema exists and create it if it doesn't

        Args:
            schema_name: Name of the schema to check/create
        """

        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_db,
                user=self.pg_user,
                password=self.pg_password
            )
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                (schema_name,)
            )

            if cursor.fetchone() is None:
                self.logger.info(f"Schema '{schema_name}' does not exist. Creating...")
                cursor.execute(f'CREATE SCHEMA "{schema_name}"')
                self.logger.info(f"Schema '{schema_name}' created successfully")
            else:
                self.logger.debug(f"Schema '{schema_name}' already exists")

            cursor.close()
            conn.close()

        except Exception as e:
            self.logger.error(f"Error ensuring schema '{schema_name}' exists: {str(e)}")
            raise e

    def stop_spark(self):
        """Stop Spark session"""
        if self.spark:
            self.logger.info("Stopping Spark session")
            self.spark.stop()

    @abstractmethod
    def get_bronze_table_name(self) -> str:
        """Return the name of the bronze table to read from"""
        pass

    @abstractmethod
    def get_silver_table_name(self) -> str:
        """Return the name of the silver table to write to"""
        pass

    @abstractmethod
    def get_write_mode(self) -> str:
        """Return the write mode for silver table"""
        return "overwrite"

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to the data

        Args:
            df: Input DataFrame from bronze layer

        Returns:
            Transformed DataFrame for silver layer
        """
        pass

    def run(self):
        """
        Main execution method.
        Orchestrates the ETL process: read -> transform -> deduplicate -> write
        """
        try:
            self.create_spark_session()

            bronze_table = self.get_bronze_table_name()
            df_bronze = self.read_from_bronze(bronze_table)

            self.logger.info("Applying transformations...")
            df_transformed = self.transform(df_bronze)

            df_silver = self.deduplicate_by_hash_and_timestamp(df_transformed)

            silver_table = self.get_silver_table_name()
            write_mode = self.get_write_mode()
            self.write_to_silver(df_silver, silver_table, mode=write_mode)

            self.logger.info("Transformation completed successfully")

        except Exception as e:
            self.logger.error(f"Error during transformation: {str(e)}")
            raise
        finally:
            self.stop_spark()
