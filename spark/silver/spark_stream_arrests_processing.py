"""
Spark Structured Streaming processor for Chicago arrests data
Reads from Kafka, applies transformations, writes to Silver layer
"""
import logging
import os
import shutil
import tempfile
import traceback
from pathlib import Path

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    current_timestamp,
    from_json,
    lit,
    md5,
    to_date,
    trim,
    upper,
    when,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ArrestStreamProcessor:
    """Stream processor for Chicago arrests data"""

    def __init__(
            self,
            bootstrap_servers: str = "127.0.0.1:9092",
            topic: str = "arrests",
            checkpoint_dir: str = "./checkpoints/arrests",
            output_dir: str = "./silver/arrests",
            pg_host: str = "127.0.0.1",
            pg_port: str = "15433",
            pg_db: str = "dbt_hw_fp",
            pg_user: str = "dbt_user",
            pg_password: str = "12345678"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.checkpoint_dir = checkpoint_dir
        self.output_dir = output_dir

        # PostgreSQL config
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_db = pg_db
        self.jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
        self.pg_user = pg_user
        self.pg_password = pg_password

        # Clean temp directories
        self.temp_dir = tempfile.mkdtemp(prefix="spark_arrest_stream_", dir="/tmp")
        logger.info(f"Using temporary directory: {self.temp_dir}")

        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for streaming"""
        logger.info("Creating Spark session for arrest streaming...")

        spark_jars = os.getenv("SPARK_JARS", "/jars/postgresql-42.7.0.jar")

        spark = (
            SparkSession.builder
            .appName("ArrestStreamProcessor")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir)
            .config("spark.jars", spark_jars)
            .config("spark.driver.extraClassPath", spark_jars)
            .config("spark.executor.extraClassPath", spark_jars)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.streaming.stateStore.compression.codec", "lz4")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.local.dir", self.temp_dir)
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark session created. Version: {spark.version}")
        return spark

    @staticmethod
    def get_schema() -> StructType:
        """Define schema for arrests JSON data"""
        return StructType([
            StructField("cb_no", IntegerType(), False),
            StructField("case_number", StringType(), False),
            StructField("arrest_date", StringType(), True),
            StructField("race", StringType(), True),
            StructField("charge_1_statute", StringType(), True),
            StructField("charge_1_description", StringType(), True),
            StructField("charge_1_type", StringType(), True),
            StructField("charge_1_class", StringType(), True),
            StructField("charge_2_statute", StringType(), True),
            StructField("charge_2_description", StringType(), True),
            StructField("charge_2_type", StringType(), True),
            StructField("charge_2_class", StringType(), True),
            StructField("charge_3_statute", StringType(), True),
            StructField("charge_3_description", StringType(), True),
            StructField("charge_3_type", StringType(), True),
            StructField("charge_3_class", StringType(), True),
            StructField("charge_4_statute", StringType(), True),
            StructField("charge_4_description", StringType(), True),
            StructField("charge_4_type", StringType(), True),
            StructField("charge_4_class", StringType(), True),
            StructField("charges_statute", StringType(), True),
            StructField("charges_description", StringType(), True),
            StructField("charges_type", StringType(), True),
            StructField("charges_class", StringType(), True)
        ])

    def read_from_kafka(self) -> DataFrame:
        """Read streaming data from Kafka"""
        logger.info(f"Reading from Kafka topic: {self.topic}")

        df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", "25")
            .load()
        )

        return df.selectExpr("CAST(value AS STRING) as json_str")

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply silver layer transformations"""
        logger.info("Applying transformations...")

        df = df.select(
            from_json(col("json_str"), self.get_schema()).alias("data")
        ).select("data.*")

        -1 if df.isStreaming else df.count()
        df = df.filter(
            col("case_number").isNotNull() &
            (trim(col("case_number")) != "")
        )

        df = df.select(
            col("cb_no").alias("arrest_id"),
            col("case_number"),
            col("arrest_date").alias("arrest_date_raw"),
            col("race").alias("arrestee_race"),
            col("charge_1_statute").alias("charge_one_statute"),
            col("charge_1_description").alias("charge_one_description"),
            col("charge_1_type").alias("charge_one_type_raw"),
            col("charge_1_class").alias("charge_one_class"),
            col("charge_2_statute"),
            col("charge_2_description"),
            col("charge_2_type"),
            col("charge_2_class"),
            col("charge_3_statute"),
            col("charge_3_description"),
            col("charge_3_type"),
            col("charge_3_class"),
            col("charge_4_statute"),
            col("charge_4_description"),
            col("charge_4_type"),
            col("charge_4_class")
        )

        df = df.withColumn(
            "arrest_date",
            coalesce(
                to_date(col("arrest_date_raw"), "MM/dd/yyyy"),
                to_date(col("arrest_date_raw"), "yyyy-MM-dd")
            )
        )

        df = df.withColumn(
            "charge_one_type",
            when(upper(trim(col("charge_one_type_raw"))) == "F", lit("Felony"))
            .when(upper(trim(col("charge_one_type_raw"))) == "M", lit("Misdemeanor"))
            .otherwise(lit("Other"))
        )

        df = (df
              .withColumn("charge_one_description",
                          coalesce(col("charge_one_description"), lit("Unknown")))
              .withColumn("charge_one_statute",
                          coalesce(col("charge_one_statute"), lit("Unknown")))
              .withColumn("charge_one_class",
                          coalesce(col("charge_one_class"), lit("Unknown")))
              .withColumn("arrestee_race",
                          coalesce(col("arrestee_race"), lit("Unknown")))
              )

        df = df.withColumn("load_timestamp", current_timestamp())
        df = df.withColumn("source_file", lit("kafka_stream"))

        df = df.withColumn(
            "hash_row",
            md5(concat(
                coalesce(col("arrest_id").cast("string"), lit("")),
                coalesce(col("case_number"), lit("")),
                coalesce(col("arrest_date").cast("string"), lit(""))
            ))
        )

        final_columns = [
            "arrest_id", "case_number", "arrest_date",
            "arrestee_race",
            "charge_one_statute", "charge_one_description",
            "charge_one_type", "charge_one_class",
            "charge_2_statute", "charge_2_description",
            "charge_2_type", "charge_2_class",
            "charge_3_statute", "charge_3_description",
            "charge_3_type", "charge_3_class",
            "charge_4_statute", "charge_4_description",
            "charge_4_type", "charge_4_class",
            "hash_row", "load_timestamp", "source_file"
        ]

        return df.select(*final_columns)

    def write_to_console(self, df: DataFrame):
        """Write streaming output to console for monitoring"""
        logger.info("Starting console output stream...")

        query = (
            df.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", "5")
            .trigger(processingTime="10 seconds")
            .start()
        )

        return query

    def write_to_parquet(self, df: DataFrame):
        """Write to Parquet files (intermediate storage)"""
        logger.info(f"Starting Parquet output stream to {self.output_dir}...")

        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        query = (
            df.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", self.output_dir)
            .option("checkpointLocation", f"{self.checkpoint_dir}/parquet")
            .partitionBy("arrest_date")
            .trigger(processingTime="30 seconds")
            .start()
        )

        return query

    def write_to_postgres(self, df: DataFrame, batch_id: int):
        """Write batch to PostgreSQL"""
        try:
            df.show()
            (
                df.write
                .format("jdbc")
                .option("url", self.jdbc_url)
                .option("dbtable", "silver.silver_arrests_stream")
                .option("user", self.pg_user)
                .option("password", self.pg_password)
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )
            logger.info(f"Batch {batch_id}: Wrote {df.count()} records to PostgreSQL")
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")
            raise e

    def write_to_postgres_stream(self, df: DataFrame):
        """Write streaming data to PostgreSQL using foreachBatch"""
        logger.info("Starting PostgreSQL output stream...")

        try:
            query = (
                df.writeStream
                .outputMode("append")
                .foreachBatch(self.write_to_postgres)
                .option("checkpointLocation", f"{self.checkpoint_dir}/postgres")
                .trigger(processingTime="30 seconds")
                .start()
            )

            return query
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error starting PostgreSQL stream: {e}")
            return None

    def ensure_postgres_table(self):
        """Ensure PostgreSQL schema and table exist"""
        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_db,
                user=self.pg_user,
                password=self.pg_password
            )
            cursor = conn.cursor()

            cursor.execute("CREATE SCHEMA IF NOT EXISTS silver;")

            create_table_sql = """
                               CREATE TABLE IF NOT EXISTS silver.silver_arrests_stream (
                                arrest_id INTEGER,
                                case_number VARCHAR(255),
                                arrest_date DATE,
                                arrestee_race VARCHAR(100),
                                charge_one_statute VARCHAR(255),
                                charge_one_description TEXT,
                                charge_one_type VARCHAR(50),
                                charge_one_class VARCHAR(10),
                                charge_2_statute VARCHAR(255),
                                charge_2_description TEXT,
                                charge_2_type VARCHAR(50),
                                charge_2_class VARCHAR(10),
                                charge_3_statute VARCHAR(255),
                                charge_3_description TEXT,
                                charge_3_type VARCHAR(50),
                                charge_3_class VARCHAR(10),
                                charge_4_statute VARCHAR(255),
                                charge_4_description TEXT,
                                charge_4_type VARCHAR(50),
                                charge_4_class VARCHAR(10),
                                hash_row VARCHAR(32),
                                load_timestamp TIMESTAMP,
                                source_file VARCHAR(50)
                            );      
                    CREATE INDEX IF NOT EXISTS idx_arrests_hash ON silver.silver_arrests_stream(hash_row);
                    CREATE INDEX IF NOT EXISTS idx_arrests_date ON silver.silver_arrests_stream(arrest_date);
                    CREATE INDEX IF NOT EXISTS idx_arrests_case ON silver.silver_arrests_stream(case_number);
                               """

            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info("✓ PostgreSQL table 'silver.silver_crimes_stream' is ready")

        except Exception as e:
            logger.error(f"Error ensuring PostgreSQL table: {e}")
            traceback.print_exc()
            raise e

    def run(self, output_mode: str = "parquet"):
        """
        Run the streaming processor

        Args:
            output_mode: 'console', 'parquet', 'postgres', or 'all'
        """
        logger.info("Starting arrest stream processor...")

        raw_df = self.read_from_kafka()
        transformed_df = self.transform(raw_df)

        queries = []

        if output_mode in ["console", "all"]:
            queries.append(self.write_to_console(transformed_df))

        if output_mode in ["parquet", "all"]:
            queries.append(self.write_to_parquet(transformed_df))

        if output_mode in ["postgres", "all"]:
            queries.append(self.write_to_postgres_stream(transformed_df))

        logger.info(f"Started {len(queries)} streaming query(ies)")
        logger.info("Press Ctrl+C to stop...")

        return queries

    def stop(self):
        """Stop Spark session and cleanup"""
        if self.spark:
            self.spark.stop()

        if os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory: {e}")


def main():
    """Main entry point"""
    processor = None
    queries = []

    try:
        processor = ArrestStreamProcessor(
            bootstrap_servers="127.0.0.1:9092",
            topic="arrests",
            checkpoint_dir="./checkpoints/arrests",
            output_dir="./silver/arrests"
        )
        processor.ensure_postgres_table()
        queries = processor.run(output_mode="all")

        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("\nStopping streams...")
        for query in queries:
            if query:
                query.stop()
    except Exception as e:
        logger.error(f"Error in arrest stream processor: {e}")
        raise
    finally:
        if processor:
            processor.stop()
        logger.info("Arrest stream processor stopped")


if __name__ == "__main__":
    main()
