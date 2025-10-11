"""
Spark Structured Streaming processor for Chicago crimes data
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
    concat_ws,
    current_timestamp,
    from_json,
    lit,
    md5,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CrimeStreamProcessor:
    """Stream processor for Chicago crimes data"""

    def __init__(
            self,
            bootstrap_servers: str = "127.0.0.1:9092",
            topic: str = "crimes",
            checkpoint_dir: str = "./checkpoints/crimes",
            output_dir: str = "./silver/crimes",
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

        self.temp_dir = tempfile.mkdtemp(prefix="spark_crime_stream_", dir="/tmp")
        logger.info(f"Using temporary directory: {self.temp_dir}")

        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for streaming"""
        logger.info("Creating Spark session for crime streaming...")

        spark_jars = os.getenv("SPARK_JARS", "/jars/postgresql-42.7.0.jar")

        spark = (
            SparkSession.builder
            .appName("CrimeStreamProcessor")
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
        """Define schema for crimes JSON data"""
        return StructType([
            StructField("id", IntegerType(), False),
            StructField("case_number", StringType(), False),
            StructField("date", StringType(), True),
            StructField("block", StringType(), True),
            StructField("iucr", StringType(), True),
            StructField("primary_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("location_description", StringType(), True),
            StructField("arrest", BooleanType(), True),
            StructField("domestic", BooleanType(), True),
            StructField("beat", IntegerType(), True),
            StructField("district", IntegerType(), True),
            StructField("ward", IntegerType(), True),
            StructField("community_area", IntegerType(), True),
            StructField("fbi_code", StringType(), True),
            StructField("x_coordinate", DoubleType(), True),
            StructField("y_coordinate", DoubleType(), True),
            StructField("year", IntegerType(), True),
            StructField("updated_on", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("location", StringType(), True)
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

        df = df.select(
            col("id").alias("crime_id"),
            col("case_number"),
            col("date").alias("crime_datetime_raw"),
            col("block"),
            col("iucr").alias("iucr_code"),
            col("primary_type"),
            col("description"),
            col("location_description"),
            col("arrest").alias("is_arrest"),
            col("domestic").alias("is_domestic"),
            col("beat").alias("beat_id"),
            col("district").alias("district_id"),
            col("ward").alias("ward_id"),
            col("community_area").alias("community_area_id"),
            col("fbi_code"),
            col("x_coordinate"),
            col("y_coordinate"),
            col("latitude"),
            col("longitude"),
            col("location").alias("location_geo_point")
        )

        df = df.withColumn(
            "crime_datetime",
            to_timestamp(col("crime_datetime_raw"), "MM/dd/yyyy hh:mm:ss a")
        )

        df = df.filter(
            col("latitude").isNotNull() & col("longitude").isNotNull()
        )

        df = df.withColumn(
            "location_description",
            coalesce(col("location_description"), lit("Unknown"))
        )

        df = (df
              .withColumn("iucr_code", coalesce(col("iucr_code"), lit("INVALID")))
              .withColumn("primary_type", coalesce(col("primary_type"), lit("INVALID")))
              )

        df = df.withColumn("crime_date", to_date(col("crime_datetime")))
        df = df.withColumn(
            "location_full_text",
            concat_ws(" - ", col("block"), col("location_description"))
        )

        df = df.withColumn("load_timestamp", current_timestamp())
        df = df.withColumn("source_file", lit("kafka_stream"))

        df = df.withColumn(
            "hash_row",
            md5(concat(
                coalesce(col("crime_id").cast("string"), lit("")),
                coalesce(col("case_number"), lit("")),
                coalesce(col("crime_datetime").cast("string"), lit(""))
            ))
        )


        final_columns = [
            "crime_id", "case_number", "crime_datetime", "crime_date",
            "block", "iucr_code", "primary_type", "description",
            "location_description", "location_full_text",
            "is_arrest", "is_domestic",
            "beat_id", "district_id", "ward_id", "community_area_id",
            "fbi_code",
            "x_coordinate", "y_coordinate", "latitude", "longitude",
            "location_geo_point",
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
        try:
            return (
                df.writeStream.outputMode("append")
                .format("parquet")
                .option("path", self.output_dir)
                .option("checkpointLocation", f"{self.checkpoint_dir}/parquet")
                .partitionBy("crime_date")
                .trigger(processingTime="30 seconds")
                .start()
            )
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error starting Parquet stream: {e}")
            raise e

    def write_to_postgres(self, df: DataFrame, batch_id: int):
        """Write batch to PostgreSQL"""
        try:
            df.show()
            (
                df.write
                .format("jdbc")
                .option("url", self.jdbc_url)
                .option("dbtable", "silver.silver_crimes_stream")
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

    def write_to_postgres_stream(self, df: DataFrame):
        """Write streaming data to PostgreSQL using foreachBatch"""
        logger.info("Starting PostgreSQL output stream...")

        query = (
            df.writeStream
            .outputMode("append")
            .foreachBatch(self.write_to_postgres)
            .option("checkpointLocation", f"{self.checkpoint_dir}/postgres")
            .trigger(processingTime="30 seconds")
            .start()
        )

        return query

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
                               CREATE TABLE IF NOT EXISTS silver.silver_crimes_stream (
            crime_id INTEGER,
            case_number VARCHAR(255),
            crime_datetime TIMESTAMP,
            crime_date DATE,
            block VARCHAR(255),
            iucr_code VARCHAR(10),
            primary_type VARCHAR(100),
            description TEXT,
            location_description VARCHAR(255),
            location_full_text TEXT,
            is_arrest BOOLEAN,
            is_domestic BOOLEAN,
            beat_id INTEGER,
            district_id INTEGER,
            ward_id INTEGER,
            community_area_id INTEGER,
            fbi_code VARCHAR(10),
            x_coordinate DOUBLE PRECISION,
            y_coordinate DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            location_geo_point VARCHAR(255),
            hash_row VARCHAR(32),
            load_timestamp TIMESTAMP,
            source_file VARCHAR(50)
        );     
        CREATE INDEX IF NOT EXISTS idx_crimes_hash ON silver.silver_crimes_stream(hash_row);
        CREATE INDEX IF NOT EXISTS idx_crimes_date ON silver.silver_crimes_stream(crime_date);
        CREATE INDEX IF NOT EXISTS idx_crimes_case ON silver.silver_crimes_stream(case_number);
        CREATE INDEX IF NOT EXISTS idx_crimes_type ON silver.silver_crimes_stream(primary_type);
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
        logger.info("Starting crime stream processor...")
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
        processor = CrimeStreamProcessor(
            bootstrap_servers="127.0.0.1:9092",
            topic="crimes",
            checkpoint_dir="./checkpoints/crimes",
            output_dir="./silver/crimes"
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
        logger.error(f"Error in crime stream processor: {e}")
        raise
    finally:
        if processor:
            processor.stop()
        logger.info("Crime stream processor stopped")


if __name__ == "__main__":
    main()
