from __future__ import annotations

import contextlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    desc,
    expr,
    from_json,
    from_unixtime,
    lit,
    to_timestamp,
    when,
    window,
)

if TYPE_CHECKING:
    from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class SparkJsonSchemaStreamProcessor:
    """
    Standalone class that:
      - Reads from two Kafka topics (transactions, user_activity)
      - Parses JSON payloads using explicit StructType schemas
      - Handles both RFC3339 timestamps and Unix timestamps in milliseconds
      - Applies a 10-minute watermark on the event-time column 'timestamp'
    """

    def __init__(
            self,
            bootstrap_servers: str = "127.0.0.1:9092",
            app_name: str = "OptimizedKafkaStreaming",
            master: str | None = "local[*]",
            session_tz: str = "Europe/Kyiv",
            starting_offsets: str = "earliest",
            driver_memory: str = "10g",
            executor_memory: str = "8g",
            max_result_size: str = "1g",
            java_home: str = '/usr/lib/jvm/java-21-openjdk-amd64'
    ) -> None:

        # Clean environment variables that might cause conflicts
        problematic_vars = ['PYSPARK_SUBMIT_ARGS', 'SPARK_SUBMIT_OPTS']
        for var in problematic_vars:
            if var in os.environ:
                logger.info(f"Clearing {var}: {os.environ[var]}")
                del os.environ[var]

        if 'JAVA_HOME' not in os.environ and os.path.exists(java_home):
            os.environ['JAVA_HOME'] = java_home
            logger.info(f"Set JAVA_HOME to: {java_home}")

        self.temp_dir = tempfile.mkdtemp(prefix="spark_optimized_", dir="/tmp")
        logger.info(f"Using temporary directory for Spark: {self.temp_dir}")
        os.environ['SPARK_LOCAL_DIRS'] = self.temp_dir

        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", session_tz)

            .config("spark.driver.memory", driver_memory)
            .config("spark.executor.memory", executor_memory)
            .config("spark.driver.maxResultSize", max_result_size)
            .config("spark.driver.memoryFraction", "0.8")
            .config("spark.executor.memoryFraction", "0.8")
            .config("spark.storage.memoryFraction", "0.6")

            .config("spark.executor.extraJavaOptions",
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1PrintRegionRememberedSetInfo "
                    "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxGCPauseMillis=200 "
                    "-XX:G1HeapRegionSize=16m -XX:+UseStringDeduplication")
            .config("spark.driver.extraJavaOptions",
                    "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=16m")

            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            # Fixed deprecated configuration
            .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
            .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "4")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

            .config("spark.sql.streaming.metricsEnabled", "true")
            .config("spark.sql.streaming.stateStore.compression.codec", "lz4")
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "300s")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")

            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.unsafe", "true")
            .config("spark.kryoserializer.buffer.max", "1024m")

            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000")

            .config("spark.jars.ivy", f"{self.temp_dir}/ivy")
            .config("spark.sql.streaming.checkpointLocation", f"{self.temp_dir}/checkpoints")
            .config("spark.local.dir", self.temp_dir)

            .config("spark.sql.files.maxPartitionBytes", "134217728")
            .config("spark.sql.files.openCostInBytes", "4194304")
        )

        if master:
            builder = builder.master(master)

        logger.info("Initializing optimized Spark session...")

        try:
            self.spark: SparkSession = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")

            # Set additional runtime configurations
            self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

            logger.info(f"Spark session created successfully. Version: {self.spark.version}")
            logger.info(f"Available cores: {self.spark.sparkContext.defaultParallelism}")

            # Test Kafka connectivity
            try:
                self.spark.readStream.format("kafka")
                logger.info("Kafka connector is available!")
            except Exception as kafka_error:
                logger.warning(f"Kafka connector not available: {kafka_error}")
                logger.info("Make sure to run with appropriate Kafka packages")

        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise e


        self.bootstrap_servers = bootstrap_servers
        self.starting_offsets = starting_offsets
        self.output_paths = {}

    @staticmethod
    def transactions_schema() -> StructType:
        """Schema for transaction messages - handles both string and numeric timestamps"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("merchant", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("timestamp", StringType(), True),
            StructField("is_fraud", BooleanType(), False),
        ])

    @staticmethod
    def user_activity_schema() -> StructType:
        """Schema for user activity messages"""
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("device", StringType(), False),
            StructField("browser", StringType(), False),
            StructField("timestamp", StringType(), True),
        ])

    @staticmethod
    def _parse_timestamp(col_expr):
        formats = [
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "yyyy-MM-dd'T'HH:mm:ssXXX",
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd'T'HH:mm:ssX",
        ]
        ts_exprs = [to_timestamp(col_expr, fmt) for fmt in formats]
        numeric_ts = (
            when(col_expr.rlike("^[0-9]+$"),
                 when(col_expr.cast("bigint") > 1e12,
                      from_unixtime(col_expr.cast("bigint") / 1000).cast("timestamp"))
                 .otherwise(from_unixtime(col_expr.cast("bigint")).cast("timestamp"))
                 )
        )
        return coalesce(*ts_exprs, numeric_ts)

    @staticmethod
    def _start_console_stream(
            df: DataFrame,
            mode: str = "append",
            trigger: str = "10 seconds",
            truncate: bool = False,
            num_rows: int = 10,
    ):
        """
        Starts a streaming query that writes the output of a DataFrame to the console.
        """
        return (
            df.writeStream
            .outputMode(mode)
            .format("console")
            .option("truncate", str(truncate).lower())
            .option("num_rows", str(num_rows))
            .trigger(processingTime=trigger)
            .start()
        )

    def _check_and_prepare_output_path(self, base_path: str, clear_existing: bool = False) -> str:
        """
        Prepare output path and handle existing data

        Args:
            base_path: Base output path
            clear_existing: Whether to clear existing data

        Returns:
            Prepared output path
        """
        path = Path(base_path)

        if path.exists():
            if clear_existing:
                logger.info(f"Clearing existing data at {path}")
                shutil.rmtree(path)
                path.mkdir(parents=True, exist_ok=True)
            elif any(path.iterdir()):
                logger.warning(f"Data exists at {path}, using different path")
                counter = 1
                while True:
                    new_path = Path(f"{base_path}_{counter}")
                    if not new_path.exists() or not any(new_path.iterdir()):
                        path = new_path
                        break
                    counter += 1

        path.mkdir(parents=True, exist_ok=True)
        self.output_paths[base_path] = str(path)
        return str(path)

    def save_optimized_parquet(
            self,
            df: DataFrame,
            base_output_path: str,
            mode: str = "append",
            partition_cols: list = None,
            max_files: int = 4
    ) -> StreamingQuery:
        """
        Save DataFrame to Parquet with optimized file management

        Args:
            df: DataFrame to save
            base_output_path: Base output path
            mode: Write mode ('append', 'overwrite')
            partition_cols: Columns to partition by
            max_files: Maximum number of files to create
        """
        output_path = self._check_and_prepare_output_path(base_output_path)
        checkpoint_path = f"{output_path}_checkpoint"

        logger.info(f"Saving optimized Parquet to {output_path}")

        # Optimize DataFrame before writing
        optimized_df = df.coalesce(max_files)

        writer = (
            optimized_df.writeStream
            .outputMode(mode)
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .option("maxFilesPerTrigger", str(max_files))
            .trigger(processingTime="60 seconds")  # Longer interval for batching
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        return writer.start()

    def read_kafka_streams(
            self,
            transactions_topic: str = "transactions",
            user_activity_topic: str = "user_activity",
    ) -> dict[str, DataFrame]:
        """Read from Kafka topics with optimized settings"""
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")

        kafka_options = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "startingOffsets": self.starting_offsets,
            "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": "10000",
            "kafka.consumer.cache.enabled": "false",
            "kafka.fetch.min.bytes": "1024",
            "kafka.fetch.max.wait.ms": "500"
        }

        transactions = (
            self.spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .option("subscribe", transactions_topic)
            .load()
            .selectExpr("CAST(value AS STRING) AS json_str", "timestamp as kafka_timestamp")
        )

        user_activity = (
            self.spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .option("subscribe", user_activity_topic)
            .load()
            .selectExpr("CAST(value AS STRING) AS json_str", "timestamp as kafka_timestamp")
        )

        return {"transactions_raw": transactions, "user_activity_raw": user_activity}

    def parse_transactions_json_with_watermark(
            self,
            transactions_json_df: DataFrame,
            watermark: str = "10 minutes",
    ) -> DataFrame:
        """Parse transactions with memory-optimized processing"""
        logger.info("Parsing transactions JSON with optimizations...")

        parsed = (
            transactions_json_df
            .select(from_json(col("json_str"), self.transactions_schema()).alias("data"), "kafka_timestamp")
            .select("data.*", "kafka_timestamp")
            .withColumn("timestamp", self._parse_timestamp(col("timestamp")))
            .select(
                "transaction_id",
                "user_id",
                "amount",
                "merchant",
                "timestamp",
                "is_fraud"
            )
            .withWatermark("timestamp", watermark)
        )
        return parsed.repartition(4, col("user_id"))

    def parse_user_activity_json_with_watermark(
            self,
            user_activity_json_df: DataFrame,
            watermark: str = "10 minutes",
            enable_deduplication: bool = True,
    ) -> DataFrame:
        """
        Parse user activity JSON with watermark and optional deduplication
        Task 2: Transaction Processing and Task 6: Event Deduplication
        """
        logger.info("Parsing user activity JSON and applying watermark...")

        parsed = (
            user_activity_json_df
            .select(from_json(col("json_str"), self.user_activity_schema()).alias("data"), "kafka_timestamp")
            .select("data.*", "kafka_timestamp")
        )

        parsed = parsed.withColumn("timestamp", self._parse_timestamp(col("timestamp")))

        # Task 6: Apply deduplication before watermark
        if enable_deduplication:
            logger.info("Applying event deduplication on event_id...")
            parsed = parsed.dropDuplicates(["event_id"])

        parsed = parsed.withWatermark("timestamp", watermark)

        return parsed.repartition(4, col("user_id"))

    @staticmethod
    def analyze_user_activity_with_windows(
            user_activity_parsed: DataFrame,
            window_duration: str = "10 minutes",
            slide_duration: str = "5 minutes"
    ) -> DataFrame:
        """
        Task 3: User Activity Analysis
        Analyze user activity with sliding window aggregation:
        - Group by event_type (click, add_to_cart, purchase)
        - Count events per user_id in time windows
        - Use sliding window aggregation
        """
        logger.info(f"Analyzing user activity with {window_duration} windows, sliding every {slide_duration}...")

        return (
            user_activity_parsed.groupBy(
                window(col("timestamp"), window_duration, slide_duration),
                col("user_id"),
                col("event_type"),
            )
            .agg(count("*").alias("event_count"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_id"),
                col("event_type"),
                col("event_count"),
            )
            .orderBy(desc("window_start"), desc("event_count"))
            .coalesce(2)
        )

    @staticmethod
    def analyze_event_type_summary(
            user_activity_parsed: DataFrame,
            window_duration: str = "10 minutes"
    ) -> DataFrame:
        """
        Create summary of event types in time windows
        """
        logger.info(f"Creating event type summary with {window_duration} windows...")

        return (
            user_activity_parsed.groupBy(
                window(col("timestamp"), window_duration), col("event_type")
            )
            .agg(
                count("*").alias("total_events"),
                count(col("user_id")).alias("unique_users"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("event_type"),
                col("total_events"),
                col("unique_users"),
            )
            .orderBy(desc("window_start"), desc("total_events"))
            .coalesce(2)
        )

    @staticmethod
    def detect_fraud_transactions(
            transactions_parsed: DataFrame
    ) -> DataFrame:
        """
        Task 4: Fraud Detection
        Apply business logic to detect fraudulent transactions:
        - Transactions over $1000
        - Merchant = "Amazon"
        - is_fraud == true
        """
        logger.info("Applying fraud detection business logic...")

        return (
            transactions_parsed
            .filter(
                (col("amount") > 1000) &
                (col("merchant") == "Amazon") &
                col("is_fraud")
            )
            .withColumn("fraud_reason", lit("High Amount Amazon Fraud"))
            .withColumn("detection_timestamp", expr("current_timestamp()"))
            .coalesce(2)
        )

    @staticmethod
    def join_transactions_with_user_activity(
            transactions_parsed: DataFrame,
            user_activity_parsed: DataFrame
    ) -> DataFrame:
        """
        Task 5: Stream-Stream Join
        Join transactions and user activity streams by user_id with time-range conditions.
        Both streams must have watermarks applied before joining.
        Time range: transaction timestamp ± 5 minutes from user activity timestamp
        """
        logger.info("Performing stream-stream join with time-range conditions...")
        transactions_opt = (
            transactions_parsed
            .select(
                col("transaction_id"),
                col("user_id").alias("txn_user_id"),
                col("amount"),
                col("merchant"),
                col("timestamp").alias("txn_timestamp"),
                col("is_fraud")
            )
            .repartition(4, col("txn_user_id"))
        )
        user_activity_opt = (
            user_activity_parsed
            .select(
                col("event_id"),
                col("user_id").alias("activity_user_id"),
                col("event_type"),
                col("device"),
                col("browser"),
                col("timestamp").alias("activity_timestamp")
            )
            .repartition(4, col("activity_user_id"))
        )

        transactions_opt = transactions_opt.filter(col("txn_timestamp").isNotNull())
        user_activity_opt = user_activity_opt.filter(col("activity_timestamp").isNotNull())

        join_condition = (
                (col("txn_user_id") == col("activity_user_id")) &
                (col("txn_timestamp") >= col("activity_timestamp") - expr("INTERVAL 5 MINUTES")) &
                (col("txn_timestamp") <= col("activity_timestamp") + expr("INTERVAL 5 MINUTES"))
        )

        return (
            transactions_opt
            .join(user_activity_opt, join_condition, "inner")
            .select(
                col("transaction_id"),
                col("txn_user_id").alias("user_id"),
                col("amount"),
                col("merchant"),
                col("txn_timestamp").alias("transaction_timestamp"),
                col("is_fraud"),
                col("event_id"),
                col("event_type"),
                col("device"),
                col("browser"),
                col("activity_timestamp"),
                (col("txn_timestamp").cast("long") - col("activity_timestamp").cast("long")).alias("time_diff_seconds"),
            )
            .coalesce(2)
        )

    def main(self) -> list[StreamingQuery]:
        """
        Demonstration of all tasks with fixed streaming query handling
        """
        logger.info("Starting Spark Structured Streaming Demo (Tasks 1-5)...")

        # Task 1: Reading from Kafka
        streams = self.read_kafka_streams()
        transactions_raw = streams["transactions_raw"]
        user_activity_raw = streams["user_activity_raw"]

        # Task 2: Transaction Processing
        transactions_parsed = self.parse_transactions_json_with_watermark(transactions_raw)
        user_activity_parsed = self.parse_user_activity_json_with_watermark(
            user_activity_raw,
            enable_deduplication=True
        )

        # Task 3: User Activity Analysis with Sliding Windows
        logger.info("\n=== Task 3: User Activity Analysis ===")

        windowed_activity = self.analyze_user_activity_with_windows(
            user_activity_parsed,
            window_duration="10 minutes",
            slide_duration="5 minutes"
        )

        event_summary = self.analyze_event_type_summary(
            user_activity_parsed,
            window_duration="10 minutes"
        )

        # Task 4: Fraud Detection
        logger.info("\n=== Task 4: Fraud Detection ===")
        fraud_transactions = self.detect_fraud_transactions(transactions_parsed)

        # Task 5: Stream-Stream Join
        logger.info("\n=== Task 5: Stream-Stream Join ===")
        joined_streams = self.join_transactions_with_user_activity(
            transactions_parsed,
            user_activity_parsed
        )

        # Show schemas for verification
        logger.info("\nTransaction DataFrame Schema:")
        transactions_parsed.printSchema()

        logger.info("\nUser Activity DataFrame Schema:")
        user_activity_parsed.printSchema()

        logger.info("\nWindowed Activity Analysis Schema:")
        windowed_activity.printSchema()

        logger.info("\nEvent Summary Schema:")
        event_summary.printSchema()

        logger.info("\nFraud Detection Schema:")
        fraud_transactions.printSchema()

        logger.info("\nJoined Streams Schema:")
        joined_streams.printSchema()

        streaming_queries = []

        logger.info("Starting console output for transactions...")
        transactions_query = self._start_console_stream(
            df=transactions_parsed,
            mode="append",
            trigger="10 seconds",
            truncate=False,
            num_rows=10
        )
        streaming_queries.append(transactions_query)

        # Console output for user activities
        logger.info("Starting console output for user activities...")
        activities_query = self._start_console_stream(
            df=user_activity_parsed,
            mode="append",
            trigger="10 seconds",
            truncate=False,
            num_rows=10
        )
        streaming_queries.append(activities_query)

        # Task 3: Windowed analysis
        logger.info("\n=== User Activity with Sliding Windows ===")
        windowed_query = self._start_console_stream(
            df=windowed_activity,
            mode="complete",
            trigger="15 seconds",
            truncate=False,
            num_rows=15
        )
        streaming_queries.append(windowed_query)

        logger.info("\n=== Event Type Summary ===")
        summary_query = self._start_console_stream(
            df=event_summary,
            mode="complete",
            trigger="15 seconds",
            truncate=False,
            num_rows=15
        )
        streaming_queries.append(summary_query)

        # Task 4: Save fraud results
        logger.info("Setting up fraud detection streaming query...")
        fraud_save_query = self.save_optimized_parquet(
            fraud_transactions,
            "./fraud_results",
            max_files=2
        )
        streaming_queries.append(fraud_save_query)

        # Task 5: Save joined results
        logger.info("Setting up joined streams streaming query...")
        joined_save_query = self.save_optimized_parquet(
            joined_streams,
            "./joined_results",
            max_files=2
        )
        streaming_queries.append(joined_save_query)

        return streaming_queries

    def stop(self):
        """Stop Spark session and cleanup"""
        if hasattr(self, 'spark'):
            self.spark.stop()

        # Cleanup temporary directory
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory: {e}")


if __name__ == "__main__":
    queries = []
    processor = None

    try:
        processor = SparkJsonSchemaStreamProcessor()
        queries = processor.main()

        logger.info("Streaming started successfully!")
        logger.info("Press Ctrl+C to stop...")
        for query in queries:
            if query is not None:
                query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("\nStopping streams...")
        for query in queries:
            if query is not None:
                with contextlib.suppress(Exception):
                    query.stop()
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error: {error_msg}")
        raise
    finally:
        for query in queries:
            if query is not None:
                with contextlib.suppress(Exception):
                    query.stop()

        if processor:
            with contextlib.suppress(Exception):
                processor.stop()

        logger.info("Spark session stopped and resources cleaned up.")
