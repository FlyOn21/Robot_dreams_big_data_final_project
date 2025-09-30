import logging
import os
import traceback
from glob import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StringType, StructField, StructType

from spark.bronze.bronze_utils.write_to_postgres import write_to_postgres_bronze

# Bronze schema for UCR codes (read all as text first, then coerce 'active' to boolean)
UCR_SCHEMA = StructType([
    StructField("iucr", StringType(), True),
    StructField("primary_description", StringType(), True),
    StructField("secondary_description", StringType(), True),
    StructField("index_code", StringType(), True),
    StructField("active", StringType(), True),  # checkbox in CSV -> will coerce to boolean after read
])

SPARK_JARS = os.getenv("SPARK_JARS", "/jars/postgresql-42.7.0.jar")

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Bronze_Chicago_UCR_Codes_Ingest")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.jars", SPARK_JARS)  # Removed f-string, not needed
        .config("spark.driver.extraClassPath", SPARK_JARS)  # Add for reliability
        .config("spark.executor.extraClassPath", SPARK_JARS)  # Add for reliability
        .getOrCreate()
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("bronze_chicago_ucr_codes")

    source_dir = os.getenv("UCR_SOURCE_DIR", "/source_data")
    # Be lenient with filename (long official name)
    patterns = [
        os.path.join(source_dir, "Chicago_Police_Department*Uniform_Crime*csv"),
        os.path.join(source_dir, "Chicago*Uniform*Crime*csv"),
        os.path.join(source_dir, "UCR*codes*.csv"),
    ]

    files = []
    for p in patterns:
        files.extend(glob(p))
    if not files:
        raise RuntimeError(f"No input CSV file found for UCR codes under {source_dir}")

    output_dir = os.getenv("BRONZE_OUTPUT_DIR", "bronze_parquet")
    table_dir = os.path.join(output_dir, "bronze_chicago_ucr_codes")

    spark = None
    try:
        spark = create_spark_session()
        logger.info("Reading UCR codes CSV: %s", files)

        df = (
            spark.read
            .option("header", "true")
            .schema(UCR_SCHEMA)
            .csv(files)
            .withColumn("load_timestamp", current_timestamp())
        )

        (
            df.write
            .mode("overwrite")  # static reference/dictionary
            .parquet(table_dir)
        )

        logger.info("Wrote %s records to %s (overwrite).", df.count(), table_dir)

        try:
            write_to_postgres_bronze(df, "bronze_chicago_ucr_codes")
            logger.info("Successfully wrote data to PostgreSQL bronze.bronze_chicago_ucr_codes")
        except Exception as e:
            logger.error("Failed to write to PostgreSQL: %s", str(e))
            traceback.print_exc()
            raise e

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
