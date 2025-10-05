import logging
import os
import traceback
from glob import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, input_file_name, sha2
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType

from spark.bronze.bronze_utils.write_to_postgres import write_to_postgres_bronze

CRIMES_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("case_number", StringType(), True),
    StructField("date", StringType(), True),
    StructField("block", StringType(), True),
    StructField("iucr", StringType(), True),
    StructField("primary_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("location_description", StringType(), True),
    StructField("arrest", BooleanType(), True),
    StructField("domestic", BooleanType(), True),
    StructField("beat", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", IntegerType(), True),
    StructField("community_area", StringType(), True),
    StructField("fbi_code", StringType(), True),
    StructField("x_coordinate", DoubleType(), True),
    StructField("y_coordinate", DoubleType(), True),
    StructField("year", IntegerType(), True),
    StructField("updated_on", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("location", StringType(), True),
])

SPARK_JARS = os.getenv("SPARK_JARS", "/jars/postgresql-42.7.0.jar")

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Bronze_Chicago_Crimes_Ingest")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.jars", SPARK_JARS)
        .config("spark.driver.extraClassPath", SPARK_JARS)
        .config("spark.executor.extraClassPath", SPARK_JARS)
        .getOrCreate()
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("bronze_chicago_crimes")

    source_dir = os.getenv("CRIMES_SOURCE_DIR", "/source_data")

    patterns = [
        os.path.join(source_dir, "Crimes*2021*.csv"),
        os.path.join(source_dir, "Crimes*2022*.csv"),
        os.path.join(source_dir, "Crimes*2023*.csv"),
    ]
    files = []
    for p in patterns:
        files.extend(glob(p))
    if not files:
        raise RuntimeError(f"No input CSV files found under {source_dir} matching Crimes*202[1-3]*.csv")

    output_dir = os.getenv("BRONZE_OUTPUT_DIR", "bronze_parquet")
    table_dir = os.path.join(output_dir, "bronze_chicago_crimes")

    spark = None
    try:
        spark = create_spark_session()
        logger.info("Reading crime CSVs: %s", files)

        df = (
            spark.read
            .option("header", "true")
            .schema(CRIMES_SCHEMA)
            .csv(files)
        )

        original_cols = list(df.columns)

        df = df.withColumn(
            "hash_row", sha2(concat_ws("|", *[col(c).cast("string") for c in original_cols]), 256)
        )
        df = df.withColumn("load_timestamp", current_timestamp())
        df = df.withColumn("source_file", input_file_name())

        (
            df.write
            .mode("append")
            .parquet(table_dir)
        )

        logger.info("Wrote %s records to %s (append).", df.count(), table_dir)

        try:
            write_to_postgres_bronze(df, "bronze_chicago_crimes")
            logger.info("Successfully wrote data to PostgreSQL bronze.bronze_chicago_crimes")
        except Exception as e:
            logger.error("Failed to write to PostgreSQL: %s", str(e))
            traceback.print_exc()
            raise e

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
