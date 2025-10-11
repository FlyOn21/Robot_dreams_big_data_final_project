import logging
import os
import traceback
from glob import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, input_file_name, sha2
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark.bronze.bronze_utils.write_to_postgres import write_to_postgres_bronze

ARRESTS_SCHEMA = StructType([
    StructField("cb_no", LongType(), True),
    StructField("case_number", StringType(), True),
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
    StructField("charges_class", StringType(), True),
])

SPARK_JARS = os.getenv("SPARK_JARS", "/jars/postgresql-42.7.0.jar")

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Bronze_Chicago_Arrests_Ingest")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.jars", SPARK_JARS)
        .config("spark.driver.extraClassPath", SPARK_JARS)
        .config("spark.executor.extraClassPath", SPARK_JARS)
        .getOrCreate()
    )



def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("bronze_chicago_arrests")

    source_dir = os.getenv("ARRESTS_SOURCE_DIR", "/source_data")
    patterns = [
        os.path.join(source_dir, "Arrests*.csv"),
        os.path.join(source_dir, "Chicago*Arrests*.csv"),
    ]

    files = []
    for p in patterns:
        files.extend(glob(p))
    if not files:
        raise RuntimeError(f"No input CSV files found under {source_dir} matching Arrests*.csv")

    output_dir = os.getenv("BRONZE_OUTPUT_DIR", "bronze_parquet")
    table_dir = os.path.join(output_dir, "bronze_chicago_arrests")

    spark = None
    try:
        spark = create_spark_session()
        logger.info("Reading arrests CSVs: %s", files)

        df = (
            spark.read
            .option("header", "true")
            .schema(ARRESTS_SCHEMA)
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
            write_to_postgres_bronze(df, "bronze_chicago_arrests")
            logger.info("Successfully wrote data to PostgreSQL bronze.bronze_chicago_arrests")
        except Exception as e:
            logger.error("Failed to write to PostgreSQL: %s", str(e))
            traceback.print_exc()
            raise e


    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
