
import logging
import os
import traceback

from base_transformer import SilverTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, trim, upper, when


class UCRCodesTransformer(SilverTransformer):
    """Transformer for Chicago UCR codes reference data from bronze to silver"""

    def __init__(self):
        super().__init__(app_name="Silver_Chicago_UCR_Codes_Transform")
        self.shuffle_partitions = os.getenv("SPARK_SHUFFLE_PARTITIONS", "50")

    def get_bronze_table_name(self) -> str:
        return "bronze_chicago_ucr_codes"

    def get_silver_table_name(self) -> str:
        return "silver_ucr_codes"

    def get_write_mode(self) -> str:
        return "overwrite"

    def select_and_rename_columns(self, df: DataFrame) -> DataFrame:
        """Select and rename columns to snake_case, keep load_timestamp"""
        self.logger.info("Selecting and renaming columns")

        return df.select(
            col("iucr").alias("iucr_code"),
            col("primary_description"),
            col("secondary_description"),
            col("index_code"),
            col("active").alias("is_active_raw"),
            col("load_timestamp")
        )

    def convert_active_to_boolean(self, df: DataFrame) -> DataFrame:
        """Convert active field to boolean"""
        self.logger.info("Converting active field to boolean")

        df = df.withColumn(
            "is_active",
            when(
                (upper(trim(col("is_active_raw"))) == "TRUE") |
                (upper(trim(col("is_active_raw"))) == "Y"),
                lit(True)
            ).when(
                (upper(trim(col("is_active_raw"))) == "FALSE") |
                (upper(trim(col("is_active_raw"))) == "N"),
                lit(False)
            ).otherwise(lit(False))
        )

        return df

    def filter_active_codes(self, df: DataFrame) -> DataFrame:
        """Filter to keep only active codes"""
        self.logger.info("Filtering for active codes only")

        initial_count = df.count()
        df = df.filter(col("is_active"))
        final_count = df.count()

        inactive_count = initial_count - final_count
        if inactive_count > 0:
            self.logger.info(f"Filtered out {inactive_count} inactive codes")

        return df

    def ensure_string_types(self, df: DataFrame) -> DataFrame:
        """Ensure all columns (except is_active) are STRING type"""
        self.logger.info("Ensuring proper data types")

        df = (df
              .withColumn("iucr_code", col("iucr_code").cast("string"))
              .withColumn("primary_description", col("primary_description").cast("string"))
              .withColumn("secondary_description", col("secondary_description").cast("string"))
              .withColumn("index_code", col("index_code").cast("string"))
              )

        return df

    def handle_null_values(self, df: DataFrame) -> DataFrame:
        """Handle NULL values - replace with 'Unknown'"""
        self.logger.info("Handling NULL values")

        df = (df
              .withColumn("primary_description",
                          coalesce(col("primary_description"), lit("Unknown")))
              .withColumn("secondary_description",
                          coalesce(col("secondary_description"), lit("Unknown")))
              .withColumn("index_code",
                          coalesce(col("index_code"), lit("Unknown")))
              )

        return df

    def deduplicate_by_iucr_code(self, df: DataFrame) -> DataFrame:
        """
        Ensure iucr_code is unique (for reference data)
        Keep the most recent record by load_timestamp
        """
        self.logger.info("Removing duplicates by iucr_code")

        from pyspark.sql import Window
        from pyspark.sql.functions import desc, row_number

        initial_count = df.count()

        #deduplicate by iucr_code (not hash_row)
        window_spec = Window.partitionBy("iucr_code").orderBy(desc("load_timestamp"))

        df = (
            df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        final_count = df.count()
        duplicates_removed = initial_count - final_count

        if duplicates_removed > 0:
            self.logger.warning(f"Removed {duplicates_removed} duplicate IUCR codes")

        return df

    def select_final_columns(self, df: DataFrame) -> DataFrame:
        """Select final columns in desired order"""
        final_columns = [
            "iucr_code",
            "primary_description",
            "secondary_description",
            "index_code",
            "is_active",
            "load_timestamp"
        ]

        return df.select(*final_columns)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to UCR codes data

        Transformation pipeline:
        1. Select and rename columns
        2. Convert active field to boolean
        3. Filter for active codes only
        4. Ensure string data types
        5. Handle NULL values
        6. Deduplicate by iucr_code (keep most recent)
        7. Select final columns

        """
        df = self.select_and_rename_columns(df)
        df = self.convert_active_to_boolean(df)
        df = self.filter_active_codes(df)
        df = self.ensure_string_types(df)
        df = self.handle_null_values(df)
        df = self.deduplicate_by_iucr_code(df)
        df = self.select_final_columns(df)

        return df


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    transformer = UCRCodesTransformer()

    try:
        transformer.run()
    except Exception as e:
        logging.error(f"Failed to process UCR codes data: {str(e)}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
