
import logging
import traceback

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, to_date, trim, upper, coalesce, lit
)

from base_transformer import SilverTransformer


class ArrestsTransformer(SilverTransformer):
    """Transformer for Chicago arrests data from bronze to silver"""

    def __init__(self):
        super().__init__(app_name="Silver_Chicago_Arrests_Transform")

    def get_bronze_table_name(self) -> str:
        return "bronze_chicago_arrests"

    def get_silver_table_name(self) -> str:
        return "silver_arrests"

    def get_write_mode(self) -> str:
        return "overwrite"

    def select_and_rename_columns(self, df: DataFrame) -> DataFrame:
        """Select and rename columns to snake_case, keep hash_row and load_timestamp"""
        self.logger.info("Selecting and renaming columns")

        return df.select(
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
            col("charge_4_class"),
            col("hash_row"),
            col("load_timestamp"),
            col("source_file")
        )

    def filter_invalid_records(self, df: DataFrame) -> DataFrame:
        """Filter out records with NULL or empty case_number"""
        self.logger.info("Filtering invalid records")

        initial_count = df.count()
        df = df.filter(
            col("case_number").isNotNull() &
            (trim(col("case_number")) != "")
        )
        final_count = df.count()

        filtered_count = initial_count - final_count
        if filtered_count > 0:
            self.logger.info(f"Filtered out {filtered_count} records with empty case_number")

        return df

    def convert_data_types(self, df: DataFrame) -> DataFrame:
        """Convert columns to appropriate data types"""
        self.logger.info("Converting data types")

        # Parse arrest_date - try multiple formats
        df = df.withColumn(
            "arrest_date",
            coalesce(
                to_date(col("arrest_date_raw"), "MM/dd/yyyy"),
                to_date(col("arrest_date_raw"), "yyyy-MM-dd")
            )
        )

        return df

    def normalize_charge_type(self, df: DataFrame) -> DataFrame:
        """Normalize charge_one_type values: F=Felony, M=Misdemeanor, other=Other"""
        self.logger.info("Normalizing charge types")

        df = df.withColumn(
            "charge_one_type",
            when(upper(trim(col("charge_one_type_raw"))) == "F", lit("Felony"))
            .when(upper(trim(col("charge_one_type_raw"))) == "M", lit("Misdemeanor"))
            .otherwise(lit("Other"))
        )

        return df

    def handle_null_values(self, df: DataFrame) -> DataFrame:
        """Handle NULL values according to business rules"""
        self.logger.info("Handling NULL values")

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

        return df

    def select_final_columns(self, df: DataFrame) -> DataFrame:
        """Select final columns in desired order"""
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

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to arrests data

        Transformation pipeline:
        1. Select and rename columns (keep hash_row, load_timestamp)
        2. Filter invalid records (empty case_number)
        3. Convert data types
        4. Normalize charge types
        5. Handle NULL values
        6. Select final columns

        Note: Deduplication by hash_row and load_timestamp
        is handled automatically by the base class
        """
        df = self.select_and_rename_columns(df)
        df = self.filter_invalid_records(df)
        df = self.convert_data_types(df)
        df = self.normalize_charge_type(df)
        df = self.handle_null_values(df)
        df = self.select_final_columns(df)

        return df


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    transformer = ArrestsTransformer()

    try:
        transformer.run()
    except Exception as e:
        logging.error(f"Failed to process arrests data: {str(e)}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()