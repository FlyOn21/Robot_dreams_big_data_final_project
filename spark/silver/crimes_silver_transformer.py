
import logging
import traceback

from base_transformer import SilverTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, concat_ws, lit, to_date, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType


class CrimesTransformer(SilverTransformer):
    """Transformer for Chicago crimes data from bronze to silver"""

    def __init__(self):
        super().__init__(app_name="Silver_Chicago_Crimes_Transform")

    def get_bronze_table_name(self) -> str:
        return "bronze_chicago_crimes"

    def get_silver_table_name(self) -> str:
        return "silver_crimes"

    def get_write_mode(self) -> str:
        return "overwrite"

    def select_and_rename_columns(self, df: DataFrame) -> DataFrame:
        """Select and rename columns to snake_case, keep hash_row and load_timestamp"""
        self.logger.info("Selecting and renaming columns")

        return df.select(
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
            col("location").alias("location_geo_point"),
            col("hash_row"),
            col("load_timestamp"),
            col("source_file")
        )

    def convert_data_types(self, df: DataFrame) -> DataFrame:
        """Convert columns to appropriate data types"""
        self.logger.info("Converting data types")

        # Parse datetime - format: "MM/dd/yyyy hh:mm:ss a"
        df = df.withColumn(
            "crime_datetime",
            to_timestamp(col("crime_datetime_raw"), "MM/dd/yyyy hh:mm:ss a")
        )

        # Convert numeric columns
        df = (df
              .withColumn("beat_id", col("beat_id").cast(IntegerType()))
              .withColumn("district_id", col("district_id").cast(IntegerType()))
              .withColumn("ward_id", col("ward_id").cast(IntegerType()))
              .withColumn("community_area_id", col("community_area_id").cast(IntegerType()))
              .withColumn("x_coordinate", col("x_coordinate").cast(DoubleType()))
              .withColumn("y_coordinate", col("y_coordinate").cast(DoubleType()))
              .withColumn("latitude", col("latitude").cast(DoubleType()))
              .withColumn("longitude", col("longitude").cast(DoubleType()))
              )

        return df

    def handle_null_values(self, df: DataFrame) -> DataFrame:
        """Handle NULL values according to business rules"""
        self.logger.info("Handling NULL values")

        df = df.filter(
            col("latitude").isNotNull() & col("longitude").isNotNull()
        )
        self.logger.info("Filtered records with NULL coordinates")

        df = df.withColumn(
            "location_description",
            coalesce(col("location_description"), lit("Unknown"))
        )

        df = (df
              .withColumn("iucr_code", coalesce(col("iucr_code"), lit("INVALID")))
              .withColumn("primary_type", coalesce(col("primary_type"), lit("INVALID")))
              )

        return df

    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """Add additional derived columns"""
        self.logger.info("Adding derived columns")

        # Extract crime_date from crime_datetime
        df = df.withColumn(
            "crime_date",
            to_date(col("crime_datetime"))
        )

        # Create location_full_text
        df = df.withColumn(
            "location_full_text",
            concat_ws(" - ", col("block"), col("location_description"))
        )

        return df

    def select_final_columns(self, df: DataFrame) -> DataFrame:
        """Select final columns in desired order"""
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

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to crimes data

        Transformation pipeline:
        1. Select and rename columns (keep hash_row, load_timestamp)
        2. Convert data types
        3. Handle NULL values
        4. Add derived columns
        5. Select final columns
        """
        df = self.select_and_rename_columns(df)
        df = self.convert_data_types(df)
        df = self.handle_null_values(df)
        df = self.add_derived_columns(df)
        df = self.select_final_columns(df)

        return df


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    transformer = CrimesTransformer()

    try:
        transformer.run()
    except Exception as e:
        logging.error(f"Failed to process crimes data: {str(e)}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
