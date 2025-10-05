import argparse
import json
import logging
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from fake_data_generator import DATA_GENERATOR, ArrestFactory, CrimeFactory, CriminalJusticeDataGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CriminalDataExporter:
    """Handles exporting criminal data to various formats"""

    def __init__(self, output_dir: str = "output", iucr_csv_path: str = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.generator = CriminalJusticeDataGenerator(iucr_csv_path=iucr_csv_path)

    def to_dict_arrest(self, arrest) -> dict:
        """Convert arrest object to dictionary - uses exact field names and ensures all fields are present"""
        return {
            'cb_no': arrest.cb_no,
            'case_number': arrest.case_number,
            'arrest_date': arrest.arrest_date.strftime('%m/%d/%Y') if hasattr(arrest.arrest_date, 'strftime') else str(arrest.arrest_date),
            'race': arrest.race,
            'charge_1_statute': arrest.charge_1_statute,
            'charge_1_description': arrest.charge_1_description,
            'charge_1_type': arrest.charge_1_type,
            'charge_1_class': arrest.charge_1_class,
            'charge_2_statute': arrest.charge_2_statute,
            'charge_2_description': arrest.charge_2_description,
            'charge_2_type': arrest.charge_2_type,
            'charge_2_class': arrest.charge_2_class,
            'charge_3_statute': arrest.charge_3_statute,
            'charge_3_description': arrest.charge_3_description,
            'charge_3_type': arrest.charge_3_type,
            'charge_3_class': arrest.charge_3_class,
            'charge_4_statute': arrest.charge_4_statute,
            'charge_4_description': arrest.charge_4_description,
            'charge_4_type': arrest.charge_4_type,
            'charge_4_class': arrest.charge_4_class,
            'charges_statute': arrest.charges_statute,
            'charges_description': arrest.charges_description,
            'charges_type': arrest.charges_type,
            'charges_class': arrest.charges_class
        }

    def to_dict_crime(self, crime) -> dict:
        """Convert crime object to dictionary - uses exact field names and ensures all fields are present"""
        return {
            'id': crime.id,
            'case_number': crime.case_number,
            'date': crime.date.strftime('%m/%d/%Y %I:%M:%S %p') if hasattr(crime.date, 'strftime') else str(crime.date),
            'block': crime.block,
            'iucr': crime.iucr,
            'primary_type': crime.primary_type,
            'description': crime.description,
            'location_description': crime.location_description,
            'arrest': crime.arrest,
            'domestic': crime.domestic,
            'beat': crime.beat,
            'district': crime.district,
            'ward': crime.ward,
            'community_area': crime.community_area,
            'fbi_code': crime.fbi_code,
            'x_coordinate': crime.x_coordinate,
            'y_coordinate': crime.y_coordinate,
            'year': crime.year,
            'updated_on': crime.updated_on,
            'latitude': crime.latitude,
            'longitude': crime.longitude,
            'location': crime.location
        }

    def save_csv(self, data: list, filename: str, data_type: str):
        """Save data to CSV file"""
        try:
            import pandas as pd

            if data_type == 'arrest':
                records = [self.to_dict_arrest(item) for item in data]
            else:
                records = [self.to_dict_crime(item) for item in data]

            df = pd.DataFrame(records)
            filepath = self.output_dir / f"{filename}.csv"
            df.to_csv(filepath, index=False)

            logger.info(f"Saved {len(records)} {data_type} records to {filepath}")
            return filepath

        except ImportError:
            logger.error("pandas not available for CSV export")
            return None

    def save_json(self, data: list, filename: str, data_type: str):
        """Save data to JSON file"""
        if data_type == 'arrest':
            records = [self.to_dict_arrest(item) for item in data]
        else:
            records = [self.to_dict_crime(item) for item in data]

        filepath = self.output_dir / f"{filename}.json"
        with open(filepath, 'w') as f:
            json.dump(records, f, indent=2, default=str)

        logger.info(f"Saved {len(records)} {data_type} records to {filepath}")
        return filepath

    def generate_sample_files(self,
                              arrest_count: int = 100,
                              crime_count: int = 100,
                              format_type: str = 'csv'):
        """Generate sample files with specified counts"""

        logger.info(f"Generating {arrest_count} arrests and {crime_count} crimes...")

        # Generate data
        arrests = self.generator.generate_arrests(arrest_count)
        crimes = self.generator.generate_crimes(crime_count)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format_type.lower() == 'csv':
            arrest_file = self.save_csv(arrests, f"arrests_{timestamp}", 'arrest')
            crime_file = self.save_csv(crimes, f"crimes_{timestamp}", 'crime')
        else:
            arrest_file = self.save_json(arrests, f"arrests_{timestamp}", 'arrest')
            crime_file = self.save_json(crimes, f"crimes_{timestamp}", 'crime')

        return arrest_file, crime_file

    def generate_correlated_dataset(self,
                                    total_crimes: int = 1000,
                                    format_type: str = 'csv'):
        """Generate correlated crime and arrest dataset"""

        logger.info(f"Generating correlated dataset with {total_crimes} crimes...")

        correlated_data = self.generator.generate_correlated_data(total_crimes)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save correlated data
        if format_type.lower() == 'csv':
            arrest_file = self.save_csv(
                correlated_data['arrests'],
                f"arrests_correlated_{timestamp}",
                'arrest'
            )
            crime_file = self.save_csv(
                correlated_data['crimes'],
                f"crimes_correlated_{timestamp}",
                'crime'
            )
        else:
            arrest_file = self.save_json(
                correlated_data['arrests'],
                f"arrests_correlated_{timestamp}",
                'arrest'
            )
            crime_file = self.save_json(
                correlated_data['crimes'],
                f"crimes_correlated_{timestamp}",
                'crime'
            )

        logger.info(f"Generated {len(correlated_data['arrests'])} arrests from {len(correlated_data['crimes'])} crimes")
        return arrest_file, crime_file


class CriminalDataStreamer:
    """Streams criminal data to various outputs"""

    def __init__(self, iucr_csv_path: str = None):
        self.generator = CriminalJusticeDataGenerator(iucr_csv_path=iucr_csv_path)
        self.stats = {
            'arrests_generated': 0,
            'crimes_generated': 0,
            'bytes_sent': 0,
            'start_time': None
        }

    def stream_to_kafka(self,
                        target_size_mb: float = 100,
                        bootstrap_servers: str = '127.0.0.1:9092'):
        """Stream data to Kafka topics"""
        try:
            from kafka import KafkaProducer

            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )

            target_bytes = target_size_mb * 1024 * 1024
            self.stats['start_time'] = time.time()

            logger.info(f"Starting Kafka streaming to {bootstrap_servers}")
            logger.info(f"Target size: {target_size_mb} MB")

            exporter = CriminalDataExporter()

            while self.stats['bytes_sent'] < target_bytes:

                if random.random() < 0.7:
                    crime = CrimeFactory()
                    crime_dict = exporter.to_dict_crime(crime)

                    producer.send('crimes', key=crime.case_number, value=crime_dict)
                    self.stats['crimes_generated'] += 1
                    self.stats['bytes_sent'] += len(json.dumps(crime_dict))

                else:
                    arrest = ArrestFactory()
                    arrest_dict = exporter.to_dict_arrest(arrest)

                    producer.send('arrests', key=arrest.case_number, value=arrest_dict)
                    self.stats['arrests_generated'] += 1
                    self.stats['bytes_sent'] += len(json.dumps(arrest_dict))

                total_records = self.stats['arrests_generated'] + self.stats['crimes_generated']
                if total_records % 1000 == 0:
                    mb_sent = self.stats['bytes_sent'] / 1024 / 1024
                    logger.info(f"Streamed {total_records} records ({mb_sent:.1f} MB)")

                time.sleep(0.3)

            producer.flush()
            producer.close()

            self._log_final_stats()

        except ImportError:
            logger.error("kafka-python not available for Kafka streaming")
        except Exception as e:
            logger.error(f"Error during Kafka streaming: {e}")

    def stream_to_console(self,
                          count: int = 100,
                          delay: float = 0.5):
        """Stream data to console output"""

        logger.info(f"Streaming {count} records to console with {delay}s delay")
        self.stats['start_time'] = time.time()

        exporter = CriminalDataExporter()

        for _ in range(count):
            if random.random() < 0.6:  # 60% crimes
                crime = CrimeFactory()
                record = exporter.to_dict_crime(crime)
                record['type'] = 'CRIME'
                self.stats['crimes_generated'] += 1
            else:
                arrest = ArrestFactory()
                record = exporter.to_dict_arrest(arrest)
                record['type'] = 'ARREST'
                self.stats['arrests_generated'] += 1

            print(json.dumps(record, default=str))
            time.sleep(delay)

        self._log_final_stats()

    def _log_final_stats(self):
        """Log final streaming statistics"""
        duration = time.time() - self.stats['start_time']
        total_records = self.stats['arrests_generated'] + self.stats['crimes_generated']

        logger.info(f"""
        Streaming Complete:
        - Total records: {total_records:,}
        - Crimes: {self.stats['crimes_generated']:,}
        - Arrests: {self.stats['arrests_generated']:,}
        - Duration: {duration:.2f} seconds
        - Rate: {total_records / duration:.1f} records/second
        """)


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Generate realistic criminal justice data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
              %(prog)s --arrests 500 --crimes 2000 --format csv
              %(prog)s --correlated 1000 --format json
              %(prog)s --stream console --count 100
              %(prog)s --stream kafka --size-mb 50
              %(prog)s --iucr-csv path/to/iucr_codes.csv --crimes 1000
        """
    )

    # Data generation options
    parser.add_argument('--arrests', type=int, default=0,
                        help='Number of arrest records to generate')
    parser.add_argument('--crimes', type=int, default=0,
                        help='Number of crime records to generate')
    parser.add_argument('--correlated', type=int, default=0,
                        help='Generate correlated crime/arrest dataset with N crimes')

    # Output options
    parser.add_argument('--format', choices=['csv', 'json'], default='csv',
                        help='Output format (default: csv)')
    parser.add_argument('--output-dir', default='output',
                        help='Output directory (default: output)')

    # IUCR codes CSV path
    parser.add_argument('--iucr-csv', type=str, default=None,
                        help='Path to IUCR codes CSV file')

    # Streaming options
    parser.add_argument('--stream', choices=['console', 'kafka'], default=None,
                        help='Stream data to specified output')
    parser.add_argument('--count', type=int, default=100,
                        help='Number of records for console streaming')
    parser.add_argument('--size-mb', type=float, default=10,
                        help='Target size in MB for kafka streaming')
    parser.add_argument('--kafka-servers', default='127.0.0.1:9092',
                        help='Kafka bootstrap servers')

    args = parser.parse_args()

    if args.stream:
        streamer = CriminalDataStreamer(iucr_csv_path=args.iucr_csv)

        if args.stream == 'console':
            streamer.stream_to_console(count=args.count)
        elif args.stream == 'kafka':
            streamer.stream_to_kafka(
                target_size_mb=args.size_mb,
                bootstrap_servers=args.kafka_servers
            )
        return

    exporter = CriminalDataExporter(output_dir=args.output_dir, iucr_csv_path=args.iucr_csv)

    if args.correlated > 0:
        logger.info(f"Generating correlated dataset with {args.correlated} crimes")
        exporter.generate_correlated_dataset(
            total_crimes=args.correlated,
            format_type=args.format
        )

    elif args.arrests > 0 or args.crimes > 0:
        arrest_count = args.arrests if args.arrests > 0 else 100
        crime_count = args.crimes if args.crimes > 0 else 100

        exporter.generate_sample_files(
            arrest_count=arrest_count,
            crime_count=crime_count,
            format_type=args.format
        )

    else:
        logger.info("No specific counts provided, generating default sample")
        exporter.generate_sample_files(
            arrest_count=50,
            crime_count=100,
            format_type=args.format
        )

    logger.info("Data generation complete!")


if __name__ == "__main__":
    main()
