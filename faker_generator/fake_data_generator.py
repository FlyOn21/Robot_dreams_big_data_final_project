import json
import random
import threading
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import factory
import pandas as pd
from faker import Faker
from faker.providers import BaseProvider

__all__ = [
    'ArrestFactory',
    'CrimeFactory',
    'CriminalJusticeDataGenerator',
    'DATA_GENERATOR',
    'Race',
    'ChargeType',
    'ChargeClass',
    'CrimeType',
    'LocationType'
]

fake = Faker()

# Global counters for unique IDs (thread-safe)
_crime_id_counter = threading.local()
_arrest_id_counter = threading.local()
_counter_lock = threading.Lock()


def load_iucr_codes_from_csv(
        csv_path: str = 'Chicago_Police_Department__Illinois_Uniform_Crime_Reporting_IUCR_Codes_20250928.csv') -> dict:
    """
    Load valid IUCR codes from the reference CSV file.
    Returns a dictionary mapping IUCR code to PRIMARY DESCRIPTION.
    """
    try:
        df = None

        possible_paths = [
            csv_path,
            Path(csv_path),
            Path.cwd() / csv_path,
            Path('data') / csv_path,
            Path('source_data') / csv_path,
            Path('..') / csv_path,
            Path('../data') / csv_path,
            Path('../source_data') / csv_path,
        ]

        for try_path in possible_paths:
            if Path(try_path).exists():
                print(f"Found IUCR CSV at: {try_path}")
                df = pd.read_csv(try_path)
                break

        if df is None:
            print(f"Warning: Could not find {csv_path} in any standard location")
            print(f"Searched paths: {[str(p) for p in possible_paths[:5]]}")
            return _get_fallback_iucr_mapping()

        if 'ACTIVE' in df.columns:
            active_df = df[df['ACTIVE'] == True].copy()
            print(f"Loaded {len(active_df)} active IUCR codes (filtered from {len(df)} total)")
        else:
            active_df = df.copy()
            print(f"Loaded {len(active_df)} IUCR codes (no ACTIVE column found)")

        iucr_mapping = {}
        for _, row in active_df.iterrows():
            iucr_code = str(row['IUCR']).strip()
            if iucr_code.isdigit() and len(iucr_code) <= 4:
                pass

            primary_desc = str(row['PRIMARY DESCRIPTION']).strip()
            iucr_mapping[iucr_code] = primary_desc

        print(f"Successfully created mapping with {len(iucr_mapping)} IUCR codes")
        sample_codes = list(iucr_mapping.keys())[:5]
        print(f"Sample IUCR codes: {sample_codes}")

        return iucr_mapping

    except Exception as e:
        print(f"Error loading IUCR codes: {e}")
        import traceback
        traceback.print_exc()
        print("Using fallback mapping with 23 codes")
        return _get_fallback_iucr_mapping()


def _get_fallback_iucr_mapping() -> dict:
    """Fallback IUCR mapping if CSV cannot be loaded"""
    return {
        "110": "HOMICIDE",
        "130": "HOMICIDE",
        "261": "CRIM SEXUAL ASSAULT",
        "265": "CRIM SEXUAL ASSAULT",
        "460": "BATTERY",
        "486": "BATTERY",
        "560": "ASSAULT",
        "610": "BURGLARY",
        "620": "BURGLARY",
        "810": "THEFT",
        "820": "THEFT",
        "840": "THEFT",
        "860": "THEFT",
        "890": "THEFT",
        "1110": "DECEPTIVE PRACTICE",
        "1120": "DECEPTIVE PRACTICE",
        "1400": "CRIMINAL DAMAGE",
        "1410": "CRIMINAL DAMAGE",
        "1811": "NARCOTICS",
        "1821": "NARCOTICS",
        "1840": "NARCOTICS",
        "2020": "WEAPONS VIOLATION",
        "2024": "WEAPONS VIOLATION"
    }


IUCR_CODES_MAPPING = load_iucr_codes_from_csv()


def get_next_crime_id():
    """Get next unique crime ID"""
    if not hasattr(_crime_id_counter, 'value'):
        _crime_id_counter.value = 1000000
    with _counter_lock:
        _crime_id_counter.value += 1
        return _crime_id_counter.value


def get_next_arrest_id():
    """Get next unique arrest ID (CB number)"""
    if not hasattr(_arrest_id_counter, 'value'):
        _arrest_id_counter.value = 50000000
    with _counter_lock:
        _arrest_id_counter.value += 1
        return _arrest_id_counter.value


# Enums for controlled vocabulary
class Race(Enum):
    WHITE = "WHITE"
    BLACK = "BLACK"
    HISPANIC = "HISPANIC"
    ASIAN = "ASIAN"
    AMERICAN_INDIAN = "AMERICAN INDIAN"
    OTHER = "OTHER"
    UNKNOWN = "UNKNOWN"


class ChargeType(Enum):
    FELONY = "FELONY"
    MISDEMEANOR = "MISDEMEANOR"
    VIOLATION = "VIOLATION"
    ORDINANCE = "ORDINANCE"


class ChargeClass(Enum):
    CLASS_A = "A"
    CLASS_B = "B"
    CLASS_C = "C"
    CLASS_1 = "1"
    CLASS_2 = "2"
    CLASS_3 = "3"
    CLASS_4 = "4"
    CLASS_X = "X"
    UNCLASSIFIED = "U"


class CrimeType(Enum):
    THEFT = "THEFT"
    BATTERY = "BATTERY"
    CRIMINAL_DAMAGE = "CRIMINAL DAMAGE"
    NARCOTICS = "NARCOTICS"
    ASSAULT = "ASSAULT"
    BURGLARY = "BURGLARY"
    MOTOR_VEHICLE_THEFT = "MOTOR VEHICLE THEFT"
    ROBBERY = "ROBBERY"
    CRIMINAL_TRESPASS = "CRIMINAL TRESPASS"
    DECEPTIVE_PRACTICE = "DECEPTIVE PRACTICE"
    OTHER_OFFENSE = "OTHER OFFENSE"
    WEAPONS_VIOLATION = "WEAPONS VIOLATION"
    PUBLIC_ORDER = "PUBLIC ORDER"
    HOMICIDE = "HOMICIDE"
    CRIM_SEXUAL_ASSAULT = "CRIM SEXUAL ASSAULT"


class LocationType(Enum):
    STREET = "STREET"
    RESIDENCE = "RESIDENCE"
    APARTMENT = "APARTMENT"
    PARKING_LOT = "PARKING LOT/GARAGE(NON.RESID.)"
    VEHICLE_NON_COMMERCIAL = "VEHICLE NON-COMMERCIAL"
    RETAIL_STORE = "RETAIL/STORE"
    RESTAURANT = "RESTAURANT"
    SCHOOL_COLLEGE = "SCHOOL, PUBLIC, BUILDING"
    BANK = "BANK"
    GAS_STATION = "GAS STATION"
    PARK = "PARK PROPERTY"
    HOSPITAL = "HOSPITAL BUILDING/GROUNDS"


# Data Models
@dataclass
class Charge:
    statute: str
    description: str
    charge_type: str
    charge_class: str


@dataclass
class Arrest:
    cb_no: int
    case_number: str
    arrest_date: str
    race: str
    charge_1_statute: str | None = None
    charge_1_description: str | None = None
    charge_1_type: str | None = None
    charge_1_class: str | None = None
    charge_2_statute: str | None = None
    charge_2_description: str | None = None
    charge_2_type: str | None = None
    charge_2_class: str | None = None
    charge_3_statute: str | None = None
    charge_3_description: str | None = None
    charge_3_type: str | None = None
    charge_3_class: str | None = None
    charge_4_statute: str | None = None
    charge_4_description: str | None = None
    charge_4_type: str | None = None
    charge_4_class: str | None = None
    charges_statute: str | None = None
    charges_description: str | None = None
    charges_type: str | None = None
    charges_class: str | None = None


@dataclass
class Crime:
    id: int
    case_number: str
    date: str
    block: str
    iucr: str
    primary_type: str
    description: str
    location_description: str
    arrest: bool
    domestic: bool
    beat: int
    district: int
    ward: int
    community_area: int
    fbi_code: str
    x_coordinate: int = 0
    y_coordinate: int = 0
    year: int = 0
    updated_on: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    location: str = ""


# Custom Provider for Criminal Justice Data
class CriminalJusticeProvider(BaseProvider):
    """Custom provider for criminal justice related data"""

    CHICAGO_LAT_MIN = 41.644
    CHICAGO_LAT_MAX = 42.023
    CHICAGO_LON_MIN = -87.940
    CHICAGO_LON_MAX = -87.524

    # Use loaded IUCR codes from CSV
    IUCR_MAPPING = IUCR_CODES_MAPPING

    FBI_CODE_MAPPING = {
        "01A": "HOMICIDE",
        "02": "CRIM SEXUAL ASSAULT",
        "03": "ROBBERY",
        "04A": "ASSAULT",
        "04B": "BATTERY",
        "05": "BURGLARY",
        "06": "THEFT",
        "07": "MOTOR VEHICLE THEFT",
        "08A": "ASSAULT",
        "08B": "BATTERY",
        "09": "CRIMINAL DAMAGE",
        "10": "DECEPTIVE PRACTICE",
        "11": "WEAPONS VIOLATION",
        "18": "NARCOTICS"
    }

    @staticmethod
    def case_number():
        """Generate realistic case number"""
        prefix = random.choice(['JA', 'JB', 'JC', 'JD', 'JE'])
        year = random.randint(2020, 2024)
        number = random.randint(100000, 999999)
        return f"{prefix}{year:02d}{number}"

    @staticmethod
    def cb_number():
        """Generate unique CB (Complaint Bureau) number"""
        return get_next_arrest_id()

    def iucr_code(self):
        """Get random IUCR code - ONLY from valid codes"""
        return random.choice(list(self.IUCR_MAPPING.keys()))

    def fbi_code(self):
        """Get random FBI code"""
        return random.choice(list(self.FBI_CODE_MAPPING.keys()))

    def chicago_coordinates(self):
        """Generate coordinates within Chicago bounds"""
        lat = random.uniform(self.CHICAGO_LAT_MIN, self.CHICAGO_LAT_MAX)
        lon = random.uniform(self.CHICAGO_LON_MIN, self.CHICAGO_LON_MAX)

        x = int((lon + 87.732) * 100000 + 1150000)
        y = int((lat - 41.833) * 100000 + 1900000)

        return lat, lon, x, y

    @staticmethod
    def chicago_district():
        """Generate Chicago police district (1-25)"""
        return random.randint(1, 25)

    def chicago_beat(self):
        """Generate beat number"""
        district = self.chicago_district()
        beat_suffix = random.randint(1, 99)
        return int(f"{district:02d}{beat_suffix:02d}")

    @staticmethod
    def chicago_ward():
        """Generate Chicago ward (1-50)"""
        return random.randint(1, 50)

    @staticmethod
    def community_area():
        """Generate community area (1-77)"""
        return random.randint(1, 77)

    @staticmethod
    def block_address():
        """Generate block address"""
        block_num = random.randint(0, 9900)
        block_num = (block_num // 100) * 100

        streets = [
            "N MICHIGAN AVE", "S STATE ST", "W ADAMS ST", "E RANDOLPH ST",
            "N CLARK ST", "S HALSTED ST", "W CHICAGO AVE", "E OHIO ST",
            "N BROADWAY", "S ASHLAND AVE", "W DIVISION ST", "E GRAND AVE",
            "N LINCOLN AVE", "S WESTERN AVE", "W NORTH AVE", "E SUPERIOR ST"
        ]

        street = random.choice(streets)
        return f"{block_num:04d} {street}"

    @staticmethod
    def statute_number():
        """Generate Illinois statute number"""
        chapter = random.randint(720, 750)
        section = random.randint(5, 35)
        subsection = random.randint(1, 20)
        return f"{chapter} ILCS {section}/{subsection}"


factory.Faker.add_provider(CriminalJusticeProvider)


class ChargeFactory(factory.Factory):
    """Factory for individual charges"""

    class Meta:
        model = Charge

    statute = factory.Faker('statute_number')
    charge_type = factory.Faker('random_element', elements=[t.value for t in ChargeType])
    charge_class = factory.Faker('random_element', elements=[c.value for c in ChargeClass])

    @factory.lazy_attribute
    def description(self):
        charge_descriptions = {
            ChargeType.FELONY.value: [
                "AGGRAVATED BATTERY", "ARMED ROBBERY", "BURGLARY",
                "POSSESSION OF CONTROLLED SUBSTANCE", "THEFT OVER $500",
                "AGGRAVATED ASSAULT", "CRIMINAL DAMAGE TO PROPERTY"
            ],
            ChargeType.MISDEMEANOR.value: [
                "SIMPLE BATTERY", "THEFT UNDER $500", "DISORDERLY CONDUCT",
                "CRIMINAL TRESPASS", "POSSESSION OF CANNABIS", "RETAIL THEFT",
                "SIMPLE ASSAULT"
            ],
            ChargeType.VIOLATION.value: [
                "SPEEDING", "PARKING VIOLATION", "NOISE VIOLATION",
                "LITTERING", "JAYWALKING"
            ],
            ChargeType.ORDINANCE.value: [
                "MUNICIPAL ORDINANCE VIOLATION", "ZONING VIOLATION",
                "BUSINESS LICENSE VIOLATION"
            ]
        }

        descriptions = charge_descriptions.get(self.charge_type, ["UNKNOWN CHARGE"])
        return random.choice(descriptions)


class ArrestFactory(factory.Factory):
    """Factory for arrest records"""

    class Meta:
        model = Arrest

    cb_no = factory.Faker('cb_number')
    case_number = factory.Faker('case_number')
    arrest_date = factory.Faker('date_between', start_date='-2y', end_date='-1d')
    race = factory.Faker('random_element', elements=[r.value for r in Race])

    def __init__(self):
        self.charges_class = None
        self.charges_type = None
        self.charges_description = None
        self.charges_statute = None

    @staticmethod
    def _charge_severity(charge_type):
        """Helper to determine charge severity"""
        severity_map = {
            ChargeType.FELONY.value: 4,
            ChargeType.MISDEMEANOR.value: 3,
            ChargeType.VIOLATION.value: 2,
            ChargeType.ORDINANCE.value: 1
        }
        return severity_map.get(charge_type, 0)

    @factory.post_generation
    def add_charges(self, create, extracted, **kwargs):
        """Add 1-4 charges to the arrest"""
        if not create:
            return

        num_charges = random.choices([1, 2, 3, 4], weights=[40, 35, 20, 5])[0]
        charges = [ChargeFactory() for _ in range(num_charges)]

        for i, charge in enumerate(charges, 1):
            if i <= 4:
                setattr(self, f'charge_{i}_statute', charge.statute)
                setattr(self, f'charge_{i}_description', charge.description)
                setattr(self, f'charge_{i}_type', charge.charge_type)
                setattr(self, f'charge_{i}_class', charge.charge_class)

        most_serious = max(charges, key=lambda x: ArrestFactory._charge_severity(x.charge_type))
        self.charges_statute = most_serious.statute
        self.charges_description = most_serious.description
        self.charges_type = most_serious.charge_type
        self.charges_class = most_serious.charge_class


class CrimeFactory(factory.Factory):
    """Factory for crime records"""

    class Meta:
        model = Crime

    id = factory.LazyFunction(get_next_crime_id)  # Now uses unique counter
    case_number = factory.Faker('case_number')
    date = factory.Faker('date_time_between', start_date='-1y', end_date='-1d')
    block = factory.Faker('block_address')
    iucr = factory.Faker('iucr_code')  # Now only uses valid codes
    location_description = factory.Faker('random_element', elements=[location.value for location in LocationType])
    arrest = factory.Faker('boolean', chance_of_getting_true=25)
    domestic = factory.Faker('boolean', chance_of_getting_true=15)
    beat = factory.Faker('chicago_beat')
    district = factory.Faker('chicago_district')
    ward = factory.Faker('chicago_ward')
    community_area = factory.Faker('community_area')
    fbi_code = factory.Faker('fbi_code')
    updated_on = factory.LazyAttribute(lambda obj: obj.date.strftime('%m/%d/%Y %I:%M:%S %p'))

    def __init__(self):
        self.x_coordinate = 0
        self.y_coordinate = 0
        self.latitude = 0.0
        self.longitude = 0.0
        self.location = ""

    @factory.lazy_attribute
    def primary_type(self):
        """Get primary type based on IUCR code"""
        provider = CriminalJusticeProvider(None)
        return provider.IUCR_MAPPING.get(self.iucr, "OTHER OFFENSE")

    @factory.lazy_attribute
    def description(self):
        """Generate detailed description based on primary type"""
        descriptions = {
            "THEFT": ["RETAIL THEFT", "THEFT FROM VEHICLE", "THEFT OF PROPERTY", "POCKET-PICKING", "$500 AND UNDER"],
            "BATTERY": ["SIMPLE BATTERY", "DOMESTIC BATTERY SIMPLE", "AGG DOMESTIC BATTERY", "AGGRAVATED"],
            "CRIMINAL DAMAGE": ["TO PROPERTY", "TO VEHICLE", "TO CITY OF CHICAGO PROPERTY",
                                "TO STATE SUPPORTED PROPERTY"],
            "NARCOTICS": ["POSS: CANNABIS 30GMS OR LESS", "POSS: HEROIN(WHITE)", "POSS: COCAINE", "MANU/DELIVER"],
            "ASSAULT": ["SIMPLE ASSAULT", "AGG ASSAULT", "DOMESTIC ASSAULT", "AGGRAVATED"],
            "BURGLARY": ["FORCIBLE ENTRY", "UNLAWFUL ENTRY", "ATTEMPTED FORCIBLE ENTRY"],
            "ROBBERY": ["ARMED: HANDGUN", "STRONGARM - NO WEAPON", "ARMED: OTHER FIREARM",
                        "ARMED: OTHER DANGEROUS WEAPON"],
            "HOMICIDE": ["FIRST DEGREE MURDER", "SECOND DEGREE MURDER", "INVOLUNTARY MANSLAUGHTER",
                         "RECKLESS HOMICIDE"],
            "MOTOR VEHICLE THEFT": ["AUTOMOBILE", "TRUCK, BUS, MOTOR HOME", "CYCLE, SCOOTER, BIKE WITH VIN"],
            "DECEPTIVE PRACTICE": ["FINANCIAL IDENTITY THEFT OVER $300", "CREDIT CARD FRAUD", "IMPERSONATION",
                                   "FORGERY"],
            "CRIMINAL TRESPASS": ["TO LAND", "TO RESIDENCE", "TO VEHICLE", "TO STATE SUPPORTED LAND"],
            "WEAPONS VIOLATION": ["UNLAWFUL POSS OF HANDGUN", "UNLAWFUL USE HANDGUN", "POSS FIREARM/AMMO:NO FOID CARD"],
            "CRIM SEXUAL ASSAULT": ["PREDATORY", "AGGRAVATED", "NON-AGGRAVATED"],
            "PROSTITUTION": ["CALL OPERATION", "SOLICIT FOR PROSTITUTE", "SOLICIT FOR BUSINESS"],
            "OFFENSE INVOLVING CHILDREN": ["CHILD ABUSE", "CHILD PORNOGRAPHY", "CRIMINAL SEXUAL ABUSE"],
            "SEX OFFENSE": ["NON AGGRAVATED", "AGGRAVATED SEXUAL ABUSE", "PUBLIC INDECENCY"],
            "KIDNAPPING": ["CHILD ABDUCTION/STRANGER", "AGGRAVATED", "UNLAWFUL RESTRAINT"],
            "ARSON": ["BY EXPLOSIVE", "BY FIRE", "AGGRAVATED"],
            "INTERFERENCE WITH PUBLIC OFFICER": ["OBSTRUCTING JUSTICE", "RESISTING/OBSTRUCTING A PEACE OFFICER"],
            "PUBLIC PEACE VIOLATION": ["RECKLESS CONDUCT", "BOMB THREAT", "MOB ACTION"],
            "INTIMIDATION": ["EDUCATIONAL INSTITUTION", "RESIDENTIAL", "AGGRAVATED"]
        }

        type_descriptions = descriptions.get(self.primary_type, None)

        if type_descriptions:
            return random.choice(type_descriptions)
        else:
            return f"{self.primary_type} - UNSPECIFIED"

    @factory.lazy_attribute
    def year(self):
        """Extract year from date"""
        return self.date.year

    # Generate all coordinates at once using post_generation
    @factory.post_generation
    def generate_coordinates(self, create, extracted, **kwargs):
        """Generate all coordinate fields together"""
        if not create:
            return

        provider = CriminalJusticeProvider(None)
        lat, lon, x, y = provider.chicago_coordinates()

        self.latitude = lat
        self.longitude = lon
        self.x_coordinate = x
        self.y_coordinate = y
        self.location = f"({lat:.6f}, {lon:.6f})"


class CriminalJusticeDataGenerator:
    """Main data generator for criminal justice data"""

    def __init__(self, iucr_csv_path: str = None):
        """
        Initialize data generator.

        Args:
            iucr_csv_path: Optional path to IUCR codes CSV file. If provided, will reload codes.
        """
        self.arrest_factory = ArrestFactory
        self.crime_factory = CrimeFactory

        # Reload IUCR codes if custom path provided
        if iucr_csv_path:
            global IUCR_CODES_MAPPING
            IUCR_CODES_MAPPING = load_iucr_codes_from_csv(iucr_csv_path)
            CriminalJusticeProvider.IUCR_MAPPING = IUCR_CODES_MAPPING
            print(f"Reloaded IUCR codes from {iucr_csv_path}")

    def generate_arrests(self, count: int = 100) -> list[ArrestFactory]:
        """Generate arrest records"""
        return [self.arrest_factory() for _ in range(count)]

    def generate_crimes(self, count: int = 100) -> list[CrimeFactory]:
        """Generate crime records"""
        return [self.crime_factory() for _ in range(count)]

    def generate_correlated_data(self, count: int = 100) -> dict:
        """Generate correlated crime and arrest data"""
        crimes_data: list[Any] = []
        arrests_data: list[Any] = []

        for _ in range(count):
            crime = self.crime_factory()
            crimes_data.append(crime)

            if crime.arrest:
                arrest = self.arrest_factory(
                    case_number=crime.case_number,
                    arrest_date=crime.date.strftime('%m/%d/%Y')
                )
                arrests_data.append(arrest)

        return {
            'crimes': crimes_data,
            'arrests': arrests_data
        }

    @staticmethod
    def save_to_csv(data: list, filename: str, data_type: str = 'crime'):
        """Save data to CSV file"""
        records = []
        if data_type == 'arrest':
            for arrest in data:
                record = {
                    'CB_NO': arrest.cb_no,
                    'CASE NUMBER': arrest.case_number,
                    'ARREST DATE': arrest.arrest_date.strftime('%m/%d/%Y') if hasattr(arrest.arrest_date,
                                                                                      'strftime') else arrest.arrest_date,
                    'RACE': arrest.race,
                    'CHARGE 1 STATUTE': arrest.charge_1_statute,
                    'CHARGE 1 DESCRIPTION': arrest.charge_1_description,
                    'CHARGE 1 TYPE': arrest.charge_1_type,
                    'CHARGE 1 CLASS': arrest.charge_1_class,
                    'CHARGE 2 STATUTE': arrest.charge_2_statute,
                    'CHARGE 2 DESCRIPTION': arrest.charge_2_description,
                    'CHARGE 2 TYPE': arrest.charge_2_type,
                    'CHARGE 2 CLASS': arrest.charge_2_class,
                    'CHARGE 3 STATUTE': arrest.charge_3_statute,
                    'CHARGE 3 DESCRIPTION': arrest.charge_3_description,
                    'CHARGE 3 TYPE': arrest.charge_3_type,
                    'CHARGE 3 CLASS': arrest.charge_3_class,
                    'CHARGE 4 STATUTE': arrest.charge_4_statute,
                    'CHARGE 4 DESCRIPTION': arrest.charge_4_description,
                    'CHARGE 4 TYPE': arrest.charge_4_type,
                    'CHARGE 4 CLASS': arrest.charge_4_class,
                    'CHARGES STATUTE': arrest.charges_statute,
                    'CHARGES DESCRIPTION': arrest.charges_description,
                    'CHARGES TYPE': arrest.charges_type,
                    'CHARGES CLASS': arrest.charges_class
                }
                records.append(record)
        else:
            for crime in data:
                record = {
                    'ID': crime.id,
                    'Case Number': crime.case_number,
                    'Date': crime.date.strftime('%m/%d/%Y %I:%M:%S %p'),
                    'Block': crime.block,
                    'IUCR': crime.iucr,
                    'Primary Type': crime.primary_type,
                    'Description': crime.description,
                    'Location Description': crime.location_description,
                    'Arrest': crime.arrest,
                    'Domestic': crime.domestic,
                    'Beat': crime.beat,
                    'District': crime.district,
                    'Ward': crime.ward,
                    'Community Area': crime.community_area,
                    'FBI Code': crime.fbi_code,
                    'X Coordinate': crime.x_coordinate,
                    'Y Coordinate': crime.y_coordinate,
                    'Year': crime.year,
                    'Updated On': crime.updated_on,
                    'Latitude': crime.latitude,
                    'Longitude': crime.longitude,
                    'Location': crime.location
                }
                records.append(record)

        df = pd.DataFrame(records)
        df.to_csv(filename, index=False)
        print(f"Saved {len(records)} {data_type} records to {filename}")


if __name__ == "__main__":
    generator = CriminalJusticeDataGenerator()

    print("Generating 50 arrest records...")
    arrests = generator.generate_arrests(50)

    print("Generating 100 crime records...")
    crimes = generator.generate_crimes(100)

    print("Generating correlated data...")
    correlated = generator.generate_correlated_data(75)

    try:
        generator.save_to_csv(arrests, 'fake_arrests.csv', 'arrest')
        generator.save_to_csv(crimes, 'fake_crimes.csv', 'crime')

        generator.save_to_csv(correlated['arrests'], 'fake_arrests_correlated.csv', 'arrest')
        generator.save_to_csv(correlated['crimes'], 'fake_crimes_correlated.csv', 'crime')

    except ImportError:
        print("pandas not available, saving as JSON instead")

        with open('fake_arrests.json', 'w') as f:
            json.dump([arrest.__dict__ for arrest in arrests], f, indent=2, default=str)

        with open('fake_crimes.json', 'w') as f:
            json.dump([crime.__dict__ for crime in crimes], f, indent=2, default=str)

    print("Data generation complete!")

DATA_GENERATOR = CriminalJusticeDataGenerator()
