from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "yellow_tripdata_2026-02.parquet"
)
ANALYSIS_START_DATE = "2026-02-01"
ANALYSIS_END_DATE = "2026-03-01"
HOURLY_OUTPUT_PATH = PROJECT_ROOT / "output" / "hourly_summary"
DAILY_OUTPUT_PATH = PROJECT_ROOT / "output" / "daily_summary"
QUALITY_OUTPUT_PATH = PROJECT_ROOT / "output" / "quality_summary"

APP_NAME = "NYC Taxi DataFrame DAG Analysis"
MASTER_URL = "local[*]"

MAX_TRIP_DISTANCE_MILES = 100.0
MAX_TRIP_DURATION_MINUTES = 300.0

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

OUTPUT_FORMAT = "parquet"
OUTPUT_MODE = "overwrite"