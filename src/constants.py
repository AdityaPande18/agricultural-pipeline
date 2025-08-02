RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"
CHECKPOINT_FILE = "ingestion_checkpoint.json"
COMPRESSION_ALGO = "snappy"

EXPECTED_SCHEMA = {
    "sensor_id": "object",
    "timestamp": "datetime64[ns]",
    "reading_type": "object",
    "value": "float64",
    "battery_level": "float64"
}
