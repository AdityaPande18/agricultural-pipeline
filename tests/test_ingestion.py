import os
import tempfile
import pandas as pd
import pytest
from datetime import datetime
from src.ingestion import IngestionHandler


data = pd.DataFrame({
    "sensor_id": ["A1", "A2"],
    "timestamp": ["2023-07-30 12:00:00", "2023-07-30 13:00:00"],
    "reading_type": ["temperature", "humidity"],
    "value": [25.4, 60.2],
    "battery_level": [98.5, 77.0]
})
data["timestamp"] = pd.to_datetime(data["timestamp"])


@pytest.fixture
def sample_parquet_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "2023-07-30.parquet")
        data.to_parquet(file_path, index=False)
        yield file_path


def test_extract_date_from_filename():
    handler = IngestionHandler()
    path = "/path/to/2023-07-30.parquet"
    assert handler.extract_date_from_filename(path) == datetime(2023, 7, 30)


def test_validate_file_path_invalid():
    handler = IngestionHandler()
    assert not handler.validate_file_path("")
    assert not handler.validate_file_path("invalid.txt")


def test_validate_file_path_valid(sample_parquet_file):
    handler = IngestionHandler()
    assert handler.validate_file_path(sample_parquet_file)


def test_read_parquet_with_duckdb(sample_parquet_file):
    handler = IngestionHandler()
    df = handler.read_parquet_with_duckdb(sample_parquet_file)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.shape == data.shape

def test_inspect_file_schema(tmp_path):
    handler = IngestionHandler()
    df = pd.DataFrame({
        "sensor_id": ["s1"],
        "timestamp": [pd.Timestamp("2023-01-01")],
        "reading_type": ["temp"],
        "value": [10.5],
        "battery_level": [80.0]
    })
    file = tmp_path / "2023-01-01.parquet"
    df.to_parquet(file)

    result = handler.inspect_file_schema(str(file))

    expected_schema = [
        ("sensor_id", "VARCHAR", "YES", None, None, None),
        ("timestamp", "TIMESTAMP_NS", "YES", None, None, None),
        ("reading_type", "VARCHAR", "YES", None, None, None),
        ("value", "DOUBLE", "YES", None, None, None),
        ("battery_level", "DOUBLE", "YES", None, None, None),
    ]

    assert result == expected_schema


def test_validate_parquet_file_valid():
    handler = IngestionHandler()
    assert handler.validate_parquet_file(data)


def test_validate_parquet_file_missing_column():
    handler = IngestionHandler()
    df = data.drop(columns=["battery_level"])
    assert not handler.validate_parquet_file(df)


def test_load_files_with_valid_input(sample_parquet_file, monkeypatch):
    handler = IngestionHandler()

    monkeypatch.setattr(handler, "list_parquet_files", lambda: [sample_parquet_file])
    df, files = handler.load_files()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert files  # non-empty dict


def test_load_files_empty(monkeypatch):
    handler = IngestionHandler()
    monkeypatch.setattr(handler, "list_parquet_files", lambda: [])
    df, files = handler.load_files()
    assert df.empty
    assert files == {}
