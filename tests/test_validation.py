import pandas as pd
import pytest
from src.validation import ValidationHandler
from datetime import datetime, timedelta

@pytest.fixture
def sample_df():
    now = pd.Timestamp.now().floor('h')
    return pd.DataFrame({
        "sensor_id": ["s1", "s1", "s2", "s2"],
        "timestamp": [now - timedelta(hours=3), now - timedelta(hours=2), now - timedelta(hours=1), now],
        "reading_type": ["temperature", "temperature", "humidity", "humidity"],
        "value": [25.0, 55.0, 30.0, 110.0],
        "battery_level": [90.0, 85.0, 80.0, 75.0],
        "anomalous_reading": [False, True, False, True]
    })

def test_validate_schema_and_types_pass(sample_df):
    handler = ValidationHandler()
    result = handler.validate_schema_and_types(sample_df)
    
    assert result["missing_summary"]["missing_sensor_id"].iloc[0] == 0
    assert not result["type_errors"], f"Unexpected type errors: {result['type_errors']}"

def test_validate_schema_and_types_missing_column():
    handler = ValidationHandler()

    data = {
        "sensor_id": ["s1"],
        "timestamp": ["2025-08-01T00:00:00"],
        "reading_type": ["temperature"],
        "value": [25.5]
        # Missing "battery_level"
    }
    df = pd.DataFrame(data)

    result = handler.validate_schema_and_types(df)

    assert any("battery_level" in err for err in result["type_errors"]), result["type_errors"]

def test_check_value_ranges(sample_df):
    handler = ValidationHandler()
    result = handler.check_value_ranges(sample_df)

    temp_outliers = result[result["reading_type"] == "temperature"]["out_of_range"].iloc[0]
    humidity_outliers = result[result["reading_type"] == "humidity"]["out_of_range"].iloc[0]

    assert temp_outliers == 1
    assert humidity_outliers == 1

def test_detect_time_gaps(sample_df):
    handler = ValidationHandler()
    result = handler.detect_time_gaps(sample_df)

    assert not result.empty
    assert set(result.columns) >= {"sensor_id", "observed_hours", "expected_hours", "percent_missing_hours"}
    assert result[result["sensor_id"] == "s1"]["expected_hours"].iloc[0] >= 2

def test_profile_anomalies(sample_df):
    handler = ValidationHandler()
    result = handler.profile_anomalies(sample_df)

    temp_anomalies = result[result["reading_type"] == "temperature"]["anomalies"].iloc[0]
    humidity_anomalies = result[result["reading_type"] == "humidity"]["anomalies"].iloc[0]

    assert temp_anomalies == 1
    assert humidity_anomalies == 1
    assert "percent_anomalous" in result.columns

def test_run_validations_creates_report(tmp_path, sample_df):
    output_file = tmp_path / "report.csv"
    handler = ValidationHandler()
    handler.run_validations(sample_df, output_path=str(output_file))

    content = output_file.read_text()
    assert "=== SCHEMA VALIDATION: MISSING VALUES ===" in content
    assert "=== VALUE RANGE CHECKS ===" in content
    assert "=== TIME GAPS ===" in content
    assert "=== ANOMALY PROFILE ===" in content
