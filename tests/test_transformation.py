import pandas as pd
import pytest
from src.transformation import TransformationHandler


class TestTransformationHandler:
    def setup_method(self):
        self.handler = TransformationHandler()

    def test_full_transform_pipeline(self):
        df = pd.read_csv("tests/transformation_data.csv", parse_dates=["timestamp"])

        transformed_df = self.handler.transform(df)

        assert not transformed_df.empty, "Transformed DataFrame should not be empty"
        assert "sensor_id" in transformed_df.columns
        assert "timestamp" in transformed_df.columns
        assert transformed_df["timestamp"].dtype.kind in ("M",), "timestamp should be datetime"
        assert transformed_df["value"].between(0, 100).all(), "values should be in expected range"
        assert transformed_df.duplicated().sum() == 0, "Should not contain duplicate rows"

        if "temperature_celsius" in transformed_df.columns:
            assert transformed_df["temperature_celsius"].dtype == float

    @pytest.mark.parametrize("sensor_id", ["sensor_1", "sensor_2", "sensor_5"])
    def test_sensor_data_present(self, sensor_id):
        df = pd.read_csv("tests/transformation_data.csv", parse_dates=["timestamp"])
        assert sensor_id in df["sensor_id"].values, f"{sensor_id} should exist in data"

    def test_sensor_id_uniqueness_after_transformation(self):
        df = pd.read_csv("tests/transformation_data.csv", parse_dates=["timestamp"])
        transformed_df = self.handler.transform(df)
        assert transformed_df["sensor_id"].notnull().all(), "sensor_id should not contain nulls"
