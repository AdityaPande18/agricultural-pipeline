import logging
import numpy as np
import pandas as pd


class TransformationHandler:
    def __init__(self):
        """
        Initialize the TransformationHandler with calibration parameters and expected ranges.
        - calibration_params: dict containing multipliers and offsets for each reading type
        - expected_ranges: dict containing expected value ranges for each reading type
        """
        self.calibration_params = {
            "temperature": {"multiplier": 1.1, "offset": 0.5},
            "humidity": {"multiplier": 1.0, "offset": 0.0},
            "soil_moisture": {"multiplier": 1.2, "offset": -1.0}
        }

        self.expected_ranges = {
            "temperature": (0, 50),
            "humidity": (10, 100),
            "soil_moisture": (0, 60),
            "light_intensity": (0, 1000)
        }

    def clean_data(self, df: pd.DataFrame):
        """
        Clean the DataFrame by removing duplicates and handling missing values.
        - Remove duplicates
        - Drop rows with null sensor_id or timestamp
        - Fill missing values in 'value' with mean and 'battery_level' with -1
        """
        df = df.drop_duplicates()

        df = df.dropna(subset=["sensor_id", "timestamp"])

        df["value"] = df["value"].fillna(df["value"].mean())
        df["battery_level"] = df["battery_level"].fillna(-1)

        return df

    def detect_outliers(self, df: pd.DataFrame):
        """
        Detect outliers using Z-score method.
        - Calculate Z-scores for 'value' column
        - Remove rows where Z-score is greater than 3
        """
        z_scores = np.abs((df["value"] - df["value"].mean()) / df["value"].std())
        return df[z_scores <= 3]

    def _normalize(self, row):
        reading = row["reading_type"]
        params = self.calibration_params.get(reading, {"multiplier": 1.0, "offset": 0.0})
        return row["value"] * params["multiplier"] + params["offset"]

    def normalize_values(self, df: pd.DataFrame):
        df["normalized_value"] = df.apply(self._normalize, axis=1)
        return df

    def add_derived_fields(self, df: pd.DataFrame):
        """
        Add derived fields:
        - Extract date from timestamp
        - Calculate daily average for each sensor and reading type
        - Calculate 7-day rolling average for each sensor and reading type
        - Identify anomalous readings based on expected ranges
        """
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["date"] = df["timestamp"].dt.date

        df["daily_avg"] = df.groupby(["sensor_id", "reading_type", "date"])["normalized_value"].transform("mean")

        df = df.sort_values(by=["sensor_id", "timestamp"])
        
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values(by=["sensor_id", "reading_type", "timestamp"])

        # Compute rolling average per sensor and reading_type
        rolling_avg = (
            df.groupby(["sensor_id", "reading_type"], group_keys=False)
            .apply(lambda group: group.set_index("timestamp")
                                    .rolling("7D")["normalized_value"]
                                    .mean()
                                    .reset_index(drop=True))
            .reset_index(drop=True)
        )

        df["rolling_7d_avg"] = rolling_avg

        def is_anomalous(row):
            low, high = self.expected_ranges.get(row["reading_type"], (-float('inf'), float('inf')))
            return not (low <= row["value"] <= high)
        df["anomalous_reading"] = df.apply(is_anomalous, axis=1)

        return df

    def adjust_timestamp(self, df: pd.DataFrame):
        """
        Adjust timestamp to a specific timezone (e.g., Asia/Kolkata).
        - Convert timestamp to UTC and then to Asia/Kolkata timezone
        - Format timestamp to ISO 8601 string
        """
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        return df

    def transform(self, df: pd.DataFrame):
        logging.info("Running Data Transformation")

        df = self.clean_data(df)
        df = self.detect_outliers(df)
        df = self.normalize_values(df)
        df = self.add_derived_fields(df)
        df = self.adjust_timestamp(df)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        logging.info("Data Transformation Completed")
        return df
