import logging

import duckdb
import pandas as pd


class ValidationHandler:
    def __init__(self):
        self.expected_ranges = {
            "temperature": (0, 50),
            "humidity": (10, 100),
            "soil_moisture": (0, 60),
            "light_intensity": (0, 1000)
        }

    def validate_schema_and_types(self, df: pd.DataFrame):
        """
        Validate the schema and data types of the DataFrame against the expected schema.
        - Check for expected columns
        - Check for expected data types
        - Check for missing values
        """
        expected_schema = {
            "sensor_id": "VARCHAR",
            "timestamp": "TIMESTAMP",
            "reading_type": "VARCHAR",
            "value": "DOUBLE",
            "battery_level": "DOUBLE"
        }

        con = duckdb.connect()
        con.register("df", df)

        errors = []

        try:
            missing_summary = con.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) - COUNT(sensor_id) AS missing_sensor_id,
                    COUNT(*) - COUNT(timestamp) AS missing_timestamp,
                    COUNT(*) - COUNT(reading_type) AS missing_reading_type,
                    COUNT(*) - COUNT(value) AS missing_value,
                    COUNT(*) - COUNT(battery_level) AS missing_battery_level
                FROM df
            """).fetchdf()

            dtype_info = con.execute("DESCRIBE df").fetchdf()

            for column, expected_type in expected_schema.items():
                match = dtype_info[dtype_info['column_name'] == column]
                if match.empty:
                    errors.append(f"Missing column: {column}")
                else:
                    actual_type = match.iloc[0]['column_type'].upper()
                    if expected_type not in actual_type:
                        errors.append(f"Column '{column}' has type '{actual_type}', expected '{expected_type}'")

            con.unregister("df")

            return {
                "missing_summary": missing_summary,
                "type_errors": errors
            }

        except Exception as e:
            logging.error(f"Schema validation failed: {e}")
            return {
                "missing_summary": pd.DataFrame(),
                "type_errors": [str(e)]
            }

    def check_value_ranges(self, df: pd.DataFrame):
        """
        Check if values in the DataFrame fall within expected ranges for each reading type.
        - For each reading type, count total values and out-of-range values
        """
        con = duckdb.connect()
        con.register("df", df)

        checks = []
        for reading_type, (low, high) in self.expected_ranges.items():
            q = f"""
                SELECT
                    '{reading_type}' AS reading_type,
                    COUNT(*) AS total,
                    SUM(CASE WHEN value < {low} OR value > {high} THEN 1 ELSE 0 END) AS out_of_range
                FROM df
                WHERE reading_type = '{reading_type}'
            """
            result = con.execute(q).fetchdf()
            checks.append(result)

        con.unregister("df")
        return pd.concat(checks, ignore_index=True)

    def detect_time_gaps(self, df: pd.DataFrame):
        """
        Detect time gaps in the data for each sensor.
        - Calculate the number of distinct hours observed
        - Calculate the expected number of hours based on the time range
        - Calculate the percentage of missing hours
        """
        con = duckdb.connect()
        con.register("df", df)

        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("h")
            con.register("df", df)

            result = con.execute("""
                SELECT
                    sensor_id,
                    COUNT(DISTINCT timestamp) AS observed_hours,
                    DATE_DIFF('hour', MIN(timestamp), MAX(timestamp)) + 1 AS expected_hours,
                    ROUND(100.0 * (DATE_DIFF('hour', MIN(timestamp), MAX(timestamp)) + 1 - COUNT(DISTINCT timestamp)) 
                        / (DATE_DIFF('hour', MIN(timestamp), MAX(timestamp)) + 1), 2) AS percent_missing_hours
                FROM df
                GROUP BY sensor_id
            """).fetchdf()

            con.unregister("df")
            return result
        except Exception as e:
            logging.error(f"Time gap detection failed: {e}")
            return pd.DataFrame()

    def profile_anomalies(self, df: pd.DataFrame):
        """
        Profile anomalies in the data.
        - Count total readings and anomalous readings for each reading type
        - Calculate the percentage of anomalous readings
        """
        con = duckdb.connect()
        con.register("df", df)

        result = con.execute("""
            SELECT
                reading_type,
                COUNT(*) AS total_readings,
                SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) AS anomalies,
                ROUND(100.0 * SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) / COUNT(*), 2) AS percent_anomalous
            FROM df
            GROUP BY reading_type
        """).fetchdf()

        con.unregister("df")
        return result

    def run_validations(self, df: pd.DataFrame, output_path="data_quality_report.csv"):
        logging.info("Running data quality validations")

        schema_check = self.validate_schema_and_types(df)
        range_check = self.check_value_ranges(df)
        time_gaps = self.detect_time_gaps(df)
        anomaly_profile = self.profile_anomalies(df)

        with open(output_path, "w") as f:
            f.write("=== SCHEMA VALIDATION: MISSING VALUES ===\n")
            if not schema_check["missing_summary"].empty:
                schema_check["missing_summary"].to_csv(f, index=False)
            else:
                f.write("Failed to compute missing summary.\n")

            f.write("\n\n=== SCHEMA VALIDATION: TYPE CHECKS ===\n")
            if schema_check["type_errors"]:
                for error in schema_check["type_errors"]:
                    f.write(f"{error}\n")
            else:
                f.write("All column types match expected schema.\n")

            f.write("\n\n=== VALUE RANGE CHECKS ===\n")
            range_check.to_csv(f, index=False)

            f.write("\n\n=== TIME GAPS ===\n")
            time_gaps.to_csv(f, index=False)

            f.write("\n\n=== ANOMALY PROFILE ===\n")
            anomaly_profile.to_csv(f, index=False)

        logging.info(f"Data quality report saved to: {output_path}")
