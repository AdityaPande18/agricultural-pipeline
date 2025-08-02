import logging
import os
from collections import defaultdict
from datetime import datetime

import duckdb
import pandas as pd

from src.checkpoint_utils import CheckpointUtils
from src.constants import EXPECTED_SCHEMA, RAW_DATA_PATH
from src.utils import log_pretty


class IngestionHandler:

    def extract_date_from_filename(self, file_path):
        """
        Extract date from filename assuming the format is like "2023-07-30.parquet".
        """
        file = file_path.split("/")[-1].split(".")[0]
        date = os.path.splitext(os.path.basename(file))[0]

        try:
            date = datetime.strptime(date, "%Y-%m-%d")
        except (ValueError, TypeError):
            logging.error(f"Invalid date format in filename: {file_path}")
            date = None

        return date

    def list_parquet_files(self, data_path=RAW_DATA_PATH):
        all_files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith(".parquet")]

        checkpoint = CheckpointUtils.load_checkpoint()
        latest_date = CheckpointUtils.get_latest_processed_date(checkpoint)

        unprocessed = []
        for file in all_files:
            if self.validate_file_path(file):
                file_date = self.extract_date_from_filename(file)
                if not latest_date or file_date > latest_date:
                    unprocessed.append(file)

        return sorted(unprocessed)

    def read_parquet_with_duckdb(self, file_path):
        try:
            con = duckdb.connect()
            df = con.execute(f"SELECT * FROM read_parquet('{file_path}')").fetch_df()

            return df
        except Exception as e:
            logging.error(f"Failed to read file: {file_path}, Error: {e}")
            return pd.DataFrame()

    def validate_file_path(self, file_path):
        if not file_path:
            logging.error("File path is empty.")
            return False
        if not os.path.exists(file_path):
            logging.error(f"File does not exist: {file_path}")
            return False
        if not file_path.endswith(".parquet"):
            logging.error(f"File is not a Parquet file: {file_path}")
            return False
        
        if not self.extract_date_from_filename(file_path):
            logging.error(f"Could not extract date from filename: {file_path}")
            return False

        return True

    def inspect_file_schema(self, file_path):
        if not self.validate_file_path(file_path):
            return None

        try:
            con = duckdb.connect()
            schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").fetchall()
            logging.info(f"File Inspection for {file_path}:")
            log_pretty(schema)
            return schema
        except Exception as e:
            logging.error(f"Failed to inspect file schema: {file_path}, Error: {e}")
            return None

    def validate_parquet_file(self, df):
        if not isinstance(df, pd.DataFrame):
            logging.error("Data is not a valid DataFrame.")
            return False

        if df.empty:
            logging.warning("DataFrame is empty.")
            return False

        for col in EXPECTED_SCHEMA.keys():
            if col not in df.columns:
                logging.error(f"Missing expected column: {col}")
                return False

        for col, expected_dtype in EXPECTED_SCHEMA.items():
            actual_dtype = str(df[col].dtype)
            if expected_dtype == "datetime64[ns]":
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    logging.error(f"Column '{col}' is not datetime. Found: {actual_dtype}")
                    return False
            elif actual_dtype != expected_dtype:
                logging.error(f"Column '{col}' has incorrect dtype. Expected: {expected_dtype}, Found: {actual_dtype}")
                return False

        logging.info("Schema validation passed.")
        return True

    def load_files(self, file_path=None):
        if file_path:
            files = [file_path]
        else:
            files = self.list_parquet_files()

        logging.info(f"Files to ingest: {files}")

        total_files = len(files)
        successful_files = 0
        skipped_files = 0
        total_records = 0
        skipped_records = 0
        all_dfs = []
        processed_files = defaultdict(list)

        for file_path in files:
            df = self.read_parquet_with_duckdb(file_path)
            if self.validate_parquet_file(df):
                logging.info(f"Valid file: {file_path} with {len(df)} records")
                all_dfs.append(df)
                total_records += len(df)
                successful_files += 1

                file_date = self.extract_date_from_filename(file_path)
                processed_files[file_date.strftime("%Y-%m-%d")].append(file_path)
            else:
                logging.warning(f"Skipped file: {file_path}")
                skipped_files += 1
                skipped_records += len(df)

        if not all_dfs:
            logging.warning("No valid data found to ingest.")
            return pd.DataFrame(), {}

        combined_df = pd.concat(all_dfs, ignore_index=True)

        logging.info("Ingestion Summary:")
        logging.info(f"     - Total files found: {total_files}")
        logging.info(f"     - Files successfully processed: {successful_files}")
        logging.info(f"     - Files skipped/failed: {skipped_files}")
        logging.info(f"     - Total records ingested: {total_records}")
        logging.info(f"     - Skipped/invalid records: {skipped_records}")

        return combined_df, processed_files
