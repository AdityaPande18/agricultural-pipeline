import logging
import os

import pandas as pd

from src.checkpoint_utils import CheckpointUtils
from src.constants import COMPRESSION_ALGO, PROCESSED_DATA_PATH


class DataLoader:

    def save_parquet_partitioned(self, df: pd.DataFrame, processed_files):
        logging.info("Running Data loading and saving")
        if df.empty:
            raise ValueError("DataFrame is empty. Cannot write output.")

        if "date" not in df.columns:
            df["date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)

        os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

        df.to_parquet(
            PROCESSED_DATA_PATH,
            engine="pyarrow",
            index=False,
            partition_cols=["date", "sensor_id"],
            compression=COMPRESSION_ALGO
        )

        logging.info(f"Transformed data saved to {PROCESSED_DATA_PATH} (partitioned by date & sensor_id)")

        CheckpointUtils.update_checkpoint(processed_files)

        logging.info("Checkpoint saved")
