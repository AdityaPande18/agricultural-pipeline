import json
import os
from datetime import datetime

from src.constants import CHECKPOINT_FILE


class CheckpointUtils:
    @staticmethod
    def load_checkpoint():
        """
        Loads checkpoint data
        If not found then loads empty dict
        """
        try:
            if os.path.exists(CHECKPOINT_FILE):
                with open(CHECKPOINT_FILE, "r") as file:
                    return json.load(file)
        except json.JSONDecodeError as e:
            pass
        except Exception as e:
            pass
        
        return {}

    @staticmethod
    def save_checkpoint(checkpoint_data):
        with open(CHECKPOINT_FILE, "w") as file:
            json.dump(checkpoint_data, file, indent=4)

    @staticmethod
    def get_latest_processed_date(checkpoint_data):
        if not checkpoint_data:
            return None
        latest_date = max(checkpoint_data.keys(), key=lambda d: datetime.strptime(d, "%Y-%m-%d"))
        return datetime.strptime(latest_date, "%Y-%m-%d") if latest_date else None

    @staticmethod
    def update_checkpoint(processed_files: dict):
        """
        Merge newly processed files into existing checkpoint.
        """
        checkpoint = CheckpointUtils.load_checkpoint()

        for date, files in processed_files.items():
            if date in checkpoint:
                existing_files = set(checkpoint[date])
                checkpoint[date] = list(existing_files.union(files))
            else:
                checkpoint[date] = files

        CheckpointUtils.save_checkpoint(checkpoint)
