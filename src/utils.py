import json
import logging
import os
from datetime import datetime
import pprint

from src.constants import CHECKPOINT_FILE


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as file:
            return json.load(file)
    return {}


def save_checkpoint(checkpoint_data):
    with open(CHECKPOINT_FILE, "w") as file:
        json.dump(checkpoint_data, file, indent=4)


def get_latest_processed_date(checkpoint_data):
    if not checkpoint_data:
        return None
    return max(checkpoint_data.keys(), key=lambda d: datetime.strptime(d, "%Y-%m-%d"))


def log_pretty(data, indent=4, width=80):
    pp = pprint.PrettyPrinter(indent=indent, width=width, compact=False)
    logging.info(pp.pformat(data))
