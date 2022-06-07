import time
import logging

logger = logging.getLogger(__name__)


def load_market_data(data_path):
    logger.info("Loading data from upstream")
    time.sleep(5)  # seconds


def merge_data(out_data_path, data_types):
    logger.info(f"Merging data - {', '.join(data_types)}")
    time.sleep(10)  # seconds
    logger.info(f"Saving data to {out_data_path}")
    time.sleep(2)  # seconds
