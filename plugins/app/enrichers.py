import logging
import time
import random

logger = logging.getLogger(__name__)

def enrich_data(data_type):
    logger.info(f"Enriching {data_type} data")
    time.sleep(random.randrange(3, 10))  # seconds