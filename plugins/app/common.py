import logging
import time

logger = logging.getLogger(__name__)

def send_email(to, subject):
    logger.info(f"Sending email to {', '.join(to)}")
    time.sleep(3)  # seconds