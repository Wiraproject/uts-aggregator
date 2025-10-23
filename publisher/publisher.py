import os
import time
import random
import requests
import logging
from datetime import datetime
from typing import List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('publisher')

AGGREGATOR_URL = os.environ.get('AGGREGATOR_URL', 'http://aggregator:8080')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '100'))
TOTAL_EVENTS = int(os.environ.get('TOTAL_EVENTS', '1000'))
DUPLICATE_RATE = float(os.environ.get('DUPLICATE_RATE', '0.2'))
INTERVAL = float(os.environ.get('INTERVAL', '5.0'))

def create_event(event_id: int, topic: str = "demo") -> Dict:
    return {
        "topic": topic,
        "event_id": f"evt-{event_id}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "publisher-service",
        "payload": {
            "index": event_id,
            "producer_id": "pub-1",
            "data": f"sample-data-{event_id}"
        }
    }

def generate_events_with_duplicates(total: int, dup_rate: float) -> List[Dict]:
    unique_count = int(total * (1 - dup_rate))
    duplicate_count = total - unique_count
    events = []
    
    logger.info(f"Generating {total} events ({unique_count} unique, {duplicate_count} duplicates)")
    
    for i in range(unique_count):
        events.append(create_event(i))
    
    for _ in range(duplicate_count):
        random_idx = random.randint(0, unique_count - 1)
        events.append(create_event(random_idx))
    
    random.shuffle(events)
    
    return events

def send_batch(events: List[Dict]) -> bool:
    try:
        response = requests.post(
            f"{AGGREGATOR_URL}/publish",
            json=events,
            timeout=10
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Sent {len(events)} events. Response: {result}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send batch: {e}")
        return False

def check_aggregator_health() -> bool:
    try:
        response = requests.get(f"{AGGREGATOR_URL}/stats", timeout=5)
        response.raise_for_status()
        stats = response.json()
        logger.info(f"Aggregator stats: {stats}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Aggregator not ready: {e}")
        return False

def wait_for_aggregator(max_retries: int = 30, delay: int = 2):
    logger.info("Waiting for aggregator to be ready...")
    for i in range(max_retries):
        if check_aggregator_health():
            logger.info("Aggregator is ready!")
            return True
        logger.info(f"Retry {i+1}/{max_retries}...")
        time.sleep(delay)
    
    logger.error("Aggregator failed to start")
    return False

def run_continuous_mode():
    logger.info(f"Starting continuous publishing mode")
    logger.info(f"Config: TOTAL_EVENTS={TOTAL_EVENTS}, DUP_RATE={DUPLICATE_RATE}, INTERVAL={INTERVAL}s")
    
    cycle = 0
    while True:
        cycle += 1
        logger.info(f"=== Publishing Cycle {cycle} ===")
        
        events = generate_events_with_duplicates(TOTAL_EVENTS, DUPLICATE_RATE)
        
        total_sent = 0
        for i in range(0, len(events), BATCH_SIZE):
            batch = events[i:i + BATCH_SIZE]
            if send_batch(batch):
                total_sent += len(batch)
            time.sleep(0.5) 
        
        logger.info(f"Cycle {cycle} complete. Sent {total_sent}/{len(events)} events")
        
        check_aggregator_health()
        
        logger.info(f"Waiting {INTERVAL}s before next cycle...")
        time.sleep(INTERVAL)

def run_one_shot_mode():
    logger.info("Starting one-shot publishing mode")
    logger.info(f"Config: TOTAL_EVENTS={TOTAL_EVENTS}, DUP_RATE={DUPLICATE_RATE}")
    
    events = generate_events_with_duplicates(TOTAL_EVENTS, DUPLICATE_RATE)
    
    total_sent = 0
    failed = 0
    
    for i in range(0, len(events), BATCH_SIZE):
        batch = events[i:i + BATCH_SIZE]
        if send_batch(batch):
            total_sent += len(batch)
        else:
            failed += len(batch)
        time.sleep(0.5)
    
    logger.info(f"Publishing complete. Sent: {total_sent}, Failed: {failed}")
    
    time.sleep(2)
    check_aggregator_health()

def main():
    logger.info("Publisher service starting...")
    logger.info(f"Aggregator URL: {AGGREGATOR_URL}")
    
    if not wait_for_aggregator():
        logger.error("Cannot connect to aggregator. Exiting.")
        return 1
    
    mode = os.environ.get('MODE', 'continuous').lower()
    
    if mode == 'one-shot':
        run_one_shot_mode()
    else:
        run_continuous_mode()
    
    return 0

if __name__ == "__main__":
    exit(main())