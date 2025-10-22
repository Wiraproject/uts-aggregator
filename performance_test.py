import requests
import time
import json
import argparse
from datetime import datetime
from typing import List, Dict

GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
BLUE = '\033[94m'
BOLD = '\033[1m'
RESET = '\033[0m'

BASE_URL = "http://localhost:8080"

def create_event(event_id: int, topic: str = "perf-test") -> Dict:
    return {
        "topic": topic,
        "event_id": f"evt-{event_id}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "performance-test",
        "payload": {
            "index": event_id,
            "data": f"test-data-{event_id}"
        }
    }

def generate_events(total: int, dup_rate: float) -> List[Dict]:
    unique_count = int(total * (1 - dup_rate))
    events = []
    
    print(f"{BLUE}Generating {total} events ({unique_count} unique, {total - unique_count} duplicates)...{RESET}")

    for i in range(unique_count):
        events.append(create_event(i))

    for i in range(total - unique_count):
        idx = i % unique_count
        events.append(create_event(idx)) 
    
    return events

def send_batch(events: List[Dict], batch_size: int = 1000) -> Dict:
    total_events = len(events)
    batches = [events[i:i + batch_size] for i in range(0, total_events, batch_size)]
    
    print(f"{BLUE}Sending {total_events} events in {len(batches)} batches of {batch_size}...{RESET}")
    
    start_time = time.time()
    sent_count = 0
    
    for idx, batch in enumerate(batches, 1):
        batch_start = time.time()
        
        try:
            response = requests.post(
                f"{BASE_URL}/publish",
                json=batch,
                timeout=30
            )
            response.raise_for_status()
            sent_count += len(batch)
            
            batch_elapsed = time.time() - batch_start
            print(f"  Batch {idx}/{len(batches)}: {len(batch)} events in {batch_elapsed:.2f}s", end='\r')
            
        except requests.exceptions.RequestException as e:
            print(f"{RED}Error sending batch {idx}: {e}{RESET}")
            return None
    
    print()
    total_elapsed = time.time() - start_time
    
    return {
        "sent_count": sent_count,
        "total_time": total_elapsed,
        "throughput": sent_count / total_elapsed
    }

def wait_for_processing(timeout: int = 60) -> bool:
    print(f"{BLUE}Waiting for processing to complete (timeout: {timeout}s)...{RESET}")
    
    start_time = time.time()
    last_stats = None
    
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{BASE_URL}/stats", timeout=5)
            stats = response.json()
            
            received = stats.get('received', 0)
            processed = stats.get('unique_processed', 0)
            dropped = stats.get('duplicate_dropped', 0)

            if received == processed + dropped:
                print(f"{GREEN}✓ All events processed!{RESET}")
                return True

            if stats != last_stats:
                print(f"  Progress: received={received}, processed={processed}, dropped={dropped}", end='\r')
                last_stats = stats
            
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"{RED}Error checking stats: {e}{RESET}")
            time.sleep(1)
    
    print(f"{RED}✗ Timeout waiting for processing{RESET}")
    return False

def get_final_stats() -> Dict:
    try:
        response = requests.get(f"{BASE_URL}/stats", timeout=5)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"{RED}Error getting stats: {e}{RESET}")
        return None

def print_results(send_metrics: Dict, stats: Dict, expected_unique: int, expected_dup: int):

    print(f"\n{BOLD}{'='*70}{RESET}")
    print(f"{BOLD}{BLUE}PERFORMANCE TEST RESULTS{RESET}")
    print(f"{BOLD}{'='*70}{RESET}\n")

    print(f"{BOLD}1. Sending Performance:{RESET}")
    print(f"   Total sent:        {send_metrics['sent_count']} events")
    print(f"   Total time:        {send_metrics['total_time']:.2f} seconds")
    print(f"   Throughput:        {send_metrics['throughput']:.2f} events/sec")

    print(f"\n{BOLD}2. Processing Results:{RESET}")
    print(f"   Received:          {stats['received']}")
    print(f"   Unique processed:  {stats['unique_processed']}")
    print(f"   Duplicates dropped: {stats['duplicate_dropped']}")
    print(f"   Topics:            {', '.join(stats.get('topics', []))}")
    print(f"   Uptime:            {stats.get('uptime_seconds', 0)} seconds")

    print(f"\n{BOLD}3. Validation:{RESET}")
    
    received_ok = stats['received'] == send_metrics['sent_count']
    print(f"   {'✓' if received_ok else '✗'} Received count matches: {received_ok}")
    
    unique_ok = stats['unique_processed'] == expected_unique
    print(f"   {'✓' if unique_ok else '✗'} Unique processed: {stats['unique_processed']} (expected: {expected_unique})")
    
    dup_ok = stats['duplicate_dropped'] == expected_dup
    print(f"   {'✓' if dup_ok else '✗'} Duplicates dropped: {stats['duplicate_dropped']} (expected: {expected_dup})")

    all_pass = received_ok and unique_ok and dup_ok
    
    print(f"\n{BOLD}4. Final Verdict:{RESET}")
    if all_pass and send_metrics['total_time'] < 30:
        print(f"   {GREEN}{BOLD}✓ TEST PASSED{RESET}")
        print(f"   {GREEN}System processed {send_metrics['sent_count']} events in {send_metrics['total_time']:.2f}s{RESET}")
    elif all_pass:
        print(f"   {YELLOW}{BOLD}⚠ TEST PASSED (with performance warning){RESET}")
        print(f"   {YELLOW}Time: {send_metrics['total_time']:.2f}s > 30s threshold{RESET}")
    else:
        print(f"   {RED}{BOLD}✗ TEST FAILED{RESET}")
        print(f"   {RED}Data validation failed - check deduplication logic{RESET}")
    
    print(f"\n{BOLD}{'='*70}{RESET}\n")

def check_server_ready() -> bool:
    """Check if server is ready"""
    try:
        response = requests.get(f"{BASE_URL}/stats", timeout=5)
        print(f"{GREEN}✓ Server is ready at {BASE_URL}{RESET}")
        return True
    except requests.exceptions.RequestException:
        print(f"{RED}✗ Server not reachable at {BASE_URL}{RESET}")
        print(f"{YELLOW}Make sure container is running: docker ps | grep aggregator{RESET}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Performance test for Event Aggregator')
    parser.add_argument('--events', type=int, default=5000, help='Total number of events (default: 5000)')
    parser.add_argument('--dup-rate', type=float, default=0.2, help='Duplication rate 0.0-1.0 (default: 0.2)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size (default: 1000)')
    parser.add_argument('--url', type=str, default='http://localhost:8080', help='Server URL')
    
    args = parser.parse_args()
    
    global BASE_URL
    BASE_URL = args.url
    
    print(f"\n{BOLD}{BLUE}Event Aggregator - Performance Test{RESET}\n")
    print(f"Configuration:")
    print(f"  Total events:      {args.events}")
    print(f"  Duplication rate:  {args.dup_rate * 100:.0f}%")
    print(f"  Batch size:        {args.batch_size}")
    print(f"  Server URL:        {BASE_URL}\n")

    if not check_server_ready():
        return 1

    expected_unique = int(args.events * (1 - args.dup_rate))
    expected_dup = args.events - expected_unique
    
    print(f"Expected results:")
    print(f"  Unique:      {expected_unique}")
    print(f"  Duplicates:  {expected_dup}\n")

    events = generate_events(args.events, args.dup_rate)

    send_metrics = send_batch(events, args.batch_size)
    if not send_metrics:
        return 1

    if not wait_for_processing(timeout=60):
        return 1

    stats = get_final_stats()
    if not stats:
        return 1

    print_results(send_metrics, stats, expected_unique, expected_dup)
    
    return 0

if __name__ == "__main__":
    exit(main())