import requests
import csv
import time
import threading
from queue import Queue

POLL_DELAY = 1  # seconds
RUN_DURATION = 201  # seconds
queue_in = Queue()
queue_filtered = Queue()
queue_out = Queue()

# Poller thread function
def poller():
    seq = 0
    start_time = time.time()
    while time.time() - start_time < RUN_DURATION:
        try:
            response = requests.get("http://localhost:9600/_node/stats", timeout=1)
            response.raise_for_status()  # Raise exception for HTTP errors
            stats = response.json()
            
            # Extract values for in, filtered, and out
            in_count = stats.get("events", {}).get("in")
            filtered_count = stats.get("events", {}).get("filtered")
            out_count = stats.get("events", {}).get("out")
            
            if in_count is not None:
                queue_in.put({"seq": seq, "count": in_count})
            if filtered_count is not None:
                queue_filtered.put({"seq": seq, "count": filtered_count})
            if out_count is not None:
                queue_out.put({"seq": seq, "count": out_count})
            
            seq += POLL_DELAY
        except requests.exceptions.RequestException as e:
            print(f"http request exception: {e}")
        
        time.sleep(POLL_DELAY)

# Stats thread function
def stats_calculator():
    # Initialize previous stats for rate calculation
    last_in = queue_in.get()
    last_filtered = queue_filtered.get()
    last_out = queue_out.get()
    data = []

    while not queue_in.empty() and not queue_filtered.empty() and not queue_out.empty():
        in_stat = queue_in.get()
        filtered_stat = queue_filtered.get()
        out_stat = queue_out.get()

        # Calculate rates
        in_rate = (in_stat["count"] - last_in["count"]) / (in_stat["seq"] - last_in["seq"])
        filtered_rate = (filtered_stat["count"] - last_filtered["count"]) / (filtered_stat["seq"] - last_filtered["seq"])
        out_rate = (out_stat["count"] - last_out["count"]) / (out_stat["seq"] - last_out["seq"])

        # Update last stats
        last_in, last_filtered, last_out = in_stat, filtered_stat, out_stat

        # Append data to list
        data.append({
            "time": in_stat["seq"],
            "in_rate": in_rate,
            "filtered_rate": filtered_rate,
            "out_rate": out_rate
        })
        print(f"{in_stat['seq']},{in_rate},{filtered_rate},{out_rate}")
    
    # Save to CSV
    with open("stats_output2.csv", mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["time", "in_rate", "filtered_rate", "out_rate"])
        writer.writeheader()
        writer.writerows(data)

# Start the threads
poller_thread = threading.Thread(target=poller)
stats_thread = threading.Thread(target=stats_calculator)

poller_thread.start()
poller_thread.join()  # Wait for poller thread to finish after RUN_DURATION
stats_thread.start()
stats_thread.join()  # Wait for stats thread to finish processing

print("Data collection completed. Results saved to stats_output.csv.")
