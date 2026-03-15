import csv
import random
import time
import math
from collections import defaultdict

import gevent
from gevent.lock import Semaphore
from locust import FastHttpUser, task, constant, events

# ---------------------------------------
# APIs (full URLs)
# ---------------------------------------

APIS = [
    "https://api1.company.com/v1/users",
    "https://api2.company.com/v1/orders",
    "https://api3.company.com/v1/products",
]

# ---------------------------------------
# Global config
# ---------------------------------------

TARGET_RPS = 5
TEST_DURATION = 60
REPORT_FILE = "endpoint_response_times.csv"

# ---------------------------------------
# Rate limiter
# ---------------------------------------

class GlobalRateLimiter:

    def __init__(self, rps):
        self.interval = 1.0 / rps
        self.lock = Semaphore()
        self.next_allowed = time.monotonic()

    def acquire(self):

        with self.lock:
            now = time.monotonic()

            if now < self.next_allowed:
                sleep_time = self.next_allowed - now
                self.next_allowed += self.interval
            else:
                sleep_time = 0
                self.next_allowed = now + self.interval

        if sleep_time > 0:
            gevent.sleep(sleep_time)


rate_limiter = GlobalRateLimiter(TARGET_RPS)

# ---------------------------------------
# Metrics storage
# ---------------------------------------

metrics_lock = Semaphore()

endpoint_metrics = defaultdict(lambda: {
    "count": 0,
    "failures": 0,
    "response_times": [],
})

# ---------------------------------------
# Percentile helper
# ---------------------------------------

def percentile(values, p):

    if not values:
        return 0

    values = sorted(values)
    k = (len(values) - 1) * (p / 100)

    f = math.floor(k)
    c = math.ceil(k)

    if f == c:
        return values[int(k)]

    d0 = values[int(f)] * (c - k)
    d1 = values[int(c)] * (k - f)

    return d0 + d1

# ---------------------------------------
# Capture request metrics
# ---------------------------------------

@events.request.add_listener
def capture_request(
    request_type,
    name,
    response_time,
    response_length,
    response,
    context,
    exception,
    start_time,
    url,
    **kwargs
):

    key = name or url

    with metrics_lock:

        endpoint_metrics[key]["count"] += 1
        endpoint_metrics[key]["response_times"].append(response_time)

        if exception:
            endpoint_metrics[key]["failures"] += 1


# ---------------------------------------
# CSV report
# ---------------------------------------

def write_csv():

    with open(REPORT_FILE, "w", newline="") as f:

        writer = csv.writer(f)

        writer.writerow([
            "endpoint",
            "requests",
            "failures",
            "failure_rate_pct",
            "min_ms",
            "avg_ms",
            "max_ms",
            "p50_ms",
            "p95_ms",
            "p99_ms"
        ])

        for endpoint, data in endpoint_metrics.items():

            times = data["response_times"]
            count = data["count"]
            failures = data["failures"]

            if times:

                min_ms = min(times)
                avg_ms = sum(times) / len(times)
                max_ms = max(times)

                p50 = percentile(times, 50)
                p95 = percentile(times, 95)
                p99 = percentile(times, 99)

            else:

                min_ms = avg_ms = max_ms = p50 = p95 = p99 = 0

            failure_rate = (failures / count * 100) if count else 0

            writer.writerow([
                endpoint,
                count,
                failures,
                round(failure_rate, 2),
                round(min_ms, 2),
                round(avg_ms, 2),
                round(max_ms, 2),
                round(p50, 2),
                round(p95, 2),
                round(p99, 2)
            ])

# ---------------------------------------
# Stop test after duration
# ---------------------------------------

def stop_test_after_duration(environment):

    gevent.sleep(TEST_DURATION)

    print(f"\nTest finished after {TEST_DURATION} seconds")

    environment.runner.quit()

@events.test_start.add_listener
def start_timer(environment, **kwargs):

    gevent.spawn(stop_test_after_duration, environment)

# ---------------------------------------
# CLI arguments
# ---------------------------------------

@events.init_command_line_parser.add_listener
def add_args(parser):

    parser.add_argument("--rps", type=float, default=5)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--report-file", type=str, default="endpoint_response_times.csv")

@events.init.add_listener
def init(environment, **kwargs):

    global TARGET_RPS
    global TEST_DURATION
    global REPORT_FILE
    global rate_limiter

    TARGET_RPS = environment.parsed_options.rps
    TEST_DURATION = environment.parsed_options.duration
    REPORT_FILE = environment.parsed_options.report_file

    rate_limiter = GlobalRateLimiter(TARGET_RPS)

# ---------------------------------------
# Write report on exit
# ---------------------------------------

@events.quitting.add_listener
def quitting(environment, **kwargs):

    write_csv()
    print(f"\nCSV report written to {REPORT_FILE}")

# ---------------------------------------
# Locust user
# ---------------------------------------

class ApiUser(FastHttpUser):

    wait_time = constant(0)
    host = ""

    @task
    def call_api(self):

        rate_limiter.acquire()

        url = random.choice(APIS)

        self.client.get(url, name=url)
