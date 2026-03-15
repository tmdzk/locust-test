import csv
import math
import random
import shlex
import subprocess
import time
from collections import defaultdict
from urllib.parse import urlencode

import gevent
from gevent.lock import Semaphore
from locust import FastHttpUser, task, constant, events

# ---------------------------------------------------
# Full URLs to test
# ---------------------------------------------------

APIS = [
    "https://api1.company.com/v1/users",
    "https://api2.company.com/v1/orders",
    "https://api3.company.com/v1/products",
]

# ---------------------------------------------------
# Global config
# ---------------------------------------------------

TARGET_RPS = 5
TEST_DURATION = 60
REPORT_FILE = "endpoint_response_times.csv"
MUNGE_COMMAND = "munge -n"
REQUEST_TIMEOUT_SECONDS = 30

# Generated once at startup, reused for all requests
MUNGE_TOKEN = None
ENCODED_MUNGE_PAYLOAD = None

# ---------------------------------------------------
# Global rate limiter
# ---------------------------------------------------

class GlobalRateLimiter:
    def __init__(self, rps: float):
        if rps <= 0:
            raise ValueError("rps must be > 0")
        self.interval = 1.0 / rps
        self.lock = Semaphore()
        self.next_allowed_time = time.monotonic()

    def acquire(self) -> None:
        with self.lock:
            now = time.monotonic()
            if now < self.next_allowed_time:
                sleep_for = self.next_allowed_time - now
                self.next_allowed_time += self.interval
            else:
                sleep_for = 0
                self.next_allowed_time = now + self.interval

        if sleep_for > 0:
            gevent.sleep(sleep_for)


rate_limiter = GlobalRateLimiter(TARGET_RPS)

# ---------------------------------------------------
# Metrics store
# ---------------------------------------------------

metrics_lock = Semaphore()
endpoint_metrics = defaultdict(lambda: {
    "count": 0,
    "failures": 0,
    "response_times_ms": [],
    "status_codes": defaultdict(int),
    "error_samples": [],
})

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def percentile(values, p):
    if not values:
        return 0.0

    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return float(sorted_values[0])

    rank = (p / 100.0) * (len(sorted_values) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)

    if lower == upper:
        return float(sorted_values[int(rank)])

    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    weight = rank - lower
    return float(lower_value + (upper_value - lower_value) * weight)


def get_munge_token() -> str:
    result = subprocess.run(
        shlex.split(MUNGE_COMMAND),
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )

    token = result.stdout.strip()
    if not token:
        raise RuntimeError("munge command returned an empty token")

    return token


def append_error_sample(endpoint: str, message: str, limit: int = 5) -> None:
    samples = endpoint_metrics[endpoint]["error_samples"]
    if len(samples) < limit:
        samples.append(message[:500])

# ---------------------------------------------------
# Request event listener
# ---------------------------------------------------

@events.request.add_listener
def on_request(
    request_type,
    name,
    response_time,
    response_length,
    response,
    context,
    exception,
    start_time,
    url,
    **kwargs,
):
    endpoint = name or url

    with metrics_lock:
        endpoint_metrics[endpoint]["count"] += 1
        endpoint_metrics[endpoint]["response_times_ms"].append(float(response_time))

        if response is not None:
            endpoint_metrics[endpoint]["status_codes"][response.status_code] += 1

        if exception:
            endpoint_metrics[endpoint]["failures"] += 1
            append_error_sample(endpoint, str(exception))

# ---------------------------------------------------
# CSV report writer
# ---------------------------------------------------

def write_csv_report(filename: str):
    with open(filename, "w", newline="") as f:
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
            "p99_ms",
            "status_codes",
            "error_samples",
        ])

        with metrics_lock:
            for endpoint, data in sorted(endpoint_metrics.items()):
                times = data["response_times_ms"]
                count = data["count"]
                failures = data["failures"]
                failure_rate = (failures / count * 100.0) if count else 0.0

                if times:
                    min_ms = min(times)
                    avg_ms = sum(times) / len(times)
                    max_ms = max(times)
                    p50_ms = percentile(times, 50)
                    p95_ms = percentile(times, 95)
                    p99_ms = percentile(times, 99)
                else:
                    min_ms = avg_ms = max_ms = p50_ms = p95_ms = p99_ms = 0.0

                status_codes = ";".join(
                    f"{code}:{num}" for code, num in sorted(data["status_codes"].items())
                )
                error_samples = " | ".join(data["error_samples"])

                writer.writerow([
                    endpoint,
                    count,
                    failures,
                    round(failure_rate, 2),
                    round(min_ms, 2),
                    round(avg_ms, 2),
                    round(max_ms, 2),
                    round(p50_ms, 2),
                    round(p95_ms, 2),
                    round(p99_ms, 2),
                    status_codes,
                    error_samples,
                ])


@events.quitting.add_listener
def on_quitting(environment, **kwargs):
    write_csv_report(REPORT_FILE)
    print(f"\nCSV report written to: {REPORT_FILE}")

# ---------------------------------------------------
# Duration control
# ---------------------------------------------------

def stop_test_after_duration(environment):
    gevent.sleep(TEST_DURATION)
    print(f"\nTest finished after {TEST_DURATION} seconds")
    environment.runner.quit()


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    gevent.spawn(stop_test_after_duration, environment)

# ---------------------------------------------------
# CLI args
# ---------------------------------------------------

@events.init_command_line_parser.add_listener
def add_custom_arguments(parser):
    parser.add_argument(
        "--rps",
        type=float,
        default=5,
        help="Total target requests per second across all endpoints",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds",
    )
    parser.add_argument(
        "--report-file",
        type=str,
        default="endpoint_response_times.csv",
        help="CSV file path for per-endpoint response-time report",
    )
    parser.add_argument(
        "--munge-command",
        type=str,
        default="munge -n",
        help="Command used to generate the shared munge token once at startup",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=30,
        help="HTTP request timeout in seconds",
    )

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    global TARGET_RPS, TEST_DURATION, REPORT_FILE, MUNGE_COMMAND
    global REQUEST_TIMEOUT_SECONDS, rate_limiter
    global MUNGE_TOKEN, ENCODED_MUNGE_PAYLOAD

    TARGET_RPS = float(environment.parsed_options.rps)
    TEST_DURATION = int(environment.parsed_options.duration)
    REPORT_FILE = environment.parsed_options.report_file
    MUNGE_COMMAND = environment.parsed_options.munge_command
    REQUEST_TIMEOUT_SECONDS = float(environment.parsed_options.request_timeout)

    rate_limiter = GlobalRateLimiter(TARGET_RPS)

    # Generate shared token once
    MUNGE_TOKEN = get_munge_token()
    ENCODED_MUNGE_PAYLOAD = urlencode({"munge_token": MUNGE_TOKEN})

    print("Successfully generated shared munge token at startup")

# ---------------------------------------------------
# Locust user
# ---------------------------------------------------

class ApiUser(FastHttpUser):
    wait_time = constant(0)
    host = ""

    @task
    def query_api(self):
        rate_limiter.acquire()

        url = random.choice(APIS)

        with self.client.post(
            url,
            data=ENCODED_MUNGE_PAYLOAD,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            name=url,
            timeout=REQUEST_TIMEOUT_SECONDS,
            catch_response=True,
        ) as response:
            if response.status_code >= 400:
                response.failure(f"HTTP {response.status_code}: {response.text[:500]}")
