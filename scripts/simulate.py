"""Drive the notification system end to end: submit jobs, watch metrics, verify the books balance."""

import argparse
import logging
import sys
import time
from datetime import UTC, datetime, timedelta

import httpx

from notify_queue.log import setup_logging

logger = logging.getLogger("notify_queue.simulate")

PRIORITIES = ("high", "medium", "low")
CHANNELS = ("email", "sms", "push")

# Retry backoff tops out at BASE*2^(MAX-2) (240s with defaults) while a
# rate-limit deferral lands at the top of the next hour, so anything pending
# more than 10 minutes out is a deferral, not work in flight.
DEFERRAL_THRESHOLD = timedelta(seconds=600)


class Simulation:
    def __init__(
        self,
        client: httpx.Client,
        api: str,
        jobs: int,
        burst: int,
        burst_recipient: str,
        poll_seconds: float,
        timeout_seconds: float,
    ) -> None:
        self.client = client
        self.api = api
        self.jobs = jobs
        self.burst = burst
        self.burst_recipient = burst_recipient
        self.poll_seconds = poll_seconds
        self.timeout_seconds = timeout_seconds
        self.total = jobs + 1 + burst

    def run(self) -> None:
        """Execute the full simulation: submit, watch, verify."""
        baseline = self.metrics()
        self.submit_mixed_jobs()
        self.submit_duplicate_pair()
        self.submit_rate_limit_burst()

        logger.info("watching metrics until the queue drains (submitted=%d)...", self.total)
        final, deferred = self.watch_until_drained()

        sent = final["sent"] - baseline["sent"]
        dead = final["dead_lettered"] - baseline["dead_lettered"]
        logger.info("done: sent=%d dead_lettered=%d deferred_to_next_window=%d", sent, dead, deferred)
        if sent + dead + deferred < self.total:
            logger.error("books do not balance: %d + %d + %d < %d", sent, dead, deferred, self.total)
            sys.exit(1)
        logger.info("books balance.")

    def metrics(self) -> dict:
        """Fetch current job counts by status."""
        return self.client.get("/metrics").json()

    def submit(self, body: dict) -> httpx.Response:
        """Submit a single job to the API."""
        return self.client.post("/jobs", json=body)

    def submit_mixed_jobs(self) -> None:
        """Submit a spread of jobs across priorities, channels, and short delays."""
        logger.info("submitting %d mixed-priority jobs with 0-14s delays...", self.jobs)
        for i in range(self.jobs):
            response = self.submit(
                {
                    "recipient": f"user{i % 10}@example.com",
                    "channel": CHANNELS[i % 3],
                    "priority": PRIORITIES[i % 3],
                    "delay_seconds": i % 15,
                    "payload": {"seq": i},
                    "callback_url": f"{self.api}/webhook-mock",
                }
            )
            if response.status_code != 201:
                logger.error("unexpected %d submitting job %d", response.status_code, i)
                sys.exit(1)

    def submit_duplicate_pair(self) -> None:
        """Submit the same idempotency key twice; expect 201 then 409."""
        logger.info("submitting a duplicate idempotency pair (expecting 201 then 409)...")
        body = {
            "recipient": "dup@example.com",
            "channel": "email",
            "priority": "high",
            "delay_seconds": 0,
            "payload": {},
            "idempotency_key": f"sim-dup-{time.time_ns()}",
        }
        first = self.submit(body).status_code
        second = self.submit(body).status_code
        logger.info("  first=%d second=%d", first, second)
        if (first, second) != (201, 409):
            logger.error("idempotency check failed")
            sys.exit(1)

    def submit_rate_limit_burst(self) -> None:
        """Flood one recipient to trigger the per-hour rate limit."""
        logger.info("submitting %d jobs to %s to trip the rate limit...", self.burst, self.burst_recipient)
        for _ in range(self.burst):
            response = self.submit(
                {
                    "recipient": self.burst_recipient,
                    "channel": "sms",
                    "priority": "medium",
                    "delay_seconds": 0,
                    "payload": {},
                }
            )
            if response.status_code != 201:
                logger.error("unexpected %d in rate-limit burst", response.status_code)
                sys.exit(1)

    def split_pending(self) -> tuple[int, int]:
        """Partition pending jobs into actively retrying vs deferred to next window."""
        response = self.client.get("/jobs", params={"status": "pending", "limit": 200})
        jobs = response.json()["jobs"]
        threshold = datetime.now(UTC) + DEFERRAL_THRESHOLD
        in_flight = sum(datetime.fromisoformat(j["send_at"]) <= threshold for j in jobs)
        return in_flight, len(jobs) - in_flight

    def watch_until_drained(self) -> tuple[dict, int]:
        """Poll metrics until no active jobs remain or the timeout expires."""
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            metrics = self.metrics()
            pending_active, deferred = self.split_pending()
            active = metrics["queued"] + metrics["claimed"] + pending_active
            line = " ".join(f"{k}={v}" for k, v in metrics.items())
            logger.info("%s active=%d deferred=%d", line, active, deferred)
            if active == 0:
                return metrics, deferred
            if time.monotonic() >= deadline:
                logger.error(
                    "timed out after %.0fs waiting for the queue to drain",
                    self.timeout_seconds,
                )
                sys.exit(1)
            time.sleep(self.poll_seconds)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the simulation."""
    parser = argparse.ArgumentParser(description="Drive the notification system end to end")
    parser.add_argument("--api", default="http://127.0.0.1:8080")
    parser.add_argument("--jobs", type=int, default=50)
    parser.add_argument("--burst", type=int, default=15)
    parser.add_argument("--burst-recipient", default="hot@example.com")
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    return parser.parse_args()


def main() -> None:
    """Entry point: set up logging, verify API reachability, and run the simulation."""
    setup_logging("simulate")
    args = parse_args()
    with httpx.Client(base_url=args.api, timeout=10.0) as client:
        try:
            client.get("/metrics").raise_for_status()
        except httpx.HTTPError:
            logger.error("api unreachable at %s", args.api)
            sys.exit(1)
        Simulation(
            client=client,
            api=args.api,
            jobs=args.jobs,
            burst=args.burst,
            burst_recipient=args.burst_recipient,
            poll_seconds=args.poll_seconds,
            timeout_seconds=args.timeout_seconds,
        ).run()


if __name__ == "__main__":
    main()
