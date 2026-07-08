import argparse
import sys
import time
from datetime import UTC, datetime, timedelta

import httpx

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
        baseline = self.metrics()
        self.submit_mixed_jobs()
        self.submit_duplicate_pair()
        self.submit_rate_limit_burst()

        print(f"watching metrics until the queue drains (submitted={self.total})...")
        final, deferred = self.watch_until_drained()

        sent = final["sent"] - baseline["sent"]
        dead = final["dead_lettered"] - baseline["dead_lettered"]
        print(f"done: sent={sent} dead_lettered={dead} deferred_to_next_window={deferred}")
        if sent + dead + deferred < self.total:
            sys.exit(f"books do not balance: {sent} + {dead} + {deferred} < {self.total}")
        print("books balance.")

    def metrics(self) -> dict:
        return self.client.get("/metrics").json()

    def submit(self, body: dict) -> httpx.Response:
        return self.client.post("/jobs", json=body)

    def submit_mixed_jobs(self) -> None:
        print(f"submitting {self.jobs} mixed-priority jobs with 0-14s delays...")
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
                sys.exit(f"unexpected {response.status_code} submitting job {i}")

    def submit_duplicate_pair(self) -> None:
        print("submitting a duplicate idempotency pair (expecting 201 then 409)...")
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
        print(f"  first={first} second={second}")
        if (first, second) != (201, 409):
            sys.exit("idempotency check failed")

    def submit_rate_limit_burst(self) -> None:
        print(f"submitting {self.burst} jobs to {self.burst_recipient} to trip the rate limit...")
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
                sys.exit(f"unexpected {response.status_code} in rate-limit burst")

    def split_pending(self) -> tuple[int, int]:
        response = self.client.get("/jobs", params={"status": "pending", "limit": 200})
        jobs = response.json()["jobs"]
        threshold = datetime.now(UTC) + DEFERRAL_THRESHOLD
        in_flight = sum(datetime.fromisoformat(j["send_at"]) <= threshold for j in jobs)
        return in_flight, len(jobs) - in_flight

    def watch_until_drained(self) -> tuple[dict, int]:
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            metrics = self.metrics()
            pending_active, deferred = self.split_pending()
            active = metrics["queued"] + metrics["claimed"] + pending_active
            line = " ".join(f"{k}={v}" for k, v in metrics.items())
            print(f"\r{line} active={active} deferred={deferred}   ", end="", flush=True)
            if active == 0:
                print()
                return metrics, deferred
            if time.monotonic() >= deadline:
                print()
                sys.exit(
                    f"timed out after {self.timeout_seconds:.0f}s waiting for the queue to drain"
                )
            time.sleep(self.poll_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drive the notification system end to end")
    parser.add_argument("--api", default="http://127.0.0.1:8080")
    parser.add_argument("--jobs", type=int, default=50)
    parser.add_argument("--burst", type=int, default=15)
    parser.add_argument("--burst-recipient", default="hot@example.com")
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with httpx.Client(base_url=args.api, timeout=10.0) as client:
        try:
            client.get("/metrics").raise_for_status()
        except httpx.HTTPError:
            sys.exit(f"api unreachable at {args.api}")
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
