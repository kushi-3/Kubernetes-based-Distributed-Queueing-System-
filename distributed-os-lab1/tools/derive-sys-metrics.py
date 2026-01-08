#!/usr/bin/env python3
import argparse
import math
import statistics
from collections import defaultdict


def parse_args():
    parser = argparse.ArgumentParser(
        description="Derive system-level response time metrics from events.log"
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Path to events.log (or any log with queue events)",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Path to output sys_metrics.csv",
    )
    parser.add_argument(
        "--step-seconds",
        type=float,
        default=10.0,
        help="Width of each time-step bucket in seconds (default: 10)",
    )
    return parser.parse_args()


def percentile(data, p):
    """Linear interpolation percentile (p in [0, 100])."""
    if not data:
        return 0.0
    xs = sorted(data)
    if len(xs) == 1:
        return float(xs[0])

    k = (len(xs) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(xs[int(k)])
    return float(xs[f] + (xs[c] - xs[f]) * (k - f))


def parse_log(path):
    """
    Parse the log and return:
      - first_ts_ms: earliest timestamp (ms)
      - jobs: {job_id: {"arrival": ms, "completion": ms}}
    We treat:
      * ARRIVE-VIA       -> arrival
      * EXITS-SERVER / DEPART-VIA -> completion
    """
    jobs = {}
    first_ts_ms = None

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            if len(parts) < 7:
                continue

            # First column is Unix time in ms
            try:
                ts_ms = int(parts[0])
            except ValueError:
                continue

            # We only care about [info] job events
            if parts[2] != "[info]":
                continue

            # Track earliest timestamp we see
            if first_ts_ms is None or ts_ms < first_ts_ms:
                first_ts_ms = ts_ms

            job_id = parts[3]
            event = parts[6]  # ARRIVE-VIA, ENTERS-SERVER, EXITS-SERVER, ...

            job = jobs.setdefault(job_id, {})

            if event == "ARRIVE-VIA":
                # only record first arrival we see
                job.setdefault("arrival", ts_ms)

            elif event in ("EXITS-SERVER", "DEPART-VIA"):
                # last completion wins if there are multiple
                job["completion"] = ts_ms

    return first_ts_ms, jobs


def compute_sys_metrics(first_ts_ms, jobs, step_seconds):
    """
    From parsed jobs, compute per-time-step response metrics.

    Returns list of rows:
      (time_step, avg, median, p10, p90, throughput)
    where time_step is the *upper bound* of the interval in seconds
    since first_ts_ms.
    """
    if first_ts_ms is None:
        return []

    # bucket -> list of response times (seconds)
    buckets = defaultdict(list)

    for job_id, info in jobs.items():
        if "arrival" not in info or "completion" not in info:
            continue

        arr = info["arrival"]
        comp = info["completion"]
        if comp < arr:
            # corrupted log for this job; ignore
            continue

        resp_sec = (comp - arr) / 1000.0
        offset_sec = (comp - first_ts_ms) / 1000.0

        # Which step does this completion fall into?
        idx = int(offset_sec // step_seconds)
        # Label the bucket by its upper bound (e.g., 10,20,30,...)
        time_step = (idx + 1) * step_seconds

        buckets[time_step].append(resp_sec)

    rows = []
    for time_step in sorted(buckets.keys()):
        resps = buckets[time_step]
        throughput = len(resps)
        avg = sum(resps) / throughput
        med = statistics.median(resps)
        p10 = percentile(resps, 10)
        p90 = percentile(resps, 90)
        rows.append((time_step, avg, med, p10, p90, throughput))

    return rows


def write_csv(path, rows):
    header = (
        "time-step,"
        "avg-response-time,"
        "median-response-time,"
        "p10-response-time,"
        "p90-response-time,"
        "throughput\n"
    )
    with open(path, "w") as f:
        f.write(header)
        for (t, avg, med, p10, p90, thr) in rows:
            f.write(
                f"{t:.1f},{avg},{med},{p10},{p90},{thr}\n"
            )


def main():
    args = parse_args()
    first_ts_ms, jobs = parse_log(args.input)
    rows = compute_sys_metrics(first_ts_ms, jobs, args.step_seconds)
    write_csv(args.output, rows)


if __name__ == "__main__":
    main()
