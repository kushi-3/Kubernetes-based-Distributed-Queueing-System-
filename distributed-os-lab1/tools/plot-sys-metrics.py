import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="path to sys_metrics.csv",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        help="directory to write plots (defaults to directory of input file)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Read metrics
    df = pd.read_csv(args.input)
    if df.empty:
        print("sys_metrics.csv is empty, nothing to plot.")
        return

    # Time axis
    t = df["time-step"]

    # Where to write images
    out_dir = args.output_dir or os.path.dirname(os.path.abspath(args.input))
    os.makedirs(out_dir, exist_ok=True)

    resp_path = os.path.join(out_dir, "sys_response.png")
    thr_path = os.path.join(out_dir, "sys_throughput.png")

    # ---------- Response time plots ----------
    plt.figure()
    plt.plot(t, df["avg-response-time"], label="avg")
    plt.plot(t, df["median-response-time"], label="median")
    plt.plot(t, df["p10-response-time"], label="p10")
    plt.plot(t, df["p90-response-time"], label="p90")
    plt.xlabel("time-step (s)")
    plt.ylabel("response time (seconds)")
    plt.title("System Response Time Metrics Over Time")
    plt.legend()
    plt.tight_layout()
    plt.savefig(resp_path)
    plt.close()

    # ---------- Throughput plot ----------
    plt.figure()
    plt.plot(t, df["throughput"])
    plt.xlabel("time-step (s)")
    plt.ylabel("throughput (jobs completed)")
    plt.title("System Throughput Over Time")
    plt.tight_layout()
    plt.savefig(thr_path)
    plt.close()


if __name__ == "__main__":
    main()
