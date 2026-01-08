import sys
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


def parse_log(filename):
    events = []
    with open(filename, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 6:
                continue

            unix_time = int(parts[0])
            timestamp_str = parts[1]
            log_type = parts[2].strip("[]")

            if log_type == "debug":
                continue

            job_id = parts[3]
            queue_id = parts[5]
            event_type = parts[6] if len(parts) > 6 else ""

            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")

            events.append(
                {
                    "time": unix_time,
                    "timestamp": timestamp,
                    "type": log_type,
                    "job": job_id,
                    "queue": queue_id,
                    "event": event_type,
                    "full_line": line.strip(),
                }
            )

    return events


def add_time_offsets(times, markers, colors):
    if len(times) == 0:
        return times

    from datetime import timedelta

    new_times = []
    min_gap = timedelta(milliseconds=400)

    for i in range(len(times)):
        t = times[i]

        if i == 0:
            new_times.append(t)
        else:
            prev_t = new_times[i - 1]
            gap = t - prev_t

            if gap < min_gap:
                new_times.append(prev_t + min_gap)
            else:
                new_times.append(t)

    return new_times


def get_job_number(job_str):
    return int(job_str[1:])


def main():
    if len(sys.argv) < 3:
        print("Usage: python plot_job_trace.py <input.log> <output.png>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    events = parse_log(input_file)

    if len(events) == 0:
        print("No events found!")
        sys.exit(1)

    all_queues = set()
    for ev in events:
        all_queues.add(ev["queue"])
    all_queues = sorted(all_queues)

    queue_colors = {}
    for i, q in enumerate(all_queues):
        hue = i / len(all_queues)
        queue_colors[q] = plt.cm.hsv(hue)

    job_traces = {}

    for ev in events:
        job = ev["job"]
        if job not in job_traces:
            job_traces[job] = {
                "times": [],
                "markers": [],
                "colors": [],
                "in_queue_start": None,
                "in_server": False,
            }

        t = ev["timestamp"]
        queue = ev["queue"]
        event = ev["event"]
        log_type = ev["type"]

        color = queue_colors.get(queue, "gray")

        if log_type == "error":
            job_traces[job]["times"].append(t)
            job_traces[job]["markers"].append("x")
            job_traces[job]["colors"].append(color)
            job_traces[job]["in_queue_start"] = None
            job_traces[job]["last_was_error"] = True

        elif event == "ARRIVE-VIA":
            job_traces[job]["in_queue_start"] = t
            job_traces[job]["pending_arrive"] = {"t": t, "color": color}

        elif event == "ENTERS-SERVER":
            if (
                "pending_arrive" in job_traces[job]
                and job_traces[job]["pending_arrive"] is not None
            ):
                arr = job_traces[job]["pending_arrive"]
                job_traces[job]["times"].append(arr["t"])
                job_traces[job]["markers"].append("s")
                job_traces[job]["colors"].append(arr["color"])
                job_traces[job]["pending_arrive"] = None

            if job_traces[job]["in_queue_start"] is not None:
                from datetime import timedelta

                start_t = job_traces[job]["in_queue_start"]
                wait_time = (t - start_t).total_seconds()
                num_squares = int(wait_time / 2)
                for i in range(num_squares):
                    square_t = start_t + timedelta(seconds=(i + 1) * 2)
                    if square_t < t:
                        job_traces[job]["times"].append(square_t)
                        job_traces[job]["markers"].append("s")
                        job_traces[job]["colors"].append(color)

            job_traces[job]["in_queue_start"] = None
            job_traces[job]["in_server"] = True

        elif event == "TASK-START":
            if (
                "pending_arrive" in job_traces[job]
                and job_traces[job]["pending_arrive"] is not None
            ):
                arr = job_traces[job]["pending_arrive"]
                job_traces[job]["times"].append(arr["t"])
                job_traces[job]["markers"].append("s")
                job_traces[job]["colors"].append(arr["color"])
                job_traces[job]["pending_arrive"] = None

            job_traces[job]["times"].append(t)
            job_traces[job]["markers"].append("o")
            job_traces[job]["colors"].append(color)

        elif event == "TASK-STOP":
            job_traces[job]["times"].append(t)
            job_traces[job]["markers"].append("o")
            job_traces[job]["colors"].append(color)

        elif event == "EXITS-SERVER":
            job_traces[job]["in_server"] = False

        elif event == "DEPART-VIA":
            pass

        elif event == "COMPLETED":
            job_traces[job]["times"].append(t)
            job_traces[job]["markers"].append("o")
            job_traces[job]["colors"].append(color)

    fig, ax = plt.subplots(figsize=(16, 10))

    all_jobs = sorted(job_traces.keys(), key=get_job_number)
    job_y_positions = {}
    for i, job in enumerate(all_jobs):
        job_y_positions[job] = i

    for job in all_jobs:
        trace = job_traces[job]
        y = job_y_positions[job]

        times = trace["times"]
        markers = trace["markers"]
        colors = trace["colors"]

        times = add_time_offsets(times, markers, colors)

        if len(times) > 1:
            ax.plot(times, [y] * len(times), color="lightgray", linewidth=1, zorder=1)

        for i in range(len(times)):
            t = times[i]
            m = markers[i]
            c = colors[i]

            if m == "s":
                ax.scatter(
                    t,
                    y,
                    marker="s",
                    color=c,
                    s=50,
                    zorder=2,
                    alpha=0.7,
                    edgecolors="black",
                    linewidths=0.5,
                )
            elif m == "o":
                ax.scatter(
                    t,
                    y,
                    marker="o",
                    color=c,
                    s=50,
                    zorder=2,
                    alpha=0.7,
                    edgecolors="black",
                    linewidths=0.5,
                )
            elif m == "x":
                ax.scatter(
                    t,
                    y,
                    marker="x",
                    color="red",
                    s=60,
                    zorder=3,
                    linewidths=2,
                    alpha=0.8,
                )

    ax.set_xlabel("Time")
    ax.set_ylabel("Job ID")
    ax.set_title("Job Event Trace")

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.xaxis.set_major_locator(mdates.SecondLocator(interval=5))
    plt.xticks(rotation=45)

    y_ticks = list(range(len(all_jobs)))
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(all_jobs)

    legend_elements = []
    legend_elements.append(
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor="gray",
            markersize=10,
            label="In Queue",
        )
    )
    legend_elements.append(
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor="gray",
            markersize=10,
            label="In Service",
        )
    )
    legend_elements.append(
        plt.Line2D(
            [0],
            [0],
            marker="x",
            color="w",
            markerfacecolor="red",
            markeredgecolor="red",
            markersize=10,
            label="Error",
        )
    )

    for q, c in queue_colors.items():
        legend_elements.append(
            plt.Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor=c,
                markersize=10,
                label=q,
            )
        )

    ax.legend(handles=legend_elements, loc="lower right")

    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    print(f"Saved plot to {output_file}")


if __name__ == "__main__":
    main()
