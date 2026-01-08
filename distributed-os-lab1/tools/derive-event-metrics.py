import argparse
from datetime import datetime
import pandas as pd

"""
Example usage: python3 derive-event-metrics.py -i examples/events.log -o emetrics.csv
"""


def main(args):

    active_jobs = {}

    with open(args.output, "w") as file:
        file.write(
            "jobid,queue-sys-id,arrival-time,time-in-queue,time-in-service,response-time,turn-around-time\n"
        )

    with open(args.input, "r") as file:
        for line in file:
            cleaned_line = line.strip().split()
            log = {
                "time": datetime.fromisoformat(cleaned_line[1].replace("Z", "+00:00")),
                "type": cleaned_line[2],
                "jid": cleaned_line[3],
                "queue": cleaned_line[5],
                "log": cleaned_line[6],
            }

            if len(cleaned_line) > 7:
                log["param1"] = cleaned_line[7]
            elif len(cleaned_line) > 8:
                log["param2"] = cleaned_line[9]

            # Handle ARRIVAL-VIA
            if log["log"] == "ARRIVE-VIA":
                active_jobs[log["jid"]] = {
                    "jid": log["jid"],
                    "queueid": log["queue"],
                    "arrive": log["time"],
                    "in-queue": 0,
                    "in-service": 0,
                    "response": 0,
                    "turn": 0,
                    "task-start": 0,
                }
                writeToFile(active_jobs[log["jid"]], args.output)

            # Handles ENTERS-SERVER
            if log["log"] == "ENTERS-SERVER":
                record = active_jobs[log["jid"]]

                new_entry = {
                    "jid": record["jid"],
                    "queueid": record["queueid"],
                    "arrive": record["arrive"],
                    "in-queue": log["time"] - record["arrive"],
                    "in-service": record["in-service"],
                    "response": record["response"],
                    "turn": record["turn"],
                    "task-start": 0,
                }
                writeToFile(new_entry, args.output)

            # Handles TASK-START
            if log["log"] == "TASK-START":
                record = active_jobs[log["jid"]]
                new_entry = {
                    "jid": record["jid"],
                    "queueid": record["queueid"],
                    "arrive": record["arrive"],
                    "in-queue": record["in-queue"],
                    "in-service": record["in-service"],
                    "response": log["time"] - record["arrive"],
                    "turn": record["turn"],
                    "task-start": log["time"],
                }

                active_jobs[log["jid"]] = new_entry
                writeToFile(new_entry, args.output)

            # Handles TASK-STOP
            if log["log"] == "TASK-STOP":
                record = active_jobs[log["jid"]]
                new_entry = {
                    "jid": record["jid"],
                    "queueid": record["queueid"],
                    "arrive": record["arrive"],
                    "in-queue": record["in-queue"],
                    "in-service": log["time"] - record["task-start"],
                    "response": record["response"],
                    "turn": record["turn"],
                    "task-start": log["time"],
                }
                active_jobs[log["jid"]] = new_entry
                writeToFile(new_entry, args.output)

            # We don't need to handle exit server because it shares same time stamp as task end

            # Handle DEPART-VIA
            if log["log"] == "DEPART-VIA":
                record = active_jobs[log["jid"]]
                new_entry = {
                    "jid": record["jid"],
                    "queueid": record["queueid"],
                    "arrive": record["arrive"],
                    "in-queue": record["in-queue"],
                    "in-service": record["in-service"],
                    "response": record["response"],
                    "turn": log["time"] - record["arrive"],
                    "task-start": record["task-start"],
                }
                active_jobs[log["jid"]] = new_entry
                writeToFile(new_entry, args.output)


def writeToFile(job, output_file):
    with open(output_file, "a") as file:
        file.write(
            f"{job['jid']},{job['queueid']},{job['arrive']},{job['in-queue']},{job['in-service']},{job['response']},{job['turn']}\n"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Input file path")
    parser.add_argument("-o", "--output", help="Output file path")
    args = parser.parse_args()
    main(args)
    df = pd.read_csv("emetrics.csv")

    cols = ["time-in-queue", "time-in-service", "response-time", "turn-around-time"]

    for col in cols:
        df[col] = pd.to_timedelta(df[col]).dt.total_seconds()

    df.to_csv("emetrics.csv", index=False)
