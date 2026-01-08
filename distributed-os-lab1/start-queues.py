#!/usr/bin/env python3
import subprocess
import base64
import json
import time
import sys
import os


RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
RESET = "\033[0m"


def initKind():
    kind_cmd = [
        "kind",
        "create",
        "cluster",
        "--name",
        "lab",
        "--config",
        "kind-config.yaml",
    ]
    run(kind_cmd, check=True)


def initDockerImages(server_type):
    cmd = [
        "docker",
        "build",
        "-f",
        "Dockerfile.queue",
        "-t",
        "doslab1/queue:latest",
        ".",
    ]
    run(cmd, check=True)

    cmd = [
        "docker",
        "build",
        "-f",
        f"Dockerfile.{server_type}-server",
        "-t",
        "doslab1/server:latest",
        ".",
    ]
    run(cmd, check=True)

    cmd = [
        "docker",
        "build",
        "-f",
        "Dockerfile.distributor",
        "-t",
        "doslab1/distributor:latest",
        ".",
    ]
    run(cmd, check=True)

    cmd = [
        "kind",
        "load",
        "docker-image",
        "doslab1/queue:latest",
        "--name",
        "lab",
    ]
    run(cmd, check=True)

    cmd = [
        "kind",
        "load",
        "docker-image",
        "doslab1/server:latest",
        "--name",
        "lab",
    ]
    run(cmd, check=True)

    cmd = [
        "kind",
        "load",
        "docker-image",
        "doslab1/distributor:latest",
        "--name",
        "lab",
    ]
    run(cmd, check=True)

    print(GREEN + "Docker images have been built and loaded into kind" + RESET)


def installHelmRepos():
    result = subprocess.run(
        ["helm", "repo", "list" "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )

    if "prometheus-community" not in result.stdout:
        cmd = [
            "helm",
            "repo",
            "add",
            "prometheus-community",
            "https://prometheus-community.github.io/helm-charts",
        ]
        run(cmd, check=True)

    if "grafana" not in result.stdout:
        cmd = [
            "helm",
            "repo",
            "add",
            "grafana",
            "https://grafana.github.io/helm-charts",
        ]
        run(cmd, check=True)

    run(["helm", "repo", "update"], check=True)


def initLogging():
    cmd = [
        "helm",
        "install",
        "loki",
        "grafana/loki-stack",
        "--namespace",
        "logging",
        "--create-namespace",
        "--set",
        "grafana.enabled=true",
        "--set",
        "promtail.enabled=true",
    ]
    run(cmd, check=True)


def initMetrics():
    cmd = [
        "helm",
        "install",
        "monitoring",
        "prometheus-community/kube-prometheus-stack",
        "--namespace",
        "monitoring",
        "--create-namespace",
    ]
    run(cmd, check=True)

    cmd = [
        "kubectl",
        "apply",
        "-f",
        "queue-servicemonitor.yaml",
        "--namespace",
        "monitoring",
    ]
    run(cmd, check=True)


def initQueues(num_queues, num_servers, max_size):
    for queue_id in range(num_queues):
        ns = f"q{queue_id}"

        run(["kubectl", "create", "namespace", ns], check=False)

        helm_cmd = [
            "helm",
            "upgrade",
            "--install",
            ns,
            "./queue-stack",
            "--namespace",
            ns,
            "--set",
            f"queue.id={queue_id}",
            "--set",
            f"queue.numServers={num_servers}",
            "--set",
            f"queue.maxSize={max_size}",
            "--set",
            f"server.replicas={num_servers}",
        ]

        run(helm_cmd, check=True)

    print(GREEN + "All queue and worker servers have been deployed" + RESET)


def initDistributors(num_queues, replicas):
    run(["kubectl", "create", "namespace", "distributors"], check=False)

    cmd = [
        "helm",
        "install",
        "distributors",
        "./distributor-stack",
        "--namespace",
        "distributors",
        "--set",
        f"distributor.numQueues={num_queues}",
        "--set",
        f"distributor.replicas={replicas}",
        "--set",
        "distributor.port=50000",
    ]
    run(cmd, check=True)

    print(GREEN + "Distributors have been deployed" + RESET)


def waitForGrafana(interval=5):
    while True:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", "monitoring", "-o", "json"],
            capture_output=True,
            text=True,
            check=True,
        )
        data = json.loads(result.stdout)

        all_ready = True
        for pod in data["items"]:
            phase = pod["status"].get("phase")
            conditions = pod["status"].get("conditions", [])
            ready = any(
                c["type"] == "Ready" and c["status"] == "True" for c in conditions
            )

            if phase != "Running" or not ready:
                all_ready = False
                break

        if all_ready:
            print(GREEN + "All monitoring pods are Ready" + RESET)
            return

        time.sleep(interval)


def portForwardLocal():
    cmd = [
        "kubectl",
        "port-forward",
        "svc/distributors-distributor",
        "50000:50000",
        "-n",
        "distributors",
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        preexec_fn=os.setpgrp,
    )

    print(BLUE + f"Localhost port forward started as PID {process.pid}" + RESET)


def portForwardGrafana():
    cmd = [
        "kubectl",
        "port-forward",
        "-n",
        "monitoring",
        "svc/monitoring-grafana",
        "3000:80",
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        preexec_fn=os.setpgrp,
    )

    print(BLUE + f"Grafana UI port forward started as PID {process.pid}" + RESET)


def getGrafanaPassword():
    result = subprocess.run(
        [
            "kubectl",
            "get",
            "secret",
            "-n",
            "monitoring",
            "monitoring-grafana",
            "-o",
            "jsonpath={.data.admin-password}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    encoded = result.stdout.strip()
    decoded = base64.b64decode(encoded).decode("utf-8").strip()

    print(GREEN + "Grafana UI hosted at: http://localhost:3000", encoded + RESET)
    print(BLUE + "Username: admin", RESET)
    print(BLUE + "Password:", decoded + RESET)


def run(cmd, check=True):
    print("+", " ".join(cmd))
    result = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr)
    if check and result.returncode != 0:
        subprocess.run(["kind", "delete", "cluster", "--name", "lab"])
        raise RuntimeError(
            RED
            + f"Command failed with exit code {result.returncode}: {' '.join(cmd)}"
            + RESET
        )


def main():
    if len(sys.argv) < 6:
        print(
            YELLOW
            + "Usage: ./start-queues.py <server-type> <num-queues> <num-servers> <max-size> <num-distributors>"
            + RESET
        )
        sys.exit(1)

    server_type = sys.argv[1]
    num_queues = int(sys.argv[2])
    num_servers = int(sys.argv[3])
    max_size = int(sys.argv[4])
    num_distributors = int(sys.argv[5])

    initKind()
    initDockerImages(server_type)
    installHelmRepos()
    initLogging()
    initMetrics()
    print(GREEN + "Kubernetes cluster created successfully" + RESET)

    initQueues(num_queues, num_servers, max_size)
    initDistributors(num_queues, num_distributors)

    print(YELLOW + "Waiting for Grafana to become ready..." + RESET)
    waitForGrafana()
    portForwardLocal()
    portForwardGrafana()
    getGrafanaPassword()


if __name__ == "__main__":
    main()
