# Distributed OS Lab 1

## Environment Setup

### Initialize Submodules

```bash
git submodule update --init --recursive third_party/prometheus-cpp
```

### Create a Python Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt
```

### Generate gRPC Code

```bash
mkdir -p build
python3 -m grpc_tools.protoc \
  -I proto \
  --python_out=build \
  --pyi_out=build \
  --grpc_python_out=build \
  proto/queue.proto
```

The following line will correct the queue_pb2_grpc.py module import.
```bash
sed -i '7s/.*/from . import queue_pb2 as queue__pb2/' build/queue_pb2_grpc.py
```

### Install Dependencies

- [Docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [Helm](https://helm.sh/docs/intro/install/)

## Generating Jobs and Task

The workload generator will generator jobs and task based on the workload.yaml

Implementations are from numpy.random

*If you use a distribution with one parameter, the second one will be ignore no matter the value.*

Avaliable distributions (followed by their parameters):
- binomial | number of trials | probability of success
- poisson | expected number of events occurring in a fixed-time interval
- uniform | lower bound | upper bound
- geometric | probability of success
- fixed | int (same number every time)
- fixed-range | left bound | right bound | (randomly chooses number between left and right bounds (inclusive))


For tasks you can specific what operations you would like to have your task randomly select from. **(It is always random)**

The formatting follows a dictionary for example:
```yaml
config:
  job:
    distribution: binomial
    parameter1: 3
    parameter2: 0.5
  task:
    distribution: fixed-range
    parameter1: 1
    parameter2: 3
    operations:
      "A" : 10
      "B" : 12
      "C" : 15
```
The YAML above generates jobs at a binomial rate with the parameters 3 and 0.5. Each job will generate its task at a fixed range between 1 and 3 and select randomly A, B, C which each have a operation time of 10, 12, and 15 respectively.

```bash
python3 workload_generator.py
```

## Running the Kubernetes Cluster

Use the `start-queues.py` script to create a Kubernetes cluster and deploy queue instances.

### Usage

```bash
python3 start-queues.py <server-type> <num-queues> <num-servers> <max-size> <num-distributors>
```

### Example

```bash
python3 start-queues.py stateless 10 4 5 1
```

## Grafana UI

The Grafana UI is available at `http://localhost:3000`.

The username and password are available in the output of the `start-queues.py` script.

### Logging

To add Loki as a data source, open:

```
http://localhost:3000/datasources/new
```

Set the connection URL to:

```
http://loki.logging.svc.cluster.local:3100
```

Then click "Save & Test" which will likely give an error message.

Select "Explore data" at the top of the page and you will be able to make log queries using PromQL.

### Metrics Dashboard

To load the provided dashboard, go to:

```
http://localhost:3000/dashboard/import
```

Upload `grafana-dashboard.json`.

## Stopping the Kubernetes Cluster

To stop everything, run:

```bash
kind delete cluster --name lab
```

