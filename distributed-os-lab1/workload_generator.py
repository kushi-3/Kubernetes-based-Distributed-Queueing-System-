import time
import yaml
import numpy as np
from build import queue_pb2, queue_pb2_grpc
import grpc
import random


def main(data):
    # Variables stores configuration data from YAML, used as shorthand
    job_config = data["config"]["job"]
    task_config = data["config"]["task"]

    while True:
        # Time between job creating (based on job distribution parameters)
        interarrival = distribution_value(
            job_config["distribution"],
            job_config["parameter1"],
            job_config["parameter2"],
        )
        time.sleep(interarrival)

        # A list of task that will be put into a job.
        list_of_task = createTasks(
            num=distribution_value(
                task_config["distribution"],
                task_config["parameter1"],
                task_config["parameter2"],
            ),
            operations=task_config["operations"],
        )

        # create a unique id based on time
        uid = int(time.time())

        # Creating a job with a id and list of tasks
        job = queue_pb2.Job(id=uid, tasks=list_of_task)
        # Connect to GRPC channel
        with grpc.insecure_channel("localhost:50000") as channel:
            # Stub the JobQueue class
            stub = queue_pb2_grpc.JobQueueStub(channel)
            # Submit the job
            response = stub.SubmitJob(job)


def distribution_value(distribution, param1, param2):
    if distribution == "binomial":
        return np.random.binomial(param1, param2, 1)[0]
    elif distribution == "poisson":
        return np.random.poisson(param1, 1)[0]
    elif distribution == "uniform":
        return np.random.uniform(param1, param2, 1)[0]
    elif distribution == "geometric":
        return np.random.geometric(param1, 1)[0]
    elif distribution == "fixed":
        return param1
    elif distribution == "fixed-range":
        return random.randint(param1, param2)


def createTasks(num=1, operations={"W": 10}):
    list_of_task = []

    # Create num number of task
    for i in range(num):
        # Retreieve the operation key and key value from YAML
        key, value = random.choice(list(operations.items()))
        # Create task instance
        task = queue_pb2.Task(id=i, operation=key, processing_time=value)
        # Append to list
        list_of_task.append(task)

    return list_of_task


if __name__ == "__main__":

    # Load workload generator configurations
    with open("workload.yaml", "r") as file:
        data = yaml.safe_load(file)

    main(data)
