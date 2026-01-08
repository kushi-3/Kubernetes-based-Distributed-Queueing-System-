#!/usr/bin/env python3
# adapted from SuperFastPython.com
# example of multiple producers and multiple consumers with threads
#
#
# Read: https://superfastpython.com/thread-producer-consumer-pattern-in-python/
# https://docs.python.org/3/library/queue.html#module-queue
# https://docs.python.org/3/library/threading.html
#
# STUDENT_TODO: implement logging to recover that info you need to build USE+Thr/Lat Plots
# https://signoz.io/guides/logging-in-python/

import argparse
import logging
import os
import socket
import sys

from datetime import datetime
from queue import Queue
from random import random
from time import sleep
from threading import Barrier
from threading import Thread

logger = logging.getLogger(__name__)


def worker_producer_from_grpc(barrier, queue, identifier, filename, sleep_time=0.001):
    # STUDENT_TODO
    pass


def worker_consumer_to_grpc(queue, identifier, filename, sleep_time=0.001):
    # STUDENT_TODO
    pass


# monitors the queue at a set interval
def worker_sidecar(q, barrier, sample_interval=0.01):
    while True:
        # sample the size of the queue. Note we could
        # periodically sample as jobs arrive.
        logger.info(f"metric-q-size {q.qsize()}")
        sleep(sample_interval)
        if barrier.n_waiting > 0:
            break
    barrier.wait()


# producer task
def worker_producer_from_file(barrier, queue, identifier, filename, sleep_time=0.001):
    logger.info(f"Producer {identifier} Running")

    # generate items
    with open(filename) as fp:
        for index, line in enumerate(fp):
            # add to the queue
            queue.put(line)
            # sleep to block and yield to other threads
            sleep(sleep_time)

    # wait for all producers to finish
    barrier.wait()
    # signal that there are no further items
    if identifier == 0:
        queue.put(None)
    logger.info(f"Producer {identifier} Done")


# consumer task
def worker_consumer_to_file(queue, identifier, filename, sleep_time=0.001):
    logger.info(f"Consumer {identifier} Running")

    # consume items
    with open(filename, "a") as fp:
        while True:
            # get a unit of work
            item = queue.get()
            # check for stop
            if item is None:
                # add the signal back for other consumers
                queue.put(item)
                # stop running
                break
            # report
            # STUDENT_TODO: Note you can change these log messages
            #               they are just examples here.
            # logger.info(f'>consumer {identifier} got {item}')
            fp.write(f">consumer {identifier} got {item}")
            # sleep to block and yield to other threads
            sleep(sleep_time)

    # all done
    logger.info(f"Consumer {identifier}: Done")


def main():
    parser = argparse.ArgumentParser(
        prog="queue",
        description="Queues jobs from multiple sources and directs them to servers.",
        epilog="Work in progress.",
    )

    parser.add_argument("-i", "--infile", action="append")
    parser.add_argument("-o", "--outfile", action="append")

    # STUDENT_TODO: Implement logging to track queue stats and job events
    # Adapt the event log structure of the simulator lab.
    # Use the following as the id for the server func
    # NOTE: All metrics (including queue metrics) will be derived from the logs.
    #       So collect the appropriate data, like how much time the server function
    #       is doing useful work.
    # https://signoz.io/guides/logging-in-python/
    # https://docs.python.org/3/library/logging.html
    # logger.debug('This is a debug message')
    # logger.info('This is an informational message')
    # logger.warning('This is a warning message')
    # logger.error('This is an error message')
    # logger.critical('This is a critical message')
    parser.add_argument("--logfile")

    # STUDENT_TODO: Implement these
    # Should take interface,num_workers
    # Ex: --ingrpc 0.0.0.0,5
    parser.add_argument("--ingrpc", action="append")
    # Should take host:port,num_workers
    # Ex: --outgrpc
    parser.add_argument("--outgrpc", action="append")

    parser.add_argument("--qsize")

    args = parser.parse_args()
    log_format = f"%(asctime)s - Q_pid{os.getpid()}@{socket.gethostname()} - %(levelname)s - %(message)s"
    if args.logfile is None:
        logging.basicConfig(level=logging.INFO, format=log_format)
    else:
        logging.basicConfig(
            filename=args.logfile, level=logging.INFO, format=log_format
        )

    logger.info("Started")

    if args.qsize is None:
        # create the shared queue
        queue = Queue()
    else:
        logger.info(f"max-q-size {int(args.qsize)}")
        queue = Queue(maxsize=int(args.qsize))

    # TODO: make command line parameters
    # logfilename = ...
    sample_interval = 0.1

    input_files = args.infile
    n_producers = len(input_files)

    output_files = args.outfile
    n_consumers = len(output_files)

    # create the shared barrier
    barrier = Barrier(n_producers)
    barrier_sc = Barrier(2)  # one for the main thread, the other for the sidecar

    # Start the sidecar
    sc = Thread(
        target=worker_sidecar,
        args=(queue, barrier_sc),
        kwargs={"sample_interval": sample_interval},
        daemon=True,
    )
    sc.start()

    # start the consumers
    consumers = [
        Thread(target=worker_consumer_to_file, args=(queue, i, output_files[i]))
        for i in range(n_consumers)
    ]
    for consumer in consumers:
        consumer.start()
    # start the producers
    producers = [
        Thread(
            target=worker_producer_from_file, args=(barrier, queue, i, input_files[i])
        )
        for i in range(n_producers)
    ]
    # start the producers
    for producer in producers:
        producer.start()
    # wait for all threads to finish
    for producer in producers:
        producer.join()
    for consumer in consumers:
        consumer.join()

    # notify sidecar that the producer and consumers have finished
    barrier_sc.wait()
    sc.join()

    logger.info("Finished")


if __name__ == "__main__":
    main()
