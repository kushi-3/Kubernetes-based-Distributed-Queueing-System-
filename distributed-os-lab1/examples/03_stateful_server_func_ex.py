#!/usr/bin/env python3
#
# Example of a stateless function streaming data
# STUDENT_TODO: implement logging to recover that info you need to build USE+Thr/Lat Plots
# https://signoz.io/guides/logging-in-python/
#
#
# - richard.m.veras@ou.edu (2025)


import argparse
import logging
import random
import os
import socket

from contextlib import ExitStack

logger = logging.getLogger(__name__)


# Note: Keeping a variable number of files open at once using "with" needs some extra work.
# https://docs.python.org/3/library/contextlib.html#contextlib.ExitStack
# https://stackoverflow.com/questions/4617034/how-can-i-open-multiple-files-using-with-open-in-python
#
#
#  STUDENT_TODO: Modify this function to fit your application. Not only can you change the
#                work being done, you can change how the outgoing edge is selected.
def apply_func(infile, outfiles, state):
    with open(infile) as fp_in, ExitStack() as stack:
        fp_outs = [stack.enter_context(open(fname, "a")) for fname in outfiles]
        for index, line in enumerate(fp_in):
            # STUDENT_TODO: You should parse out something specific
            #               to read or write from the input.
            #               This is just a demo, so we are going to use
            #               the line as a key and increment
            #               everytime we see this value.
            key = line.rstrip()
            if line in state.keys():
                state[key] = state[key] + 1
            else:
                state[key] = 1

            # STUDENT_TODO: Do some real work here.
            # What we have here is fake work.
            # we will dump out the current state of the kv-store
            result = str(state) + "\n"

            # Pick the outgoing edge using random. You can adapt
            # this to your problem, maybe base destinations on
            # tasks, jobid, tags, etc.
            out_edge = random.randint(0, len(outfiles) - 1)
            fp_outs[out_edge].write(result)


def main():
    parser = argparse.ArgumentParser(
        prog="server-func-ex",
        description="Example of a server function.",
        epilog="Work in progress.",
    )

    parser.add_argument("-i", "--infile")
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
    parser.add_argument("--ingrpc")
    # Should take host:port,num_workers
    # Ex: --outgrpc
    parser.add_argument("--outgrpc", action="append")

    args = parser.parse_args()

    if (not args.infile is None) and (not args.ingrpc is None):
        print("Need exacly one input.")
        exit(-1)

    log_format = f"%(asctime)s - sv_pid{os.getpid()}@{socket.gethostname()} - %(levelname)s - %(message)s"
    if args.logfile is None:
        logging.basicConfig(level=logging.INFO, format=log_format)
    else:
        logging.basicConfig(
            filename=args.logfile, level=logging.INFO, format=log_format
        )

    logger.info("Started")

    # NOTE: This state only lives in this one process. You must determine how
    #       you distribute this data.
    state = {}

    # STUDENT_TODO: Adapt this to handle input from grpc, and more than
    #               one output channel.
    apply_func(args.infile, args.outfile, state)

    logger.info("Finished")


if __name__ == "__main__":
    main()
