#!/usr/bin/env bash
#
#
# This script orchestrates a queue, a server function
#
# Small demos
# mkfifo
#
#
# Some fake work that has no meaning, but we can run it through the code:
# for i in `seq 1 100`; do echo "jid_$(echo ${i}| md5sum| awk '{print $1}'), 'tag', ['Rd k1, Areg', 'Rd k${i}, Breg', 'Add Areg, Breg, Creg', 'St Creg, k${i}' ] "; done > data0.txt

QUEUE_PROG=./02_producer_and_consumer_instrumented.py
SERVER_PROG=./03_stateless_server_func_ex.py

# Source and sink
SOURCE0=data0.txt
SOURCE1=data_large.txt
SINK0=outdata0.txt
SINK1=outdata1.txt

# Edges
Q0_2_Q0_S0=/tmp/fifo_q0_2_s0
Q1_2_Q0=/tmp/fifo_q1_2_q0

# These should be what's passed as the --logfile flag
Q0_LOG=q0.log
Q1_LOG=q1.log
Q0_S0_LOG=q0_s0.log

# Create the Links
mkfifo ${Q0_2_Q0_S0}
mkfifo ${Q1_2_Q0}


# start the servers
${SERVER_PROG} -i ${Q0_2_Q0_S0} -o ${SINK0} -o ${SINK1} --logfile ${Q0_S0_LOG} & 

# Start the queue
${QUEUE_PROG}  -i ${SOURCE0} -i ${Q1_2_Q0}    -o ${Q0_2_Q0_S0} --logfile ${Q0_LOG} &
${QUEUE_PROG}  -i ${SOURCE1}                  -o ${Q1_2_Q0}    --logfile ${Q1_LOG} &



# Delete Edges
#rm ${Q0_2_Q0_S0}
