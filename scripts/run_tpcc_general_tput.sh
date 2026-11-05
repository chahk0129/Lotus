#!/bin/bash

# Function to run experiments with given parameters
# Usage: run_experiment <config_parameters>
# Each run is capped at RUN_TIMEOUT seconds (one-sided loading of 64 warehouses/node is slow);
# on timeout the leftover processes are killed on every node so the sweep can continue.
RUN_TIMEOUT=${RUN_TIMEOUT:-2400}
# Resumable: every finished config (success, failure or timeout) is appended to $done_file.
# Rerun with RESUME=1 to skip those and continue where the sweep stopped; without RESUME=1
# the sweep starts from scratch.
run_experiment() {
    local params="$*"
    if grep -qxF "$params" "$done_file" 2>/dev/null; then
        echo "Skipping (already done): $params"
        return
    fi
    echo "[$(date '+%F %T')] Running experiment with params: $params"
    timeout $RUN_TIMEOUT python3 run.py $params
    if [ $? -eq 124 ]; then
        echo "TIMEOUT after ${RUN_TIMEOUT}s: $params" | tee -a outputs/failed_params.txt
        bash scripts/kill.sh > /dev/null 2>&1
    fi
    echo "$params" >> "$done_file"
}

# Run TPC-C benchmark for disaggregated memory DB
config="exp_profile/tpcc.json RUN_TIME=10 WARMUP_TIME=10 TXN_TYPE=STORED_PROCEDURE"
output_dir="outputs/tpcc_general"
output_dir_tput="$output_dir/throughput"
mkdir -p $output_dir
mkdir -p $output_dir_tput
done_file="$output_dir/done.txt"
if [ "$RESUME" != "1" ]; then
    rm -f outputs/stats.csv "$done_file"
fi

## transport config
twosided_config="$config TRANSPORT=TWO_SIDED COLLECT_LATENCY=false"
onesided_config="$config TRANSPORT=ONE_SIDED COLLECT_LATENCY=false BATCH=0"

## workload config
num_wh="4 16 64"

## compute thread config
threads="1 4 8 16 32 40"

## cc schemes
cc_one="NO_WAIT WAIT_DIE"
cc_two="NO_WAIT WAIT_DIE WOUND_WAIT"

for wh in $num_wh; do
    echo "Starting workload with $wh warehouses"

    for thd in $threads; do
        echo "Starting with $thd client threads"

        for cc in $cc_one; do
            param="$onesided_config NUM_WH=$wh NUM_CLIENT_THREADS=$thd CC_ALG=$cc"
            run_experiment "$param"
        done

        for cc in $cc_two; do
            param="$twosided_config NUM_WH=$wh NUM_CLIENT_THREADS=$thd CC_ALG=$cc BATCH=1"
            run_experiment "$param"
        done
    done
    # all configs of this warehouse count are done (a resumed run may find them already moved)
    if [ -f outputs/stats.csv ]; then
        mv outputs/stats.csv $output_dir_tput/${wh}.csv
    fi
done
echo "SWEEP_DONE"
