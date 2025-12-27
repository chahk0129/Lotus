#!/bin/bash

# Function to run experiments with given parameters
# Usage: run_experiment <config_parameters>
run_experiment() {
    local params="$*"
    echo "Running experiment with params: $params"
    python run.py $params
}

# Run YCSB benchmark for disaggregated memory DB
config="exp_profile/ycsb.json"
output_dir="outputs/ycsb_request"
output_dir_tput="$output_dir/throughput_onesided"
mkdir -p $output_dir
mkdir -p $output_dir_tput
rm outputs/stats.csv

## transport config
twosided_config="$config TRANSPORT=TWO_SIDED COLLECT_LATENCY=false"
onesided_config="$config TRANSPORT=ONE_SIDED COLLECT_LATENCY=false BATCH=0"

## workload config
workload_a="READ_PERC=0.5 UPDATE_PERC=0.5 INSERT_PERC=0.0 SCAN_PERC=0.0"
workload_b="READ_PERC=0.95 UPDATE_PERC=0.05 INSERT_PERC=0.0 SCAN_PERC=0.0"
workload_c="READ_PERC=1.0 UPDATE_PERC=0.0 INSERT_PERC=0.0 SCAN_PERC=0.0"
declare -A workloads
workloads["a"]="$workload_a"
workloads["b"]="$workload_b"
workloads["c"]="$workload_c"

## request config
request="1 2 4 8 16 32"

## cc schemes
cc_one="NO_WAIT WAIT_DIE"
cc_two="NO_WAIT WAIT_DIE WOUND_WAIT"

for wk_name in "${!workloads[@]}"; do
    wk="${workloads[$wk_name]}"
    echo "Starting workload $wk_name"

    for req in $request; do
        echo "Starting with $req requests"

        for cc in $cc_one; do
            param="$onesided_config $wk REQ_PER_QUERY=$req CC_ALG=$cc"
            run_experiment "$param"
        done

        for cc in $cc_two; do
            param="$twosided_config $wk REQ_PER_QUERY=$req CC_ALG=$cc BATCH=1 TXN_TYPE=STORED_PROCEDURE"
            run_experiment "$param"
        done
    done
    mv outputs/stats.csv $output_dir_tput/${wk_name}.csv
done