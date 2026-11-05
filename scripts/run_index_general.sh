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
output_dir="outputs/index_general"
output_dir_tput="$output_dir/throughput"
mkdir -p $output_dir
mkdir -p $output_dir_tput
rm outputs/stats.csv

## transport config
twosided_config="$config TRANSPORT=TWO_SIDED COLLECT_LATENCY=false PARTITIONED=true"
onesided_config="$config TRANSPORT=ONE_SIDED COLLECT_LATENCY=false PARTITIONED=true"

## workload config
workload_a="READ_PERC=0.5 UPDATE_PERC=0.5 INSERT_PERC=0.0 SCAN_PERC=0.0"
workload_b="READ_PERC=0.95 UPDATE_PERC=0.05 INSERT_PERC=0.0 SCAN_PERC=0.0"
workload_c="READ_PERC=1.0 UPDATE_PERC=0.0 INSERT_PERC=0.0 SCAN_PERC=0.0"
declare -A workloads
workloads["a"]="$workload_a"
workloads["b"]="$workload_b"
workloads["c"]="$workload_c"

## compute thread config
threads="1 4 8 16 32 40"

for wk_name in "${!workloads[@]}"; do
    wk="${workloads[$wk_name]}"
    echo "Starting workload $wk_name"

    for thd in $threads; do
        echo "Starting with $thd client threads"

        ## one-sided
        param="$onesided_config $wk NUM_CLIENT_THREADS=$thd"
        run_experiment "$param"

        ## two-sided
        param="$twosided_config $wk NUM_CLIENT_THREADS=$thd"
        run_experiment "$param"

    done
    mv outputs/stats.csv $output_dir_tput/${wk_name}.csv
done