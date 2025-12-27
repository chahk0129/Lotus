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
output_dir="outputs/ycsb_cache"
output_dir_tput="$output_dir/throughput"
output_dir_latency="$output_dir/latency"
mkdir -p $output_dir
mkdir -p $output_dir_tput
mkdir -p $output_dir_latency
rm outputs/stats.csv

## transport config
twosided_config="$config TRANSPORT=TWO_SIDED COLLECT_LATENCY=true"
onesided_config="$config TRANSPORT=ONE_SIDED COLLECT_LATENCY=true BATCH=0"

## workload config
workload_a="READ_PERC=0.5 UPDATE_PERC=0.5 INSERT_PERC=0.0 SCAN_PERC=0.0"
workload_b="READ_PERC=0.95 UPDATE_PERC=0.05 INSERT_PERC=0.0 SCAN_PERC=0.0"
workload_c="READ_PERC=1.0 UPDATE_PERC=0.0 INSERT_PERC=0.0 SCAN_PERC=0.0"
declare -A workloads
workloads["a"]="$workload_a"
workloads["b"]="$workload_b"
workloads["c"]="$workload_c"

## cache config
cache_size="0 160"

## cc schemes
cc_one="NO_WAIT WAIT_DIE"
cc_two="NO_WAIT WAIT_DIE WOUND_WAIT"

for wk_name in "${!workloads[@]}"; do
    wk="${workloads[$wk_name]}"
    echo "Starting workload $wk_name"

    for cache in $cache_size; do
        echo "Starting with $cache MB cache"

        for cc in $cc_one; do
            param="$onesided_config $wk CACHE_SIZE=$cache CC_ALG=$cc"
            run_experiment "$param"
            mv build/cdf.txt $output_dir_latency/${wk_name}_cache${cache}_${cc}_onesided.txt
        done

        for cc in $cc_two; do
            param="$twosided_config $wk CACHE_SIZE=$cache CC_ALG=$cc BATCH=1"
            run_experiment "$param"
            mv build/cdf.txt $output_dir_latency/${wk_name}_cache${cache}_${cc}_twosided.txt
        done
    done
    mv outputs/stats.csv $output_dir_tput/${wk_name}.csv
done