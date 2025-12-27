#!/bin/bash

scripts=(
    "run_ycsb_general_tput.sh"
    "run_ycsb_general_latency.sh"
    "run_ycsb_request.sh"
    "run_ycsb_cache.sh"
    "run_ycsb_incremental.sh"
    "run_index_general.sh"
    "run_index_cache.sh"
)

for script in "${scripts[@]}"; do
    echo "Running $script ..."
    bash scripts/ntp.sh
    bash scripts/$script
done