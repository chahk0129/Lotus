#!/bin/bash

IFCONFIG_FILE="ifconfig.txt"
PROCESS_NAMES="run_memory run_compute"

# Check if ifconfig.txt exists
if [ ! -f "$IFCONFIG_FILE" ]; then
    echo "Error: $IFCONFIG_FILE not found!"
    exit 1
fi

# Parse all hosts from ifconfig.txt (both servers and clients)
echo "Parsing hosts from $IFCONFIG_FILE..."
all_hosts=()
reading_section=false

while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]]; then
        continue
    fi
    
    # Check for section markers (=s for servers, =c for clients)
    if [[ "$line" =~ ^= ]]; then
        reading_section=true
        continue
    fi
    
    # Add hosts to array if we're reading a section
    if [ "$reading_section" = true ]; then
        # Avoid duplicates
        if [[ ! " ${all_hosts[*]} " =~ " ${line} " ]]; then
            all_hosts+=("$line")
        fi
    fi
done < "$IFCONFIG_FILE"

# Check if we found any hosts
if [ ${#all_hosts[@]} -eq 0 ]; then
    echo "Error: No hosts found in $IFCONFIG_FILE"
    exit 1
fi

echo "Found ${#all_hosts[@]} unique host(s):"
for i in "${!all_hosts[@]}"; do
    echo "  [$i] ${all_hosts[$i]}"
done

echo ""
echo "Killing processes: $PROCESS_NAMES"

# Get current hostname for comparison
current_hostname=$(hostname)
current_fqdn=$(hostname -f)

# Function to check if host is current machine
is_current_machine() {
    local host="$1"
    [[ "$host" == "$current_hostname" || "$host" == "$current_fqdn" ]]
}

# Function to kill processes on a host
kill_processes_on_host() {
    local host="$1"
    local is_local="$2"
    
    for process in $PROCESS_NAMES; do
        if [ "$is_local" = true ]; then
            echo "Killing $process on current machine ($host)..."
            pkill -f "$process" 2>/dev/null && echo "✓ Killed $process" || echo "- No $process process found"
        else
            echo "Killing $process on $host..."
            ssh "$host" "pkill -f '$process' 2>/dev/null && echo '✓ Killed $process on $host' || echo '- No $process process found on $host'" 2>/dev/null &
        fi
    done
}

# Kill processes on current machine if it's in the list
current_machine_found=false
for host in "${all_hosts[@]}"; do
    if is_current_machine "$host"; then
        kill_processes_on_host "$host" true
        current_machine_found=true
        break
    fi
done

# Kill processes on remote machines
ssh_pids=()
for host in "${all_hosts[@]}"; do
    if ! is_current_machine "$host"; then
        kill_processes_on_host "$host" false
        ssh_pids+=($!)
    fi
done

# Wait for all SSH commands to complete
if [ ${#ssh_pids[@]} -gt 0 ]; then
    echo "Waiting for all remote kill commands to complete..."
    for pid in "${ssh_pids[@]}"; do
        wait "$pid"
    done
fi

sleep 3
echo ""
echo "Process termination complete!"