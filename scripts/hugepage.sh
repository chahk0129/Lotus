#!/bin/bash
IFCONFIG_FILE="ifconfig.txt"
HUGEPAGE=32768

# Check if ifconfig.txt exists
if [ ! -f "$IFCONFIG_FILE" ]; then
    echo "Error: $IFCONFIG_FILE not found!"
    exit 1
fi

# Parse all hosts from ifconfig.txt (both servers and clients)
echo "Parsing hosts from $IFCONFIG_FILE ..."
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
echo "Setting hugepages to $HUGEPAGE on all nodes..."

# Set hugepages on current machine
current_hostname=$(hostname)
echo "Setting hugepages on current machine ($current_hostname)..."
sudo sysctl -w vm.nr_hugepages=$HUGEPAGE

# Set hugepages on remote machines
ssh_pids=()
for host in "${all_hosts[@]}"; do
    if [[ "$host" != "$current_hostname" ]]; then
        echo "Setting hugepages on $host..."
        ssh "$host" "sudo sysctl -w vm.nr_hugepages=$HUGEPAGE" &
        ssh_pids+=($!)
    fi
done

# Wait for all SSH commands to complete
if [ ${#ssh_pids[@]} -gt 0 ]; then
    echo "Waiting for all remote configurations to complete..."
    for pid in "${ssh_pids[@]}"; do
        wait "$pid"
    done
fi
