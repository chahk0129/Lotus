#!/bin/bash

IFCONFIG_FILE="ifconfig.txt"

# check ifconfig file
if [ ! -f $IFCONFIG_FILE ]; then
	echo "Error: $IFCONFIG_FILE not found!";
	exit 1
fi

# parse client hosts from ifconfig file
echo "Parsing client hosts from $IFCONFIG_FILE"
hosts=()
client_section=false

while IFS= read -r line; do
	# skip comments
	if [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]]; then
		continue
	fi

	# check for client section
	if [[ "$line" == "=c" ]]; then
		client_section=true
		continue
	elif [[ "$line" =~ ^= ]]; then
		client_section=false
		continue
	fi

	# add client hosts
	if [ "$client_section" = true ]; then
		hosts+=("$line")
	fi
done < "$IFCONFIG_FILE"

if [ ${#hosts[@]} -eq 0 ]; then
	echo "Error: No client hosts found in $IFCONFIG_FILE"
	exit 1
fi

echo "Found ${#hosts[@]} client hosts."
for i in "${!hosts[@]}"; do
	echo "Client $i: ${hosts[$i]}"
done


# set up NTP configuration
## ntp master (current node should be the master)
master_host="${hosts[0]}"
current_host=$(hostname)
if [[ "$current_host" == "$master_host" ]]; then
	echo "Configuring NTP master: $current_host"
	sudo cp scripts/master.conf /etc/ntp.conf
	sudo hwclock --systohc
else
	echo "Error: Current host ($current_host) is not the designated NTP master ($master_host)."
	exit 1
fi

## ntp slaves
cur_dir=$(pwd)
slave_pids=()
if [ ${#hosts[@]} -gt 1 ]; then
	for ((i=1; i<${#hosts[@]}; i++)); do
		slave_host="${hosts[$i]}"
		echo "Configuring NTP slave:  $slave_host"
		ssh "$slave_host" "
			cd $cur_dir &&
			sudo cp scripts/slave.conf /etc/ntp.conf && 
			sudo hwclock --systohc
		" &

		slave_pids+=($!)
	done

	for pid in ${slave_pids[@]}; do
		wait $pid
	done
fi

## restart ntp service
sudo systemctl restart ntp
for ((i=1; i<${#hosts[@]}; i++)); do
	slave_host="${hosts[$i]}"
	ssh "$slave_host" "sudo systemctl restart ntp" &
done
wait