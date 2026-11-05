#!/bin/bash

# cityhash (for Sherman / DEFT)
git clone https://github.com/google/cityhash.git
cd cityhash
./configure
make all -j && make install -j
rm -rf cityhash

boost="libboost-system-dev libboost-coroutine-dev"
memcached="libmemcached-dev memcached"
jemalloc="libjemalloc-dev libjemalloc2"
others="cmake python3 python3-pip"

packages="$boost $memcached $jemalloc $others"
sudo apt install -y $packages

# for script runs
pip3 install pandas numpy
