import os, sys, re, os.path
import subprocess, json
import threading
import multiprocessing
import time
import socket
import pandas as pd
import numpy as np
import signal

dbms_cfg = ["config-std.h", "config.h"]
MAX_RUNTIME = 60 * 60 # 1 hour

def print_usage():
    print("Usage: path/to/json/config [args]")
    exit(1)

def load_environment(ifconfig):
    with open(ifconfig, 'r') as f:
        content = f.read()

    servers = []
    clients = []
    section = None
    for line in content.strip().split('\n'):
        l = line.strip()

        # skip empty lines and comments
        if not l or l.startswith('#'):
            continue
        
        # check for section markers
        if l == '=s':
            section = 'servers'
            continue
        elif l == '=c':
            section = 'clients'
            continue

        # add hostnames
        if section == 'servers':
            servers.append(l)
        elif section == 'clients':
            clients.append(l)
    return {'servers': servers, 'clients': clients}

def load_cmd(env):
    cmds = []
    localhost = socket.gethostname()
    cur_dir = os.getcwd()
    for node in env['servers']:
        if node == localhost:
            cmd = "cd {}; ./run_memory > {} 2>&1".format(cur_dir + "/build", localhost + "_memory.txt")
        else:
            cmd = "ssh {} 'cd {}; ./run_memory > {} 2>&1'".format(node, cur_dir + "/build", node + "_memory.txt")
        cmds.append(cmd)
    for node in env['clients']:
        if node == localhost:
            cmd = "cd {}; ./run_compute > {} 2>&1".format(cur_dir + "/build", localhost + "_compute.txt")
        else:
            cmd = "ssh {} 'cd {}; ./run_compute > {} 2>&1'".format(node, cur_dir + "/build", node + "_compute.txt")
        cmds.append(cmd)
    return cmds

def replace(fname, pattern, replacement):
    f = open(fname)
    s = f.read()
    f.close()
    s = re.sub(pattern, replacement, s)
    f = open(fname, 'w')
    f.write(s)
    f.close()

def check_job(job, arg):
    return ((arg in job) and (job[arg] == "true"))

def set_ndebug(ndebug):
    f = open("system/global.h", "r")
    content = f.read()
    f.close()
    
    # Check if #define NDEBUG is already present (and not commented out)
    ndebug_already_defined = False
    for line in content.split('\n'):
        if line.strip().startswith("#define NDEBUG"):
            ndebug_already_defined = True
            break
    
    # If NDEBUG is already defined and we want it, do nothing
    if ndebug_already_defined and ndebug:
        return
    
    # If NDEBUG is not defined and we don't want it, do nothing  
    if not ndebug_already_defined and not ndebug:
        return
    
    # Only make changes if current state doesn't match desired state
    if ndebug and not ndebug_already_defined:
        # Add #define NDEBUG
        replace("system/global.h", r"#include <cassert>", "#define NDEBUG\n#include <cassert>")
    elif not ndebug and ndebug_already_defined:
        # Remove #define NDEBUG line
        replace("system/global.h", r"#define NDEBUG\n", "")

def parse_input(args):
    idx = 0
    fname = args[idx]
    if ".json" not in fname:
        print_usage()
    idx += 1
    job = json.load(open(fname, 'r'))
    if len(args) > idx:
        # further parse args to override json settings
        for item in args[idx:]:
            key = item.split('=')[0]
            value = item.split('=')[1]
            job[key] = value
    ndebug = check_job(job, "NDEBUG")
    set_ndebug(ndebug)
    return job

def compile(job):
    os.system("cp {} {}".format(dbms_cfg[0], dbms_cfg[1]))
    for (param, value) in job.items():
        pattern = r"\#define\s*" + re.escape(param) + r'.*'
        replacement = "#define " + param + ' ' + str(value)
        replace(dbms_cfg[1], pattern, replacement)

    cur_dir = os.getcwd()
    os.chdir("build")
    os.system("make clean > compile.txt 2>&1")
    os.system("cmake .. -DCMAKE_BUILD_TYPE=release >> compile.txt 2>&1")
    ret = os.system("make -j > compile.txt 2>&1")
    if ret != 0:
        print("Compilation failed! Check compile.txt for details.")
        exit(1)
    os.system("rm -f compile.txt")
    os.chdir(cur_dir)

def kill(env):
    print("Killing all processes...")
    os.system("bash scripts/kill.sh")
    # localhost = socket.gethostname()
    # all_nodes = env['servers'] + env['clients']
    # for node in all_nodes:
    #     try:
    #         if node == localhost: # kill local process
    #             os.system("pkill -f run_memory")
    #             os.system("pkill -f run_compute")
    #         else: # kill remote process
    #             os.system("ssh {} 'pkill -f run_memory && pkill -f run_compute' 2>/dev/null &".format(node))
    #     except Exception as e:
    #         print("Failed to kill processes on node {}: {}".format(node, str(e)))

def run(cmds, env):
    procs = []
    for cmd in cmds:
        p = subprocess.Popen(cmd, shell=True)
        procs.append(p)

    # monitor processes
    start_time = time.time()
    error_occurred = False
    while error_occurred == False:
        running = [p for p in procs if p.poll() is None]
        if not running:
            break # all processes have finished

        time.sleep(2)
        # check return codes
        for p in procs:
            if p.poll() is not None: # process has finished
                if p.returncode != 0:
                    print(f"Error: A process has exited with non-zero return code: {p.returncode}")
                    error_occurred = True

        # check for timeout
        if (error_occurred == False):
            elapsed_time = time.time() - start_time
            if elapsed_time > MAX_RUNTIME:
                print("Error: Maximum runtime exceeded ({} seconds).".format(MAX_RUNTIME))
                error_occurred = True

    if error_occurred == True:
        for p in procs:
            if p.poll() is None:
                try:
                    p.terminate()
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
        kill(env)

    return error_occurred
    # for p in procs:
    #     p.wait()

def str_to_num(s):
    try:
        if '.' in s or 'e' in s.lower():
            return float(s)
        else:
            return int(s)
    except:
        return s

def parse_output(job, env):
    cur_dir = os.getcwd()
    os.chdir("build")
    for node in env['clients']:
        fname = node + "_compute.txt"
        output = open(fname)
        success = False
        for line in output:    
            if success == True:
                if ":" in line:
                    data = line.split(':')
                    key = data[0].strip()
                    data[1] = data[1].strip()
                    value = str_to_num(re.split(r'\s+', data[1])[0])

                    if key in job:
                        job[key] += value
                    else:
                        job[key] = value
            else:
                if "=Stats=" in line:
                    success = True
        output.close()
        if not success:
            print("Job failed on client {}".format(node))
        # else:
            # os.system("rm -f {}".format(fname))
    for node in env['servers']:
        fname = node + "_memory.txt"
        os.system("rm -f {}".format(fname))
    os.chdir(cur_dir)


def store_output(job, fname=None):
    if fname is None:
        fname = "outputs/stats.csv"
    append = os.path.exists(fname)
    df = pd.DataFrame([job])
    df.to_csv(fname, mode='a', header=not append, index=False)
    print("Throughput: {} ops/sec".format(job["Throughput"]))

def store_failed_parameters(job, fname=None):
    if fname is None:
        fname = "outputs/failed_params.txt"
    os.makedirs("outputs", exist_ok=True)
    param_str = " ".join([f"{k}={v}" for k, v in job.items()])
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(fname, 'a') as f:
        f.write(f"[{timestamp}] FAILED: {param_str}\n")
    
if __name__ == "__main__":
    ifconfig = "ifconfig.txt"
    if len(sys.argv) < 2:
        print_usage()

    job = parse_input(sys.argv[1:]) # prase arguments
    compile(job) # compile the code given the job settings

    env = load_environment(ifconfig) # load distributed system environment
    cmds = load_cmd(env) # load commands to run

    ret = run(cmds, env) # run the commands
    if ret == False:
        parse_output(job, env) # parse output files to extract metrics
        store_output(job)
    else:
        print("Experiment failed due to errors in execution ...")
        store_failed_parameters(job)
        sys.exit(1)
