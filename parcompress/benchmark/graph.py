from collections import defaultdict
from pprint import pprint
import matplotlib.pyplot as plt

def create_graph(file):
    seq = defaultdict(lambda: {"time": [], "average": 0})
    par = defaultdict(lambda: defaultdict(lambda: {"time": [], "average": 0, "speedup": 1}))
    parws = defaultdict(lambda: defaultdict(lambda: {"time": [], "average": 0, "speedup": 1}))
    # Open file
    with open(file, "r") as f:
        # Iterate through each line 
        for line in f:
            line = line.strip()
            line_arr = line.split(",")
            run_type = line_arr[0]
            file_size = line_arr[1]
            # Sequential run
            if run_type == 's':
                time = float(line_arr[2])
                # Append time
                seq[file_size]["time"].append(time)
                # Calculate average time
                if len(seq[file_size]["time"]) == 5:
                    seq[file_size]["average"] = round(sum(seq[file_size]["time"]) / 5, 2)
            # Parallel run
            elif run_type == 'p':
                threads = int(line_arr[2])
                time = float(line_arr[3])
                # Append time 
                par[file_size][threads]["time"].append(time)
                # Calculate average time 
                if len(par[file_size][threads]["time"]) == 5:
                    par[file_size][threads]["average"] = round(sum(par[file_size][threads]["time"]) / 5, 2)
            # Parallel run with work stealing
            else:
                threads = int(line_arr[2])
                time = float(line_arr[3])
                # Append time 
                parws[file_size][threads]["time"].append(time)
                # Calculate average time 
                if len(parws[file_size][threads]["time"]) == 5:
                    parws[file_size][threads]["average"] = round(sum(parws[file_size][threads]["time"]) / 5, 2)
                        
    # Run this to update speedup in a seperate loop to ensure the all averages 
    # are calculated first
    for file_size in seq:
        for threads in par[file_size]:
            par[file_size][threads]["speedup"] = round(seq[file_size]["average"] / par[file_size][threads]["average"], 2)
            parws[file_size][threads]["speedup"] = round(seq[file_size]["average"] / parws[file_size][threads]["average"], 2)
    
    # Commend out below to check data
    # pprint(seq)
    # pprint(par)
    
    # Create speed up graph
    plt.figure(figsize=(10, 7))
    # Plot a line on each file size with varying number of threads
    for file_size in par:
        threads = []
        speedup = []
        for thread_count in par[file_size]:
            threads.append(int(thread_count))
            speedup.append(par[file_size][thread_count]["speedup"])
        # Plot the line for this file size
        plt.plot(threads, speedup, marker='o', label=file_size + " (P)")
    for file_size in parws:
        threads = []
        speedup = []
        for thread_count in parws[file_size]:
            threads.append(int(thread_count))
            speedup.append(parws[file_size][thread_count]["speedup"])
        # Plot the line for this file size
        plt.plot(threads, speedup, marker='x', label=file_size + " (PWS)")
    plt.title('Speedup Graph')
    plt.xlabel('Threads')
    plt.ylabel('Speedup')
    plt.grid(True)
    plt.legend(title='File Size')
    plt.savefig('speedup.png')

# Call the function to create the graph
create_graph("slurm/out/measurements.txt")