#!/bin/bash
#
#SBATCH --mail-user=XXX@XXX.com
#SBATCH --mail-type=ALL
#SBATCH --job-name=parcompress_benchmark
#SBATCH --output=/parallel-compression/parcompress/benchmark/slurm/out/measurements.txt
#SBATCH --error=/parallel-compression/parcompress/benchmark/slurm/err/measurements_err.txt
#SBATCH --chdir=/parallel-compression/parcompress/benchmark
#SBATCH --partition=fast
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --mem-per-cpu=900
#SBATCH --exclusive
#SBATCH --time=2:00:00

module load golang/1.22

PROGRAM_PATH="/parallel-compression/parcompress/process/process.go"
SIZES=("small" "medium" "large")
THREADS=(2 4 6 8 12)

for size in "${SIZES[@]}"
do
    for i in {1..5}
    do
        output=$(go run $PROGRAM_PATH $size s)
        echo "s,$size,$output"
    done
done

for size in "${SIZES[@]}"
do
    for threads in "${THREADS[@]}"
    do
        for i in {1..5}
        do
            output=$(go run $PROGRAM_PATH $size p $threads false)
            echo "p,$size,$threads,$output"
        done
    done
done

for size in "${SIZES[@]}"
do
    for threads in "${THREADS[@]}"
    do
        for i in {1..5}
        do
            output=$(go run $PROGRAM_PATH $size p $threads true)
            echo "pws,$size,$threads,$output"
        done
    done
done

sleep 10

python graph.py