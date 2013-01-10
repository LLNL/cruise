#!/bin/bash

#MOAB variabes
#MSUB -l nodes=64k
#MSUB -l walltime=60:00
##MSUB -A asccasc

#configuration parameters\
export MIN_NUM_NODES=$SLURM_NNODES
export MAX_NUM_NODES=$SLURM_NNODES
export iters=1

export OUTPUT_DIR=$PWD/logs_check_debug_procs
mkdir -p $OUTPUT_DIR

export OMP_NUM_THREADS=1
export OMP_STACK_SIZE=16M
export OMP_WAIT_POLICY=active
export XLSMPOPTS="spins=0:yields=0"
export BG_PERSISTMEMRESET=1
export BG_PERSISTMEMSIZE=8192
export BG_SMP_FAST_WAKEUP=yes
export BG_THREADLAYOUT=1
#export BG_SHAREDMEMSIZE=64
export BG_SHAREDMEMSIZE=128
export BG_MAPCOMMONHEAP=1
export BG_MAPNOALIASES=0
export BG_MAPALIGN16=0
export PAMI_BGQ_NODE_L2ATOMICSIZE=65536
export PAMI_DEVICE=M
export PAMID_VERBOSE=0
export PAMID_CONTEXT_MAX=1

bench=memcpy_check_debug
export BG_PERSISTMEMSIZE=0
export BENCHMARK_PARAMS="10000000 10 0" # 128 MB
export PROCS_PER_NODE=1
    for (( i = $MIN_NUM_NODES; i <= $MAX_NUM_NODES; i = i*2 ))
    do
        totprocs=$(($i * $PROCS_PER_NODE))
        for (( j = 0; j < $iters; j++ ))
        do
            echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
            echo "srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j"
            srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j
        done

        # insert a newline to make output easier to read
        echo ""
    done
export PROCS_PER_NODE=2
    for (( i = $MIN_NUM_NODES; i <= $MAX_NUM_NODES; i = i*2 ))
    do
        totprocs=$(($i * $PROCS_PER_NODE))
        for (( j = 0; j < $iters; j++ ))
        do
            echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
            echo "srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j"
            srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j
        done

        # insert a newline to make output easier to read
        echo ""
    done
export PROCS_PER_NODE=4
    for (( i = $MIN_NUM_NODES; i <= $MAX_NUM_NODES; i = i*2 ))
    do
        totprocs=$(($i * $PROCS_PER_NODE))
        for (( j = 0; j < $iters; j++ ))
        do
            echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
            echo "srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j"
            srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j
        done

        # insert a newline to make output easier to read
        echo ""
    done
export PROCS_PER_NODE=8
    for (( i = $MIN_NUM_NODES; i <= $MAX_NUM_NODES; i = i*2 ))
    do
        totprocs=$(($i * $PROCS_PER_NODE))
        for (( j = 0; j < $iters; j++ ))
        do
            echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
            echo "srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j"
            srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j
        done

        # insert a newline to make output easier to read
        echo ""
    done

export PROCS_PER_NODE=16
    for (( i = $MIN_NUM_NODES; i <= $MAX_NUM_NODES; i = i*2 ))
    do
        totprocs=$(($i * $PROCS_PER_NODE))
        for (( j = 0; j < $iters; j++ ))
        do
            echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
            echo "srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j"
            srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j
        done

        # insert a newline to make output easier to read
        echo ""
    done

export PROCS_PER_NODE=32
    for (( i = $MIN_NUM_NODES; i <= $MAX_NUM_NODES; i = i*2 ))
    do
        totprocs=$(($i * $PROCS_PER_NODE))
        for (( j = 0; j < $iters; j++ ))
        do
            echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
            echo "srun -N $i -n $totprocs -O $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j"
            srun -N $i -n $totprocs -O $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j
        done

        # insert a newline to make output easier to read
        echo ""
    done

export PROCS_PER_NODE=64
    for (( i = $MIN_NUM_NODES; i <= $MAX_NUM_NODES; i = i*2 ))
    do
        totprocs=$(($i * $PROCS_PER_NODE))
        for (( j = 0; j < $iters; j++ ))
        do
            echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
            echo "srun -N $i -n $totprocs -O $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j"
            srun -N $i -n $totprocs -O $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j
        done

        # insert a newline to make output easier to read
        echo ""
    done
