#!/bin/bash

#MOAB variabes
#MSUB -l nodes=1
#MSUB -l walltime=30:00
##MSUB -A asccasc

#configuration parameters\
export MIN_NUM_NODES=$SLURM_NNODES
export MAX_NUM_NODES=$SLURM_NNODES
export iters=1

export OUTPUT_DIR=$PWD/logs
mkdir -p $OUTPUT_DIR

export OMP_NUM_THREADS=1
export OMP_STACK_SIZE=16M
export OMP_WAIT_POLICY=active
export XLSMPOPTS="spins=0:yields=0"
export BG_PERSISTMEMRESET=1
export BG_PERSISTMEMSIZE=8192
export BG_SMP_FAST_WAKEUP=yes
export BG_THREADLAYOUT=1
export BG_SHAREDMEMSIZE=64
export BG_MAPCOMMONHEAP=1
export BG_MAPNOALIASES=0
export BG_MAPALIGN16=0
export PAMI_BGQ_NODE_L2ATOMICSIZE=65536
export PAMI_DEVICE=M
export PAMID_VERBOSE=0
export PAMID_CONTEXT_MAX=1

bench=memcpy
export BG_PERSISTMEMSIZE=0
export BENCHMARK_PARAMS="100000000 10 0" # 128 MB
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

export BENCHMARK_PARAMS="50000000 10 0" # 128 MB
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

bench=memcpy_1
export BG_PERSISTMEMSIZE=0
export BENCHMARK_PARAMS="100000000 10 0" # 128 MB
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

export BENCHMARK_PARAMS="50000000 10 0" # 128 MB
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

bench=scrmfs
export SCRMFS_CHUNK_BITS=22
export SCRMFS_CHUNK_MEM=128MB
export BG_PERSISTMEMSIZE=8192
export BENCHMARK_PARAMS="100000000 10 0" # 128 MB
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

export BENCHMARK_PARAMS="50000000 10 0" # 128 MB
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

export BENCHMARK_PARAMS="50000000 10 0" # 128 MB
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

export BG_PERSISTMEMSIZE=9000
export BENCHMARK_PARAMS="50000000 10 0" # 128 MB
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

bench=ramdisk
export BG_PERSISTMEMSIZE=8192
export BENCHMARK_PARAMS="100000000 3 0" # 128 MB
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

export BENCHMARK_PARAMS="50000000 3 0" # 128 MB
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

export BENCHMARK_PARAMS="50000000 3 0" # 128 MB
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

export BG_PERSISTMEMSIZE=9000
export BENCHMARK_PARAMS="50000000 3 0" # 128 MB
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

