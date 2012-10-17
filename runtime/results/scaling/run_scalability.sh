#!/bin/bash

#MOAB variabes
#MSUB -l nodes=1
#MSUB -l walltime=30:00
#MSUB -A asccasc

#configuration parameters\
export PROCS_PER_NODE=16
export MAX_NUM_NODES=1
export TEST_MEMCPY_SRC=$PWD/test_memcpy.c
export TEST_MOD_RAMDISK_SRC=$PWD/test_ramdisk.c
export OUTPUT_DIR=$PWD/logs
export CC=mpiicc
export SCRMFS_INSTALL_DIR=../../install
export CONTAINER_DIR=../../container
export LD_LIBRARY_PATH=$CONTAINER_DIR/lib:$LD_LIBRARY_PATH
export IPC_CLEANUP=../../ipc_cleanup
export PRE_SCRMFS_FLAGS=`$SCRMFS_INSTALL_DIR/bin/scrmfs-config --pre-ld-flags`
export POST_SCRMFS_FLAGS=`$SCRMFS_INSTALL_DIR/bin/scrmfs-config --post-ld-flags`

export RUN_BENCHMARKS="ramdisk memcpy scrmfs " 
export BENCHMARK_DIR=$PWD
#export BENCHMARK_PARAMS="104857600 5 0" # 100 MB
export BENCHMARK_PARAMS="134217728 5 0" # 128 MB

. /usr/local/tools/dotkit/init.sh
use -q ic-11.1.046 

srun -n $MAX_NUM_NODES -N $MAX_NUM_NODES hostname

#build benchmarks
for bench in `echo $RUN_BENCHMARKS`
do
    if [ "$bench" = "scrmfs" ]
    then
        #compile scrmfs-aware test_ramdisk
        echo "building test_$bench.."
        $CC $PRE_SCRMFS_FLAGS -L$CONTAINER_DIR/lib -O3 -o test_$bench $BENCHMARK_DIR/test_ramdisk.c $POST_SCRMFS_FLAGS -lcontainer
    else
        #compile native benchmarks
        echo "building test_$bench.."
        $CC -O3 -o test_$bench $BENCHMARK_DIR/test_$bench.c
    fi

done

#cleanup nodes before running
#srun -n $MAX_NUM_NODES -N $MAX_NUM_NODES $IPC_CLEAUP

#run benchmarks
for bench in `echo $RUN_BENCHMARKS`
do
    for (( i=1, totprocs=$PROCS_PER_NODE ; i <= $MAX_NUM_NODES; i = i*2, totprocs = i*$PROCS_PER_NODE ))
    do
        for (( j=0; j<3; j++ ))
        do
            echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
            echo "srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j"
            srun -N $i -n $totprocs $PWD/test_$bench $BENCHMARK_PARAMS  > $OUTPUT_DIR/$bench-n$totprocs-N$i-iter$j
            #cleanup all nodes
            echo "srun -n $MAX_NUM_NODES -N $MAX_NUM_NODES $IPC_CLEANUP"
            srun -n $MAX_NUM_NODES -N $MAX_NUM_NODES $IPC_CLEANUP
        done
    done
done
