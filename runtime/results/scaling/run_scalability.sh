#!/bin/bash

#configuration parameters\
export PROCS_PER_NODE=8
export MAX_NUM_NODES=8
export TEST_MEMCPY_SRC=$PWD/test_memcpy.c
export TEST_MOD_RAMDISK_SRC=$PWD/test_ramdisk.c
export OUTPUT_DIR=$PWD/logs
export CC=mpicc
export SCRMFS_INSTALL_DIR=/home/rajachan/memfs/runtime/install
export IPC_CLEANUP=/home/rajachan/memfs/runtime/ipc_cleanup
export PRE_SCRMFS_FLAGS=`$SCRMFS_INSTALL_DIR/bin/scrmfs-config --pre-ld-flags`
export POST_SCRMFS_FLAGS=`$SCRMFS_INSTALL_DIR/bin/scrmfs-config --post-ld-flags`

export RUN_BENCHMARKS="ramdisk memcpy" #scrmfs " 
export BENCHMARK_DIR=$PWD


#build benchmarks
for bench in `echo $RUN_BENCHMARKS`
do
    if [ "$bench" = "scrmfs" ]
    then
        #compile scrmfs-aware test_ramdisk
        echo "building test_$bench.."
        $CC $PRE_SCRMFS_FLAGS -O3 -o test_$bench $test_scrmfs_src $POST_SCRMFS_FLAGS
    else
        #compile native benchmarks
        echo "building test_$bench.."
        $CC -O3 -o test_$bench $BENCHMARK_DIR/test_$bench.c
    fi

done

#run benchmarks
for bench in `echo $RUN_BENCHMARKS`
do
    for (( i=1, totprocs=$PROCS_PER_NODE ; i <= $MAX_NUM_NODES; i = i*2, totprocs = i*$PROCS_PER_NODE ))
    do
        echo "Running test_$bench on $i nodes ($totprocs ranks); $PROCS_PER_NODE procs/node"
        # run all sruns in background, so that it gets queued if nodes are not available
        srun -N $i -n $totprocs $PWD/test_$bench > $OUTPUT_DIR/$bench-n$totprocs-N$i &
        # clearup shm segments
        $IPC_CLEANUP
    done
done
