---
# CRUISE: Checkpoint-Restart In User-SpacE
[![Build Status](https://travis-ci.org/LLNL/cruise.svg?branch=master)](https://travis-ci.org/LLNL/cruise)
---

With the massive scale of high-performance computing systems, long-running
scientific parallel applications periodically save the state of their execution
to files called checkpoints to recover from system failures.  Checkpoints are
stored on external parallel file systems, but limited bandwidth makes this a
time-consuming operation.  Multilevel checkpointing systems, like the Scalable
Checkpoint/Restart (SCR) library, alleviate this bottleneck by caching
checkpoints in storage located close to the compute nodes.  However, most large
scale systems do not provide file storage on compute nodes, preventing the use
of SCR.

CRUISE is a novel user-space file system that stores data in main memory and
transparently spills over to other storage, like local flash memory or the
parallel file system, as needed.  This technique extends the reach of libraries
like SCR to systems where they otherwise could not be used.  CRUISE also exposes
file contents for Remote Direct Memory Access, allowing external tools to copy
checkpoints to the parallel file system in the background with reduced CPU
interruption. 

More information about the project, and relevant publications, can be found
[HERE](http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi/checkpoint-file-system-research).
