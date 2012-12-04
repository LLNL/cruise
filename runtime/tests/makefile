MPICC = mpiicc
CC = icc
CFLAGS = -g -O0
LIBS = -lpthread

PRE_SCRMFS_FLAGS := $(shell echo `../install/bin/scrmfs-config --pre-ld-flags`)
POST_SCRMFS_FLAGS := $(shell echo `../install/bin/scrmfs-config --post-ld-flags`)

CONTAINER_HOME = ../container
CONTAINER_INCS = -I$(CONTAINER_HOME)/include
CONTAINER_LDFLAGS = -L$(CONTAINER_HOME)/lib
CONTAINER_LIBS = -lcontainer

NUMA_HOME = ../numactl
NUMA_INCS = -I$(NUMA_HOME)
NUMA_LDFLAGS = -L$(NUMA_HOME)
NUMA_LIBS = -lnuma

MPICC = mpicc
CC = gcc
LIBS = -lpthread $(NUMA_LIBS) $(CONTAINER_LIBS)
LDFLAGS = $(NUMA_LDFLAGS) $(CONTAINER_LDFLAGS)


all: test1 test1_container test_writeread

clean: 
	rm -f *.o test1 test1_container test_writeread

test1: test1.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test1.c -o test1 $(LDFLAGS) $(LIBS) $(LDFLAGS) $(POST_SCRMFS_FLAGS) 

test1_container: test1.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test1.c -o test1_container $(LDFLAGS) $(LIBS) $(LDFLAGS) $(POST_SCRMFS_FLAGS)

test_writeread: test_writeread.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_writeread.c -o test_writeread $(LDFLAGS) $(LIBS) $(LDFLAGS) $(POST_SCRMFS_FLAGS)
