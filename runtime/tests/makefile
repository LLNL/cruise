MPICC = mpiicc
CC = icc
#MPICC = mpicc
#CC = gcc
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

LIBS = -lpthread $(NUMA_LIBS) $(CONTAINER_LIBS)
LDFLAGS = $(NUMA_LDFLAGS) $(CONTAINER_LDFLAGS)


all: test1 test1_container test_writeread test_truncate test_fopen

clean: 
	rm -f *.o test1 test1_container test_writeread test_truncate test_fopen

test1: test1.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test1.c -o test1 $(LDFLAGS) $(LIBS) $(LDFLAGS) $(POST_SCRMFS_FLAGS) 

test1_container: test1.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test1.c -o test1_container $(LDFLAGS) $(LIBS) $(CONTAINER_LDFLAGS) $(CONTAINER_LIBS) $(POST_SCRMFS_FLAGS)
 
test_writeread: test_writeread.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_writeread.c -o test_writeread $(LDFLAGS) $(LIBS) $(CONTAINER_LDFLAGS) $(CONTAINER_LIBS) $(POST_SCRMFS_FLAGS)

test_truncate: test_truncate.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_truncate.c -o test_truncate $(LDFLAGS) $(LIBS) $(CONTAINER_LDFLAGS) $(CONTAINER_LIBS) $(POST_SCRMFS_FLAGS)

test_fopen: test_fopen.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_fopen.c -o test_fopen $(LDFLAGS) $(LIBS) $(CONTAINER_LDFLAGS) $(CONTAINER_LIBS) $(POST_SCRMFS_FLAGS)
