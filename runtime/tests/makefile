# Intel compilers
MPICC = mpiicc
CC = icc

# GNU compilers
#MPICC = mpicc
#CC = gcc

CFLAGS = -g -O0
LIBS = -lpthread

NUMA_LIBS = -lnuma

#CONTAINER_HOME = ../container
#CONTAINER_INCS = -I$(CONTAINER_HOME)/include
#CONTAINER_LDFLAGS = -L$(CONTAINER_HOME)/lib
#CONTAINER_LIBS = -lcontainer 

SCRMFS_LIBS = $(LIBS) $(NUMA_LIBS) $(CONTAINER_LIBS)
SCRMFS_LDFLAGS = $(LDFLAGS) $(CONTAINER_LDFLAGS)

PRE_SCRMFS_FLAGS := $(shell echo `../install/bin/scrmfs-config --pre-ld-flags`)
POST_SCRMFS_FLAGS := $(shell echo `../install/bin/scrmfs-config --post-ld-flags`)

all: test1 test_writeread test_truncate test_fopen test_fprintf test_ungetc test_scanf test_wscanf test_memcpy test_ramdisk

clean: 
	rm -f *.o test1 test1_container test_writeread test_truncate test_fopen test_fprintf test_ungetc test_scanf test_wscanf test_memcpy test_ramdisk

test1: test1.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test1.c -o test1 $(SCRMFS_LDFLAGS) $(SCRMFS_LIBS) $(POST_SCRMFS_FLAGS) 

test_writeread: test_writeread.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_writeread.c -o test_writeread $(SCRMFS_LDFLAGS) $(SCRMFS_LIBS) $(POST_SCRMFS_FLAGS)

test_truncate: test_truncate.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_truncate.c -o test_truncate $(SCRMFS_LDFLAGS) $(SCRMFS_LIBS) $(POST_SCRMFS_FLAGS)

test_fopen: test_fopen.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_fopen.c -o test_fopen $(SCRMFS_LDFLAGS) $(SCRMFS_LIBS) $(POST_SCRMFS_FLAGS)

test_fprintf: test_fprintf.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_fprintf.c -o test_fprintf $(SCRMFS_LDFLAGS) $(SCRMFS_LIBS) $(POST_SCRMFS_FLAGS)

test_ungetc: test_ungetc.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_ungetc.c -o test_ungetc $(SCRMFS_LDFLAGS) $(SCRMFS_LIBS) $(POST_SCRMFS_FLAGS) 

test_scanf: test_scanf.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_scanf.c -o test_scanf $(SCRMFS_LDFLAGS) $(SCRMFS_LIBS) $(POST_SCRMFS_FLAGS) 

test_wscanf: test_wscanf.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test_wscanf.c -o test_wscanf $(SCRMFS_LDFLAGS) $(SCRMFS_LIBS) $(POST_SCRMFS_FLAGS) 

test_memcpy: test_memcpy.c
	$(MPICC) $(CFLAGS) $(INCLUDES) test_memcpy.c -o test_memcpy $(LDFLAGS) $(LIBS)

test_ramdisk: test_ramdisk.c
	$(MPICC) $(CFLAGS) $(INCLUDES) test_ramdisk.c -o test_ramdisk $(LDFLAGS) $(LIBS)

