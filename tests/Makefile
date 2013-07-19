MPICC = mpicc 	#mpiicc
CC = gcc		#icc

CFLAGS = -g -O0
LIBS = -lpthread

NUMA_LIBS = -lnuma

CRUISE_LIBS = $(LIBS) $(NUMA_LIBS) $(CONTAINER_LIBS)
CRUISE_LDFLAGS = $(LDFLAGS) $(CONTAINER_LDFLAGS)

PRE_CRUISE_FLAGS := $(shell echo `../install/bin/cruise-config --pre-ld-flags`)
POST_CRUISE_FLAGS := $(shell echo `../install/bin/cruise-config --post-ld-flags`)

all: test_writeread test_truncate test_fopen test_fprintf test_ungetc test_scanf test_wscanf test_memcpy test_ramdisk test1

clean: 
	rm -f *.o test1 test1_container test_writeread test_truncate test_fopen test_fprintf test_ungetc test_scanf test_wscanf test_memcpy test_ramdisk

test1: test1.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_CRUISE_FLAGS) test1.c -o test1 $(CRUISE_LDFLAGS) $(CRUISE_LIBS) $(POST_CRUISE_FLAGS) 

test_writeread: test_writeread.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_CRUISE_FLAGS) test_writeread.c -o test_writeread $(CRUISE_LDFLAGS) $(CRUISE_LIBS) $(POST_CRUISE_FLAGS)

test_truncate: test_truncate.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_CRUISE_FLAGS) test_truncate.c -o test_truncate $(CRUISE_LDFLAGS) $(CRUISE_LIBS) $(POST_CRUISE_FLAGS)

test_fopen: test_fopen.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_CRUISE_FLAGS) test_fopen.c -o test_fopen $(CRUISE_LDFLAGS) $(CRUISE_LIBS) $(POST_CRUISE_FLAGS)

test_fprintf: test_fprintf.c
	$(MPICC) $(CFLAGS) $(INCLUDES) $(PRE_CRUISE_FLAGS) test_fprintf.c -o test_fprintf $(CRUISE_LDFLAGS) $(CRUISE_LIBS) $(POST_CRUISE_FLAGS)

test_ungetc: test_ungetc.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_CRUISE_FLAGS) test_ungetc.c -o test_ungetc $(CRUISE_LDFLAGS) $(CRUISE_LIBS) $(POST_CRUISE_FLAGS) 

test_scanf: test_scanf.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_CRUISE_FLAGS) test_scanf.c -o test_scanf $(CRUISE_LDFLAGS) $(CRUISE_LIBS) $(POST_CRUISE_FLAGS) 

test_wscanf: test_wscanf.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_CRUISE_FLAGS) test_wscanf.c -o test_wscanf $(CRUISE_LDFLAGS) $(CRUISE_LIBS) $(POST_CRUISE_FLAGS) 

test_memcpy: test_memcpy.c
	$(MPICC) $(CFLAGS) $(INCLUDES) test_memcpy.c -o test_memcpy $(LDFLAGS) $(LIBS)

test_ramdisk: test_ramdisk.c
	$(MPICC) $(CFLAGS) $(INCLUDES) test_ramdisk.c -o test_ramdisk $(LDFLAGS) $(LIBS)

