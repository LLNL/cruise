PRE_SCRMFS_FLAGS := $(shell echo `../../install/bin/scrmfs-config --pre-ld-flags`)
POST_SCRMFS_FLAGS := $(shell echo `../../install/bin/scrmfs-config --post-ld-flags`)

OPT := -g -O0

all: clean
	mpicc $(OPT) -o test_memcpy test_memcpy.c
	mpicc $(OPT) -o test_ramdisk_real test_ramdisk.c
	mpicc $(PRE_SCRMFS_FLAGS) $(OPT) -o test_ramdisk test_ramdisk.c $(POST_SCRMFS_FLAGS)

clean:
	rm -f test_memcpy test_ramdisk_real test_ramdisk *.o
