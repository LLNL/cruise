MPICC := mpiicc
CONTAINER_DIR := ../../container
PRE_SCRMFS_FLAGS := $(shell echo `../../install/bin/scrmfs-config --pre-ld-flags`)
POST_SCRMFS_FLAGS := $(shell echo `../../install/bin/scrmfs-config --post-ld-flags`)

OPT := -g -O0

all: clean
	$(MPICC) $(OPT) -o test_memcpy test_memcpy.c
	$(MPICC) $(OPT) -o test_ramdisk test_ramdisk.c
	$(MPICC) $(PRE_SCRMFS_FLAGS) $(OPT) -o test_scrmfs test_scrmfs.c $(POST_SCRMFS_FLAGS) -L$(CONTAINER_DIR)/lib -lcontainer

clean:
	rm -f test_memcpy test_ramdisk test_scrmfs *.o
