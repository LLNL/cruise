CC = gcc
LIBS = -lpthread

PRE_SCRMFS_FLAGS = -L/g/g23/mohror1/SCR/memFS-git/memfs/runtime/install/lib -lz -Wl,-u,MPI_Wtime,-wrap,access,-wrap,write,-wrap,open,-wrap,creat,-wrap,creat64,-wrap,open64,-wrap,close,-wrap,unlink,-wrap,truncate,-wrap,rename,-wrap,read,-wrap,lseek,-wrap,lseek64,-wrap,pread,-wrap,pwrite,-wrap,readv,-wrap,writev,-wrap,__xstat,-wrap,__lxstat,-wrap,__fxstat,-wrap,__xstat64,-wrap,__lxstat64,-wrap,__fxstat64,-wrap,mmap,-wrap,mmap64,-wrap,fopen,-wrap,fclose,-wrap,fread,-wrap,fwrite,-wrap,fseek,-wrap,fopen64,-wrap,pread64,-wrap,pwrite64,-wrap,fsync,-wrap,fdatasync,-wrap,mkdir,-wrap,rmdir,-wrap,flock,-wrap,posix_fadvise
POST_SCRMFS_FLAGS = -L/g/g23/mohror1/SCR/memFS-git/memfs/runtime/install/lib -lscrmfs-posix

all: test1

clean: 
	rm -f *.o test1

test1: test1.c
	$(CC) $(CFLAGS) $(INCLUDES) $(PRE_SCRMFS_FLAGS) test1.c -o test1 $(LDFLAGS) $(LIBS) $(POST_SCRMFS_FLAGS)
