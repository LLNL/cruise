#include "scrmfs-runtime-config.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <search.h>
#include <assert.h>
#include <libgen.h>
#define __USE_GNU
#include <pthread.h>

#define ENABLE_NUMA_POLICY
#ifdef ENABLE_NUMA_POLICY
#include <numa.h>
#endif

#include <sys/ipc.h>
#include <sys/shm.h>

#include "scrmfs-file.h"
#include "scrmfs-stack.h"
#include "utlist.h"

#ifdef HAVE_CONTAINER_LIB
#include "container/src/container.h"
#include "scrmfs-container.h"
#endif /* HAVE_CONTAINER_LIB */

#ifdef MACHINE_BGQ
/* BG/Q to get personality and persistent memory */
#include <sys/mman.h>
#include <hwi/include/common/uci.h>
#include <firmware/include/personality.h>
#include <spi/include/kernel/memory.h>
#include <mpix.h>
#endif /* MACHINE_BGQ */

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

static int scrmfs_use_memfs = 1;
static int scrmfs_use_spillover;
static int scrmfs_use_single_shm = 0;
static int scrmfs_use_containers;         /* set by env var SCRMFS_USE_CONTAINERS=1 */
static int scrmfs_page_size = 0;

static int    scrmfs_max_files;  /* maximum number of files to store */
static size_t scrmfs_chunk_mem;  /* number of bytes in memory to be used for chunk storage */
static int    scrmfs_chunk_bits; /* we set chunk size = 2^scrmfs_chunk_bits */
static off_t  scrmfs_chunk_size; /* chunk size in bytes */
static off_t  scrmfs_chunk_mask; /* mask applied to logical offset to determine physical offset within chunk */
static int    scrmfs_max_chunks; /* maximum number of chunks that fit in memory */

static size_t scrmfs_spillover_size;  /* number of bytes in spillover to be used for chunk storage */
static int    scrmfs_spillover_max_chunks; /* maximum number of chunks that fit in spillover storage */

#ifdef ENABLE_NUMA_POLICY
static char scrmfs_numa_policy[10];
#endif

#ifdef HAVE_CONTAINER_LIB
static char scrmfs_container_info[100];   /* not sure what this is for */
static cs_store_handle_t cs_store_handle; /* the container store handle */
static cs_set_handle_t cs_set_handle;     /* the container set handle */
static initial_container_size;
#endif /* HAVE_CONTAINER_LIB */

//#define SCRMFS_DEBUG
#ifdef SCRMFS_DEBUG
    #define debug(fmt, args... )  printf("%s: "fmt, __func__, ##args)
#else
    #define debug(fmt, args... )
#endif

#ifdef SCRMFS_PRELOAD
#define __USE_GNU
#include <dlfcn.h>
#include <stdlib.h>

#define SCRMFS_FORWARD_DECL(name,ret,args) \
  static ret (*__real_ ## name)args = NULL;

#define SCRMFS_DECL(__name) __name

#define MAP_OR_FAIL(func) \
    if (!(__real_ ## func)) \
    { \
        __real_ ## func = dlsym(RTLD_NEXT, #func); \
        if(!(__real_ ## func)) { \
           fprintf(stderr, "SCRMFS failed to map symbol: %s\n", #func); \
           exit(1); \
       } \
    }


#else

#define SCRMFS_FORWARD_DECL(name,ret,args) \
  extern ret __real_ ## name args;

#define SCRMFS_DECL(__name) __wrap_ ## __name

#define MAP_OR_FAIL(func)

#endif

extern pthread_mutex_t scrmfs_stack_mutex;

/* ---------------------------------------
 * POSIX wrappers: paths
 * --------------------------------------- */

SCRMFS_FORWARD_DECL(access, int, (const char *pathname, int mode));
SCRMFS_FORWARD_DECL(mkdir, int, (const char *path, mode_t mode));
SCRMFS_FORWARD_DECL(rmdir, int, (const char *path));
SCRMFS_FORWARD_DECL(unlink, int, (const char *path));
/* TODO: remove */
SCRMFS_FORWARD_DECL(rename, int, (const char *oldpath, const char *newpath));
SCRMFS_FORWARD_DECL(truncate, int, (const char *path, off_t length));
SCRMFS_FORWARD_DECL(stat, int,( const char *path, struct stat *buf));
SCRMFS_FORWARD_DECL(__lxstat, int, (int vers, const char* path, struct stat *buf));
SCRMFS_FORWARD_DECL(__lxstat64, int, (int vers, const char* path, struct stat64 *buf));
SCRMFS_FORWARD_DECL(__xstat, int, (int vers, const char* path, struct stat *buf));
SCRMFS_FORWARD_DECL(__xstat64, int, (int vers, const char* path, struct stat64 *buf));

/* ---------------------------------------
 * POSIX wrappers: file descriptors
 * --------------------------------------- */

SCRMFS_FORWARD_DECL(creat, int, (const char* path, mode_t mode));
SCRMFS_FORWARD_DECL(creat64, int, (const char* path, mode_t mode));
SCRMFS_FORWARD_DECL(open, int, (const char *path, int flags, ...));
SCRMFS_FORWARD_DECL(open64, int, (const char *path, int flags, ...));
SCRMFS_FORWARD_DECL(read, ssize_t, (int fd, void *buf, size_t count));
SCRMFS_FORWARD_DECL(write, ssize_t, (int fd, const void *buf, size_t count));
SCRMFS_FORWARD_DECL(readv, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
SCRMFS_FORWARD_DECL(writev, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
SCRMFS_FORWARD_DECL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
SCRMFS_FORWARD_DECL(pread64, ssize_t, (int fd, void *buf, size_t count, off64_t offset));
SCRMFS_FORWARD_DECL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
SCRMFS_FORWARD_DECL(pwrite64, ssize_t, (int fd, const void *buf, size_t count, off64_t offset));
SCRMFS_FORWARD_DECL(posix_fadvise, int, (int fd, off_t offset, off_t len, int advice));
SCRMFS_FORWARD_DECL(lseek, off_t, (int fd, off_t offset, int whence));
SCRMFS_FORWARD_DECL(lseek64, off64_t, (int fd, off64_t offset, int whence));
SCRMFS_FORWARD_DECL(ftruncate, int, (int fd, off_t length));
SCRMFS_FORWARD_DECL(fsync, int, (int fd));
SCRMFS_FORWARD_DECL(fdatasync, int, (int fd));
SCRMFS_FORWARD_DECL(flock, int, (int fd, int operation));
SCRMFS_FORWARD_DECL(mmap, void*, (void *addr, size_t length, int prot, int flags, int fd, off_t offset));
SCRMFS_FORWARD_DECL(mmap64, void*, (void *addr, size_t length, int prot, int flags, int fd, off64_t offset));
SCRMFS_FORWARD_DECL(munmap, int,(void *addr, size_t length));
SCRMFS_FORWARD_DECL(msync, int, (void *addr, size_t length, int flags));
SCRMFS_FORWARD_DECL(__fxstat, int, (int vers, int fd, struct stat *buf));
SCRMFS_FORWARD_DECL(__fxstat64, int, (int vers, int fd, struct stat64 *buf));
SCRMFS_FORWARD_DECL(close, int, (int fd));

/* ---------------------------------------
 * POSIX wrappers: file streams
 * --------------------------------------- */

SCRMFS_FORWARD_DECL(fopen, FILE*, (const char *path, const char *mode));
SCRMFS_FORWARD_DECL(fopen64, FILE*, (const char *path, const char *mode));
SCRMFS_FORWARD_DECL(fgetc, int, (FILE *stream));
SCRMFS_FORWARD_DECL(fputc, int, (int c, FILE *stream));
SCRMFS_FORWARD_DECL(getc, int, (FILE *stream));
SCRMFS_FORWARD_DECL(putc, int, (int c, FILE *stream));
SCRMFS_FORWARD_DECL(fgets, char*, (char* s, int n, FILE* stream));
SCRMFS_FORWARD_DECL(fputs, int, (const char* s, FILE* stream));
SCRMFS_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nitems, FILE *stream));
SCRMFS_FORWARD_DECL(fwrite, size_t, (const void *ptr, size_t size, size_t nitems, FILE *stream));
SCRMFS_FORWARD_DECL(fseek,  int, (FILE *stream, long offset,  int whence));
SCRMFS_FORWARD_DECL(fseeko, int, (FILE *stream, off_t offset, int whence));
SCRMFS_FORWARD_DECL(ftell,  long,  (FILE *stream));
SCRMFS_FORWARD_DECL(ftello, off_t, (FILE *stream));
SCRMFS_FORWARD_DECL(rewind, void, (FILE *stream));
SCRMFS_FORWARD_DECL(fflush, int, (FILE *stream));
SCRMFS_FORWARD_DECL(feof, int, (FILE *stream));
SCRMFS_FORWARD_DECL(ferror, int, (FILE *stream));
SCRMFS_FORWARD_DECL(clearerr, void, (FILE *stream));
SCRMFS_FORWARD_DECL(fileno, int, (FILE *stream));
SCRMFS_FORWARD_DECL(fclose, int, (FILE *stream));

/* keep track of what we've initialized */
static int scrmfs_initialized = 0;

/* global persistent memory block (metadata + data) */
static void* scrmfs_superblock = NULL;
static void* free_fid_stack = NULL;
static void* free_chunk_stack = NULL;
static void* free_spillchunk_stack = NULL;
static scrmfs_filename_t* scrmfs_filelist    = NULL;
static scrmfs_filemeta_t* scrmfs_filemetas   = NULL;
static scrmfs_chunkmeta_t* scrmfs_chunkmetas = NULL;
static char* scrmfs_chunks = NULL;
static int scrmfs_spilloverblock = 0;

/* array of file descriptors */
static scrmfs_fd_t scrmfs_fds[SCRMFS_MAX_FILEDESCS];
static rlim_t scrmfs_fd_limit;

/* array of file streams */
static scrmfs_stream_t scrmfs_streams[SCRMFS_MAX_FILEDESCS];

/* mount point information */
static char*  scrmfs_mount_prefix = NULL;
static size_t scrmfs_mount_prefixlen = 0;
static key_t  scrmfs_mount_shmget_key = 0;

/* mutex to lock stack operations */
pthread_mutex_t scrmfs_stack_mutex = PTHREAD_MUTEX_INITIALIZER;

/* returns 1 if two input parameters will overflow their type when
 * added together */
static inline int scrmfs_would_overflow_offt(off_t a, off_t b)
{
    /* compute min and max values */
    unsigned long long bits = sizeof(off_t) * 8;
    off_t max = (off_t) ( (1ULL << (bits-1ULL)) - 1ULL);
    off_t min = (off_t) (-(1ULL << (bits-1ULL))       );

    /* if both parameters are positive, they could overflow when
     * added together */
    if (a > 0 && b > 0) {
        /* if the distance between a and max is greater than or equal to
         * b, then we could add a and b and still not exceed max */
        if (max - a >= b) {
            return 0;
        }
        return 1;
    }

    /* if both parameters are negative, they could underflow when
     * added together */
    if (a < 0 && b < 0) {
        /* if the distance between min and a is less than or equal to
         * b, then we could add a and b and still not exceed min */
        if (min - a <= b) {
            return 0;
        }
        return 1;
    }

    /* if a and b are mixed signs or at least one of them is 0,
     * then adding them together will produce a result closer to 0
     * or at least no further away than either value already is*/
    return 0;
}

/* returns 1 if two input parameters will overflow their type when
 * added together */
static inline int scrmfs_would_overflow_long(long a, long b)
{
    /* compute min and max values */
    unsigned long long bits = sizeof(long) * 8;
    long max = (long) ( (1ULL << (bits-1ULL)) - 1ULL);
    long min = (long) (-(1ULL << (bits-1ULL))       );

    /* if both parameters are positive, they could overflow when
     * added together */
    if (a > 0 && b > 0) {
        /* if the distance between a and max is greater than or equal to
         * b, then we could add a and b and still not exceed max */
        if (max - a >= b) {
            return 0;
        }
        return 1;
    }

    /* if both parameters are negative, they could underflow when
     * added together */
    if (a < 0 && b < 0) {
        /* if the distance between min and a is less than or equal to
         * b, then we could add a and b and still not exceed min */
        if (min - a <= b) {
            return 0;
        }
        return 1;
    }

    /* if a and b are mixed signs or at least one of them is 0,
     * then adding them together will produce a result closer to 0
     * or at least no further away than either value already is*/
    return 0;
}

#if 0
/* simple memcpy which compilers should be able to vectorize
 * from: http://software.intel.com/en-us/articles/memcpy-performance/
 * icc -restrict -O3 ... */
static inline void* scrmfs_memcpy(void *restrict b, const void *restrict a, size_t n)
{
    char *s1 = b;
    const char *s2 = a;
    for(; 0<n; --n)*s1++ = *s2++;
    return b;
}
#endif

static inline int scrmfs_stack_lock()
{
    if (scrmfs_use_single_shm) {
        return pthread_mutex_lock(&scrmfs_stack_mutex);
    }
    return 0;
}

static inline int scrmfs_stack_unlock()
{
    if (scrmfs_use_single_shm) {
        return pthread_mutex_unlock(&scrmfs_stack_mutex);
    }
    return 0;
}

/* sets flag if the path is a special path */
static inline int scrmfs_intercept_path(const char* path)
{
    /* initialize our globals if we haven't already */
    if (! scrmfs_initialized) {
      return 0;
    }

    /* if the path starts with our mount point, intercept it */
    if (strncmp(path, scrmfs_mount_prefix, scrmfs_mount_prefixlen) == 0) {
        return 1;
    }
    return 0;
}

/* given an fd, return 1 if we should intercept this file, 0 otherwise,
 * convert fd to new fd value if needed */
static inline void scrmfs_intercept_fd(int* fd, int* intercept)
{
    int oldfd = *fd;

    /* initialize our globals if we haven't already */
    if (! scrmfs_initialized) {
        *intercept = 0;
        return;
    }

    if (oldfd < scrmfs_fd_limit) {
        /* this fd is a real system fd, so leave it as is */
        *intercept = 0;
    } else if (oldfd < 0) {
        /* this is an invalid fd, so we should not intercept it */
        *intercept = 0;
    } else {
        /* this is an fd we generated and returned to the user,
         * so intercept the call and shift the fd */
        int newfd = oldfd - scrmfs_fd_limit;
        *fd = newfd;
        *intercept = 1;
        debug("Changing fd from exposed %d to internal %d\n", oldfd, newfd);
    }

    return;
}

/* given an fd, return 1 if we should intercept this file, 0 otherwise,
 * convert fd to new fd value if needed */
static inline int scrmfs_intercept_stream(FILE* stream)
{
    /* initialize our globals if we haven't already */
    if (! scrmfs_initialized) {
        return 0;
    }

    /* check whether this pointer lies within range of our
     * file stream array */
    scrmfs_stream_t* ptr   = (scrmfs_stream_t*) stream;
    scrmfs_stream_t* start = &(scrmfs_streams[0]);
    scrmfs_stream_t* end   = &(scrmfs_streams[SCRMFS_MAX_FILEDESCS]);
    if (ptr >= start && ptr < end) {
        return 1;
    }

    return 0;
}

/* given a path, return the file id */
static int scrmfs_get_fid_from_path(const char* path)
{
    int i = 0;
    while (i < scrmfs_max_files)
    {
        if(scrmfs_filelist[i].in_use &&
           strcmp((void *)&scrmfs_filelist[i].filename, path) == 0)
        {
            debug("File found: scrmfs_filelist[%d].filename = %s\n",
                  i, (char*)&scrmfs_filelist[i].filename
            );
            return i;
        }
        i++;
    }

    /* couldn't find specified path */
    return -1;
}

/* given a file descriptor, return the file id */
static inline int scrmfs_get_fid_from_fd(int fd)
{
  /* check that file descriptor is within range */
  if (fd < 0 || fd >= SCRMFS_MAX_FILEDESCS) {
    return -1;
  }

  /* right now, the file descriptor is equal to the file id */
  return fd;
}

/* return address of file descriptor structure or NULL if fd is out
 * of range */
static inline scrmfs_fd_t* scrmfs_get_filedesc_from_fd(int fd)
{
    if (fd >= 0 && fd < SCRMFS_MAX_FILEDESCS) {
        scrmfs_fd_t* filedesc = &(scrmfs_fds[fd]);
        return filedesc;
    }
    return NULL;
}

/* given a file id, return a pointer to the meta data,
 * otherwise return NULL */
static inline scrmfs_filemeta_t* scrmfs_get_meta_from_fid(int fid)
{
    /* check that the file id is within range of our array */
    if (fid >= 0 && fid < scrmfs_max_files) {
        /* get a pointer to the file meta data structure */
        scrmfs_filemeta_t* meta = &scrmfs_filemetas[fid];
        return meta;
    }
    return NULL;
}

static scrmfs_chunkmeta_t* scrmfs_get_chunkmeta(int fid, int cid)
{
    /* lookup file meta data for specified file id */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
    if (meta != NULL) {
        /* now lookup chunk meta data for specified chunk id */
        if (cid >= 0 && cid < scrmfs_max_chunks) {
           scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[cid]);
           return chunk_meta;
        }
    }

    /* failed to find file or chunk id is out of range */
    return (scrmfs_chunkmeta_t *)NULL;
}

/* ---------------------------------------
 * Operations on file chunks
 * --------------------------------------- */

/* given a logical chunk id and an offset within that chunk, return the pointer
 * to the memory location corresponding to that location */
static inline void* scrmfs_compute_chunk_buf(const scrmfs_filemeta_t* meta, int logical_id, off_t logical_offset)
{
    /* get pointer to chunk meta */
    const scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[logical_id]);

    /* identify physical chunk id */
    int physical_id = chunk_meta->id;

    /* compute the start of the chunk */
    char *start = NULL;
    if (physical_id < scrmfs_max_chunks) {
        start = scrmfs_chunks + (physical_id << scrmfs_chunk_bits);
    } else {
        /* chunk is in spill over */
        debug("wrong chunk ID\n");
        return NULL;
    }

    /* now add offset */
    char* buf = start + logical_offset;
    return (void*)buf;
}

/* given a chunk id and an offset within that chunk, return the offset
 * in the spillover file corresponding to that location */
static inline off_t scrmfs_compute_spill_offset(const scrmfs_filemeta_t* meta, int logical_id, off_t logical_offset)
{
    /* get pointer to chunk meta */
    const scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[logical_id]);

    /* identify physical chunk id */
    int physical_id = chunk_meta->id;

    /* compute start of chunk in spill over device */
    off_t start = 0;
    if (physical_id < scrmfs_max_chunks) {
        debug("wrong spill-chunk ID\n");
        return -1;
    } else {
        /* compute buffer loc within spillover device chunk */
        /* account for the scrmfs_max_chunks added to identify location when
         * grabbing this chunk */
        start = ((physical_id - scrmfs_max_chunks) << scrmfs_chunk_bits);
    }
    off_t buf = start + logical_offset;
    return buf;
}

/* allocate a new chunk for the specified file and logical chunk id */
static int scrmfs_chunk_alloc(int fid, scrmfs_filemeta_t* meta, int chunk_id)
{
    /* get pointer to chunk meta data */
    scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[chunk_id]);
    
    /* allocate a chunk and record its location */
    if (scrmfs_use_memfs) {
        /* allocate a new chunk from memory */
        scrmfs_stack_lock();
        int id = scrmfs_stack_pop(free_chunk_stack);
        scrmfs_stack_unlock();

        /* if we got one return, otherwise try spill over */
        if (id >= 0) {
            /* got a chunk from memory */
            chunk_meta->location = CHUNK_LOCATION_MEMFS;
            chunk_meta->id = id;
        } else if (scrmfs_use_spillover) {
            /* shm segment out of space, grab a block from spill-over device */
            debug("getting blocks from spill-over device\n");

            /* TODO: missing lock calls? */
            /* add scrmfs_max_chunks to identify chunk location */
            scrmfs_stack_lock();
            id = scrmfs_stack_pop(free_spillchunk_stack) + scrmfs_max_chunks;
            scrmfs_stack_unlock();
            if (id < scrmfs_max_chunks) {
                debug("spill-over device out of space (%d)\n", id);
                return SCRMFS_ERR_NOSPC;
            }

            /* got one from spill over */
            chunk_meta->location = CHUNK_LOCATION_SPILLOVER;
            chunk_meta->id = id;
        } else {
            /* spill over isn't available, so we're out of space */
            debug("memfs out of space (%d)\n", id);
            return SCRMFS_ERR_NOSPC;
        }
    } else if (scrmfs_use_spillover) {
        /* memory file system is not enabled, but spill over is */

        /* shm segment out of space, grab a block from spill-over device */
        debug("getting blocks from spill-over device \n");

        /* TODO: missing lock calls? */
        /* add scrmfs_max_chunks to identify chunk location */
        scrmfs_stack_lock();
        int id = scrmfs_stack_pop(free_spillchunk_stack) + scrmfs_max_chunks;
        scrmfs_stack_unlock();
        if (id < scrmfs_max_chunks) {
            debug("spill-over device out of space (%d)\n", id);
            return SCRMFS_ERR_NOSPC;
        }

        /* got one from spill over */
        chunk_meta->location = CHUNK_LOCATION_SPILLOVER;
        chunk_meta->id = id;
    }
  #ifdef HAVE_CONTAINER_LIB
    else if (scrmfs_use_containers) {
        /* I'm using the same infrastructure as memfs (chunks) because
           it just makes life easier, and I think cleaner. If the size of the container
           is not big enough, we extend it by the size of a chunk */
        scrmfs_stack_lock();
        int id = scrmfs_stack_pop(free_chunk_stack);
        scrmfs_stack_unlock();

        if (id < 0) {
            /* out of space */
            fprintf(stderr, "Failed to allocate chunk (%d)\n", id);
            return SCRMFS_ERR_NOSPC;
        }
        scrmfs_filemeta_t* file_meta = scrmfs_get_meta_from_fid(fid);
        if(file_meta->container_data.container_size < scrmfs_chunk_size + file_meta->size){
           //TODO extend container not implemented yet. always returns out of space
           cs_container_handle_t* ch = &(file_meta->container_data.cs_container_handle );
           int ret = scrmfs_container_extend(cs_set_handle, ch, scrmfs_chunk_size);
           if (ret != SCRMFS_SUCCESS){
              return ret;
           }
           file_meta->container_data.container_size += scrmfs_chunk_size;
        }
      
        /* allocate chunk from containers */
        chunk_meta->location = CHUNK_LOCATION_CONTAINER;
        chunk_meta->id = id;
    }
  #endif /* HAVE_CONTAINER_LIB */
    else {
        /* don't know how to allocate chunk */
        chunk_meta->location = CHUNK_LOCATION_NULL;
        return SCRMFS_ERR_IO;
    }

    return SCRMFS_SUCCESS;
}

static int scrmfs_chunk_free(int fid, scrmfs_filemeta_t* meta, int chunk_id)
{
    /* get pointer to chunk meta data */
    scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[chunk_id]);

    /* get physical id of chunk */
    int id = chunk_meta->id;
    debug("free chunk %d from location %d\n", id, chunk_meta->location);

    /* determine location of chunk */
    if (chunk_meta->location == CHUNK_LOCATION_MEMFS) {
        scrmfs_stack_lock();
        scrmfs_stack_push(free_chunk_stack, id);
        scrmfs_stack_unlock();
    } else if (chunk_meta->location == CHUNK_LOCATION_SPILLOVER) {
        /* TODO: free spill over chunk */
    }
  #ifdef HAVE_CONTAINER_LIB
    else if (chunk_meta->location == CHUNK_LOCATION_CONTAINER) {
        scrmfs_stack_lock();
        scrmfs_stack_push(free_chunk_stack, id);
        scrmfs_stack_unlock();
    }
  #endif /* HAVE_CONTAINER_LIB */
    else {
        /* unkwown chunk location */
        debug("unknown chunk location %d\n", chunk_meta->location);
        return SCRMFS_ERR_IO;
    }

    /* update location of chunk */
    chunk_meta->location = CHUNK_LOCATION_NULL;

    return SCRMFS_SUCCESS;
}

/* read data from specified chunk id, chunk offset, and count into user buffer,
 * count should fit within chunk starting from specified offset */
static int scrmfs_chunk_read(scrmfs_filemeta_t* meta, int chunk_id, off_t chunk_offset, void* buf, size_t count)
{
    /* get chunk meta data */
    scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[chunk_id]);

    /* determine location of chunk */
    if (chunk_meta->location == CHUNK_LOCATION_MEMFS) {
        /* just need a memcpy to read data */
        void* chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, chunk_offset);
        memcpy(buf, chunk_buf, count);
    } else if (chunk_meta->location == CHUNK_LOCATION_SPILLOVER) {
        /* spill over to a file, so read from file descriptor */
        MAP_OR_FAIL(pread);
        off_t spill_offset = scrmfs_compute_spill_offset(meta, chunk_id, chunk_offset);
        ssize_t rc = __real_pread(scrmfs_spilloverblock, buf, count, spill_offset);
        /* TODO: check return code for errors */
    }
  #ifdef HAVE_CONTAINER_LIB
    else if (chunk_meta->location == CHUNK_LOCATION_CONTAINER) {
        /* read chunk from containers */
        cs_container_handle_t ch = meta->container_data.cs_container_handle;

        int ret = scrmfs_container_read(ch, buf, count, chunk_offset);
        if (ret != SCRMFS_SUCCESS){
            fprintf(stderr, "Container read failed\n");
            return ret;
        }
    }
  #endif /* HAVE_CONTAINER_LIB */
    else {
        /* unknown chunk type */
        debug("unknown chunk type in read\n");
        return SCRMFS_ERR_IO;
    }

    /* assume read was successful if we get to here */
    return SCRMFS_SUCCESS;
}

/* read data from specified chunk id, chunk offset, and count into user buffer,
 * count should fit within chunk starting from specified offset */
static int scrmfs_chunk_write(scrmfs_filemeta_t* meta, int chunk_id, off_t chunk_offset, const void* buf, size_t count)
{
    /* get chunk meta data */
    scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[chunk_id]);

    /* determine location of chunk */
    if (chunk_meta->location == CHUNK_LOCATION_MEMFS) {
        /* just need a memcpy to write data */
        void* chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, chunk_offset);
        memcpy(chunk_buf, buf, count);
//        _intel_fast_memcpy(chunk_buf, buf, count);
//        scrmfs_memcpy(chunk_buf, buf, count);
    } else if (chunk_meta->location == CHUNK_LOCATION_SPILLOVER) {
        /* spill over to a file, so write to file descriptor */
        MAP_OR_FAIL(pwrite);
        off_t spill_offset = scrmfs_compute_spill_offset(meta, chunk_id, chunk_offset);
        ssize_t rc = __real_pwrite(scrmfs_spilloverblock, buf, count, spill_offset);
        if ( rc < 0 ) 
            perror("pwrite failed");
        /* TODO: check return code for errors */
    }
  #ifdef HAVE_CONTAINER_LIB
    else if (chunk_meta->location == CHUNK_LOCATION_CONTAINER) {
        /* write chunk to containers */
        cs_container_handle_t ch = meta->container_data.cs_container_handle;

        int ret = scrmfs_container_write(ch, buf, count, chunk_offset);
        if (ret != SCRMFS_SUCCESS){
            fprintf(stderr, "container write failed for single container write: %d\n");
            return ret;
        }
    }
  #endif /* HAVE_CONTAINER_LIB */
    else {
        /* unknown chunk type */
        debug("unknown chunk type in read\n");
        return SCRMFS_ERR_IO;
    }

    /* assume read was successful if we get to here */
    return SCRMFS_SUCCESS;
}

/* ---------------------------------------
 * Operations on file ids
 * --------------------------------------- */

/* given an SCRMFS error code, return corresponding errno code */
static int scrmfs_err_map_to_errno(int rc)
{
    switch(rc) {
    case SCRMFS_SUCCESS:     return 0;
    case SCRMFS_FAILURE:     return EIO;
    case SCRMFS_ERR_NOSPC:   return ENOSPC;
    case SCRMFS_ERR_IO:      return EIO;
    case SCRMFS_ERR_NAMETOOLONG: return ENAMETOOLONG;
    case SCRMFS_ERR_NOENT:   return ENOENT;
    case SCRMFS_ERR_EXIST:   return EEXIST;
    case SCRMFS_ERR_NOTDIR:  return ENOTDIR;
    case SCRMFS_ERR_NFILE:   return ENFILE;
    case SCRMFS_ERR_INVAL:   return EINVAL;
    case SCRMFS_ERR_OVERFLOW: return EOVERFLOW;
    case SCRMFS_ERR_FBIG:    return EFBIG;
    case SCRMFS_ERR_BADF:    return EBADF;
    case SCRMFS_ERR_ISDIR:   return EISDIR;
    default:                 return EIO;
    }
}

/* checks to see if fid is a directory
 * returns 1 for yes
 * returns 0 for no */
static int scrmfs_fid_is_dir(int fid)
{
   scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
   if (meta) {
     /* found a file with that id, return value of directory flag */
     int rc = meta->is_dir;
     return rc;
   } else {
     /* if it doesn't exist, then it's not a directory? */
     return 0;
   }
}

/* checks to see if a directory is empty
 * assumes that check for is_dir has already been made
 * only checks for full path matches, does not check relative paths,
 * e.g. ../dirname will not work
 * returns 1 for yes it is empty
 * returns 0 for no */
static int scrmfs_fid_is_dir_empty(const char * path)
{
    int i = 0;
    while (i < scrmfs_max_files) {
       if(scrmfs_filelist[i].in_use) {
           /* if the file starts with the path, it is inside of that directory 
            * also check to make sure that it's not the directory entry itself */
           char * strptr = strstr(path, scrmfs_filelist[i].filename);
           if (strptr == scrmfs_filelist[i].filename && strcmp(path,scrmfs_filelist[i].filename)) {
              debug("File found: scrmfs_filelist[%d].filename = %s\n",
                    i, (char*)&scrmfs_filelist[i].filename
              );
              return 0;
           }
       } 
       ++i;
    }

    /* couldn't find any files with this prefix, dir must be empty */
    return 1;
}

/* return current size of given file id */
static off_t scrmfs_fid_size(int fid)
{
    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
    return meta->size;
}

static int scrmfs_fid_stat(int fid, struct stat* buf)
{
   scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
   if (meta == NULL) {
      return -1;
   }
   
   /* initialize all the values */
   buf->st_dev = 0;     /* ID of device containing file */
   buf->st_ino = 0;     /* inode number */
   buf->st_mode = 0;    /* protection */
   buf->st_nlink = 0;   /* number of hard links */
   buf->st_uid = 0;     /* user ID of owner */
   buf->st_gid = 0;     /* group ID of owner */
   buf->st_rdev = 0;    /* device ID (if special file) */
   buf->st_size = 0;    /* total size, in bytes */
   buf->st_blksize = 0; /* blocksize for file system I/O */
   buf->st_blocks = 0;  /* number of 512B blocks allocated */
   buf->st_atime = 0;   /* time of last access */
   buf->st_mtime = 0;   /* time of last modification */
   buf->st_ctime = 0;   /* time of last status change */

   buf->st_size = meta->size;
   if(scrmfs_fid_is_dir(fid)) {
      buf->st_mode |= S_IFDIR;
   } else {
      buf->st_mode |= S_IFREG;
   }

   return 0;
}

/* allocate a file id slot for a new file 
 * return the fid or -1 on error */
static int scrmfs_fid_alloc()
{
    scrmfs_stack_lock();
    int fid = scrmfs_stack_pop(free_fid_stack);
    scrmfs_stack_unlock();
    debug("scrmfs_stack_pop() gave %d\n", fid);
    if (fid < 0) {
        /* need to create a new file, but we can't */
        debug("scrmfs_stack_pop() failed (%d)\n", fid);
        return -1;
    }
    return fid;
}

/* return the file id back to the free pool */
static int scrmfs_fid_free(int fid)
{
    scrmfs_stack_lock();
    scrmfs_stack_push(free_fid_stack, fid);
    scrmfs_stack_unlock();
    return SCRMFS_SUCCESS;
}

/* add a new file and initialize metadata
 * returns the new fid, or negative value on error */
static int scrmfs_fid_create_file(const char * path)
{
    int fid = scrmfs_fid_alloc();
    if (fid < 0)  {
        /* was there an error? if so, return it */
        errno = ENOSPC;
        return fid;
    }

    /* mark this slot as in use and copy the filename */
    scrmfs_filelist[fid].in_use = 1;
    strcpy((void *)&scrmfs_filelist[fid].filename, path);
    debug("Filename %s got scrmfs fd %d\n",scrmfs_filelist[fid].filename,fid);

    /* initialize meta data */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
    meta->size   = 0;
    meta->chunks = 0;
    meta->is_dir = 0;
    meta->flock_status = UNLOCKED;
    /* PTHREAD_PROCESS_SHARED allows Process-Shared Synchronization*/
    pthread_spin_init(&meta->fspinlock, PTHREAD_PROCESS_SHARED);
   
    return fid;
}

/* add a new directory and initialize metadata
 * returns the new fid, or a negative value on error */
static int scrmfs_fid_create_directory(const char * path)
{
   int fid = scrmfs_fid_create_file(path);
   if (fid < 0) {
       /* was there an error? if so, return it */
       errno = ENOSPC;
       return fid;
   }

   scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
   meta->is_dir = 1;
   return fid;
}

/* read count bytes from file starting from pos and store into buf,
 * all bytes are assumed to exist, so checks on file size should be
 * done before calling this routine */
static int scrmfs_fid_read(int fid, off_t pos, void* buf, size_t count)
{
    int rc;

    /* get meta for this file id */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* get pointer to position within first chunk */
    int chunk_id = pos >> scrmfs_chunk_bits;
    off_t chunk_offset = pos & scrmfs_chunk_mask;

    /* determine how many bytes remain in the current chunk */
    size_t remaining = scrmfs_chunk_size - chunk_offset;
    if (count <= remaining) {
        /* all bytes for this read fit within the current chunk */
  #ifdef HAVE_CONTAINER_LIB
        if(scrmfs_use_containers){
           rc = scrmfs_chunk_read(meta, chunk_id, pos, buf, count);
           if(rc != SCRMFS_SUCCESS){
              fprintf(stderr, "container read failed with code %d at position %d\n", rc, pos);
              return rc;
           }
        }
        else
           rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, buf, count);
  #else
        rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, buf, count);
  #endif
    } else {
        /* read what's left of current chunk */
        char* ptr = (char*) buf;
  #ifdef HAVE_CONTAINER_LIB
        off_t currpos = pos;
        if(scrmfs_use_containers){
           rc = scrmfs_chunk_read(meta, chunk_id, currpos, (void*)ptr, remaining);
           if(rc != SCRMFS_SUCCESS){
              fprintf(stderr, "container read failed with code %d at position %d\n", rc, currpos);
              return rc;
           }
           currpos += remaining;
        }
        else 
           rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, (void*)ptr, remaining);
  #else
        rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, (void*)ptr, remaining);
  #endif
        ptr += remaining;
   
        /* read from the next chunk */
        size_t processed = remaining;
        while (processed < count && rc == SCRMFS_SUCCESS) {
            /* get pointer to start of next chunk */
            chunk_id++;

            /* compute size to read from this chunk */
            size_t num = count - processed;
            if (num > scrmfs_chunk_size) {
                num = scrmfs_chunk_size;
            }
   
            /* read data */
   #ifdef HAVE_CONTAINER_LIB
            if(scrmfs_use_containers){
              rc = scrmfs_chunk_read(meta, chunk_id, currpos, (void*)ptr, num);
              if(rc != SCRMFS_SUCCESS){
                 fprintf(stderr, "container read failed with code %d at position %d\n", rc, currpos);
                 return rc;
              }
              currpos += num;
            }
            else
               rc = scrmfs_chunk_read(meta, chunk_id, 0, (void*)ptr, num);
   #else
            rc = scrmfs_chunk_read(meta, chunk_id, 0, (void*)ptr, num);
   #endif
            ptr += num;

            /* update number of bytes written */
            processed += num;
        }
    }

    return rc;
}

/* write count bytes from buf into file starting at offset pos,
 * all bytes are assumed to be allocated to file, so file should
 * be extended before calling this routine */
static int scrmfs_fid_write(int fid, off_t pos, const void* buf, size_t count)
{
    int rc;

    /* get meta for this file id */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* get pointer to position within first chunk */
    int chunk_id = pos >> scrmfs_chunk_bits;
    off_t chunk_offset = pos & scrmfs_chunk_mask;

    /* determine how many bytes remain in the current chunk */
    size_t remaining = scrmfs_chunk_size - chunk_offset;
    if (count <= remaining) {
        /* all bytes for this write fit within the current chunk */
  #ifdef HAVE_CONTAINER_LIB
        if(scrmfs_use_containers){
           rc = scrmfs_chunk_write(meta, chunk_id, pos, buf, count);
           if(rc != SCRMFS_SUCCESS){
              fprintf(stderr, "container write failed with code %d to position %d\n", rc, pos);
              return rc;
           }
        }
        else{
          rc = scrmfs_chunk_write(meta, chunk_id, chunk_offset, buf, count);
        }
  #else
        rc = scrmfs_chunk_write(meta, chunk_id, chunk_offset, buf, count);
  #endif /* HAVE_CONTAINER_LIB */
    } else {
        /* otherwise, fill up the remainder of the current chunk */
        char* ptr = (char*) buf;
#ifdef HAVE_CONTAINER_LIB
        off_t currpos = pos;
        if(scrmfs_use_containers){
           rc = scrmfs_chunk_write(meta, chunk_id, currpos, (void*)ptr, remaining);
           if(rc != SCRMFS_SUCCESS){
              fprintf(stderr, "container write failed with code %d to position %d\n", rc, currpos);
              return rc;
           }
           currpos += remaining;
        }
        else
           rc = scrmfs_chunk_write(meta, chunk_id, chunk_offset, (void*)ptr, remaining);
#else
        rc = scrmfs_chunk_write(meta, chunk_id, chunk_offset, (void*)ptr, remaining);
#endif
        ptr += remaining;

        /* then write the rest of the bytes starting from beginning
         * of chunks */
        size_t processed = remaining;
        while (processed < count && rc == SCRMFS_SUCCESS) {
            /* get pointer to start of next chunk */
            chunk_id++;

            /* compute size to write to this chunk */
            size_t num = count - processed;
            if (num > scrmfs_chunk_size) {
              num = scrmfs_chunk_size;
            }
   
            /* write data */
  #ifdef HAVE_CONTAINER_LIB
            if(scrmfs_use_containers){
              rc = scrmfs_chunk_write(meta, chunk_id, currpos, (void*)ptr, num);
              if(rc != SCRMFS_SUCCESS){
                 fprintf(stderr, "container write failed with code %d to position %d\n", rc, currpos);
                 return rc;
              }
              currpos += num;
            }
            else
               rc = scrmfs_chunk_write(meta, chunk_id, 0, (void*)ptr, num);
  #else
            rc = scrmfs_chunk_write(meta, chunk_id, 0, (void*)ptr, num);
  #endif
            ptr += num;

            /* update number of bytes processed */
            processed += num;
        }
    }

        
    return rc;
}

/* given a file id, write zero bytes to region of specified offset
 * an length, assumes space is already reserved */
static int scrmfs_fid_write_zero(int fid, off_t pos, off_t count)
{
    int rc = SCRMFS_SUCCESS;

    /* allocate an aligned chunk of memory */
    size_t buf_size = 1024 * 1024;
    void* buf = (void*) malloc(buf_size);
    if (buf == NULL) {
        return SCRMFS_ERR_IO;
    }

    /* set values in this buffer to zero */
    memset(buf, 0, buf_size);

    /* write zeros to file */
    off_t written = 0;
    off_t curpos = pos;
    while (written < count) {
        /* compute number of bytes to write on this iteration */
        size_t num = buf_size;
        off_t remaining = count - written;
        if (remaining < (off_t) buf_size) {
            num = (size_t) remaining;
        }

        /* write data to file */
        int write_rc = scrmfs_fid_write(fid, curpos, buf, num);
        if (write_rc != SCRMFS_SUCCESS) {
            rc = SCRMFS_ERR_IO;
            break;
        }

        /* update the number of bytes written */
        curpos  += (off_t) num;
        written += (off_t) num;
    }

    /* free the buffer */
    free(buf);

    return rc;
}

/* increase size of file if length is greater than current size,
 * and allocate additional chunks as needed to reserve space for
 * length bytes */
static int scrmfs_fid_extend(int fid, off_t length)
{
    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* if we write past the end of the file, we need to update the
     * file size, and we may need to allocate more chunks */
    if (length > meta->size) {
        /* TODO: check that we don't overrun the max number of chunks for a file */

        /* determine whether we need to allocate more chunks */
        off_t maxsize = meta->chunks << scrmfs_chunk_bits;
        if (length > maxsize) {
            /* compute number of additional bytes we need */
            off_t additional = length - maxsize;
            while (additional > 0) {
                /* allocate a new chunk */
                int rc = scrmfs_chunk_alloc(fid, meta, meta->chunks);
                if (rc != SCRMFS_SUCCESS) {
                    debug("failed to allocate chunk\n");
                    return SCRMFS_ERR_NOSPC;
                }

                /* increase chunk count and subtract bytes from the number we need */
                meta->chunks++;
                additional -= scrmfs_chunk_size;
            }
        }

        /* we have storage to extend file so update its size */
        meta->size = length;
    }

    return SCRMFS_SUCCESS;
}

/* truncate file id to given length, frees resources if length is
 * less than size and allocates and zero-fills new bytes if length
 * is more than size */
static int scrmfs_fid_truncate(int fid, off_t length)
{
    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* get current size of file */
    off_t size = meta->size;

    /* drop data if length is less than current size,
     * allocate new space and zero fill it if bigger */
    if (length < size) {
        /* determine the number of chunks to leave after truncating */
        off_t num_chunks = 0;
        if (length > 0) {
            num_chunks = (length >> scrmfs_chunk_bits) + 1;
        }

        /* clear off any extra chunks */
        while (meta->chunks > num_chunks) {
            meta->chunks--;
            scrmfs_chunk_free(fid, meta, meta->chunks);
        }
    } else if (length > size) {
        /* file size has been extended, allocate space */
        int extend_rc = scrmfs_fid_extend(fid, length);
        if (extend_rc != SCRMFS_SUCCESS) {
            return SCRMFS_ERR_NOSPC;
        }

        /* write zero values to new bytes */
        off_t gap_size = length - size;
        int zero_rc = scrmfs_fid_write_zero(fid, size, gap_size);
        if (zero_rc != SCRMFS_SUCCESS) {
            return SCRMFS_ERR_IO;
        }
    }

    /* set the new size */
    meta->size = length;

    return SCRMFS_SUCCESS;
}

/* opens a new file id with specified path, access flags, and permissions,
 * fills outfid with file id and outpos with position for current file pointer,
 * returns SCRMFS error code */
static int scrmfs_fid_open(const char* path, int flags, mode_t mode, int* outfid, off_t* outpos)
{
    /* check that path is short enough */
    size_t pathlen = strlen(path) + 1;
    if (pathlen > SCRMFS_MAX_FILENAME) {
        return SCRMFS_ERR_NAMETOOLONG;
    }

    /* assume that we'll place the file pointer at the start of the file */
    off_t pos = 0;

    /* check whether this file already exists */
    int fid = scrmfs_get_fid_from_path(path);
    debug("scrmfs_get_fid_from_path() gave %d\n", fid);
    if (fid < 0) {
        /* file does not exist */

        /* create file if O_CREAT is set */
        if (flags & O_CREAT) {
            debug("Couldn't find entry for %s in SCRMFS\n", path);
            debug("scrmfs_superblock = %p; free_fid_stack = %p; free_chunk_stack = %p; scrmfs_filelist = %p; chunks = %p\n",
                  scrmfs_superblock, free_fid_stack, free_chunk_stack, scrmfs_filelist, scrmfs_chunks
            );

            /* allocate a file id slot for this new file */
            fid = scrmfs_fid_create_file(path);
            if (fid < 0){
               debug("Failed to create new file %s\n", path);
               return SCRMFS_ERR_NFILE;
            }
#ifdef HAVE_CONTAINER_LIB
            if(scrmfs_use_containers){
              scrmfs_filemeta_t* file_meta = scrmfs_get_meta_from_fid(fid);
              cs_container_handle_t ch;
              size_t size = scrmfs_chunk_size;
              //size_t size = 10000;
              int ret = scrmfs_container_open(cs_set_handle, &ch, fid, size, path);
              file_meta->container_data.cs_container_handle = ch;
              if (ret != CS_SUCCESS) {
                fprintf(stderr, "creation of container failed: %d\n", ret);
                return SCRMFS_ERR_IO;
              }
              file_meta->container_data.container_size = initial_container_size;
              file_meta->filename = (char*)malloc(strlen(path)+1);
              strcpy(file_meta->filename, path);

              debug("creation of container succeeded size: %d\n", size);
            }
#endif  //have containers
        } else {
            /* ERROR: trying to open a file that does not exist without O_CREATE */
            debug("Couldn't find entry for %s in SCRMFS\n", path);
            return SCRMFS_ERR_NOENT;
        }
    } else {
        /* file already exists */

        /* if O_CREAT and O_EXCL are set, this is an error */
        if ((flags & O_CREAT) && (flags & O_EXCL)) {
            /* ERROR: trying to open a file that exists with O_CREATE and O_EXCL */
            return SCRMFS_ERR_EXIST;
        }

        /* if O_DIRECTORY is set and fid is not a directory, error */
        if ((flags & O_DIRECTORY) && !scrmfs_fid_is_dir(fid)) {
            return SCRMFS_ERR_NOTDIR;
        }

        /* if O_DIRECTORY is not set and fid is a directory, error */ 
        if (!(flags & O_DIRECTORY) && scrmfs_fid_is_dir(fid)) {
            return SCRMFS_ERR_NOTDIR;
        }

        /* if O_TRUNC is set with RDWR or WRONLY, need to truncate file */
        if ((flags & O_TRUNC) && (flags & (O_RDWR | O_WRONLY))) {
            scrmfs_fid_truncate(fid, 0);
        }

        /* if O_APPEND is set, we need to place file pointer at end of file */
        if (flags & O_APPEND) {
            scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
            pos = meta->size;
        }
    }

    /* TODO: allocate a free file descriptor and associate it with fid */
    /* set in_use flag and file pointer */
    *outfid = fid;
    *outpos = pos;
    debug("SCRMFS_open generated fd %d for file %s\n", fid, path);    


    /* don't conflict with active system fds that range from 0 - (fd_limit) */
    return SCRMFS_SUCCESS;
}

static int scrmfs_fid_close(int fid)
{
    /* TODO: clear any held locks */

    /* nothing to do here, just a place holder */
    return SCRMFS_SUCCESS;
}

/* delete a file id and return file its resources to free pools */
static int scrmfs_fid_unlink(int fid)
{
    /* return data to free pools */
    scrmfs_fid_truncate(fid, 0);

    /* set this file id as not in use */
    scrmfs_filelist[fid].in_use = 0;

#ifdef HAVE_CONTAINER_LIB
    if(scrmfs_use_containers){
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        int ret = cs_set_container_remove(cs_set_handle, meta->filename);
        free(meta->filename);
        meta->filename = NULL;
        // removal of containers will always fail because it's not implemented yet
        if(ret != CS_SUCCESS){
          //debug("Container remove failed\n");
        }
    }
#endif

    /* add this id back to the free stack */
    scrmfs_fid_free(fid);

    return SCRMFS_SUCCESS;
}

/* ---------------------------------------
 * Operations to mount file system
 * --------------------------------------- */

/* initialize our global pointers into the given superblock */
static void* scrmfs_init_pointers(void* superblock)
{
    char* ptr = (char*) superblock;

    /* stack to manage free file ids */
    free_fid_stack = ptr;
    ptr += scrmfs_stack_bytes(scrmfs_max_files);

    /* record list of file names */
    scrmfs_filelist = (scrmfs_filename_t*) ptr;
    ptr += scrmfs_max_files * sizeof(scrmfs_filename_t);

    /* array of file meta data structures */
    scrmfs_filemetas = (scrmfs_filemeta_t*) ptr;
    ptr += scrmfs_max_files * sizeof(scrmfs_filemeta_t);

    /* array of chunk meta data strucutres for each file */
    scrmfs_chunkmetas = (scrmfs_chunkmeta_t*) ptr;
    ptr += scrmfs_max_files * scrmfs_max_chunks * sizeof(scrmfs_chunkmeta_t);

    /* stack to manage free memory data chunks */
    free_chunk_stack = ptr;
    ptr += scrmfs_stack_bytes(scrmfs_max_chunks);

    if (scrmfs_use_spillover) {
        /* stack to manage free spill-over data chunks */
        free_spillchunk_stack = ptr;
        ptr += scrmfs_stack_bytes(scrmfs_spillover_max_chunks);
    }

    /* Only set this up if we're using memfs */
    if (scrmfs_use_memfs) {
      /* round ptr up to start of next page */
      unsigned long long ull_ptr  = (unsigned long long) ptr;
      unsigned long long ull_page = (unsigned long long) scrmfs_page_size;
      unsigned long long num_pages = ull_ptr / ull_page;
      if (ull_ptr > num_pages * ull_page) {
        ptr = (char*)((num_pages + 1) * ull_page);
      }

      /* pointer to start of memory data chunks */
      scrmfs_chunks = ptr;
      ptr += scrmfs_max_chunks * scrmfs_chunk_size;
    } else{
      scrmfs_chunks = NULL;
    }

    return ptr;
}

/* initialize data structures for first use */
static int scrmfs_init_structures()
{
    int i;
    for (i = 0; i < scrmfs_max_files; i++) {
        /* indicate that file id is not in use by setting flag to 0 */
        scrmfs_filelist[i].in_use = 0;

        /* set pointer to array of chunkmeta data structures */
        scrmfs_filemeta_t* filemeta = &scrmfs_filemetas[i];
        scrmfs_chunkmeta_t* chunkmetas = &(scrmfs_chunkmetas[scrmfs_max_chunks * i]);
        filemeta->chunk_meta = chunkmetas;
    }

    scrmfs_stack_init(free_fid_stack, scrmfs_max_files);

    scrmfs_stack_init(free_chunk_stack, scrmfs_max_chunks);

    if (scrmfs_use_spillover) {
        scrmfs_stack_init(free_spillchunk_stack, scrmfs_spillover_max_chunks);
    }

    debug("Meta-stacks initialized!\n");

    return SCRMFS_SUCCESS;
}

static int scrmfs_get_spillblock(size_t size, const char *path)
{
    void *scr_spillblock = NULL;
    int spillblock_fd;

    MAP_OR_FAIL(open);
    spillblock_fd = __real_open(path, O_RDWR | O_CREAT | O_EXCL | S_IRWXU);
    if (spillblock_fd < 0) {
        if (errno == EEXIST) {
            /* spillover block exists; attach and return */
            spillblock_fd = __real_open(path, O_RDWR );
        } else {
            perror("open() in scrmfs_get_spillblock() failed");
            return -1;
        }
    } else {
        /* new spillover block created */
        /* TODO: align to SSD block size*/
    
        /*temp*/
        off_t rc = lseek(spillblock_fd, size, SEEK_SET);
        if (rc < 0)
            perror("lseek failed");
    }

    return spillblock_fd;
}

/* create superblock of specified size and name, or attach to existing
 * block if available */
static void* scrmfs_superblock_shmget(size_t size, key_t key)
{
    void *scr_shmblock = NULL;

    /* TODO:improve error-propogation */
    //int scr_shmblock_shmid = shmget(key, size, IPC_CREAT | IPC_EXCL | S_IRWXU | SHM_HUGETLB);
    int scr_shmblock_shmid = shmget(key, size, IPC_CREAT | IPC_EXCL | S_IRWXU);
    if (scr_shmblock_shmid < 0) {
        if (errno == EEXIST) {
            /* superblock already exists, attach to it */
            scr_shmblock_shmid = shmget(key, size, 0);
            scr_shmblock = shmat(scr_shmblock_shmid, NULL, 0);
            if(scr_shmblock < 0) {
                perror("shmat() failed");
                return NULL;
            }
            debug("Superblock exists at %p!\n",scr_shmblock);

            /* init our global variables to point to spots in superblock */
            scrmfs_init_pointers(scr_shmblock);
        } else {
            perror("shmget() failed");
            return NULL;
        }
    } else {
        /* brand new superblock created, attach to it */
        scr_shmblock = shmat(scr_shmblock_shmid, NULL, 0);
        if(scr_shmblock < 0) {
            perror("shmat() failed");
        }
        debug("Superblock created at %p!\n",scr_shmblock);

#ifdef ENABLE_NUMA_POLICY
        numa_exit_on_error = 1;
        /* check to see if NUMA control capability is available */
        if (numa_available() < 0 ) {
            debug("NUMA support unavailable!\n");
        } else {
            /* if support available, set NUMA policy for scr_shmblock */
            // if ( scrmfs_use_single_shm ) {
            if ( strcmp(scrmfs_numa_policy,"interleaved") == 0) {
                /* interleave the shared-memory segment
                 * across all memory banks when all process share 1-superblock */
                debug("Interleaving superblock across all memory banks\n");
                numa_interleave_memory(scr_shmblock, size, numa_all_nodes_ptr);
            } else if( strcmp(scrmfs_numa_policy,"local") == 0) {
               /* each process has its own superblock, let it be allocated from
                * the closest memory bank */
                debug("Assigning memory from closest bank\n");
                numa_setlocal_memory(scr_shmblock, size);
            }
        }
#endif
        /* init our global variables to point to spots in superblock */
        scrmfs_init_pointers(scr_shmblock);

        /* initialize data structures within block */
        scrmfs_init_structures();
    }
    
    return scr_shmblock;
}

#ifdef MACHINE_BGQ
static void* scrmfs_superblock_bgq(size_t size, const char* name)
{
    /* BGQ allocates memory in units of 1MB */
    unsigned long block_size = 1024*1024;

    /* round request up to integer number of blocks */
    unsigned long num_blocks = (unsigned long)size / block_size;
    if (block_size * num_blocks < size) {
        num_blocks++;
    }
    unsigned long size_1MB = num_blocks * block_size;

    /* open file in persistent memory */
    int fd = persist_open((char*)name, O_RDWR, 0600);
    if (fd < 0) {
        perror("unable to open persistent memory file");
        return NULL;
    }

    /* truncate file to correct size */
    int rc = ftruncate(fd, (off_t)size_1MB);
    if (rc < 0) {
      perror("ftruncate of persistent memory region failed");
      close(fd);
      return NULL;
    }

    /* mmap file */
    void* shmptr = mmap(NULL, (size_t)size_1MB, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (shmptr == MAP_FAILED) {
        perror("mmap of shared memory region failed");
        close(fd);
        return NULL;
    }

    /* close persistent memory file */
    close(fd);

    /* init our global variables to point to spots in superblock */
    scrmfs_init_pointers(shmptr);

    /* initialize data structures within block */
    scrmfs_init_structures();

    return shmptr;
}
#endif /* MACHINE_BGQ */

/* converts string like 10mb to unsigned long long integer value of 10*1024*1024 */
static int scrmfs_abtoull(char* str, unsigned long long* val)
{
  /* check that we have a string */
  if (str == NULL) {
    debug("scr_abtoull: Can't convert NULL string to bytes @ %s:%d",
            __FILE__, __LINE__
    );
    return SCRMFS_FAILURE;
  }

  /* check that we have a value to write to */
  if (val == NULL) {
    debug("scr_abtoull: NULL address to store value @ %s:%d",
            __FILE__, __LINE__
    );
    return SCRMFS_FAILURE;
  }

  /* pull the floating point portion of our byte string off */
  errno = 0;
  char* next = NULL;
  double num = strtod(str, &next);
  if (errno != 0) {
    debug("scr_abtoull: Invalid double: %s @ %s:%d",
            str, __FILE__, __LINE__
    );
    return SCRMFS_FAILURE;
  }

  /* now extract any units, e.g. KB MB GB, etc */
  unsigned long long units = 1;
  if (*next != '\0') {
    switch(*next) {
    case 'k':
    case 'K':
      units = 1024;
      break;
    case 'm':
    case 'M':
      units = 1024*1024;
      break;
    case 'g':
    case 'G':
      units = 1024*1024*1024;
      break;
    default:
      debug("scr_abtoull: Unexpected byte string %s @ %s:%d",
              str, __FILE__, __LINE__
      );
      return SCRMFS_FAILURE;
    }

    next++;

    /* handle optional b or B character, e.g. in 10KB */
    if (*next == 'b' || *next == 'B') {
      next++;
    }

    /* check that we've hit the end of the string */
    if (*next != 0) {
      debug("scr_abtoull: Unexpected byte string: %s @ %s:%d",
              str, __FILE__, __LINE__
      );
      return SCRMFS_FAILURE;
    }
  }

  /* check that we got a positive value */
  if (num < 0) {
    debug("scr_abtoull: Byte string must be positive: %s @ %s:%d",
            str, __FILE__, __LINE__
    );
    return SCRMFS_FAILURE;
  }

  /* multiply by our units and set out return value */
  *val = (unsigned long long) (num * (double) units);

  return SCRMFS_SUCCESS;
}

static int scrmfs_init(int rank)
{
    if (! scrmfs_initialized) {
        char* env;
        unsigned long long bytes;

        /* look up page size for buffer alignment */
        scrmfs_page_size = getpagesize();

        /* will we use containers or shared memory to store the files? */
        scrmfs_use_containers = 0;
        scrmfs_use_spillover = 0;

#ifdef HAVE_CONTAINER_LIB
        env = getenv("SCRMFS_USE_CONTAINERS");
        if (env) {
            int val = atoi(env);
            if (val != 0) {
                scrmfs_use_memfs = 0;
                scrmfs_use_spillover = 0;
                scrmfs_use_containers = 1;
            }
        }
#endif

        env = getenv("SCRMFS_USE_SPILLOVER");
        if (env) {
            int val = atoi(env);
            if (val != 0) {
                scrmfs_use_spillover = 1;
            }
        }

        debug("are we using containers? %d\n", scrmfs_use_containers);
        debug("are we using spillover? %d\n", scrmfs_use_spillover);

        /* determine max number of files to store in file system */
        scrmfs_max_files = SCRMFS_MAX_FILES;
        env = getenv("SCRMFS_MAX_FILES");
        if (env) {
            int val = atoi(env);
            scrmfs_max_files = val;
        }

        /* determine number of bits for chunk size */
        scrmfs_chunk_bits = SCRMFS_CHUNK_BITS;
        env = getenv("SCRMFS_CHUNK_BITS");
        if (env) {
            int val = atoi(env);
            scrmfs_chunk_bits = val;
        }

        /* determine maximum number of bytes of memory to use for chunk storage */
        scrmfs_chunk_mem = SCRMFS_CHUNK_MEM;
        env = getenv("SCRMFS_CHUNK_MEM");
        if (env) {
            scrmfs_abtoull(env, &bytes);
            scrmfs_chunk_mem = (size_t) bytes;
        }

        /* set chunk size, set chunk offset mask, and set total number of chunks */
        scrmfs_chunk_size = 1 << scrmfs_chunk_bits;
        scrmfs_chunk_mask = scrmfs_chunk_size - 1;
        scrmfs_max_chunks = scrmfs_chunk_mem >> scrmfs_chunk_bits;

        /* determine maximum number of bytes of spillover to use for chunk storage */
        scrmfs_spillover_size = SCRMFS_SPILLOVER_SIZE;
        env = getenv("SCRMFS_SPILLOVER_SIZE");
        if (env) {
            scrmfs_abtoull(env, &bytes);
            scrmfs_spillover_size = (size_t) bytes;
        }

        /* set number of chunks in spillover device */
        scrmfs_spillover_max_chunks = scrmfs_spillover_size >> scrmfs_chunk_bits;

#ifdef ENABLE_NUMA_POLICY
        env = getenv("SCRMFS_NUMA_POLICY");
        if ( env ) {
            sprintf(scrmfs_numa_policy, env);
            debug("NUMA policy used: %s\n", scrmfs_numa_policy);
        } else {
            sprintf(scrmfs_numa_policy, "default");
        }
#endif

        /* record the max fd for the system */
        /* RLIMIT_NOFILE specifies a value one greater than the maximum
         * file descriptor number that can be opened by this process */
        struct rlimit *r_limit = malloc(sizeof(r_limit));
        if (r_limit == NULL) {
            perror("failed to allocate memory for call to getrlimit");
            return SCRMFS_FAILURE;
        }
        if (getrlimit(RLIMIT_NOFILE, r_limit) < 0) {
            perror("rlimit failed");
            free(r_limit);
            return SCRMFS_FAILURE;
        }
        scrmfs_fd_limit = r_limit->rlim_cur;
        free(r_limit);
        debug("FD limit for system = %ld\n", scrmfs_fd_limit);

        /* determine the size of the superblock */
        /* generous allocation for chunk map (one file can take entire space)*/
        size_t superblock_size = 0;
        superblock_size += scrmfs_stack_bytes(scrmfs_max_files);         /* free file id stack */
        superblock_size += scrmfs_max_files * sizeof(scrmfs_filename_t); /* file name struct array */
        superblock_size += scrmfs_max_files * sizeof(scrmfs_filemeta_t); /* file meta data struct array */
        superblock_size += scrmfs_max_files * scrmfs_max_chunks * sizeof(scrmfs_chunkmeta_t);
                                                                         /* chunk meta data struct array for each file */
        superblock_size += scrmfs_stack_bytes(scrmfs_max_chunks);        /* free chunk stack */
        if (scrmfs_use_memfs) {
           superblock_size += scrmfs_page_size + 
               (scrmfs_max_chunks * scrmfs_chunk_size);         /* memory chunks */
        }
        if (scrmfs_use_spillover) {
           superblock_size +=
               scrmfs_stack_bytes(scrmfs_spillover_max_chunks);     /* free spill over chunk stack */
        }
        if (scrmfs_use_containers) {
           /* nothing additional */
        }

        /* get a superblock of persistent memory and initialize our
         * global variables for this block */
      #ifdef MACHINE_BGQ
        char bgqname[100];
        snprintf(bgqname, sizeof(bgqname), "memory_rank_%d", rank);
        scrmfs_superblock = scrmfs_superblock_bgq(superblock_size, bgqname);
      #else /* MACHINE_BGQ */
        scrmfs_superblock = scrmfs_superblock_shmget(superblock_size, scrmfs_mount_shmget_key);
      #endif /* MACHINE_BGQ */
        if (scrmfs_superblock == NULL) {
            debug("scrmfs_superblock_shmget() failed\n");
            return SCRMFS_FAILURE;
        }
      
        char spillfile_prefix[100];
        sprintf(spillfile_prefix,"/tmp/spill_file_%d", rank);

        /* initialize spillover store */
        if (scrmfs_use_spillover) {
            size_t spillover_size = scrmfs_max_chunks * scrmfs_chunk_size;
            scrmfs_spilloverblock = scrmfs_get_spillblock(spillover_size, spillfile_prefix);

            if(scrmfs_spilloverblock < 0) {
                debug("scrmfs_get_spillblock() failed!\n");
                return SCRMFS_FAILURE;
            }
        }

      #ifdef HAVE_CONTAINER_LIB
        /* initialize the container store */
        if (scrmfs_use_containers) {
           initial_container_size = scrmfs_chunk_size * scrmfs_max_chunks; 
           int ret = scrmfs_container_init(scrmfs_container_info, &cs_store_handle, & cs_set_handle, initial_container_size, "cset1");
           if (ret != SCRMFS_SUCCESS) {
              fprintf(stderr, "failed to create container store\n");
              return SCRMFS_FAILURE;
           }
           debug("successfully created container store with size %d\n", initial_container_size);
        }
      #endif /* HAVE_CONTAINER_LIB */

        /* remember that we've now initialized the library */
        scrmfs_initialized = 1;
    }
    return SCRMFS_SUCCESS;
}

/* mount memfs at some prefix location */
int scrmfs_mount(const char prefix[], size_t size, int rank)
{
    scrmfs_mount_prefix = strdup(prefix);
    scrmfs_mount_prefixlen = strlen(scrmfs_mount_prefix);

    /* KMM commented out because we're just using a single rank, so use PRIVATE
     * downside, can't attach to this in another srun (PRIVATE, that is) */
    //scrmfs_mount_shmget_key = SCRMFS_SUPERBLOCK_KEY + rank;

    char * env = getenv("SCRMFS_USE_SINGLE_SHM");
    if (env) {
        int val = atoi(env);
        if (val != 0) {
            scrmfs_use_single_shm = 1;
        }
    }

    if (scrmfs_use_single_shm) {
        scrmfs_mount_shmget_key = SCRMFS_SUPERBLOCK_KEY + rank;
    } else {
        scrmfs_mount_shmget_key = IPC_PRIVATE;
    }

    /* initialize our library */
    scrmfs_init(rank);

    /* add mount point as a new directory in the file list */
    if (scrmfs_get_fid_from_path(prefix) >= 0) {
        /* we can't mount this location, because it already exists */
        errno = EEXIST;
        return -1;
    } else {
        /* claim an entry in our file list */
        int fid = scrmfs_fid_create_directory(prefix);
        if (fid < 0) {
            /* if there was an error, return it */
            return fid;
        }
    }

    return 0;
}

/* ---------------------------------------
 * POSIX wrappers: paths
 * --------------------------------------- */

int SCRMFS_DECL(access)(const char *path, int mode)
{
    /* determine whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* check if path exists */
        if (scrmfs_get_fid_from_path(path) < 0) {
            debug("access: scrmfs_get_id_from path failed, returning -1, %s\n", path);
            errno = ENOENT;
            return -1;
        }

        /* currently a no-op */
        debug("access: path intercepted, returning 0, %s\n", path);
        return 0;
    } else {
        debug("access: calling MAP_OR_FAIL, %s\n", path);
        MAP_OR_FAIL(access);
        int ret = __real_access(path, mode);
        debug("access: returning __real_access %d,%s\n", ret, path);
        return ret;
    }
}

int SCRMFS_DECL(mkdir)(const char *path, mode_t mode)
{
    /* Support for directories is very limited at this time
     * mkdir simply puts an entry into the filelist for the
     * requested directory (assuming it does not exist)
     * It doesn't check to see if parent directory exists */

    /* determine whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* check if it already exists */
        if (scrmfs_get_fid_from_path(path) >= 0) {
            errno = EEXIST;
            return -1;
        }

        /* add directory to file list */
        int fid = scrmfs_fid_create_directory(path);
        return 0;
    } else {
        MAP_OR_FAIL(mkdir);
        int ret = __real_mkdir(path, mode);
        return ret;
    }
}

int SCRMFS_DECL(rmdir)(const char *path)
{
    /* determine whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* check if the mount point itself is being deleted */
        if (! strcmp(path, scrmfs_mount_prefix)) {
            errno = EBUSY;
            return -1;
        }

        /* check if path exists */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            errno = ENOENT;
            return -1;
        }

        /* is it a directory? */
        if (!scrmfs_fid_is_dir(fid)) {
            errno = ENOTDIR;
            return -1;
        }

        /* is it empty? */
        if (!scrmfs_fid_is_dir_empty(path)) {
            errno = ENOTEMPTY;
            return -1;
        }

        /* remove the directory from the file list */ 
        int ret = scrmfs_fid_unlink(fid);
        return 0;
    } else {
        MAP_OR_FAIL(rmdir);
        int ret = __real_rmdir(path);
        return ret;
    }
}

int SCRMFS_DECL(rename)(const char *oldpath, const char *newpath)
{
    /* TODO: allow oldpath / newpath to split across memfs and normal
     * linux fs, which means we'll need to do a read / write */

    /* check whether the old path is in our file system */
    if (scrmfs_intercept_path(oldpath)) {
        /* for now, we can only rename within our file system */
        if (! scrmfs_intercept_path(newpath)) {
            /* ERROR: can't yet rename across file systems */
            errno = EXDEV;
            return -1;
        }

        /* verify that we really have a file by the old name */
        int fid = scrmfs_get_fid_from_path(oldpath);
        debug("orig file in position %d\n",fid);
        if (fid < 0) {
            /* ERROR: oldname does not exist */
            debug("Couldn't find entry for %s in SCRMFS\n",oldpath);
            errno = ENOENT;
            return -1;
        }

        /* verify that we don't already have a file by the new name */
        if (scrmfs_get_fid_from_path(newpath) < 0) {
            /* check that new name is within bounds */
            size_t newpathlen = strlen(newpath) + 1;
            if (newpathlen > SCRMFS_MAX_FILENAME) {
                errno = ENAMETOOLONG;
                return -1;
            }

            /* finally overwrite the old name with the new name */
            debug("Changing %s to %s\n",(char*)&scrmfs_filelist[fid].filename, newpath);
            strcpy((void *)&scrmfs_filelist[fid].filename, newpath);
        } else {
            /* ERROR: new name already exists */
            debug("File %s exists\n",newpath);
            errno = EEXIST;
            return -1;
        }
        return 0;
    } else {
        /* for now, we can only rename within our file system */
        if (scrmfs_intercept_path(newpath)) {
            /* ERROR: can't yet rename across file systems */
            errno = EXDEV;
            return -1;
        }

        /* both files are normal linux files, delegate to system call */
        MAP_OR_FAIL(rename);
        int ret = __real_rename(oldpath,newpath);
        return ret;
    }
}

int SCRMFS_DECL(truncate)(const char* path, off_t length)
{
    /* determine whether we should intercept this path or not */
    if (scrmfs_intercept_path(path)) {
        /* lookup the fd for the path */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            /* ERROR: file does not exist */
            debug("Couldn't find entry for %s in SCRMFS\n", path);
            errno = ENOENT;
            return -1;
        }

        /* truncate the file */
        int rc = scrmfs_fid_truncate(fid, length);
        if (rc != SCRMFS_SUCCESS) {
            debug("scrmfs_fid_truncate failed for %s in SCRMFS\n", path);
            errno = EIO;
            return -1;
        }

        return 0;
    } else {
        MAP_OR_FAIL(truncate);
        int ret = __real_truncate(path, length);
        return ret;
    }
}

int SCRMFS_DECL(unlink)(const char *path)
{
    /* determine whether we should intercept this path or not */
    if (scrmfs_intercept_path(path)) {
        /* get file id for path name */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            /* ERROR: file does not exist */
            debug("Couldn't find entry for %s in SCRMFS\n",path);
            errno = ENOENT;
            return -1;
        }

        /* check that it's not a directory */
        if (scrmfs_fid_is_dir(fid)) {
            /* ERROR: is a directory */
            debug("Attempting to unlink a directory %s in SCRMFS\n",path);
            errno = EISDIR;
            return -1;
        }

        /* delete the file */
        scrmfs_fid_unlink(fid);

        return 0;
    } else {
        MAP_OR_FAIL(unlink);
        int ret = __real_unlink(path);
        return ret;
    }
}

int SCRMFS_DECL(stat)( const char *path, struct stat *buf)
{
    debug("stat was called for %s....\n",path);
    if (scrmfs_intercept_path(path)) {
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            errno = ENOENT;
            return -1;
        }

        scrmfs_fid_stat(fid, buf);

        return 0;
    } else {
        MAP_OR_FAIL(stat);
        int ret = __real_stat(path, buf);
        return ret;
    }
} 

int SCRMFS_DECL(__xstat)(int vers, const char *path, struct stat *buf)
{
    debug("xstat was called for %s....\n",path);
    if (scrmfs_intercept_path(path)) {
        /* get file id for path */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            /* file doesn't exist */
            errno = ENOENT;
            return -1;
        }

        /* get meta data for this file */
        scrmfs_fid_stat(fid, buf);

        return 0;
    } else { 
        MAP_OR_FAIL(__xstat);
        int ret = __real___xstat(vers, path, buf);
        return ret;
    }
}

int SCRMFS_DECL(__xstat64)(int vers, const char *path, struct stat64 *buf)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOENT;
        return -1;
    } else {
        MAP_OR_FAIL(__xstat64);
        int ret = __real___xstat64(vers, path, buf);
        return ret;
    }
}

int SCRMFS_DECL(__lxstat)(int vers, const char *path, struct stat *buf)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOENT;
        return -1;
    } else {
        MAP_OR_FAIL(__lxstat);
        int ret = __real___lxstat(vers, path, buf);
        return ret;
    }
}

int SCRMFS_DECL(__lxstat64)(int vers, const char *path, struct stat64 *buf)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOENT;
        return -1;
    } else {
        MAP_OR_FAIL(__lxstat64);
        int ret = __real___lxstat64(vers, path, buf);
        return ret;
    }
}

/* ---------------------------------------
 * POSIX wrappers: file descriptors
 * --------------------------------------- */

/* read count bytes info buf from file starting at offset pos,
 * returns number of bytes actually read in retcount,
 * retcount will be less than count only if an error occurs
 * or end of file is reached */
static int scrmfs_fd_read(int fd, off_t pos, void* buf, size_t count, size_t* retcount)
{
    /* get the file id for this file descriptor */
    int fid = scrmfs_get_fid_from_fd(fd);
    if (fid < 0) {
        return SCRMFS_ERR_BADF;
    }
       
    /* it's an error to read from a directory */
    if(scrmfs_fid_is_dir(fid)){
       /* TODO: note that read/pread can return this, but not fread */
       return SCRMFS_ERR_ISDIR;
    }

    /* TODO: is it safe to assume that off_t is bigger than size_t? */
    /* check that we don't overflow the file length */
    if (scrmfs_would_overflow_offt(pos, (off_t) count)) {
        return SCRMFS_ERR_OVERFLOW;
    }

    /* check that we don't try to read past the end of the file */
    off_t lastread = pos + (off_t) count;
    off_t filesize = scrmfs_fid_size(fid);
    if (filesize < lastread) {
        /* adjust count so we don't read past end of file */
        count = (size_t) (filesize - pos);
    }

    /* record number of bytes that we'll actually read */
    *retcount = count;

    /* if we don't read any bytes, return success */
    if (count == 0){
        return SCRMFS_SUCCESS;
    }

    /* read data from file */
    int read_rc = scrmfs_fid_read(fid, pos, buf, count);
    return read_rc;
}

/* write count bytes from buf into file starting at offset pos,
 * allocates new bytes and updates file size as necessary,
 * fills any gaps with zeros */
static int scrmfs_fd_write(int fd, off_t pos, const void* buf, size_t count)
{
    /* get the file id for this file descriptor */
    int fid = scrmfs_get_fid_from_fd(fd);
    if (fid < 0) {
        return SCRMFS_ERR_BADF;
    }

    /* it's an error to write to a directory */
    if(scrmfs_fid_is_dir(fid)){
        return SCRMFS_ERR_INVAL;
    }

    /* TODO: is it safe to assume that off_t is bigger than size_t? */
    /* check that our write won't overflow the length */
    if (scrmfs_would_overflow_offt(pos, (off_t) count)) {
        return SCRMFS_ERR_OVERFLOW;
    }

    /* get current file size before extending the file */
    off_t filesize = scrmfs_fid_size(fid);

    /* extend file size and allocate chunks if needed */
    off_t newpos = pos + (off_t) count;
    int extend_rc = scrmfs_fid_extend(fid, newpos);
    if (extend_rc != SCRMFS_SUCCESS) {
        return extend_rc;
    }

    /* fill any new bytes between old size and pos with zero values */
    if (filesize < pos) {
        off_t gap_size = pos - filesize;
        int zero_rc = scrmfs_fid_write_zero(fid, filesize, gap_size);
        if (zero_rc != SCRMFS_SUCCESS) {
            return zero_rc;
        }
    }

    debug("request to write %d bytes to position %d\n", count, pos);
    /* finally write specified data to file */
    int write_rc = scrmfs_fid_write(fid, pos, buf, count);
    
    return write_rc;
}

int SCRMFS_DECL(creat)(const char* path, mode_t mode)
{
    /* equivalent to open(path, O_WRONLY|O_CREAT|O_TRUNC, mode) */

    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* TODO: handle relative paths using current working directory */

        /* create the file */
        int fid;
        off_t pos;
        int rc = scrmfs_fid_open(path, O_WRONLY | O_CREAT | O_TRUNC, mode, &fid, &pos);
        if (rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(rc);
            return -1;
        }

        /* TODO: allocate a free file descriptor and associate it with fid */
        /* set in_use flag and file pointer */
        scrmfs_fds[fid].pos = pos;
        debug("SCRMFS_open generated fd %d for file %s\n", fid, path);    

        /* don't conflict with active system fds that range from 0 - (fd_limit) */
        int ret = fid + scrmfs_fd_limit;
        return ret;
    } else {
        MAP_OR_FAIL(creat);
        int ret = __real_creat(path, mode);
        return ret ;
    }
}

int SCRMFS_DECL(creat64)(const char* path, mode_t mode)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        return -1;
    } else {
        MAP_OR_FAIL(creat64);
        int ret = __real_creat64(path, mode);
        return ret;
    }
}

int SCRMFS_DECL(open)(const char *path, int flags, ...)
{
    int ret;

    /* if O_CREAT is set, we should also have some mode flags */
    int mode = 0;
    if (flags & O_CREAT) {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);
    }
    
    /* determine whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* TODO: handle relative paths using current working directory */

        /* create the file */
        int fid;
        off_t pos;
        int rc = scrmfs_fid_open(path, flags, mode, &fid, &pos);
        if (rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(rc);
            return -1;
        }

        /* TODO: allocate a free file descriptor and associate it with fid */
        /* set in_use flag and file pointer */
        scrmfs_fds[fid].pos = pos;
        debug("SCRMFS_open generated fd %d for file %s\n", fid, path);    

        /* don't conflict with active system fds that range from 0 - (fd_limit) */
        ret = fid + scrmfs_fd_limit;
        return ret;
    } else {
        MAP_OR_FAIL(open);
        if (flags & O_CREAT) {
            ret = __real_open(path, flags, mode);
        } else {
            ret = __real_open(path, flags);
        }
        return ret;
    }
}

int SCRMFS_DECL(open64)(const char* path, int flags, ...)
{
    int ret;

    /* if O_CREAT is set, we should also have some mode flags */
    int mode = 0;
    if (flags & O_CREAT) {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);
    }

    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        return -1;
    } else {
        MAP_OR_FAIL(open64);
        if (flags & O_CREAT) {
            ret = __real_open64(path, flags, mode);
        } else {
            ret = __real_open64(path, flags);
        }
    }

    return ret;
}

off_t SCRMFS_DECL(lseek)(int fd, off_t offset, int whence)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* TODO: check that fd is actually in use */

        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);

        /* check that file descriptor is good */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        if (meta == NULL) {
            /* bad file descriptor */
            errno = EBADF;
            return (off_t)-1;
        }

        debug("seeking from %ld\n", scrmfs_fds[fd].pos);        
        off_t current_pos = scrmfs_fds[fd].pos;

        switch (whence)
        {
            case SEEK_SET:
                /* seek to offset */
                current_pos = offset;
                break;
            case SEEK_CUR:
                /* seek to current position + offset */
                current_pos += offset;
                break;
            case SEEK_END:
                /* seek to EOF + offset */
                current_pos = meta->size + offset;
                break;
            default:
                errno = EINVAL;
                return (off_t)-1;
        }

        scrmfs_fds[fd].pos = current_pos;

        debug("seeking to %ld\n", scrmfs_fds[fd].pos);        
        return current_pos;
    } else {
        MAP_OR_FAIL(lseek);
        int ret = __real_lseek(fd, offset, whence);
        return ret;
    }
}

off64_t SCRMFS_DECL(lseek64)(int fd, off64_t offset, int whence)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return (off64_t)-1;
    } else {
      MAP_OR_FAIL(lseek64);
      off64_t ret = __real_lseek64(fd, offset, whence);
      return ret;
    }
}

int SCRMFS_DECL(posix_fadvise)(int fd, off_t offset, off_t len, int advice)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* check that the file descriptor is valid */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            errno = EBADF;
            return errno;
        }

        /* process advice from caller */
        switch( advice ) {
            case POSIX_FADV_NORMAL:
            case POSIX_FADV_SEQUENTIAL:
                /* can use this hint for a better compression strategy */
            case POSIX_FADV_RANDOM:
            case POSIX_FADV_NOREUSE:
            case POSIX_FADV_WILLNEED:
                /* with the spill-over case, we can use this hint to
                 * to better manage the in-memory parts of a file. On
                 * getting this advice, move the chunks that are on the
                 * spill-over device to the in-memory portion
                 */
            case POSIX_FADV_DONTNEED:
                /* similar to the previous case, but move contents from memory
                 * to the spill-over device instead.
                 */

                /* ERROR: fn not yet supported */
                fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
                break;
            default:
                /* this function returns the errno itself, not -1 */
                errno = EINVAL;
                return errno;
        }

        /* just a hint so return success even if we don't do anything */
        return 0;
    } else {
      MAP_OR_FAIL(posix_fadvise);
      int ret = __real_posix_fadvise(fd, offset, len, advice);
      return ret;
    }
}

ssize_t SCRMFS_DECL(read)(int fd, void *buf, size_t count)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return (ssize_t)-1;
        }

        /* read data from file */
        size_t retcount;
        int read_rc = scrmfs_fd_read(fd, filedesc->pos, buf, count, &retcount);
        if (read_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(read_rc);
            return (ssize_t)-1;
        }

        /* update position */
        filedesc->pos += (off_t) retcount;

        /* return number of bytes read */
        return (ssize_t) retcount;
    } else {
        MAP_OR_FAIL(read);
        ssize_t ret = __real_read(fd, buf, count);
        return ret;
    }
}

/* TODO: find right place to msync spillover mapping */
ssize_t SCRMFS_DECL(write)(int fd, const void *buf, size_t count)
{
    ssize_t ret;

    /* check file descriptor to determine whether we should pick off this call */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return (ssize_t)-1;
        }

        /* write data to file */
        int write_rc = scrmfs_fd_write(fd, filedesc->pos, buf, count);
        if (write_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(write_rc);
            return (ssize_t)-1;
        }

        /* update file position */
        filedesc->pos += count;

        /* return number of bytes read */
        ret = count;
    } else {
        MAP_OR_FAIL(write);
        ret = __real_write(fd, buf, count);
    }

    return ret;
}

ssize_t SCRMFS_DECL(readv)(int fd, const struct iovec *iov, int iovcnt)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(readv);
        ssize_t ret = __real_readv(fd, iov, iovcnt);
        return ret;
    }
}

ssize_t SCRMFS_DECL(writev)(int fd, const struct iovec *iov, int iovcnt)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(writev);
        ssize_t ret = __real_writev(fd, iov, iovcnt);
        return ret;
    }
}

ssize_t SCRMFS_DECL(pread)(int fd, void *buf, size_t count, off_t offset)
{
    /* equivalent to read(), except that it shall read from a given
     * position in the file without changing the file pointer */

    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return (ssize_t)-1;
        }

        /* read data from file */
        size_t retcount;
        int read_rc = scrmfs_fd_read(fd, offset, buf, count, &retcount);
        if (read_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(read_rc);
            return (ssize_t)-1;
        }

        /* return number of bytes read */
        return (ssize_t) retcount;
    } else {
        MAP_OR_FAIL(pread);
        ssize_t ret = __real_pread(fd, buf, count, offset);
        return ret;
    }
}

ssize_t SCRMFS_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(pread64);
        ssize_t ret = __real_pread64(fd, buf, count, offset);
        return ret;
    }
}

ssize_t SCRMFS_DECL(pwrite)(int fd, const void *buf, size_t count, off_t offset)
{
    /* equivalent to write(), except that it writes into a given
     * position without changing the file pointer */

    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return (ssize_t)-1;
        }

        /* write data to file */
        int write_rc = scrmfs_fd_write(fd, offset, buf, count);
        if (write_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(write_rc);
            return (ssize_t)-1;
        }

        /* return number of bytes read */
        return (ssize_t) count;
    } else {
        MAP_OR_FAIL(pwrite);
        ssize_t ret = __real_pwrite(fd, buf, count, offset);
        return ret;
    }
}

ssize_t SCRMFS_DECL(pwrite64)(int fd, const void *buf, size_t count, off64_t offset)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(pwrite64);
        ssize_t ret = __real_pwrite64(fd, buf, count, offset);
        return ret;
    }
}

int SCRMFS_DECL(ftruncate)(int fd, off_t length)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return -1;
        }

        /* truncate the file */
        int rc = scrmfs_fid_truncate(fid, length);
        if (rc != SCRMFS_SUCCESS) {
            errno = EIO;
            return -1;
        }

        return 0;
    } else {
        MAP_OR_FAIL(ftruncate);
        int ret = __real_ftruncate(fd, length);
        return ret;
    }
}

int SCRMFS_DECL(fsync)(int fd)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return -1;
        }

        /* TODO: if using spill over we may have some fsyncing to do */

        /* nothing to do in our case */
        return 0;
    } else {
        MAP_OR_FAIL(fsync);
        int ret = __real_fsync(fd);
        return ret;
    }
}

int SCRMFS_DECL(fdatasync)(int fd)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(fdatasync);
        int ret = __real_fdatasync(fd);
        return ret;
    }
}

int SCRMFS_DECL(flock)(int fd, int operation)
{
    int intercept;
    int ret;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
     // KMM I removed the locking code because it was causing
     // hangs
      /*
        -- currently handling the blocking variants only 
        switch (operation)
        {
            case LOCK_EX:
                debug("locking file %d..\n",fid);
                ret = pthread_spin_lock(&meta->fspinlock);
                if ( ret ) {
                    perror("pthread_spin_lock() failed");
                    return -1;
                }
                meta->flock_status = EX_LOCKED;
                break;
            case LOCK_SH:
                -- not needed for CR; will not be supported,
                --  update flock_status anyway 
                meta->flock_status = SH_LOCKED;
                break;
            case LOCK_UN:
                ret = pthread_spin_unlock(&meta->fspinlock);
                debug("unlocking file %d..\n",fid);
                meta->flock_status = UNLOCKED;
                break;
            default:
                errno = EINVAL;
                return -1;
        }
       */

        return 0;
    } else {
        MAP_OR_FAIL(flock);
        ret = __real_flock(fd,operation);
        return ret;
    }
}

/* TODO: handle different flags */
void* SCRMFS_DECL(mmap)(void *addr, size_t length, int prot, int flags,
    int fd, off_t offset)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            errno = EBADF;
            return MAP_FAILED;
        }

        /* TODO: handle addr properly based on flags */

        /* allocate memory required to mmap the data if addr is NULL;
         * using posix_memalign instead of malloc to align mmap'ed area
         * to page size */
        if (! addr) {
            int ret = posix_memalign(&addr, sysconf(_SC_PAGE_SIZE), length);
            if (ret) {
                /* posix_memalign does not set errno */
                if (ret == EINVAL) {
                    errno = EINVAL;
                    return MAP_FAILED;
                }

                if (ret == ENOMEM) {
                    errno = ENOMEM;
                    return MAP_FAILED;
                }
            }
        }

        /* TODO: do we need to extend file if offset+length goes past current end? */

        /* check that we don't copy past the end of the file */
        off_t last_byte = offset + length;
        off_t file_size = scrmfs_fid_size(fid);
        if (last_byte > file_size) {
            /* trying to copy past the end of the file, so
             * adjust the total amount to be copied */
            length = (size_t) (file_size - offset);
        }

        /* read data from file */
        int rc = scrmfs_fid_read(fid, offset, addr, length);
        if (rc != SCRMFS_SUCCESS) {
            /* TODO: need to free memory in this case? */
            errno = ENOMEM;
            return MAP_FAILED;
        }

        return addr;
    } else {
        MAP_OR_FAIL(mmap);
        void* ret = __real_mmap(addr, length, prot, flags, fd, offset);
        return ret;
    }
}

int SCRMFS_DECL(munmap)(void *addr, size_t length)
{
    fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
    errno = ENOSYS;
    return ENODEV;
}

int SCRMFS_DECL(msync)(void *addr, size_t length, int flags)
{
    /* TODO: need to keep track of all the mmaps that are linked to
     * a given file before this function can be implemented*/
    fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
    errno = ENOSYS;
    return ENOMEM;
}

void* SCRMFS_DECL(mmap64)(void *addr, size_t length, int prot, int flags,
    int fd, off64_t offset)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOSYS;
        return MAP_FAILED;
    } else {
        MAP_OR_FAIL(mmap64);
        void* ret = __real_mmap64(addr, length, prot, flags, fd, offset);
        return ret;
    }
}

int SCRMFS_DECL(__fxstat)(int vers, int fd, struct stat *buf)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(__fxstat);
        int ret = __real___fxstat(vers, fd, buf);
        return ret;
    }
}

int SCRMFS_DECL(__fxstat64)(int vers, int fd, struct stat64 *buf)
{
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(__fxstat64);
        int ret = __real___fxstat64(vers, fd, buf);
        return ret;
    }
}

int SCRMFS_DECL(close)(int fd)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        debug("closing fd %d\n", fd);

        /* TODO: what to do if underlying file has been deleted? */

        /* check that fd is actually in use */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            errno = EBADF;
            return -1;
        }

        /* close the file id */
        int close_rc = scrmfs_fid_close(fid);
        if (close_rc != SCRMFS_SUCCESS) {
            errno = EIO;
            return -1;
        }

        /* TODO: free file descriptor */

        return 0;
    } else {
        MAP_OR_FAIL(close);
        int ret = __real_close(fd);
        return ret;
    }
}

/* ---------------------------------------
 * POSIX wrappers: file streams
 * --------------------------------------- */

#if 0
static int scrmfs_stream_alloc(int fd)
{
    /* check that file descriptor is valid */
    if (fd >= 0 && fd < SCRMFS_FILE_DESCS) {
        /* TODO: use a stack to allocate a new stream */
        /* check that stream corresponding to this file descriptor is free */
        if (scrmfs_streams[fd].fd < 0) {
            scrmfs_streams[fd].fd = fd;
            return fd;
        }
    }
    return -1;
}

static int scrmfs_stream_free(int sid)
{
    if (sid >= 0 && sid < SCRMFS_FILE_DESCS) {
        /* mark file descriptor as -1 to indicate stream is not in use */
        scrmfs_streams[sid].fd = -1;
    }
}
#endif

static int scrmfs_fopen_parse_mode(const char* mode, int* read, int* write, int* append, int* plus)
{
    /* we'll set each of these to 1 as we find them */
    *read   = 0;
    *write  = 0;
    *append = 0;
    *plus   = 0;

    /* ensure that user specifed an input mode */
    if (mode == NULL) {
        return SCRMFS_ERR_INVAL;
    }

    /* get number of characters in mode */
    size_t len = strlen(mode);
    if (len <= 0 || len > 3) {
        return SCRMFS_ERR_INVAL;
    }

    /* first character must either be r, w, or a */
    char first = mode[0];
    switch (first) {
    case 'r':
        *read = 1;
        break;
    case 'w':
        *write = 1;
        break;
    case 'a':
        *append = 1;
        break;
    default:
        return SCRMFS_ERR_INVAL;
    }

    /* optional second character may either by + or b */
    if (len > 1) {
        char second = mode[1];
        if (second == '+') {
            /* second character is a plus */
            *plus = 1;

            /* if there is a third character, it must be b */
            if (len > 2) {
                char third = mode[2];
                if (third != 'b') {
                    /* third character something other than + or b */
                    return SCRMFS_ERR_INVAL;
                }
            }
        } else if (second == 'b') {
            /* second character is a b, if there is a third it must be + */
            if (len > 2) {
                char third = mode[2];
                if (third == '+') {
                    *plus = 1;
                } else {
                    /* third character something other than + or b */
                    return SCRMFS_ERR_INVAL;
                }
            }
        } else {
            /* second character something other than + or b */
            return SCRMFS_ERR_INVAL;
        }
    }

    return SCRMFS_SUCCESS;
}

static int scrmfs_fopen(const char* path, const char* mode, FILE** outstream)
{
    /* assume that we'll fail */
    *outstream = NULL;

    /* parse the fopen mode string */
    int read, write, append, plus;
    int parse_rc = scrmfs_fopen_parse_mode(mode, &read, &write, &append, &plus);
    if (parse_rc != SCRMFS_SUCCESS) {
        return parse_rc;
    }

    /* TODO: get real permissions */
    /* assume default permissions */
    mode_t perms = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

    int open_rc;
    int fid;
    off_t pos;
    if (read) {
      if (plus) {
          /* r+ ==> open file for update (reading and writing) */
          open_rc = scrmfs_fid_open(path, O_RDWR, perms, &fid, &pos);
      } else {
          /* r  ==> open file for reading */
          open_rc = scrmfs_fid_open(path, O_RDONLY, perms, &fid, &pos);
      }
    } else if (write) {
      if (plus) {
          /* w+ ==> truncate to zero length or create file for update (read/write) */
          open_rc = scrmfs_fid_open(path, O_RDWR | O_CREAT | O_TRUNC, perms, &fid, &pos);
      } else {
          /* w  ==> truncate to zero length or create file for writing */
          open_rc = scrmfs_fid_open(path, O_WRONLY | O_CREAT | O_TRUNC, perms, &fid, &pos);
      }
    } else if (append) {
      /* TODO: need to ignore fseek in this mode */

      if (plus) {
          /* a+ ==> append, open or create file for update, at end of file */
          open_rc = scrmfs_fid_open(path, O_RDWR | O_CREAT | O_APPEND, perms, &fid, &pos);
      } else {
          /* a  ==> append, open or create file for writing, at end of file */
          open_rc = scrmfs_fid_open(path, O_WRONLY | O_CREAT | O_APPEND, perms, &fid, &pos);
      }
    }

    /* check the open return code */
    if (open_rc != SCRMFS_SUCCESS) {
        return open_rc;
    }

    /* allocate a stream for this file */
    scrmfs_stream_t* s = &(scrmfs_streams[fid]);

    /* allocate a file descriptor for this file */
    int fd = fid;

    /* clear the error and eof indicators and remember the file descriptor */
    s->err = 0;
    s->eof = 0;
    s->fd  = fd;

    /* set the file pointer in the file descriptor */
    scrmfs_fds[fd].pos = pos;

    /* set return parameter and return */
    *outstream = (FILE*)s;
    return SCRMFS_SUCCESS;
}

FILE* SCRMFS_DECL(fopen)(const char *path, const char *mode)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        FILE* stream;
        int rc = scrmfs_fopen(path, mode, &stream);
        if (rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(rc);
            return NULL;
        }
        return stream;
    } else {
        MAP_OR_FAIL(fopen);
        FILE* ret = __real_fopen(path, mode);
        return ret;
    }
}

FILE* SCRMFS_DECL(fopen64)(const char *path, const char *mode)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOENT;
        return NULL;
    } else {
        MAP_OR_FAIL(fopen64);
        FILE* ret = __real_fopen64(path, mode);
        return ret;
    }
}

/* reads count bytes from stream into buf, sets stream EOF and error indicators as appropriate,
 * sets errno if error, updates file position, returns number of bytes read in retcount */
static int scrmfs_stream_read(FILE* stream, void* buf, size_t count, size_t* retcount)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

    /* TODO: check that stream is valid */

    /* get pointer to file descriptor structure */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
    if (filedesc == NULL) {
        /* ERROR: invalid file descriptor */
        s->err = 1;
        errno = EBADF;
        return SCRMFS_ERR_BADF;
    }

    /* don't attempt read if end-of-file indicator is set */
    if (s->eof) {
        return SCRMFS_FAILURE;
    }

    /* read data from file */
    int read_rc = scrmfs_fd_read(s->fd, filedesc->pos, buf, count, retcount);
    if (read_rc != SCRMFS_SUCCESS) {
        /* ERROR: read error, set error indicator and errno */
        s->err = 1;
        errno = scrmfs_err_map_to_errno(read_rc);
        return read_rc;
    }

    /* update file position */
    filedesc->pos += (off_t) *retcount;

    /* set end of file indicator if we hit the end */
    if (*retcount < count) {
        s->eof = 1;
    }

    /* return success */
    return SCRMFS_SUCCESS;
}

/* writes count bytes from buf to stream, sets stream EOF and error indicators as appropriate,
 * sets errno if error, updates file position */
static int scrmfs_stream_write(FILE* stream, const void* buf, size_t count)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

    /* TODO: check that stream is valid */

    /* get pointer to file descriptor structure */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
    if (filedesc == NULL) {
        /* ERROR: invalid file descriptor */
        s->err = 1;
        errno = EBADF;
        return SCRMFS_ERR_BADF;
    }

    /* TODO: if stream is append mode, always append to end of file */

    /* write data to file */
    int write_rc = scrmfs_fd_write(s->fd, filedesc->pos, buf, count);
    if (write_rc != SCRMFS_SUCCESS) {
        /* ERROR: write error, set error indicator and errno */
        s->err = 1;
        errno = scrmfs_err_map_to_errno(write_rc);
        return write_rc;
    }

    /* update file position */
    filedesc->pos += (off_t) count;

    return SCRMFS_SUCCESS;
}

int SCRMFS_DECL(fgetc)(FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* read next character from file */
        unsigned char charbuf;
        size_t count = 1;
        size_t retcount;
        int read_rc = scrmfs_stream_read(stream, &charbuf, count, &retcount);
        if (read_rc != SCRMFS_SUCCESS || retcount == 0) {
            /* stream read sets error indicator, EOF indicator, and errno for us */
            return EOF;
        }

        /* return byte read cast as an int */
        return (int) charbuf;
    } else {
        MAP_OR_FAIL(fgetc);
        int ret = __real_fgetc(stream);
        return ret;
    }
}

int SCRMFS_DECL(fputc)(int c, FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* write data to file */
        unsigned char charbuf = (unsigned char) c;
        size_t count = 1;
        int write_rc = scrmfs_stream_write(stream, &charbuf, count);
        if (write_rc != SCRMFS_SUCCESS) {
            /* stream write sets error indicator, EOF indicator, and errno for us */
            return EOF;
        }

        /* return value written */
        return (int) charbuf;
    } else {
        MAP_OR_FAIL(fputc);
        int ret = __real_fputc(c, stream);
        return ret;
    }
}

int SCRMFS_DECL(getc)(FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* read next character from file */
        unsigned char charbuf;
        size_t count = 1;
        size_t retcount;
        int read_rc = scrmfs_stream_read(stream, &charbuf, count, &retcount);
        if (read_rc != SCRMFS_SUCCESS || retcount == 0) {
            /* stream read sets error indicator, EOF indicator, and errno for us */
            return EOF;
        }

        /* return byte read cast as an int */
        return (int) charbuf;
    } else {
        MAP_OR_FAIL(getc);
        int ret = __real_getc(stream);
        return ret;
    }
}

int SCRMFS_DECL(putc)(int c, FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* write data to file */
        unsigned char charbuf = (unsigned char) c;
        size_t count = 1;
        int write_rc = scrmfs_stream_write(stream, &charbuf, count);
        if (write_rc != SCRMFS_SUCCESS) {
            /* stream write sets error indicator, EOF indicator, and errno for us */
            return EOF;
        }

        /* return value written */
        return (int) charbuf;
    } else {
        MAP_OR_FAIL(putc);
        int ret = __real_putc(c, stream);
        return ret;
    }
}

char* SCRMFS_DECL(fgets)(char* s, int n, FILE* stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* TODO: this isn't the most efficient algorithm, but it works,
         * would be faster to read a large block of characters then
         * scan for newline */

        /* read one character at a time until we hit a newline, read
         * n-1 characters or hit the end of the file (or a read error) */
        int limit = 0;
        while (limit < n-1) {
            /* read the next character from the file */
            char charbuf;
            size_t retcount;
            int read_rc = scrmfs_stream_read(stream, &charbuf, 1, &retcount);
            if (read_rc != SCRMFS_SUCCESS) {
                /* stream read sets error indicator, EOF indicator,
                 * and errno for us */
                return NULL;
            }

            /* if we hit the end of the file, terminate the string
             * and return NULL */
            if (retcount < 1) {
                s[limit] = '\0';
                return NULL;
            }

            /* copy character to buffer */
            s[limit] = charbuf;
            limit++;

            /* if we hit a newline, break early */
            if (charbuf == '\n') {
                break;
            }
        }

        return s;
    } else {
        MAP_OR_FAIL(fgets);
        char* ret = __real_fgets(s, n, stream);
        return ret;
    }
}

int SCRMFS_DECL(fputs)(const char* s, FILE* stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* TODO: check that s is not NULL */

        /* get length of string, less the NUL-terminating byte */
        size_t count = strlen(s);

        /* write data to file */
        int write_rc = scrmfs_stream_write(stream, (const void*)s, count);
        if (write_rc != SCRMFS_SUCCESS) {
            /* stream write sets error indicator, EOF indicator, and errno for us */
            return EOF;
        }

        /* return success */
        return 0;
    } else {
        MAP_OR_FAIL(fputs);
        int ret = __real_fputs(s, stream);
        return ret;
    }
}

size_t SCRMFS_DECL(fread)(void *ptr, size_t size, size_t nitems, FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* must return 0 and do nothing if size or nitems is zero */
        if (size == 0 || nitems == 0) {
            return 0;
        }

        /* TODO: need to call fgetc size times? */
        /* compute total number of bytes that we'll write */
        size_t count = size * nitems;

        /* read next character from file */
        size_t retcount;
        int read_rc = scrmfs_stream_read(stream, ptr, count, &retcount);
        if (read_rc != SCRMFS_SUCCESS) {
            /* stream read sets error indicator, EOF indicator, and errno for us */
            return 0;
        }

        /* return number of items read */
        if (retcount < count) {
            /* adjust return value if we read less data than requested */
            size_t nitems_read = retcount / size;
            return nitems_read;
        } else {
            return nitems;
        }
    } else {
        MAP_OR_FAIL(fread);
        size_t ret = __real_fread(ptr, size, nitems, stream);
        return ret;
    }
}

size_t SCRMFS_DECL(fwrite)(const void *ptr, size_t size, size_t nitems, FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* must return 0 and do nothing if size or nitems is zero */
        if (size == 0 || nitems == 0) {
            return 0;
        }

        /* TODO: need to call fputc size times? */
        /* compute total number of bytes that we'll write */
        size_t count = size * nitems;

        /* write data to file */
        int write_rc = scrmfs_stream_write(stream, ptr, count);
        if (write_rc != SCRMFS_SUCCESS) {
            /* stream write sets error indicator, EOF indicator, and errno for us */
            return 0;
        }

        /* return number of items written */
        return nitems;
    } else {
        MAP_OR_FAIL(fwrite);
        size_t ret = __real_fwrite(ptr, size, nitems, stream);
        return ret;
    }
}

/* both fseek and fseeko delegate to this function */
int scrmfs_fseek(FILE *stream, off_t offset, int whence)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

    /* get pointer to file descriptor structure */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
    if (filedesc == NULL) {
        /* ERROR: invalid file descriptor */
        s->err = 1;
        errno = EBADF;
        return -1;
    }

    /* get the file id for this file descriptor */
    int fid = scrmfs_get_fid_from_fd(s->fd);
    if (fid < 0) {
        /* couldn't find file id, so assume we're at the end,
         * feof defines to errors */
        s->err = 1;
        errno = EBADF;
        return -1;
    }

    /* get current position */
    off_t current_pos = filedesc->pos;

    /* update current position based on whence and offset */
    off_t filesize;
    switch (whence)
    {
        case SEEK_SET:
            /* seek to offset */
            current_pos = (off_t) offset;
            break;
        case SEEK_CUR:
            /* seek to current position + offset */
            if (scrmfs_would_overflow_long((long)current_pos, (long)offset)) {
                s->err = 1;
                errno  = EOVERFLOW;
                return -1;
            }
            current_pos += (off_t) offset;
            break;
        case SEEK_END:
            /* seek to EOF + offset */
            filesize = scrmfs_fid_size(fid);
            if (scrmfs_would_overflow_long((long)filesize, (long)offset)) {
                s->err = 1;
                errno  = EOVERFLOW;
                return -1;
            }
            current_pos = filesize + (off_t) offset;
            break;
        default:
            s->err = 1;
            errno = EINVAL;
            return -1;
    }

    /* TODO: only update file descriptor if most recent call is fflush? */
    /* save new position */
    filedesc->pos = current_pos;

    /* clear end-of-file indicator */
    s->eof = 0;

    return 0;
}

int SCRMFS_DECL(fseek)(FILE *stream, long offset, int whence)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        off_t offset_offt = (off_t) offset;
        int rc = scrmfs_fseek(stream, offset_offt, whence);
        return rc;
    } else {
        MAP_OR_FAIL(fseek);
        int ret = __real_fseek(stream, offset, whence);
        return ret;
    }
}

int SCRMFS_DECL(fseeko)(FILE *stream, off_t offset, int whence)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        int rc = scrmfs_fseek(stream, offset, whence);
        return rc;
    } else {
        MAP_OR_FAIL(fseeko);
        int ret = __real_fseeko(stream, offset, whence);
        return ret;
    }
}

long SCRMFS_DECL(ftell)(FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            s->err = 1;
            errno = EBADF;
            return (long)-1;
        }

        /* get current position */
        off_t current_pos = filedesc->pos;
        return (long)current_pos;
    } else {
        MAP_OR_FAIL(ftell);
        long ret = __real_ftell(stream);
        return ret;
    }
}

off_t SCRMFS_DECL(ftello)(FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            s->err = 1;
            errno = EBADF;
            return (off_t)-1;
        }

        /* get current position */
        off_t current_pos = filedesc->pos;
        return current_pos;
    } else {
        MAP_OR_FAIL(ftello);
        off_t ret = __real_ftello(stream);
        return ret;
    }
}

/* equivalent to fseek(stream, 0L, SEEK_SET) except shall also clear error indicator */
void SCRMFS_DECL(rewind)(FILE* stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: check that stream is active */

        /* TODO: do we need to clear the error even if the seek fails? */
        /* clear error indicator */
        s->err = 0;

        /* now seek to front of file */
        int rc = scrmfs_fseek(stream, (off_t) 0L, SEEK_SET);
        errno = scrmfs_err_map_to_errno(rc);
        return;
    } else {
        MAP_OR_FAIL(rewind);
        __real_rewind(stream);
        return;
    }
}

int SCRMFS_DECL(fflush)(FILE* stream)
{
    /* if stream is NULL, flush output on all streams */
    if (stream == NULL) {
        /* TODO: for loop over all active scrmfs streams and flush each one */
        /* nothing to do right now */

        MAP_OR_FAIL(fflush);
        int ret = __real_fflush(stream);
        return ret;
    }

    /* otherwise, check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* TODO: flush output on stream */
        /* nothing to do right now */
        return 0;
    } else {
        MAP_OR_FAIL(fflush);
        int ret = __real_fflush(stream);
        return ret;
    }
}

/* return non-zero if and only if end-of-file indicator is set for stream */
int SCRMFS_DECL(feof)(FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: ensure stream is active */

        int ret = s->eof;
        return ret;
    } else {
        MAP_OR_FAIL(feof);
        int ret = __real_feof(stream);
        return ret;
    }
}

int SCRMFS_DECL(ferror)(FILE* stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream and file descriptor */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: ensure stream is active */

        int ret = s->err;
        return ret;
    } else {
        MAP_OR_FAIL(ferror);
        int ret = __real_ferror(stream);
        return ret;
    }
}

void SCRMFS_DECL(clearerr)(FILE* stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: ensure stream is active */

        /* clear error and end-of-file indicators for stream */
        s->err = 0;
        s->eof = 0;
        return;
    } else {
        MAP_OR_FAIL(clearerr);
        __real_clearerr(stream);
        return;
    }

}

int SCRMFS_DECL(fileno)(FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: check that stream is valid */

        /* return file descriptor associated with stream */
        return s->fd;
    } else {
        MAP_OR_FAIL(fileno);
        int ret = __real_fileno(stream);
        return ret;
    }
}

int SCRMFS_DECL(fclose)(FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(s->fd);
        if (fid < 0) {
            errno = EBADF;
            return EOF;
        }

        /* TODO: flush stream */

        /* close the file */
        int close_rc = scrmfs_fid_close(fid);
        if (close_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(close_rc);
            return EOF;
        }

        /* set file descriptor to -1 to indicate stream is invalid */
        s->fd = -1;

        /* currently a no-op */
        return 0;
    } else {
        MAP_OR_FAIL(fclose);
        int ret = __real_fclose(stream);
        return ret;
    }
}

/* ---------------------------------------
 * APIs exposed to external libraries
 * --------------------------------------- */

/* get information about the chunk data region
 * for external async libraries to register during their init */
size_t scrmfs_get_data_region(void **ptr)
{
    *ptr = scrmfs_chunks;
    return scrmfs_chunk_mem;
}

/* get a list of chunks for a given file (useful for RDMA, etc.) */
chunk_list_t* scrmfs_get_chunk_list(char* path)
{
    if (scrmfs_intercept_path(path)) {
        int i = 0;
        chunk_list_t *chunk_list = NULL;
        chunk_list_t *chunk_list_elem;
    
        /* get the file id for this file descriptor */
        /* Rag: We decided to use the path instead.. Can add flexibility to support both */
        //int fid = scrmfs_get_fid_from_fd(fd);
        int fid = scrmfs_get_fid_from_path(path);

        /* get meta data for this file */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        
        while ( i < meta->chunks ) {
            chunk_list_elem = (chunk_list_t*)malloc(sizeof(chunk_list_t));

            /* get the chunk id for the i-th chunk and
             * add it to the chunk_list */
            scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[i]);
            chunk_list_elem->chunk_id = chunk_meta->id;
            chunk_list_elem->location = chunk_meta->location;

            if ( chunk_meta->location == CHUNK_LOCATION_MEMFS ) {
                /* update the list_elem with the memory address of this chunk */
                chunk_list_elem->chunk_mr = scrmfs_compute_chunk_buf( meta, chunk_meta->id, 0);
                chunk_list_elem->spillover_offset = 0;
            } else if ( chunk_meta->location == CHUNK_LOCATION_SPILLOVER ) {
                /* update the list_elem with the offset of this chunk in the spillover file*/
                chunk_list_elem->spillover_offset = scrmfs_compute_spill_offset( meta, chunk_meta->id, 0);
                chunk_list_elem->chunk_mr = NULL;
            } else {
                /*TODO: Handle the container case.*/
            }

            /* currently using macros from utlist.h to
             * handle link-list operations */
            LL_APPEND( chunk_list, chunk_list_elem);
            i++;
        }

        return chunk_list;
    } else {
        /* file not managed by SCRMFS */
        errno = EACCES;
        return NULL;
    }
}

/* debug function to print list of chunks constituting a file
 * and to test above function*/
void scrmfs_print_chunk_list(char* path)
{
    chunk_list_t *chunk_list;
    chunk_list_t *chunk_element;

    chunk_list = scrmfs_get_chunk_list(path);

    fprintf(stdout,"-------------------------------------\n");
    LL_FOREACH(chunk_list,chunk_element) {
        printf("%d,%d,%p,%ld\n",chunk_element->chunk_id,
                                chunk_element->location,
                                chunk_element->chunk_mr,
                                chunk_element->spillover_offset);
    }

    LL_FOREACH(chunk_list,chunk_element) {
        free(chunk_element);
    }
    fprintf(stdout,"\n");
    fprintf(stdout,"-------------------------------------\n");
    
}

/*
 * http://pubs.opengroup.org/onlinepubs/009695399/
 * http://www.open-std.org/jtc1/sc22/wg14/www/docs/n1124.pdf
 * https://github.com/fakechroot/fakechroot/tree/master/src
 *
 * fopen
 * -- fdopen
 * -- freopen
 *
 * fgetc
 * fputc
 * getc
 * putc
 * fgets
 * fputs
 * -- ungetc
 * -- fgetwc
 * -- fputwc
 * -- getwc*
 * -- putwc*
 * -- fgetws
 * -- fputws
 * -- ungetwc
 * fread
 * fwrite
 *
 * -- fscanf
 * -- vfscanf
 * -- fprintf
 * -- vfprintf
 *
 * fseek
 * fseeko
 * ftell
 * ftello
 * rewind
 * -- fsetpos
 * -- fgetpos
 *
 * -- setbuf
 * -- setvbuf
 *
 * fflush
 * feof
 * ferror
 * clearerr
 * fileno
 * -- flockfile
 * -- ftrylockfile
 * -- funlockfile
 *
 * - fopen64
 * -- freopen64
 * -- fseeko64
 * -- fgetpos64
 * -- fsetpos64
 * -- ftello64
 */
