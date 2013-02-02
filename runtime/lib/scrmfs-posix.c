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
#include <limits.h>
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

static int scrmfs_fpos_enabled   = 1;  /* whether we can use fgetpos/fsetpos */

static int scrmfs_use_memfs      = 1;
static int scrmfs_use_spillover;
static int scrmfs_use_single_shm = 0;
static int scrmfs_use_containers;         /* set by env var SCRMFS_USE_CONTAINERS=1 */
static int scrmfs_page_size      = 0;

static off_t scrmfs_max_offt;
static off_t scrmfs_min_offt;
static off_t scrmfs_max_long;
static off_t scrmfs_min_long;

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
static int scrmfs_numa_bank = -1;
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
SCRMFS_FORWARD_DECL(remove, int, (const char *path));
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
SCRMFS_FORWARD_DECL(setvbuf, int, (FILE *stream, char* buf, int type, size_t size));
SCRMFS_FORWARD_DECL(setbuf, void*, (FILE *stream, char* buf));
SCRMFS_FORWARD_DECL(ungetc, int, (int c, FILE *stream));
SCRMFS_FORWARD_DECL(fgetc, int, (FILE *stream));
SCRMFS_FORWARD_DECL(fputc, int, (int c, FILE *stream));
SCRMFS_FORWARD_DECL(getc, int, (FILE *stream));
SCRMFS_FORWARD_DECL(putc, int, (int c, FILE *stream));
SCRMFS_FORWARD_DECL(fgets, char*, (char* s, int n, FILE* stream));
SCRMFS_FORWARD_DECL(fputs, int, (const char* s, FILE* stream));
SCRMFS_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nitems, FILE *stream));
SCRMFS_FORWARD_DECL(fwrite, size_t, (const void *ptr, size_t size, size_t nitems, FILE *stream));
SCRMFS_FORWARD_DECL(fprintf, int, (FILE* stream, const char* format, ...));
SCRMFS_FORWARD_DECL(vfprintf, int, (FILE* stream, const char* format, va_list ap));
SCRMFS_FORWARD_DECL(fscanf, int, (FILE* stream, const char* format, ...));
SCRMFS_FORWARD_DECL(vfscanf, int, (FILE* stream, const char* format, va_list ap));
SCRMFS_FORWARD_DECL(fseek,  int, (FILE *stream, long offset,  int whence));
SCRMFS_FORWARD_DECL(fseeko, int, (FILE *stream, off_t offset, int whence));
SCRMFS_FORWARD_DECL(ftell,  long,  (FILE *stream));
SCRMFS_FORWARD_DECL(ftello, off_t, (FILE *stream));
SCRMFS_FORWARD_DECL(rewind, void, (FILE *stream));
SCRMFS_FORWARD_DECL(fgetpos, int, (FILE *stream, fpos_t* pos));
SCRMFS_FORWARD_DECL(fsetpos, int, (FILE *stream, const fpos_t* pos));
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
    /* if both parameters are positive, they could overflow when
     * added together */
    if (a > 0 && b > 0) {
        /* if the distance between a and max is greater than or equal to
         * b, then we could add a and b and still not exceed max */
        if (scrmfs_max_offt - a >= b) {
            return 0;
        }
        return 1;
    }

    /* if both parameters are negative, they could underflow when
     * added together */
    if (a < 0 && b < 0) {
        /* if the distance between min and a is less than or equal to
         * b, then we could add a and b and still not exceed min */
        if (scrmfs_min_offt - a <= b) {
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
    /* if both parameters are positive, they could overflow when
     * added together */
    if (a > 0 && b > 0) {
        /* if the distance between a and max is greater than or equal to
         * b, then we could add a and b and still not exceed max */
        if (scrmfs_max_long - a >= b) {
            return 0;
        }
        return 1;
    }

    /* if both parameters are negative, they could underflow when
     * added together */
    if (a < 0 && b < 0) {
        /* if the distance between min and a is less than or equal to
         * b, then we could add a and b and still not exceed min */
        if (scrmfs_min_long - a <= b) {
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

/* given an input mode, mask it with umask and return, can specify
 * an input mode==0 to specify all read/write bits */
static mode_t scrmfs_getmode(mode_t perms)
{
    /* perms == 0 is shorthand for all read and write bits */
    if (perms == 0) {
        perms = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
    }

    /* get current user mask */
    mode_t mask = umask(0);
    umask(mask);

    /* mask off bits from desired permissions */
    mode_t ret = perms & ~mask & 0777;
    return ret;
}

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
static inline void* scrmfs_compute_chunk_buf(
  const scrmfs_filemeta_t* meta,
  int logical_id,
  off_t logical_offset)
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
static inline off_t scrmfs_compute_spill_offset(
  const scrmfs_filemeta_t* meta,
  int logical_id,
  off_t logical_offset)
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
static int scrmfs_chunk_read(
  scrmfs_filemeta_t* meta, /* pointer to file meta data */
  int chunk_id,            /* logical chunk id to read data from */
  off_t chunk_offset,      /* logical offset within chunk to read from */
  void* buf,               /* buffer to store data to */
  size_t count)            /* number of bytes to read */
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
static int scrmfs_chunk_write(
  scrmfs_filemeta_t* meta, /* pointer to file meta data */
  int chunk_id,            /* logical chunk id to write to */
  off_t chunk_offset,      /* logical offset within chunk to write to */
  const void* buf,         /* buffer holding data to be written */
  size_t count)            /* number of bytes to write */
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
        if (rc < 0)  {
            perror("pwrite failed");
        }
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

/* fill in limited amount of stat information */
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

   /* set the file size */
   buf->st_size = meta->size;

   /* specify whether item is a file or directory */
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
   /* set everything we do for a file... */
   int fid = scrmfs_fid_create_file(path);
   if (fid < 0) {
       /* was there an error? if so, return it */
       errno = ENOSPC;
       return fid;
   }

   /* ...and a little more */
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
        if(scrmfs_use_containers) {
           rc = scrmfs_chunk_read(meta, chunk_id, pos, buf, count);
           if(rc != SCRMFS_SUCCESS) {
              fprintf(stderr, "container read failed with code %d at position %d\n", rc, pos);
              return rc;
           }
        } else }
           rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, buf, count);
        }
  #else
        rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, buf, count);
  #endif
    } else {
        /* read what's left of current chunk */
        char* ptr = (char*) buf;
  #ifdef HAVE_CONTAINER_LIB
        off_t currpos = pos;
        if(scrmfs_use_containers) {
           rc = scrmfs_chunk_read(meta, chunk_id, currpos, (void*)ptr, remaining);
           if(rc != SCRMFS_SUCCESS) {
              fprintf(stderr, "container read failed with code %d at position %d\n", rc, currpos);
              return rc;
           }
           currpos += remaining;
        } else  {
           rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, (void*)ptr, remaining);
        }
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
            if(scrmfs_use_containers) {
              rc = scrmfs_chunk_read(meta, chunk_id, currpos, (void*)ptr, num);
              if(rc != SCRMFS_SUCCESS) {
                 fprintf(stderr, "container read failed with code %d at position %d\n", rc, currpos);
                 return rc;
              }
              currpos += num;
            } else {
               rc = scrmfs_chunk_read(meta, chunk_id, 0, (void*)ptr, num);
            }
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
        if(scrmfs_use_containers) {
           rc = scrmfs_chunk_write(meta, chunk_id, pos, buf, count);
           if(rc != SCRMFS_SUCCESS) {
              fprintf(stderr, "container write failed with code %d to position %d\n", rc, pos);
              return rc;
           }
        } else {
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
        if(scrmfs_use_containers) {
           rc = scrmfs_chunk_write(meta, chunk_id, currpos, (void*)ptr, remaining);
           if(rc != SCRMFS_SUCCESS) {
              fprintf(stderr, "container write failed with code %d to position %d\n", rc, currpos);
              return rc;
           }
           currpos += remaining;
        } else {
           rc = scrmfs_chunk_write(meta, chunk_id, chunk_offset, (void*)ptr, remaining);
        }
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
            if(scrmfs_use_containers) {
              rc = scrmfs_chunk_write(meta, chunk_id, currpos, (void*)ptr, num);
              if(rc != SCRMFS_SUCCESS) {
                 fprintf(stderr, "container write failed with code %d to position %d\n", rc, currpos);
                 return rc;
              }
              currpos += num;
            } else {
               rc = scrmfs_chunk_write(meta, chunk_id, 0, (void*)ptr, num);
            }
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
 * and length, assumes space is already reserved */
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
            if(scrmfs_use_containers) {
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
    if(scrmfs_use_containers) {
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

    mode_t perms = scrmfs_getmode(0);

    MAP_OR_FAIL(open);
    spillblock_fd = __real_open(path, O_RDWR | O_CREAT | O_EXCL, perms);
    if (spillblock_fd < 0) {
        if (errno == EEXIST) {
            /* spillover block exists; attach and return */
            spillblock_fd = __real_open(path, O_RDWR);
        } else {
            perror("open() in scrmfs_get_spillblock() failed");
            return -1;
        }
    } else {
        /* new spillover block created */
        /* TODO: align to SSD block size*/
    
        /*temp*/
        off_t rc = lseek(spillblock_fd, size, SEEK_SET);
        if (rc < 0) {
            perror("lseek failed");
        }
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
            if ( scrmfs_numa_bank >= 0 ) {
                /* specifically allocate pages from user-set bank */
                numa_tonode_memory(scr_shmblock, size, scrmfs_numa_bank);
            } else if ( strcmp(scrmfs_numa_policy,"interleaved") == 0) {
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

        /* as a hack to support fgetpos/fsetpos, we store the value of
         * a void* in an fpos_t so check that there's room and at least
         * print a message if this won't work */
        if (sizeof(fpos_t) < sizeof(void*)) {
            fprintf(stderr, "ERROR: fgetpos/fsetpos will not work correctly.\n");
            scrmfs_fpos_enabled = 0;
        }

        /* look up page size for buffer alignment */
        scrmfs_page_size = getpagesize();

        /* compute min and max off_t values */
        unsigned long long bits;
        bits = sizeof(off_t) * 8;
        scrmfs_max_offt = (off_t) ( (1ULL << (bits-1ULL)) - 1ULL);
        scrmfs_min_offt = (off_t) (-(1ULL << (bits-1ULL))       );

        /* compute min and max long values */
        scrmfs_max_long = LONG_MAX;
        scrmfs_min_long = LONG_MIN;

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

        /* determine maximum number of bytes of memory for chunk storage */
        scrmfs_chunk_mem = SCRMFS_CHUNK_MEM;
        env = getenv("SCRMFS_CHUNK_MEM");
        if (env) {
            scrmfs_abtoull(env, &bytes);
            scrmfs_chunk_mem = (size_t) bytes;
        }

        /* set chunk size, set chunk offset mask, and set total number
         * of chunks */
        scrmfs_chunk_size = 1 << scrmfs_chunk_bits;
        scrmfs_chunk_mask = scrmfs_chunk_size - 1;
        scrmfs_max_chunks = scrmfs_chunk_mem >> scrmfs_chunk_bits;

        /* determine maximum number of bytes of spillover for chunk storage */
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

        env = getenv("SCRMFS_USE_NUMA_BANK");
        if (env) {
            int val = atoi(env);
            if (val >= 0) {
                scrmfs_numa_bank = val;
            } else {
                fprintf(stderr,"Incorrect NUMA bank specified in SCRMFS_USE_NUMA_BANK."
                                "Proceeding with default allocation policy!\n");
            }
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
        if (! scrmfs_fid_is_dir(fid)) {
            errno = ENOTDIR;
            return -1;
        }

        /* is it empty? */
        if (! scrmfs_fid_is_dir_empty(path)) {
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

int SCRMFS_DECL(remove)(const char *path)
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
            /* TODO: shall be equivalent to rmdir(path) */
            /* ERROR: is a directory */
            debug("Attempting to remove a directory %s in SCRMFS\n",path);
            errno = EISDIR;
            return -1;
        }

        /* shall be equivalent to unlink(path) */
        /* delete the file */
        scrmfs_fid_unlink(fid);

        return 0;
    } else {
        MAP_OR_FAIL(remove);
        int ret = __real_remove(path);
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

    /* check that file descriptor is open for read */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
    if (! filedesc->read) {
        return SCRMFS_ERR_BADF;
    }

    /* TODO: is it safe to assume that off_t is bigger than size_t? */
    /* check that we don't overflow the file length */
    if (scrmfs_would_overflow_offt(pos, (off_t) count)) {
        return SCRMFS_ERR_OVERFLOW;
    }

    /* TODO: check that file is open for reading */

    /* check that we don't try to read past the end of the file */
    off_t lastread = pos + (off_t) count;
    off_t filesize = scrmfs_fid_size(fid);
    if (filesize < lastread) {
        /* adjust count so we don't read past end of file */
        if (filesize > pos) {
            /* read all bytes until end of file */
            count = (size_t) (filesize - pos);
        } else {
            /* pos is already at or past the end of the file */
            count = 0;
        }
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

    /* check that file descriptor is open for write */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
    if (! filedesc->write) {
        return SCRMFS_ERR_BADF;
    }

    /* TODO: is it safe to assume that off_t is bigger than size_t? */
    /* check that our write won't overflow the length */
    if (scrmfs_would_overflow_offt(pos, (off_t) count)) {
        /* TODO: want to return EFBIG here for streams */
        return SCRMFS_ERR_OVERFLOW;
    }

    /* TODO: check that file is open for writing */

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

    /* finally write specified data to file */
    debug("request to write %d bytes to position %d\n", count, pos);
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
        scrmfs_fd_t* filedesc = &(scrmfs_fds[fid]);
        filedesc->pos   = pos;
        filedesc->read  = mode & (O_RDONLY | O_RDWR);
        filedesc->write = mode & (O_WRONLY | O_RDWR);
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
        scrmfs_fd_t* filedesc = &(scrmfs_fds[fid]);
        filedesc->pos   = pos;
        filedesc->read  = mode & (O_RDONLY | O_RDWR);
        filedesc->write = mode & (O_WRONLY | O_RDWR);
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

        /* get file descriptor for fd */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);

        /* get current file position */
        off_t current_pos = filedesc->pos;

        /* compute final file position */
        debug("seeking from %ld\n", current_pos);        
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
        debug("seeking to %ld\n", current_pos);        

        /* set and return final file position */
        filedesc->pos = current_pos;
        return current_pos;
    } else {
        MAP_OR_FAIL(lseek);
        off_t ret = __real_lseek(fd, offset, whence);
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

        /* check that file descriptor is open for write */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (! filedesc->write) {
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

static int scrmfs_stream_set_pointers(scrmfs_stream_t* s)
{
    /* get pointer to file descriptor structure */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
    if (filedesc == NULL) {
        /* ERROR: invalid file descriptor */
        s->err = 1;
        errno = EBADF;
        return SCRMFS_ERR_BADF;
    }

    /* if we have anything on the push back buffer, that must be
     * our current file pointer, since any seek would have cleared
     * the buffer and each read/write/unget keeps it up-to-date */
    if (s->ubuflen > 0) {
        s->_p = s->ubuf + s->ubufsize - s->ubuflen;
        s->_r = s->ubuflen;
        return SCRMFS_SUCCESS;
    }

    /* check that current falls within buffer */
    off_t current = filedesc->pos;
    off_t start  = s->bufpos;
    off_t length = s->buflen;
    if (current < start || current >= start + length) {
        /* file position is out of range of current buffer */
        s->_p = NULL;
        s->_r = 0;
    } else {
        /* determine number of bytes to copy from stream buffer */
        size_t stream_offset    = (size_t) (current - s->bufpos);
        char*  stream_start     = (char*)s->buf + stream_offset;
        size_t stream_remaining = s->buflen - stream_offset;
        s->_p = (unsigned char*) stream_start;
        s->_r = stream_remaining;
    }

    return SCRMFS_SUCCESS;
}

/* given a mode like "r", "wb+", or "a+" return flags read, write,
 * append, and plus to indicate which were set,
 * returns SCRMFS_ERR_INVAL if invalid character is found */
static int scrmfs_fopen_parse_mode(
  const char* mode,
  int* read,
  int* write,
  int* append,
  int* plus)
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

    /* optional second character may either be + or b */
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

/* calls scrmfs_fid_open to open specified file in mode according to
 * fopen mode semantics, initializes outstream and returns
 * SCRMFS_SUCCESS if successful, returns some other SCRMFS error
 * otherwise */
static int scrmfs_fopen(
  const char* path,
  const char* mode,
  FILE** outstream)
{
    /* assume that we'll fail */
    *outstream = NULL;

    /* parse the fopen mode string */
    int read, write, append, plus;
    int parse_rc = scrmfs_fopen_parse_mode(mode, &read, &write, &append, &plus);
    if (parse_rc != SCRMFS_SUCCESS) {
        return parse_rc;
    }

    /* TODO: get real permissions via umask */
    /* assume default permissions */
    mode_t perms = scrmfs_getmode(0);

    int open_rc;
    int fid;
    off_t pos;
    if (read) {
      /* read shall fail if file does not already exist, scrmfs_fid_open
       * returns SCRMFS_ERR_NOENT if file does not exist w/o O_CREAT */
      if (plus) {
          /* r+ ==> open file for update (reading and writing) */
          open_rc = scrmfs_fid_open(path, O_RDWR, perms, &fid, &pos);
      } else {
          /* r  ==> open file for reading */
          open_rc = scrmfs_fid_open(path, O_RDONLY, perms, &fid, &pos);
      }
    } else if (write) {
      if (plus) {
          /* w+ ==> truncate to zero length or create file for update
           * (read/write) */
          open_rc = scrmfs_fid_open(path, O_RDWR | O_CREAT | O_TRUNC, perms, &fid, &pos);
      } else {
          /* w  ==> truncate to zero length or create file for
           * writing */
          open_rc = scrmfs_fid_open(path, O_WRONLY | O_CREAT | O_TRUNC, perms, &fid, &pos);
      }
    } else if (append) {
      /* force all writes to end of file when append is set */
      if (plus) {
          /* a+ ==> append, open or create file for update, at end
           * of file */
          open_rc = scrmfs_fid_open(path, O_RDWR | O_CREAT | O_APPEND, perms, &fid, &pos);
      } else {
          /* a  ==> append, open or create file for writing, at end
           * of file */
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

    /* clear error and eof indicators and record file descriptor */
    s->err = 0;
    s->eof = 0;
    s->fd  = fd;

    /* record the access mode for the stream */
    s->append = append;

    /* set orientation to NULL */
    s->orient = SCRMFS_STREAM_ORIENTATION_NULL;

    /* default to fully buffered, set buffer to NULL to indicate
     * setvbuf has not been called */
    s->buf      = NULL;
    s->buffree  = 0;
    s->buftype  = _IOFBF;
    s->bufsize  = 0;
    s->bufpos   = 0;
    s->buflen   = 0;
    s->bufdirty = 0;

    /* initialize the ungetc buffer */
    s->ubuf     = NULL;
    s->ubufsize = 0;
    s->ubuflen  = 0;

//ATM
    s->_p = NULL;
    s->_r = 0;

    /* set file pointer and read/write mode in file descriptor */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
    filedesc->pos   = pos;
    filedesc->read  = read  || plus;
    filedesc->write = write || plus;

    /* set return parameter and return */
    *outstream = (FILE*)s;
    return SCRMFS_SUCCESS;
}

/* associate buffer with stream, allocates a buffer of specified size
 * if buf is NULL, otherwise uses buffer passed by caller, also sets
 * stream to fully/line/unbuffered, returns SCRMFS error codes */
static int scrmfs_setvbuf(
  FILE* stream,
  char* buf,
  int type,
  size_t size)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

    /* TODO: check that stream is valid */

    /* check whether we've already associated a buffer */
    if (s->buf != NULL) {
        /* ERROR: stream already has buffer */
        return SCRMFS_ERR_BADF;
    }

    /* check that the type argument is valid */
    if (type != _IOFBF && type != _IOLBF && type != _IONBF) {
        /* ERROR: invalid type argument */
        return SCRMFS_ERR_INVAL;
    }

    /* check that size is valid */
    if (size <= 0) {
        /* ERROR: invalid size argument */
        return SCRMFS_ERR_INVAL;
    }

    /* associate buffer with stream */
    if (buf == NULL) {
        /* allocate buffer */
        s->buf = malloc(size);
        if (s->buf == NULL) {
            /* ERROR: no memory */
            return SCRMFS_ERR_NOMEM;
        }
        /* remember that we need to free the buffer at the end */
        s->buffree = 1;
    } else {
        /* caller provided buffer, remember that we don't need to
         * free it when closing the stream */
        s->buf = buf;
        s->buffree = 0;
    }

    /* set properties of buffer */
    s->buftype  = type;
    s->bufsize  = size;
    s->bufpos   = 0;
    s->buflen   = 0;
    s->bufdirty = 0;

    return SCRMFS_SUCCESS;
}

/* calls scrmfs_fd_write to flush stream if it is dirty,
 * returns SCRMFS error codes, sets stream error indicator and errno
 * upon error */
static int scrmfs_stream_flush(FILE* stream)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

    /* TODO: check that stream is valid */

    /* if buffer is dirty, write data to file */
    if (s->buf != NULL && s->bufdirty) {
        int write_rc = scrmfs_fd_write(s->fd, s->bufpos, s->buf, s->buflen);
        if (write_rc != SCRMFS_SUCCESS) {
            s->err = 1;
            errno = scrmfs_err_map_to_errno(write_rc);
            return write_rc;
        }

        /* indicate that buffer is now flushed */
        s->bufdirty = 0;
    }

    return SCRMFS_SUCCESS;
}

/* reads count bytes from stream into buf, sets stream EOF and error
 * indicators as appropriate, sets errno if error, updates file
 * position, returns number of bytes read in retcount, returns SCRMFS
 * error codes*/
static int scrmfs_stream_read(
  FILE* stream,
  void* buf,
  size_t count,
  size_t* retcount)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

//ATM
    /* clear pointers, will force a reset when refill is called */
    s->_p = NULL;
    s->_r = 0;

    /* get pointer to file descriptor structure */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
    if (filedesc == NULL) {
        /* ERROR: invalid file descriptor */
        s->err = 1;
        errno = EBADF;
        return SCRMFS_ERR_BADF;
    }

    /* bail with error if stream not open for reading */
    if (! filedesc->read) {
        s->err = 1;
        errno = EBADF;
        return SCRMFS_ERR_BADF;
    }

    /* associate buffer with stream if we need to */
    if (s->buf == NULL) {
        int setvbuf_rc = scrmfs_setvbuf(stream, NULL, s->buftype, SCRMFS_STREAM_BUFSIZE);
        if (setvbuf_rc != SCRMFS_SUCCESS) {
            /* ERROR: failed to associate buffer */
            s->err = 1;
            errno = scrmfs_err_map_to_errno(setvbuf_rc);
            return setvbuf_rc;
        }
    }

    /* don't attempt read if end-of-file indicator is set */
    if (s->eof) {
        return SCRMFS_FAILURE;
    }

    /* track our current position in the file and number of bytes
     * left to read */
    off_t current = filedesc->pos;
    size_t remaining = count;

    /* check that current + count doesn't overflow */
    if (scrmfs_would_overflow_offt(current, (off_t) count)) {
        s->err = 1;
        errno = EOVERFLOW;
        return SCRMFS_ERR_OVERFLOW;
    }

    /* take bytes from push back buffer if they exist */
    size_t ubuflen = s->ubuflen;
    if (ubuflen > 0) {
        /* determine number of bytes to take from push-back buffer */
        size_t ubuf_chars = ubuflen;
        if (remaining < ubuflen) {
            ubuf_chars = remaining;
        }

        /* copy bytes from push back buffer to user buffer */
        unsigned char* ubuf_start = s->ubuf + s->ubufsize - ubuflen;
        memcpy(buf, ubuf_start, ubuf_chars);

        /* drop bytes from push back buffer */
        s->ubuflen -= ubuf_chars;

        /* update our current file position and remaining count */
        current   += ubuf_chars;
        remaining -= ubuf_chars;
    }

    /* TODO: if count is large enough, read directly to user's
     * buffer and be done with it */

    /* read data from file into buffer */
    int eof = 0;
    while (remaining > 0 && !eof) {
        /* check that current falls within buffer */
        off_t start  = s->bufpos;
        off_t length = s->buflen;
        if (current < start || current >= start + length) {
            /* current is outside the range of our buffer */

            /* flush buffer if needed before read */
            int flush_rc = scrmfs_stream_flush(stream);
            if (flush_rc != SCRMFS_SUCCESS) {
                /* ERROR: flush sets error indicator and errno */
                return flush_rc;
            }

            /* read data from file into buffer */
            size_t bufcount;
            int read_rc = scrmfs_fd_read(s->fd, current, s->buf, s->bufsize, &bufcount);
            if (read_rc != SCRMFS_SUCCESS) {
                /* ERROR: read error, set error indicator and errno */
                s->err = 1;
                errno = scrmfs_err_map_to_errno(read_rc);
                return read_rc;
            }

            /* record new buffer range within file */
            s->bufpos  = current;
            s->buflen  = bufcount;

            /* set end-of-file flag if our read was short */
            if (bufcount < s->bufsize) {
              eof = 1;
            }
        }

        /* determine number of bytes to copy from stream buffer */
        size_t stream_offset    = (size_t) (current - s->bufpos);
        size_t stream_remaining = s->buflen - stream_offset;
        size_t bytes = stream_remaining;
        if (bytes > remaining) {
            bytes = remaining;
        }

        /* copy data from stream buffer to user buffer */
        if (bytes > 0) {
            char* buf_start    = (char*)buf + (count - remaining);
            char* stream_start = (char*)s->buf + stream_offset;
            memcpy(buf_start, stream_start, bytes);
        }

        /* update our current position and the number of bytes
         * left to read */
        current   += bytes;
        remaining -= bytes;
    }

    /* set number of bytes read */
    *retcount = (count - remaining);

    /* update file position */
    filedesc->pos += (off_t) *retcount;

//ATM
//    scrmfs_stream_set_pointers(s);

    /* set end of file indicator if we hit the end */
    if (*retcount < count) {
        s->eof = 1;
    }

    /* return success */
    return SCRMFS_SUCCESS;
}

/* writes count bytes from buf to stream, sets stream EOF and error
 * indicators as appropriate, sets errno if error, updates file
 * position, return SCRMFS error codes */
static int scrmfs_stream_write(
  FILE* stream,
  const void* buf,
  size_t count)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

//ATM
    /* clear pointers, will force a reset when refill is called */
    s->_p = NULL;
    s->_r = 0;

    /* TODO: check that stream is valid */

    /* get pointer to file descriptor structure */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
    if (filedesc == NULL) {
        /* ERROR: invalid file descriptor */
        s->err = 1;
        errno = EBADF;
        return SCRMFS_ERR_BADF;
    }

    /* bail with error if stream not open for writing */
    if (! filedesc->write) {
        s->err = 1;
        errno = EBADF;
        return SCRMFS_ERR_BADF;
    }

    /* TODO: Don't know what to do with push back bytes if write
     * overlaps.  Can't find defined behavoir in C and POSIX standards. */

    /* set the position to write */
    off_t current;
    if (s->append) {
        /* if in append mode, always write to end of file */
        int fid = scrmfs_get_fid_from_fd(s->fd);
        if (fid < 0) {
            s->err = 1;
            errno = EBADF;
            return SCRMFS_ERR_BADF;
        }
        current = scrmfs_fid_size(fid);

        /* like a seek, we discard push back bytes */
        s->ubuflen;
    } else {
        /* otherwise, write at current file pointer */
        current = filedesc->pos;

        /* drop bytes from push back if write overlaps */
        if (s->ubuflen > 0) {
            if (count >= s->ubuflen) {
                s->ubuflen = 0;
            } else {
                s->ubuflen -= count;
            }
        }
    }

    /* check that current + count doesn't overflow */
    if (scrmfs_would_overflow_offt(current, (off_t) count)) {
        s->err = 1;
        errno = EFBIG;
        return SCRMFS_ERR_FBIG;
    }

    /* associate buffer with stream if we need to */
    if (s->buf == NULL) {
        int setvbuf_rc = scrmfs_setvbuf(stream, NULL, s->buftype, SCRMFS_STREAM_BUFSIZE);
        if (setvbuf_rc != SCRMFS_SUCCESS) {
            /* ERROR: failed to associate buffer */
            s->err = 1;
            errno = scrmfs_err_map_to_errno(setvbuf_rc);
            return setvbuf_rc;
        }
    }

    /* if unbuffered, write data directly to file */
    if (s->buftype == _IONBF) {
        /* write data directly to file */
        int write_rc = scrmfs_fd_write(s->fd, current, buf, count);
        if (write_rc != SCRMFS_SUCCESS) {
            /* ERROR: write error, set error indicator and errno */
            s->err = 1;
            errno = scrmfs_err_map_to_errno(write_rc);
            return write_rc;
        }

        /* update file position */
        filedesc->pos = current + (off_t) count;

//ATM
//        scrmfs_stream_set_pointers(s);

        return SCRMFS_SUCCESS;
    }

    /* TODO: if count is large enough, write directly to file
     * and be done with it */

    /* write data from buffer to file */
    size_t remaining = count;
    while (remaining > 0) {
        /* if buffer is clean, set start of buffer to current */
        if (! s->bufdirty) {
            s->bufpos = current;
            s->buflen = 0;
        }

        /* determine number of bytes to copy to buffer and whether
         * we need to flush the stream after the write */
        size_t bytes;
        int need_flush = 0;
        size_t stream_offset    = s->buflen;
        size_t stream_remaining = s->bufsize - stream_offset;
        if (s->buftype == _IOLBF) {
            /* line buffered, scan to first newline or end of
             * user buffer and counts bytes as we go */
            bytes = 0;
            const char* ptr = (const char*)buf + (count - remaining);
            while (bytes < remaining) {
                bytes++;
                if (*ptr == '\n') {
                  /* found a newline, write up to and including newline
                   * then flush stream */
                  need_flush = 1;
                  break;
                }
                ptr++;
            }

            /* error if we exhaust buffer before finding a newline */
            if (bytes > stream_remaining ||
                (bytes == stream_remaining && !need_flush))
            {
                /* ERROR: write error, set error indicator and errno */
                s->err = 1;
                errno = ENOMEM;
                return SCRMFS_ERR_NOMEM;
            }
        } else {
            /* fully buffered, write until we hit the buffer limit */
            bytes = remaining;
            if (bytes >= stream_remaining) {
                bytes = stream_remaining;
                need_flush = 1;
            }
        }

        /* copy data from user buffer to stream buffer */
        if (bytes > 0) {
            char* buf_start    = (char*)buf + (count - remaining);
            char* stream_start = (char*)s->buf + stream_offset;
            memcpy(stream_start, buf_start, bytes);

            /* mark buffer as dirty and increase number of bytes */
            s->bufdirty = 1;
            s->buflen += bytes;
        }

        /* if we've filled the buffer, flush it */
        if (need_flush) {
            /* flush stream */
            int flush_rc = scrmfs_stream_flush(stream);
            if (flush_rc != SCRMFS_SUCCESS) {
                /* ERROR: flush sets error indicator and errno */
                return flush_rc;
            }
        }

        /* update our current position and the number of bytes
         * left to write */
        current   += bytes;
        remaining -= bytes;
    }

    /* TODO: Don't know whether to update file position for append
     * write or leave position where it is.  glibc seems to update
     * so let's do the same here. */

    /* update file position */
    filedesc->pos = current;

//ATM
//    scrmfs_stream_set_pointers(s);

    return SCRMFS_SUCCESS;
}

/* fseek, fseeko, rewind, and fsetpos all call this function, sets error
 * indicator and errno if necessary, returns -1 on error, returns
 * 0 for success */
static int scrmfs_fseek(FILE *stream, off_t offset, int whence)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

//ATM
    /* clear pointers, will force a reset when refill is called */
    s->_p = NULL;
    s->_r = 0;

    /* get pointer to file descriptor structure */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
    if (filedesc == NULL) {
        /* ERROR: invalid file descriptor */
        s->err = 1;
        errno = EBADF;
        return -1;
    }

    /* flush stream if we need to */
    int flush_rc = scrmfs_stream_flush(stream);
    if (flush_rc != SCRMFS_SUCCESS) {
        /* ERROR: flush sets error indicator and errno */
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
            current_pos = offset;
            break;
        case SEEK_CUR:
            /* seek to current position + offset */
            if (scrmfs_would_overflow_offt(current_pos, offset)) {
                s->err = 1;
                errno  = EOVERFLOW;
                return -1;
            }
            current_pos += offset;
            break;
        case SEEK_END:
            /* seek to EOF + offset */
            filesize = scrmfs_fid_size(fid);
            if (scrmfs_would_overflow_offt(filesize, offset)) {
                s->err = 1;
                errno  = EOVERFLOW;
                return -1;
            }
            current_pos = filesize + offset;
            break;
        default:
            s->err = 1;
            errno = EINVAL;
            return -1;
    }

    /* discard contents of push back buffer */
    if (s->ubuf != NULL) {
        s->ubuflen = 0;
    }

    /* TODO: only update file descriptor if most recent call is
     * fflush? */
    /* save new position */
    filedesc->pos = current_pos;

//ATM
//    scrmfs_stream_set_pointers(s);

    /* clear end-of-file indicator */
    s->eof = 0;

    return 0;
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

int SCRMFS_DECL(setvbuf)(FILE* stream, char* buf, int type, size_t size)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_stream(stream)) {
        int rc = scrmfs_setvbuf(stream, buf, type, size);
        if (rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(rc);
            return 1;
        }
        return 0;
    } else {
        MAP_OR_FAIL(setvbuf);
        int ret = __real_setvbuf(stream, buf, type, size);
        return ret;
    }
}

void SCRMFS_DECL(setbuf)(FILE* stream, char* buf)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_stream(stream)) {
        if (buf != NULL) {
            scrmfs_setvbuf(stream, buf, _IOFBF, BUFSIZ);
        } else {
            scrmfs_setvbuf(stream, buf, _IONBF, BUFSIZ);
        }
        return;
    } else {
        MAP_OR_FAIL(setbuf);
        __real_setbuf(stream, buf);
        return;
    }
}

int SCRMFS_DECL(ungetc)(int c, FILE *stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* operation shall fail and input stream left unchanged */
        if (c == EOF) {
            return EOF;
        }

        /* convert int to unsigned char */
        unsigned char uc = (unsigned char) c;

        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* get filedescriptor and check that stream is valid */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
        if (filedesc == NULL) {
            return EOF;
        }

        /* check that pos > 0 */
        if (filedesc->pos <= 0) {
            return EOF;
        }

        /* allocate bigger push-back buffer if needed */
        size_t oldsize = s->ubufsize;
        size_t len     = s->ubuflen;
        size_t remaining = oldsize - len;
        if (remaining == 0) {
            /* start with a 32-byte push-back buffer,
             * but double current size if we already have one */
            size_t newsize = 1;
            if (oldsize > 0) {
                newsize = oldsize * 2;
            }

            /* make sure we don't get to big */
            if (newsize > 1024) {
                return EOF;
            }

            /* allocate new buffer */
            unsigned char* newbuf = (unsigned char*) malloc(newsize);
            if (newbuf == NULL) {
                return EOF;
            }

            /* copy old bytes to new buffer and free old buffer */
            if (len > 0) {
                unsigned char* oldbuf = s->ubuf;
                unsigned char* oldstart = oldbuf + oldsize - len;
                unsigned char* newstart = newbuf + newsize - len;
                memcpy(newstart, oldstart, len);
                free(s->ubuf);
            }

            /* record details of new buffer */
            s->ubuf     = newbuf;
            s->ubufsize = newsize;
            s->ubuflen  = len;
        }

        /* push char onto buffer */
        s->ubuflen++;
        unsigned char* pos = s->ubuf + s->ubufsize - s->ubuflen;
        *pos = uc;

        /* decrement file position */
        filedesc->pos--;

// ATM
        /* update buffer pointer and remaining count */
        s->_p = pos;
        s->_r = s->ubuflen;

        /* clear end-of-file flag */
        s->eof = 0;

        return (int) uc;
    } else {
        MAP_OR_FAIL(ungetc);
        int ret = __real_ungetc(c, stream);
        return ret;
    }
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
            /* stream read sets error indicator, EOF indicator,
             * and errno for us */
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
            /* stream write sets error indicator, EOF indicator,
             * and errno for us */
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
            /* stream read sets error indicator, EOF indicator,
             * and errno for us */
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
            /* stream write sets error indicator, EOF indicator,
             * and errno for us */
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

        /* lookup stream */
        scrmfs_stream_t* stm = (scrmfs_stream_t*) stream;

        /* TODO: check that stream is valid */

        /* check that we got a positive buffer size */
        if (n <= 0) {
            /* caller specified a buffer length <= 0 */
            stm->err = 1;
            errno = ENOMEM;
            return NULL;
        }

        /* read one character at a time until we hit a newline, read
         * n-1 characters or hit end of the file (or a read error) */
        int limit = 0;
        while (limit < n-1) {
            /* read the next character from the file */
            char charbuf;
            size_t retcount;
            int read_rc = scrmfs_stream_read(stream, &charbuf, 1, &retcount);
            if (read_rc != SCRMFS_SUCCESS) {
                /* if error flag is not set, we must have hit EOF,
                 * terminate string before returning */
                if (! stm->err) {
                    s[limit] = '\0';
                }

                /* stream read sets error indicator, EOF indicator,
                 * and errno for us */
                return NULL;
            }

            /* copy character to buffer */
            s[limit] = charbuf;
            limit++;

            /* if we hit a newline, break after copying it */
            if (charbuf == '\n') {
                break;
            }
        }

        /* terminate string with a NUL */
        s[limit] = '\0';

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
            /* stream write sets error indicator, EOF indicator,
             * and errno for us */
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
            /* stream read sets error indicator, EOF indicator,
             * and errno for us */
            return 0;
        }

        /* return number of items read */
        if (retcount < count) {
            /* adjust return value if read less data than requested */
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
            /* stream write sets error indicator, EOF indicator,
             * and errno for us */
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

int SCRMFS_DECL(fprintf)(FILE *stream, const char* format, ...)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* delegate work to vfprintf */
        va_list args;
        va_start(args, format);
        int ret = SCRMFS_DECL(vfprintf)(stream, format, args);
        va_end(args);
        return ret;
    } else {
        va_list args;
        va_start(args, format);
        MAP_OR_FAIL(vfprintf);
        int ret = __real_vfprintf(stream, format, args);
        va_end(args);
        return ret;
    }
}

int SCRMFS_DECL(vfprintf)(FILE *stream, const char* format, va_list ap)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: check that stream is active */

        
        /* get length of component string */
        va_list ap2;
        va_copy(ap2, ap);
        size_t chars = (size_t) vsnprintf(NULL, 0, format, ap2);
        va_end(ap2);

        /* allocate space to hold string, add one for the terminating NUL */
        size_t strlen = chars + 1;
        char* str = (char*) malloc(strlen);
        if (str == NULL) {
            s->err = 1;
            errno = ENOMEM;
            return -1;
        }

        /* copy formatted string into new memory */
        va_list ap3;
        va_copy(ap3, ap);
        int printf_rc = vsnprintf(str, strlen, format, ap3);
        va_end(ap3);
        if (printf_rc != chars) {
            s->err = 1;
            /* assuming that vsnprintf sets errno for us */
            return printf_rc;
        }

        /* write data to file */
        int write_rc = scrmfs_stream_write(stream, str, chars);
        if (write_rc != SCRMFS_SUCCESS) {
            /* stream write sets error indicator, EOF indicator,
             * and errno for us */
            return -1;
        }

        /* free the string */
        free(str);

        /* return number of bytes written */
        return chars;
    } else {
        va_list ap2;
        va_copy(ap2, ap);
        MAP_OR_FAIL(vfprintf);
        int ret = __real_vfprintf(stream, format, ap2);
        va_end(ap2);
        return ret;
    }
}

int SCRMFS_DECL(fscanf)(FILE *stream, const char* format, ...)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* delegate work to vfscanf */
        va_list args;
        va_start(args, format);
        int ret = SCRMFS_DECL(vfscanf)(stream, format, args);
        va_end(args);
        return ret;
    } else {
        va_list args;
        va_start(args, format);
        MAP_OR_FAIL(vfscanf);
        int ret = __real_vfscanf(stream, format, args);
        va_end(args);
        return ret;
    }
}

int SCRMFS_DECL(vfscanf)(FILE *stream, const char* format, va_list ap)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        va_list args;
        va_copy(args, ap);
        int ret = __svfscanf(stream, format, args);
        va_end(args);
        return ret;
    } else {
        va_list args;
        va_copy(args, ap);
        MAP_OR_FAIL(vfscanf);
        int ret = __real_vfscanf(stream, format, args);
        va_end(args);
        return ret;
    }
}

/* TODO: return error if new position overflows long */
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

/* TODO: set EOVERFLOW if position overflows long */
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

/* equivalent to fseek(stream, 0L, SEEK_SET) except shall also clear
 * error indicator */
void SCRMFS_DECL(rewind)(FILE* stream)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: check that stream is active */

        /* seek to front of file */
        int rc = scrmfs_fseek(stream, (off_t) 0L, SEEK_SET);

        /* set errno */
        errno = scrmfs_err_map_to_errno(rc);

        /* clear error indicator if seek successful */
        if (rc == 0) {
            s->err = 0;
        }

        return;
    } else {
        MAP_OR_FAIL(rewind);
        __real_rewind(stream);
        return;
    }
}

struct scrmfs_fpos_t {
    off_t pos;
};

int SCRMFS_DECL(fgetpos)(FILE* stream, fpos_t* pos)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* check that we can store a pointer in fpos_t */
        if (! scrmfs_fpos_enabled) {
            errno = EOVERFLOW;
            return 1;
        }

        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: check that stream is active */

        /* get file descriptor for stream */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return 1;
        }

        /* allocate memory to hold state of stream */
        struct scrmfs_fpos_t* state = malloc(sizeof(struct scrmfs_fpos_t));
        if (state == NULL) {
            errno = ENOMEM;
            return 1;
        }

        /* record state */
        state->pos = filedesc->pos;

        /* save pointer to state in output parameter */
        void** ptr = (void**) pos;
        *ptr = (void*) state;

        return 0;
    } else {
        MAP_OR_FAIL(fgetpos);
        int ret = __real_fgetpos(stream, pos);
        return ret;
    }
}

int SCRMFS_DECL(fsetpos)(FILE* stream, const fpos_t* pos)
{
    /* check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: check that stream is active */

        /* check that we can store a pointer in fpos_t */
        if (! scrmfs_fpos_enabled) {
            s->err = 1;
            errno = EOVERFLOW;
            return -1;
        }

        /* get pointer to state from pos input parameter,
         * assumes pos is a pointer to void*, deference to get value
         * of void*, which we then cast to a state pointer, ugh */
        struct scrmfs_fpos_t* state = (struct scrmfs_fpos_t*) *(void**) pos;

        /* semantics of fsetpos seem to match a seek */
        int seek_rc = scrmfs_fseek(stream, state->pos, SEEK_SET);
        if (seek_rc != 0) {
            return seek_rc;
        }

        /* free memory */
        free(state);

        return 0;
    } else {
        MAP_OR_FAIL(fsetpos);
        int ret = __real_fsetpos(stream, pos);
        return ret;
    }
}

int SCRMFS_DECL(fflush)(FILE* stream)
{
    /* if stream is NULL, flush output on all streams */
    if (stream == NULL) {
        /* first, have real library flush all of its streams,
         * important to do this in this order since it may set errno,
         * which could override our setting for scrmfs streams */
        MAP_OR_FAIL(fflush);
        int ret = __real_fflush(NULL);

        /* flush each active scrmfs stream */
        int fid;
        for (fid = 0; fid < SCRMFS_MAX_FILEDESCS; fid++) {
            /* get stream and check whether it's active */
            scrmfs_stream_t* s = &(scrmfs_streams[fid]);
            if (s->fd >= 0) {
                /* attempt to flush stream */
                int flush_rc = scrmfs_stream_flush((FILE*)s);
                if (flush_rc != SCRMFS_SUCCESS) {
                    /* ERROR: flush sets error indicator and errno */
                    ret = EOF;
                }
            }
        }

        return ret;
    }

    /* otherwise, check whether we should intercept this stream */
    if (scrmfs_intercept_stream(stream)) {
        /* lookup stream */
        scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

        /* TODO: check that stream is active */

        /* flush output on stream */
        int rc = scrmfs_stream_flush(stream);
        if (rc != SCRMFS_SUCCESS) {
            /* ERROR: flush sets error indicator and errno */
            return EOF;
        }

        return 0;
    } else {
        MAP_OR_FAIL(fflush);
        int ret = __real_fflush(stream);
        return ret;
    }
}

/* return non-zero if and only if end-of-file indicator is set
 * for stream */
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
        int fd = s->fd;
        if (fd < 0) {
            errno = EBADF;
            return -1;
        }

        /* return file descriptor associated with stream */
        return fd;
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

        /* flush stream */
        int flush_rc = scrmfs_stream_flush(stream);
        if (flush_rc != SCRMFS_SUCCESS) {
            /* ERROR: flush sets error indicator and errno */
            return EOF;
        }

        /* free the buffer */
        if (s->buffree) {
            free(s->buf);
            s->buf = NULL;
            s->buffree = 0;
        }

        /* free the push back buffer */
        if (s->ubuf != NULL) {
            free(s->ubuf);
            s->ubuf = NULL;
        }

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
#if 0
    if (scrmfs_intercept_path(path)) {
        int i = 0;
        chunk_list_t *chunk_list = NULL;
        chunk_list_t *chunk_list_elem;
    
        /* get the file id for this file descriptor */
        /* Rag: We decided to use the path instead.. Can add flexibility to support both */
        //int fid = scrmfs_get_fid_from_fd(fd);
        int fid = scrmfs_get_fid_from_path(path);
        if ( fid < 0 ) {
            errno = EACCES;
            return NULL;
        }

        /* get meta data for this file */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        if (meta) {
        
            while ( i < meta->chunks ) {
                chunk_list_elem = (chunk_list_t*)malloc(sizeof(chunk_list_t));

                /* get the chunk id for the i-th chunk and
                 * add it to the chunk_list */
                scrmfs_chunkmeta_t* chunk_meta = &(meta->chunk_meta[i]);
                chunk_list_elem->chunk_id = chunk_meta->id;
                chunk_list_elem->location = chunk_meta->location;

                if ( chunk_meta->location == CHUNK_LOCATION_MEMFS ) {
                    /* update the list_elem with the memory address of this chunk */
                    chunk_list_elem->chunk_offset = scrmfs_compute_chunk_buf( meta, chunk_meta->id, 0);
                    chunk_list_elem->spillover_offset = 0;
                } else if ( chunk_meta->location == CHUNK_LOCATION_SPILLOVER ) {
                    /* update the list_elem with the offset of this chunk in the spillover file*/
                    chunk_list_elem->spillover_offset = scrmfs_compute_spill_offset( meta, chunk_meta->id, 0);
                    chunk_list_elem->chunk_offset = NULL;
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
            return NULL;
        }
    } else {
        /* file not managed by SCRMFS */
        errno = EACCES;
        return NULL;
    }
#endif
    return NULL;
}

/* debug function to print list of chunks constituting a file
 * and to test above function*/
void scrmfs_print_chunk_list(char* path)
{
#if 0
    chunk_list_t *chunk_list;
    chunk_list_t *chunk_element;

    chunk_list = scrmfs_get_chunk_list(path);

    fprintf(stdout,"-------------------------------------\n");
    LL_FOREACH(chunk_list,chunk_element) {
        printf("%d,%d,%p,%ld\n",chunk_element->chunk_id,
                                chunk_element->location,
                                chunk_element->chunk_offset,
                                chunk_element->spillover_offset);
    }

    LL_FOREACH(chunk_list,chunk_element) {
        free(chunk_element);
    }
    fprintf(stdout,"\n");
    fprintf(stdout,"-------------------------------------\n");
#endif
}

/*
 * FreeBSD http://www.freebsd.org/
 *   svn co svn://svn.freebsd.org/base/head/lib/libc/ freebsd_libc.svn
 *
 * Bionic libc BSD from google
 * https://github.com/android/platform_bionic/blob/master/libc/docs/OVERVIEW.TXT
 *
 * LGPL libc http://uclibc.org/
 *
 * http://www.gnu.org/software/libc/manual/html_node/I_002fO-Overview.html#I_002fO-Overview
 * http://pubs.opengroup.org/onlinepubs/009695399/
 * http://www.open-std.org/jtc1/sc22/wg14/www/docs/n1124.pdf (7.19)
 * https://github.com/fakechroot/fakechroot/tree/master/src
 *
 * fopen
 * -- fdopen
 * -- freopen
 *
 * ungetc
 * fgetc
 * fputc
 * getc
 * putc
 * fgets
 * fputs
 * -- ungetwc
 * -- fgetwc
 * -- fputwc
 * -- getwc*
 * -- putwc*
 * -- fgetws
 * -- fputws
 * fread
 * fwrite
 *
 * fscanf
 * vfscanf
 * fprintf
 * vfprintf
 * -- fwscanf
 * -- vfwscanf
 * -- fwprintf
 * -- vfwprintf
 *
 * fseek
 * fseeko
 * ftell
 * ftello
 * rewind
 * fsetpos
 * fgetpos
 *
 * setvbuf
 * setbuf
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
 * -- fopen64
 * -- freopen64
 * -- fseeko64
 * -- fgetpos64
 * -- fsetpos64
 * -- ftello64
 */


/*-
 * Copyright (c) 1990, 1993
 *	The Regents of the University of California.  All rights reserved.
 *
 * Copyright (c) 2011 The FreeBSD Foundation
 * All rights reserved.
 * Portions of this software were developed by David Chisnall
 * under sponsorship from the FreeBSD Foundation.
 *
 * This code is derived from software contributed to Berkeley by
 * Chris Torek.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#if defined(LIBC_SCCS) && !defined(lint)
//ATMstatic char sccsid[] = "@(#)vfscanf.c	8.1 (Berkeley) 6/4/93";
#endif /* LIBC_SCCS and not lint */
#include <sys/cdefs.h>
//ATM __FBSDID("$FreeBSD$");

//ATM #include "namespace.h"
#include <ctype.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <wchar.h>
#include <wctype.h>
//ATM #include "un-namespace.h"

//ATM #include "collate.h"
//ATM #include "libc_private.h"
//ATM #include "local.h"
//ATM #include "xlocale_private.h"

#ifndef NO_FLOATING_POINT
//ATM #include <locale.h>
#endif

#define	BUF		513	/* Maximum length of numeric string. */

/*
 * Flags used during conversion.
 */
#define	LONG		0x01	/* l: long or double */
#define	LONGDBL		0x02	/* L: long double */
#define	SHORT		0x04	/* h: short */
#define	SUPPRESS	0x08	/* *: suppress assignment */
#define	POINTER		0x10	/* p: void * (as hex) */
#define	NOSKIP		0x20	/* [ or c: do not skip blanks */
#define	LONGLONG	0x400	/* ll: long long (+ deprecated q: quad) */
#define	INTMAXT		0x800	/* j: intmax_t */
#define	PTRDIFFT	0x1000	/* t: ptrdiff_t */
#define	SIZET		0x2000	/* z: size_t */
#define	SHORTSHORT	0x4000	/* hh: char */
#define	UNSIGNED	0x8000	/* %[oupxX] conversions */

/*
 * The following are used in integral conversions only:
 * SIGNOK, NDIGITS, PFXOK, and NZDIGITS
 */
#define	SIGNOK		0x40	/* +/- is (still) legal */
#define	NDIGITS		0x80	/* no digits detected */
#define	PFXOK		0x100	/* 0x prefix is (still) legal */
#define	NZDIGITS	0x200	/* no zero digits detected */
#define	HAVESIGN	0x10000	/* sign detected */

/*
 * Conversion types.
 */
#define	CT_CHAR		0	/* %c conversion */
#define	CT_CCL		1	/* %[...] conversion */
#define	CT_STRING	2	/* %s conversion */
#define	CT_INT		3	/* %[dioupxX] conversion */
#define	CT_FLOAT	4	/* %[efgEFG] conversion */

//ATM
#undef __inline
#define __inline

static const u_char *__sccl(char *, const u_char *);
#ifndef NO_FLOATING_POINT
//ATM static int parsefloat(scrmfs_stream_t *, char *, char *, locale_t);
static int parsefloat(scrmfs_stream_t *, char *, char *);
#endif

//ATM __weak_reference(__vfscanf, vfscanf);

/*
 * Conversion functions are passed a pointer to this object instead of
 * a real parameter to indicate that the assignment-suppression (*)
 * flag was specified.  We could use a NULL pointer to indicate this,
 * but that would mask bugs in applications that call scanf() with a
 * NULL pointer.
 */
static const int suppress;
#define	SUPPRESS_PTR	((void *)&suppress)

//ATM static const mbstate_t initial_mbs;

// ATM
static int __srefill(scrmfs_stream_t* stream)
{
    /* lookup stream */
    scrmfs_stream_t* s = (scrmfs_stream_t*) stream;

    /* get pointer to file descriptor structure */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(s->fd);
    if (filedesc == NULL) {
       /* ERROR: invalid file descriptor */
       s->err = 1;
       errno = EBADF;
       return 1;
    }
            
    /* bail with error if stream not open for reading */
    if (! filedesc->read) {
        s->err = 1;
        errno = EBADF;
        return 1;
    }

    /* associate buffer with stream if we need to */
    if (s->buf == NULL) {
        int setvbuf_rc = scrmfs_setvbuf(stream, NULL, s->buftype, SCRMFS_STREAM_BUFSIZE);
        if (setvbuf_rc != SCRMFS_SUCCESS) {
            /* ERROR: failed to associate buffer */
            s->err = 1;
            errno = scrmfs_err_map_to_errno(setvbuf_rc);
            return 1;
        }
    }

    /* if we have anything on the push back buffer, that must be
     * our current file pointer, since any seek would have cleared
     * the buffer and each read/write/unget keeps it up-to-date */
    if (s->ubuflen > 0) {
        s->_p = s->ubuf + s->ubufsize - s->ubuflen;
        s->_r = s->ubuflen;
        return 1;
    }

    /* check that current falls within buffer */
    off_t current = filedesc->pos;
    off_t start  = s->bufpos;
    off_t length = s->buflen;
    if (current < start || current >= start + length) {
        /* current is outside the range of our buffer */

        /* flush buffer if needed before read */
        int flush_rc = scrmfs_stream_flush((FILE*)stream);
        if (flush_rc != SCRMFS_SUCCESS) {
            /* ERROR: flush sets error indicator and errno */
            return 1;
        }

        /* read data from file into buffer */
        size_t bufcount;
        int read_rc = scrmfs_fd_read(s->fd, current, s->buf, s->bufsize, &bufcount);
        if (read_rc != SCRMFS_SUCCESS) {
            /* ERROR: read error, set error indicator and errno */
            s->err = 1;
            errno = scrmfs_err_map_to_errno(read_rc);
            return 1;
        }

        /* record new buffer range within file */
        s->bufpos = current;
        s->buflen = bufcount;
    }

    /* determine number of bytes to copy from stream buffer */
    size_t stream_offset    = (size_t) (current - s->bufpos);
    size_t stream_remaining = s->buflen - stream_offset;
    unsigned char* stream_start = (unsigned char*)s->buf + stream_offset;
    s->_p = stream_start;
    s->_r = stream_remaining;

    if (stream_remaining == 0) {
        return 1;
    }

    return 0;
}

/*
 * The following conversion functions return the number of characters consumed,
 * or -1 on input failure.  Character class conversion returns 0 on match
 * failure.
 */

static __inline int
convert_char(scrmfs_stream_t *fp, char * p, int width)
{
	int n;

	if (p == SUPPRESS_PTR) {
		size_t sum = 0;
		for (;;) {
			if ((n = fp->_r) < width) {
				sum += n;
				width -= n;
				fp->_p += n;
				if (__srefill(fp)) {
					if (sum == 0)
						return (-1);
					break;
				}
			} else {
				sum += width;
				fp->_r -= width;
				fp->_p += width;
				break;
			}
		}
		return (sum);
	} else {
		//ATM size_t r = __fread(p, 1, width, fp);
		size_t r = fread(p, 1, width, (FILE*)fp);

		if (r == 0)
			return (-1);
		return (r);
	}
}

//ATM
#if 0
static __inline int
convert_wchar(scrmfs_stream_t *fp, wchar_t *wcp, int width, locale_t locale)
{
	mbstate_t mbs;
	int n, nread;
	wint_t wi;

	mbs = initial_mbs;
	n = 0;
	while (width-- != 0 &&
	    (wi = __fgetwc_mbs(fp, &mbs, &nread, locale)) != WEOF) {
		if (wcp != SUPPRESS_PTR)
			*wcp++ = (wchar_t)wi;
		n += nread;
	}
	if (n == 0)
		return (-1);
	return (n);
}
#endif

static __inline int
convert_ccl(scrmfs_stream_t *fp, char * p, int width, const char *ccltab)
{
	char *p0;
	int n;

	if (p == SUPPRESS_PTR) {
		n = 0;
		while (ccltab[*fp->_p]) {
			n++, fp->_r--, fp->_p++;
			if (--width == 0)
				break;
			if (fp->_r <= 0 && __srefill(fp)) {
				if (n == 0)
					return (-1);
				break;
			}
		}
	} else {
		p0 = p;
		while (ccltab[*fp->_p]) {
			fp->_r--;
			*p++ = *fp->_p++;
			if (--width == 0)
				break;
			if (fp->_r <= 0 && __srefill(fp)) {
				if (p == p0)
					return (-1);
				break;
			}
		}
		n = p - p0;
		if (n == 0)
			return (0);
		*p = 0;
	}
	return (n);
}

//ATM
#if 0
static __inline int
convert_wccl(scrmfs_stream_t *fp, wchar_t *wcp, int width, const char *ccltab,
    locale_t locale)
{
	mbstate_t mbs;
	wint_t wi;
	int n, nread;

	mbs = initial_mbs;
	n = 0;
	if (wcp == SUPPRESS_PTR) {
		while ((wi = __fgetwc_mbs(fp, &mbs, &nread, locale)) != WEOF &&
		    width-- != 0 && ccltab[wctob(wi)])
			n += nread;
		if (wi != WEOF)
			__ungetwc(wi, fp, __get_locale());
	} else {
		while ((wi = __fgetwc_mbs(fp, &mbs, &nread, locale)) != WEOF &&
		    width-- != 0 && ccltab[wctob(wi)]) {
			*wcp++ = (wchar_t)wi;
			n += nread;
		}
		if (wi != WEOF)
			__ungetwc(wi, fp, __get_locale());
		if (n == 0)
			return (0);
		*wcp = 0;
	}
	return (n);
}
#endif

static __inline int
convert_string(scrmfs_stream_t *fp, char * p, int width)
{
	char *p0;
	int n;

	if (p == SUPPRESS_PTR) {
		n = 0;
		while (!isspace(*fp->_p)) {
			n++, fp->_r--, fp->_p++;
			if (--width == 0)
				break;
			if (fp->_r <= 0 && __srefill(fp))
				break;
		}
	} else {
		p0 = p;
		while (!isspace(*fp->_p)) {
			fp->_r--;
			*p++ = *fp->_p++;
			if (--width == 0)
				break;
			if (fp->_r <= 0 && __srefill(fp))
				break;
		}
		*p = 0;
		n = p - p0;
	}
	return (n);
}

//ATM
#if 0
static __inline int
convert_wstring(scrmfs_stream_t *fp, wchar_t *wcp, int width, locale_t locale)
{
	mbstate_t mbs;
	wint_t wi;
	int n, nread;

	mbs = initial_mbs;
	n = 0;
	if (wcp == SUPPRESS_PTR) {
		while ((wi = __fgetwc_mbs(fp, &mbs, &nread, locale)) != WEOF &&
		    width-- != 0 && !iswspace(wi))
			n += nread;
		if (wi != WEOF)
			__ungetwc(wi, fp, __get_locale());
	} else {
		while ((wi = __fgetwc_mbs(fp, &mbs, &nread, locale)) != WEOF &&
		    width-- != 0 && !iswspace(wi)) {
			*wcp++ = (wchar_t)wi;
			n += nread;
		}
		if (wi != WEOF)
			__ungetwc(wi, fp, __get_locale());
		*wcp = '\0';
	}
	return (n);
}
#endif

/*
 * Read an integer, storing it in buf.  The only relevant bit in the
 * flags argument is PFXOK.
 *
 * Return 0 on a match failure, and the number of characters read
 * otherwise.
 */
static __inline int
parseint(scrmfs_stream_t *fp, char * __restrict buf, int width, int base, int flags)
{
	/* `basefix' is used to avoid `if' tests */
	static const short basefix[17] =
		{ 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
	char *p;
	int c;

	flags |= SIGNOK | NDIGITS | NZDIGITS;
	for (p = buf; width; width--) {
		c = *fp->_p;
		/*
		 * Switch on the character; `goto ok' if we accept it
		 * as a part of number.
		 */
		switch (c) {

		/*
		 * The digit 0 is always legal, but is special.  For
		 * %i conversions, if no digits (zero or nonzero) have
		 * been scanned (only signs), we will have base==0.
		 * In that case, we should set it to 8 and enable 0x
		 * prefixing.  Also, if we have not scanned zero
		 * digits before this, do not turn off prefixing
		 * (someone else will turn it off if we have scanned
		 * any nonzero digits).
		 */
		case '0':
			if (base == 0) {
				base = 8;
				flags |= PFXOK;
			}
			if (flags & NZDIGITS)
				flags &= ~(SIGNOK|NZDIGITS|NDIGITS);
			else
				flags &= ~(SIGNOK|PFXOK|NDIGITS);
			goto ok;

		/* 1 through 7 always legal */
		case '1': case '2': case '3':
		case '4': case '5': case '6': case '7':
			base = basefix[base];
			flags &= ~(SIGNOK | PFXOK | NDIGITS);
			goto ok;

		/* digits 8 and 9 ok iff decimal or hex */
		case '8': case '9':
			base = basefix[base];
			if (base <= 8)
				break;	/* not legal here */
			flags &= ~(SIGNOK | PFXOK | NDIGITS);
			goto ok;

		/* letters ok iff hex */
		case 'A': case 'B': case 'C':
		case 'D': case 'E': case 'F':
		case 'a': case 'b': case 'c':
		case 'd': case 'e': case 'f':
			/* no need to fix base here */
			if (base <= 10)
				break;	/* not legal here */
			flags &= ~(SIGNOK | PFXOK | NDIGITS);
			goto ok;

		/* sign ok only as first character */
		case '+': case '-':
			if (flags & SIGNOK) {
				flags &= ~SIGNOK;
				flags |= HAVESIGN;
				goto ok;
			}
			break;

		/*
		 * x ok iff flag still set & 2nd char (or 3rd char if
		 * we have a sign).
		 */
		case 'x': case 'X':
			if (flags & PFXOK && p ==
			    buf + 1 + !!(flags & HAVESIGN)) {
				base = 16;	/* if %i */
				flags &= ~PFXOK;
				goto ok;
			}
			break;
		}

		/*
		 * If we got here, c is not a legal character for a
		 * number.  Stop accumulating digits.
		 */
		break;
	ok:
		/*
		 * c is legal: store it and look at the next.
		 */
		*p++ = c;
		if (--fp->_r > 0)
			fp->_p++;
		else if (__srefill(fp))
			break;		/* EOF */
	}
	/*
	 * If we had only a sign, it is no good; push back the sign.
	 * If the number ends in `x', it was [sign] '0' 'x', so push
	 * back the x and treat it as [sign] '0'.
	 */
	if (flags & NDIGITS) {
		if (p > buf)
			//ATM (void) __ungetc(*(u_char *)--p, fp);
			(void) ungetc(*(u_char *)--p, (FILE*)fp);
		return (0);
	}
	c = ((u_char *)p)[-1];
	if (c == 'x' || c == 'X') {
		--p;
		//ATM (void) __ungetc(c, fp);
		(void) ungetc(c, (FILE*)fp);
	}
	return (p - buf);
}

//ATM
#if 0
/*
 * __vfscanf - MT-safe version
 */
int
__vfscanf(scrmfs_stream_t *fp, char const *fmt0, va_list ap)
{
	int ret;

	FLOCKFILE(fp);
	ret = __svfscanf(fp, __get_locale(), fmt0, ap);
	FUNLOCKFILE(fp);
	return (ret);
}
int
vfscanf_l(scrmfs_stream_t *fp, locale_t locale, char const *fmt0, va_list ap)
{
	int ret;
	FIX_LOCALE(locale);

	FLOCKFILE(fp);
	ret = __svfscanf(fp, locale, fmt0, ap);
	FUNLOCKFILE(fp);
	return (ret);
}
#endif

/*
 * __svfscanf - non-MT-safe version of __vfscanf
 */
static int
//ATM __svfscanf(scrmfs_stream_t *fp, locale_t locale, const char *fmt0, va_list ap)
__svfscanf(scrmfs_stream_t *fp, const char *fmt0, va_list ap)
{
#define	GETARG(type)	((flags & SUPPRESS) ? SUPPRESS_PTR : va_arg(ap, type))
	const u_char *fmt = (const u_char *)fmt0;
	int c;			/* character from format, or conversion */
	size_t width;		/* field width, or 0 */
	int flags;		/* flags as defined above */
	int nassigned;		/* number of fields assigned */
	int nconversions;	/* number of conversions */
	int nr;			/* characters read by the current conversion */
	int nread;		/* number of characters consumed from fp */
	int base;		/* base argument to conversion function */
	char ccltab[256];	/* character class table for %[...] */
	char buf[BUF];		/* buffer for numeric conversions */

	//ATM ORIENT(fp, -1);

	nassigned = 0;
	nconversions = 0;
	nread = 0;
	for (;;) {
		c = *fmt++;
		if (c == 0)
			return (nassigned);
		if (isspace(c)) {
			while ((fp->_r > 0 || __srefill(fp) == 0) && isspace(*fp->_p))
				nread++, fp->_r--, fp->_p++;
			continue;
		}
		if (c != '%')
			goto literal;
		width = 0;
		flags = 0;
		/*
		 * switch on the format.  continue if done;
		 * break once format type is derived.
		 */
again:		c = *fmt++;
		switch (c) {
		case '%':
literal:
			if (fp->_r <= 0 && __srefill(fp))
				goto input_failure;
			if (*fp->_p != c)
				goto match_failure;
			fp->_r--, fp->_p++;
			nread++;
			continue;

		case '*':
			flags |= SUPPRESS;
			goto again;
		case 'j':
			flags |= INTMAXT;
			goto again;
		case 'l':
			if (flags & LONG) {
				flags &= ~LONG;
				flags |= LONGLONG;
			} else
				flags |= LONG;
			goto again;
		case 'q':
			flags |= LONGLONG;	/* not quite */
			goto again;
		case 't':
			flags |= PTRDIFFT;
			goto again;
		case 'z':
			flags |= SIZET;
			goto again;
		case 'L':
			flags |= LONGDBL;
			goto again;
		case 'h':
			if (flags & SHORT) {
				flags &= ~SHORT;
				flags |= SHORTSHORT;
			} else
				flags |= SHORT;
			goto again;

		case '0': case '1': case '2': case '3': case '4':
		case '5': case '6': case '7': case '8': case '9':
			width = width * 10 + c - '0';
			goto again;

		/*
		 * Conversions.
		 */
		case 'd':
			c = CT_INT;
			base = 10;
			break;

		case 'i':
			c = CT_INT;
			base = 0;
			break;

		case 'o':
			c = CT_INT;
			flags |= UNSIGNED;
			base = 8;
			break;

		case 'u':
			c = CT_INT;
			flags |= UNSIGNED;
			base = 10;
			break;

		case 'X':
		case 'x':
			flags |= PFXOK;	/* enable 0x prefixing */
			c = CT_INT;
			flags |= UNSIGNED;
			base = 16;
			break;

#ifndef NO_FLOATING_POINT
		case 'A': case 'E': case 'F': case 'G':
		case 'a': case 'e': case 'f': case 'g':
			c = CT_FLOAT;
			break;
#endif

		case 'S':
			flags |= LONG;
			/* FALLTHROUGH */
		case 's':
			c = CT_STRING;
			break;

		case '[':
			fmt = __sccl(ccltab, fmt);
			flags |= NOSKIP;
			c = CT_CCL;
			break;

		case 'C':
			flags |= LONG;
			/* FALLTHROUGH */
		case 'c':
			flags |= NOSKIP;
			c = CT_CHAR;
			break;

		case 'p':	/* pointer format is like hex */
			flags |= POINTER | PFXOK;
			c = CT_INT;		/* assumes sizeof(uintmax_t) */
			flags |= UNSIGNED;	/*      >= sizeof(uintptr_t) */
			base = 16;
			break;

		case 'n':
			if (flags & SUPPRESS)	/* ??? */
				continue;
			if (flags & SHORTSHORT)
				*va_arg(ap, char *) = nread;
			else if (flags & SHORT)
				*va_arg(ap, short *) = nread;
			else if (flags & LONG)
				*va_arg(ap, long *) = nread;
			else if (flags & LONGLONG)
				*va_arg(ap, long long *) = nread;
			else if (flags & INTMAXT)
				*va_arg(ap, intmax_t *) = nread;
			else if (flags & SIZET)
				*va_arg(ap, size_t *) = nread;
			else if (flags & PTRDIFFT)
				*va_arg(ap, ptrdiff_t *) = nread;
			else
				*va_arg(ap, int *) = nread;
			continue;

		default:
			goto match_failure;

		/*
		 * Disgusting backwards compatibility hack.	XXX
		 */
		case '\0':	/* compat */
			return (EOF);
		}

		/*
		 * We have a conversion that requires input.
		 */
		if (fp->_r <= 0 && __srefill(fp))
			goto input_failure;

		/*
		 * Consume leading white space, except for formats
		 * that suppress this.
		 */
		if ((flags & NOSKIP) == 0) {
			while (isspace(*fp->_p)) {
				nread++;
				if (--fp->_r > 0)
					fp->_p++;
				else if (__srefill(fp))
					goto input_failure;
			}
			/*
			 * Note that there is at least one character in
			 * the buffer, so conversions that do not set NOSKIP
			 * ca no longer result in an input failure.
			 */
		}

		/*
		 * Do the conversion.
		 */
		switch (c) {

		case CT_CHAR:
			/* scan arbitrary characters (sets NOSKIP) */
			if (width == 0)
				width = 1;
			if (flags & LONG) {
//ATM				nr = convert_wchar(fp, GETARG(wchar_t *),
//				    width, locale);
			} else {
				nr = convert_char(fp, GETARG(char *), width);
			}
			if (nr < 0)
				goto input_failure;
			break;

		case CT_CCL:
			/* scan a (nonempty) character class (sets NOSKIP) */
			if (width == 0)
				width = (size_t)~0;	/* `infinity' */
			if (flags & LONG) {
//ATM				nr = convert_wccl(fp, GETARG(wchar_t *), width,
//				    ccltab, locale);
			} else {
				nr = convert_ccl(fp, GETARG(char *), width,
				    ccltab);
			}
			if (nr <= 0) {
				if (nr < 0)
					goto input_failure;
				else /* nr == 0 */
					goto match_failure;
			}
			break;

		case CT_STRING:
			/* like CCL, but zero-length string OK, & no NOSKIP */
			if (width == 0)
				width = (size_t)~0;
			if (flags & LONG) {
//ATM				nr = convert_wstring(fp, GETARG(wchar_t *),
//				    width, locale);
			} else {
				nr = convert_string(fp, GETARG(char *), width);
			}
			if (nr < 0)
				goto input_failure;
			break;

		case CT_INT:
			/* scan an integer as if by the conversion function */
#ifdef hardway
			if (width == 0 || width > sizeof(buf) - 1)
				width = sizeof(buf) - 1;
#else
			/* size_t is unsigned, hence this optimisation */
			if (--width > sizeof(buf) - 2)
				width = sizeof(buf) - 2;
			width++;
#endif
			nr = parseint(fp, buf, width, base, flags);
			if (nr == 0)
				goto match_failure;
			if ((flags & SUPPRESS) == 0) {
				uintmax_t res;

				buf[nr] = '\0';
				if ((flags & UNSIGNED) == 0)
				    res = strtoimax(buf, (char **)NULL, base);
				else
				    res = strtoumax(buf, (char **)NULL, base);
				if (flags & POINTER)
					*va_arg(ap, void **) =
							(void *)(uintptr_t)res;
				else if (flags & SHORTSHORT)
					*va_arg(ap, char *) = res;
				else if (flags & SHORT)
					*va_arg(ap, short *) = res;
				else if (flags & LONG)
					*va_arg(ap, long *) = res;
				else if (flags & LONGLONG)
					*va_arg(ap, long long *) = res;
				else if (flags & INTMAXT)
					*va_arg(ap, intmax_t *) = res;
				else if (flags & PTRDIFFT)
					*va_arg(ap, ptrdiff_t *) = res;
				else if (flags & SIZET)
					*va_arg(ap, size_t *) = res;
				else
					*va_arg(ap, int *) = res;
			}
			break;

#ifndef NO_FLOATING_POINT
		case CT_FLOAT:
			/* scan a floating point number as if by strtod */
			if (width == 0 || width > sizeof(buf) - 1)
				width = sizeof(buf) - 1;
//ATM			nr = parsefloat(fp, buf, buf + width, locale);
			nr = parsefloat(fp, buf, buf + width);
			if (nr == 0)
				goto match_failure;
			if ((flags & SUPPRESS) == 0) {
				if (flags & LONGDBL) {
					long double res = strtold(buf, NULL);
					*va_arg(ap, long double *) = res;
				} else if (flags & LONG) {
					double res = strtod(buf, NULL);
					*va_arg(ap, double *) = res;
				} else {
					float res = strtof(buf, NULL);
					*va_arg(ap, float *) = res;
				}
			}
			break;
#endif /* !NO_FLOATING_POINT */
		}
		if (!(flags & SUPPRESS))
			nassigned++;
		nread += nr;
		nconversions++;
	}
input_failure:
	return (nconversions != 0 ? nassigned : EOF);
match_failure:
	return (nassigned);
}

/*
 * Fill in the given table from the scanset at the given format
 * (just after `[').  Return a pointer to the character past the
 * closing `]'.  The table has a 1 wherever characters should be
 * considered part of the scanset.
 */
static const u_char *
__sccl(tab, fmt)
	char *tab;
	const u_char *fmt;
{
	int c, n, v, i;
//ATM
//	struct xlocale_collate *table =
//		(struct xlocale_collate*)__get_locale()->components[XLC_COLLATE];

	/* first `clear' the whole table */
	c = *fmt++;		/* first char hat => negated scanset */
	if (c == '^') {
		v = 1;		/* default => accept */
		c = *fmt++;	/* get new first char */
	} else
		v = 0;		/* default => reject */

	/* XXX: Will not work if sizeof(tab*) > sizeof(char) */
	(void) memset(tab, v, 256);

	if (c == 0)
		return (fmt - 1);/* format ended before closing ] */

	/*
	 * Now set the entries corresponding to the actual scanset
	 * to the opposite of the above.
	 *
	 * The first character may be ']' (or '-') without being special;
	 * the last character may be '-'.
	 */
	v = 1 - v;
	for (;;) {
		tab[c] = v;		/* take character c */
doswitch:
		n = *fmt++;		/* and examine the next */
		switch (n) {

		case 0:			/* format ended too soon */
			return (fmt - 1);

		case '-':
			/*
			 * A scanset of the form
			 *	[01+-]
			 * is defined as `the digit 0, the digit 1,
			 * the character +, the character -', but
			 * the effect of a scanset such as
			 *	[a-zA-Z0-9]
			 * is implementation defined.  The V7 Unix
			 * scanf treats `a-z' as `the letters a through
			 * z', but treats `a-a' as `the letter a, the
			 * character -, and the letter a'.
			 *
			 * For compatibility, the `-' is not considerd
			 * to define a range if the character following
			 * it is either a close bracket (required by ANSI)
			 * or is not numerically greater than the character
			 * we just stored in the table (c).
			 */
			n = *fmt;
			if (n == ']'
//ATM
//			    || (table->__collate_load_error ? n < c :
//				__collate_range_cmp (table, n, c) < 0
//			       )
                            || n < c
			   ) {
				c = '-';
				break;	/* resume the for(;;) */
			}
			fmt++;
			/* fill in the range */
//ATM			if (table->__collate_load_error) {
				do {
					tab[++c] = v;
				} while (c < n);
//ATM
#if 0
			} else {
				for (i = 0; i < 256; i ++)
					if (   __collate_range_cmp (table, c, i) < 0
					    && __collate_range_cmp (table, i, n) <= 0
					   )
						tab[i] = v;
			}
#endif
#if 1	/* XXX another disgusting compatibility hack */
			c = n;
			/*
			 * Alas, the V7 Unix scanf also treats formats
			 * such as [a-c-e] as `the letters a through e'.
			 * This too is permitted by the standard....
			 */
			goto doswitch;
#else
			c = *fmt++;
			if (c == 0)
				return (fmt - 1);
			if (c == ']')
				return (fmt);
#endif
			break;

		case ']':		/* end of scanset */
			return (fmt);

		default:		/* just another character */
			c = n;
			break;
		}
	}
	/* NOTREACHED */
}

#ifndef NO_FLOATING_POINT
static int
//parsefloat(scrmfs_stream_t *fp, char *buf, char *end, locale_t locale)
parsefloat(scrmfs_stream_t *fp, char *buf, char *end)
{
	char *commit, *p;
	int infnanpos = 0, decptpos = 0;
	enum {
		S_START, S_GOTSIGN, S_INF, S_NAN, S_DONE, S_MAYBEHEX,
		S_DIGITS, S_DECPT, S_FRAC, S_EXP, S_EXPDIGITS
	} state = S_START;
	unsigned char c;
//ATM
        const char us_decpt[] = ".";
	const char *decpt = us_decpt;
	//ATMconst char *decpt = localeconv_l(locale)->decimal_point;
	//ATM_Bool gotmantdig = 0, ishex = 0;
	int gotmantdig = 0, ishex = 0;

	/*
	 * We set commit = p whenever the string we have read so far
	 * constitutes a valid representation of a floating point
	 * number by itself.  At some point, the parse will complete
	 * or fail, and we will ungetc() back to the last commit point.
	 * To ensure that the file offset gets updated properly, it is
	 * always necessary to read at least one character that doesn't
	 * match; thus, we can't short-circuit "infinity" or "nan(...)".
	 */
	commit = buf - 1;
	for (p = buf; p < end; ) {
		c = *fp->_p;
reswitch:
		switch (state) {
		case S_START:
			state = S_GOTSIGN;
			if (c == '-' || c == '+')
				break;
			else
				goto reswitch;
		case S_GOTSIGN:
			switch (c) {
			case '0':
				state = S_MAYBEHEX;
				commit = p;
				break;
			case 'I':
			case 'i':
				state = S_INF;
				break;
			case 'N':
			case 'n':
				state = S_NAN;
				break;
			default:
				state = S_DIGITS;
				goto reswitch;
			}
			break;
		case S_INF:
			if (infnanpos > 6 ||
			    (c != "nfinity"[infnanpos] &&
			     c != "NFINITY"[infnanpos]))
				goto parsedone;
			if (infnanpos == 1 || infnanpos == 6)
				commit = p;	/* inf or infinity */
			infnanpos++;
			break;
		case S_NAN:
			switch (infnanpos) {
			case 0:
				if (c != 'A' && c != 'a')
					goto parsedone;
				break;
			case 1:
				if (c != 'N' && c != 'n')
					goto parsedone;
				else
					commit = p;
				break;
			case 2:
				if (c != '(')
					goto parsedone;
				break;
			default:
				if (c == ')') {
					commit = p;
					state = S_DONE;
				} else if (!isalnum(c) && c != '_')
					goto parsedone;
				break;
			}
			infnanpos++;
			break;
		case S_DONE:
			goto parsedone;
		case S_MAYBEHEX:
			state = S_DIGITS;
			if (c == 'X' || c == 'x') {
				ishex = 1;
				break;
			} else {	/* we saw a '0', but no 'x' */
				gotmantdig = 1;
				goto reswitch;
			}
		case S_DIGITS:
			if ((ishex && isxdigit(c)) || isdigit(c)) {
				gotmantdig = 1;
				commit = p;
				break;
			} else {
				state = S_DECPT;
				goto reswitch;
			}
		case S_DECPT:
			if (c == decpt[decptpos]) {
				if (decpt[++decptpos] == '\0') {
					/* We read the complete decpt seq. */
					state = S_FRAC;
					if (gotmantdig)
						commit = p;
				}
				break;
			} else if (!decptpos) {
				/* We didn't read any decpt characters. */
				state = S_FRAC;
				goto reswitch;
			} else {
				/*
				 * We read part of a multibyte decimal point,
				 * but the rest is invalid, so bail.
				 */
				goto parsedone;
			}
		case S_FRAC:
			if (((c == 'E' || c == 'e') && !ishex) ||
			    ((c == 'P' || c == 'p') && ishex)) {
				if (!gotmantdig)
					goto parsedone;
				else
					state = S_EXP;
			} else if ((ishex && isxdigit(c)) || isdigit(c)) {
				commit = p;
				gotmantdig = 1;
			} else
				goto parsedone;
			break;
		case S_EXP:
			state = S_EXPDIGITS;
			if (c == '-' || c == '+')
				break;
			else
				goto reswitch;
		case S_EXPDIGITS:
			if (isdigit(c))
				commit = p;
			else
				goto parsedone;
			break;
		default:
			abort();
		}
		*p++ = c;
		if (--fp->_r > 0)
			fp->_p++;
		else if (__srefill(fp))
			break;	/* EOF */
	}

parsedone:
	while (commit < --p)
		//ATM __ungetc(*(u_char *)p, fp);
		ungetc(*(u_char *)p, (FILE*)fp);
	*++commit = '\0';
	return (commit - buf);
}
#endif
