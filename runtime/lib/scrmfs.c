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
#include <sched.h>

#include "scrmfs-internal.h"

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

extern pthread_mutex_t scrmfs_stack_mutex;

/* keep track of what we've initialized */
int scrmfs_initialized = 0;

/* global persistent memory block (metadata + data) */
static void* scrmfs_superblock = NULL;
static void* free_fid_stack = NULL;
static void* free_chunk_stack = NULL;
static void* free_spillchunk_stack = NULL;
scrmfs_filename_t* scrmfs_filelist    = NULL;
static scrmfs_filemeta_t* scrmfs_filemetas   = NULL;
static scrmfs_chunkmeta_t* scrmfs_chunkmetas = NULL;
static char* scrmfs_chunks = NULL;
static int scrmfs_spilloverblock = 0;

/* array of file descriptors */
scrmfs_fd_t scrmfs_fds[SCRMFS_MAX_FILEDESCS];
rlim_t scrmfs_fd_limit;

/* array of file streams */
scrmfs_stream_t scrmfs_streams[SCRMFS_MAX_FILEDESCS];

/* mount point information */
char*  scrmfs_mount_prefix = NULL;
size_t scrmfs_mount_prefixlen = 0;
static key_t  scrmfs_mount_shmget_key = 0;

/* mutex to lock stack operations */
pthread_mutex_t scrmfs_stack_mutex = PTHREAD_MUTEX_INITIALIZER;

/* single function to route all unsupported wrapper calls through */
int scrmfs_vunsupported(
  const char* fn_name,
  const char* file,
  int line,
  const char* fmt,
  va_list args)
{
    /* print a message about where in the CRUISE code we are */
    printf("UNSUPPORTED: %s() at %s:%d: ", fn_name, file, line);

    /* print string with more info about call, e.g., param values */
    va_list args2;
    va_copy(args2, args);
    vprintf(fmt, args2);
    va_end(args2);

    /* TODO: optionally abort */

    return SCRMFS_SUCCESS;
}

/* single function to route all unsupported wrapper calls through */
int scrmfs_unsupported(
  const char* fn_name,
  const char* file,
  int line,
  const char* fmt,
  ...)
{
    /* print string with more info about call, e.g., param values */
    va_list args;
    va_start(args, fmt);
    int rc = scrmfs_vunsupported(fn_name, file, line, fmt, args);
    va_end(args);
    return rc;
}

/* given an SCRMFS error code, return corresponding errno code */
int scrmfs_err_map_to_errno(int rc)
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

/* returns 1 if two input parameters will overflow their type when
 * added together */
inline int scrmfs_would_overflow_offt(off_t a, off_t b)
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
inline int scrmfs_would_overflow_long(long a, long b)
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
mode_t scrmfs_getmode(mode_t perms)
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
inline int scrmfs_intercept_path(const char* path)
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
inline void scrmfs_intercept_fd(int* fd, int* intercept)
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
inline int scrmfs_intercept_stream(FILE* stream)
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
inline int scrmfs_get_fid_from_path(const char* path)
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
inline int scrmfs_get_fid_from_fd(int fd)
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
inline scrmfs_fd_t* scrmfs_get_filedesc_from_fd(int fd)
{
    if (fd >= 0 && fd < SCRMFS_MAX_FILEDESCS) {
        scrmfs_fd_t* filedesc = &(scrmfs_fds[fd]);
        return filedesc;
    }
    return NULL;
}

/* given a file id, return a pointer to the meta data,
 * otherwise return NULL */
inline scrmfs_filemeta_t* scrmfs_get_meta_from_fid(int fid)
{
    /* check that the file id is within range of our array */
    if (fid >= 0 && fid < scrmfs_max_files) {
        /* get a pointer to the file meta data structure */
        scrmfs_filemeta_t* meta = &scrmfs_filemetas[fid];
        return meta;
    }
    return NULL;
}

/* given a file id and logical chunk id, return pointer to meta data
 * for specified chunk, return NULL if not found */
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
        /* unknown chunk type */
        debug("chunks not stored in containers\n");
        return SCRMFS_ERR_IO;
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
        /* unknown chunk type */
        debug("chunks not stored in containers\n");
        return SCRMFS_ERR_IO;
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
        //MAP_OR_FAIL(pread);
        off_t spill_offset = scrmfs_compute_spill_offset(meta, chunk_id, chunk_offset);
        ssize_t rc = pread(scrmfs_spilloverblock, buf, count, spill_offset);
        /* TODO: check return code for errors */
    }
  #ifdef HAVE_CONTAINER_LIB
    else if (chunk_meta->location == CHUNK_LOCATION_CONTAINER) {
        /* unknown chunk type */
        debug("chunks not stored in containers\n");
        return SCRMFS_ERR_IO;
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
        //MAP_OR_FAIL(pwrite);
        off_t spill_offset = scrmfs_compute_spill_offset(meta, chunk_id, chunk_offset);
        ssize_t rc = pwrite(scrmfs_spilloverblock, buf, count, spill_offset);
        if (rc < 0)  {
            perror("pwrite failed");
        }
        /* TODO: check return code for errors */
    }
  #ifdef HAVE_CONTAINER_LIB
    else if (chunk_meta->location == CHUNK_LOCATION_CONTAINER) {
        /* unknown chunk type */
        debug("chunks not stored in containers\n");
        return SCRMFS_ERR_IO;
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
 * Operations on file storage
 * --------------------------------------- */

/* allocate and initialize data management resource for file */
static int scrmfs_fid_store_alloc(int fid)
{
    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    if (scrmfs_use_memfs || scrmfs_use_spillover) {
        /* we used fixed-size chunk storage for memfs and spillover */
        meta->storage = FILE_STORAGE_FIXED_CHUNK;
    }
  #ifdef HAVE_CONTAINER_LIB
    else if (scrmfs_use_containers) {
        /* record that we're using containers to store this file */
        meta->storage = SCRMFS_STORAGE_CONTAINER;

        /* create container and associate it with file */
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

    return SCRMFS_SUCCESS;
}

/* free data management resource for file */
static int scrmfs_fid_store_free(int fid)
{
    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

  #ifdef HAVE_CONTAINER_LIB
    if (meta->storage == SCRMFS_CONTAINER) {
        int ret = cs_set_container_remove(cs_set_handle, meta->filename);
        free(meta->filename);
        meta->filename = NULL;
        // removal of containers will always fail because it's not implemented yet
        if (ret != CS_SUCCESS) {
            //debug("Container remove failed\n");
            return SCRMFS_FAILURE;
        }
    }
  #endif

    return SCRMFS_SUCCESS;
}

/* if length is greater than reserved space, reserve space up to length */
static int scrmfs_fid_store_fixed_extend(int fid, scrmfs_filemeta_t* meta, off_t length)
{
    /* determine whether we need to allocate more chunks */
    off_t maxsize = meta->chunks << scrmfs_chunk_bits;
    if (length > maxsize) {
        /* compute number of additional bytes we need */
        off_t additional = length - maxsize;
        while (additional > 0) {
            /* check that we don't overrun max number of chunks for file */
            if (meta->chunks == scrmfs_max_chunks) {
                debug("failed to allocate chunk\n");
                return SCRMFS_ERR_NOSPC;
            }

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

    return SCRMFS_SUCCESS;
}

/* if length is shorter than reserved space, give back space down to length */
static int scrmfs_fid_store_fixed_shrink(int fid, scrmfs_filemeta_t* meta, off_t length)
{
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

    return SCRMFS_SUCCESS;
}

/* read data from file stored as fixed-size chunks */
static int scrmfs_fid_store_fixed_read(int fid, scrmfs_filemeta_t* meta, off_t pos, void* buf, size_t count)
{
    int rc;

    /* get pointer to position within first chunk */
    int chunk_id = pos >> scrmfs_chunk_bits;
    off_t chunk_offset = pos & scrmfs_chunk_mask;

    /* determine how many bytes remain in the current chunk */
    size_t remaining = scrmfs_chunk_size - chunk_offset;
    if (count <= remaining) {
        /* all bytes for this read fit within the current chunk */
        rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, buf, count);
    } else {
        /* read what's left of current chunk */
        char* ptr = (char*) buf;
        rc = scrmfs_chunk_read(meta, chunk_id, chunk_offset, (void*)ptr, remaining);
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
            rc = scrmfs_chunk_read(meta, chunk_id, 0, (void*)ptr, num);
            ptr += num;

            /* update number of bytes written */
            processed += num;
        }
    }

    return rc;
}

/* write data to file stored as fixed-size chunks */
static int scrmfs_fid_store_fixed_write(int fid, scrmfs_filemeta_t* meta, off_t pos, const void* buf, size_t count)
{
    int rc;

    /* get pointer to position within first chunk */
    int chunk_id = pos >> scrmfs_chunk_bits;
    off_t chunk_offset = pos & scrmfs_chunk_mask;

    /* determine how many bytes remain in the current chunk */
    size_t remaining = scrmfs_chunk_size - chunk_offset;
    if (count <= remaining) {
        /* all bytes for this write fit within the current chunk */
        rc = scrmfs_chunk_write(meta, chunk_id, chunk_offset, buf, count);
    } else {
        /* otherwise, fill up the remainder of the current chunk */
        char* ptr = (char*) buf;
        rc = scrmfs_chunk_write(meta, chunk_id, chunk_offset, (void*)ptr, remaining);
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
            rc = scrmfs_chunk_write(meta, chunk_id, 0, (void*)ptr, num);
            ptr += num;

            /* update number of bytes processed */
            processed += num;
        }
    }

    return rc;
}

/* if length is greater than reserved space, reserve space up to length */
static int scrmfs_fid_store_container_extend(int fid, scrmfs_filemeta_t* file_meta, off_t length)
{
  #ifdef HAVE_CONTAINER_LIB
    /* I'm using the same infrastructure as memfs (chunks) because
     * it just makes life easier, and I think cleaner. If the size of the container
     * is not big enough, we extend it by the size of a chunk */
    if(file_meta->container_data.container_size < scrmfs_chunk_size + file_meta->size){
       //TODO extend container not implemented yet. always returns out of space
       cs_container_handle_t* ch = &(file_meta->container_data.cs_container_handle);
       int ret = scrmfs_container_extend(cs_set_handle, ch, scrmfs_chunk_size);
       if (ret != SCRMFS_SUCCESS) {
          return ret;
       }
       file_meta->container_data.container_size += scrmfs_chunk_size;
    }
  #endif /* HAVE_CONTAINER_LIB */

    return SCRMFS_SUCCESS;
}

/* if length is less than reserved space, give up space down to length */
static int scrmfs_fid_store_container_shrink(int fid, scrmfs_filemeta_t* file_meta, off_t length)
{
    /* TODO: shrink container space */
    return SCRMFS_SUCCESS;
}

/* read data from file stored as a container */
static int scrmfs_fid_store_container_read(int fid, scrmfs_filemeta_t* meta, off_t pos, void* buf, size_t count)
{
  #ifdef HAVE_CONTAINER_LIB
    /* get handle for container */
    cs_container_handle_t ch = meta->container_data.cs_container_handle;

    /* read chunk from containers */
    int ret = scrmfs_container_read(ch, buf, count, chunk_offset);
    if (ret != SCRMFS_SUCCESS){
        fprintf(stderr, "Container read failed\n");
        return ret;
    }
  #endif /* HAVE_CONTAINER_LIB */

    return SCRMFS_SUCCESS;
}

/* write data to file stored as container */
static int scrmfs_fid_store_container_write(int fid, scrmfs_filemeta_t* meta, off_t pos, const void* buf, size_t count)
{
  #ifdef HAVE_CONTAINER_LIB
    /* get handle for container */
    cs_container_handle_t ch = meta->container_data.cs_container_handle;

    /* write data to container */
    int ret = scrmfs_container_write(ch, buf, count, chunk_offset);
    if (ret != SCRMFS_SUCCESS){
        fprintf(stderr, "container write failed for single container write: %d\n");
        return ret;
    }
  #endif /* HAVE_CONTAINER_LIB */

    return SCRMFS_SUCCESS;
}

/* ---------------------------------------
 * Operations on file ids
 * --------------------------------------- */

/* checks to see if fid is a directory
 * returns 1 for yes
 * returns 0 for no */
int scrmfs_fid_is_dir(int fid)
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
int scrmfs_fid_is_dir_empty(const char * path)
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
off_t scrmfs_fid_size(int fid)
{
    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
    return meta->size;
}

/* fill in limited amount of stat information */
int scrmfs_fid_stat(int fid, struct stat* buf)
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
int scrmfs_fid_alloc()
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
int scrmfs_fid_free(int fid)
{
    scrmfs_stack_lock();
    scrmfs_stack_push(free_fid_stack, fid);
    scrmfs_stack_unlock();
    return SCRMFS_SUCCESS;
}

/* add a new file and initialize metadata
 * returns the new fid, or negative value on error */
int scrmfs_fid_create_file(const char * path)
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
    meta->size    = 0;
    meta->chunks  = 0;
    meta->is_dir  = 0;
    meta->storage = FILE_STORAGE_NULL;
    meta->flock_status = UNLOCKED;
    /* PTHREAD_PROCESS_SHARED allows Process-Shared Synchronization*/
    pthread_spin_init(&meta->fspinlock, PTHREAD_PROCESS_SHARED);

    return fid;
}

/* add a new directory and initialize metadata
 * returns the new fid, or a negative value on error */
int scrmfs_fid_create_directory(const char * path)
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
int scrmfs_fid_read(int fid, off_t pos, void* buf, size_t count)
{
    int rc;

    /* short-circuit a 0-byte read */
    if (count == 0) {
        return SCRMFS_SUCCESS;
    }

    /* get meta for this file id */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* determine storage type to read file data */
    if (meta->storage == FILE_STORAGE_FIXED_CHUNK) {
        /* file stored in fixed-size chunks */
        rc = scrmfs_fid_store_fixed_read(fid, meta, pos, buf, count);
    } else if (meta->storage == FILE_STORAGE_CONTAINER) {
        /* file stored in container */
        rc = scrmfs_fid_store_fixed_read(fid, meta, pos, buf, count);
    } else {
        /* unknown storage type */
        rc = SCRMFS_ERR_IO;
    }

    return rc;
}

/* write count bytes from buf into file starting at offset pos,
 * all bytes are assumed to be allocated to file, so file should
 * be extended before calling this routine */
int scrmfs_fid_write(int fid, off_t pos, const void* buf, size_t count)
{
    int rc;

    /* short-circuit a 0-byte write */
    if (count == 0) {
        return SCRMFS_SUCCESS;
    }

    /* get meta for this file id */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* determine storage type to write file data */
    if (meta->storage == FILE_STORAGE_FIXED_CHUNK) {
        /* file stored in fixed-size chunks */
        rc = scrmfs_fid_store_fixed_write(fid, meta, pos, buf, count);
    } else if (meta->storage == FILE_STORAGE_CONTAINER) {
        /* file stored in container */
        rc = scrmfs_fid_store_fixed_write(fid, meta, pos, buf, count);
    } else {
        /* unknown storage type */
        rc = SCRMFS_ERR_IO;
    }

    return rc;
}

/* given a file id, write zero bytes to region of specified offset
 * and length, assumes space is already reserved */
int scrmfs_fid_write_zero(int fid, off_t pos, off_t count)
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
int scrmfs_fid_extend(int fid, off_t length)
{
    int rc;

    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* determine file storage type */
    if (meta->storage == FILE_STORAGE_FIXED_CHUNK) {
        /* file stored in fixed-size chunks */
        rc = scrmfs_fid_store_fixed_extend(fid, meta, length);
    } else if (meta->storage == FILE_STORAGE_CONTAINER) {
        /* file stored in container */
        rc = scrmfs_fid_store_container_extend(fid, meta, length);
    } else {
        /* unknown storage type */
        rc = SCRMFS_ERR_IO;
    }

    /* TODO: move this statement elsewhere */
    /* increase file size up to length */
    if (length > meta->size) {
        meta->size = length;
    }

    return rc;
}

/* if length is less than reserved space, give back space down to length */
int scrmfs_fid_shrink(int fid, off_t length)
{
    int rc;

    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* determine file storage type */
    if (meta->storage == FILE_STORAGE_FIXED_CHUNK) {
        /* file stored in fixed-size chunks */
        rc = scrmfs_fid_store_fixed_shrink(fid, meta, length);
    } else if (meta->storage == FILE_STORAGE_CONTAINER) {
        /* file stored in container */
        rc = scrmfs_fid_store_container_shrink(fid, meta, length);
    } else {
        /* unknown storage type */
        rc = SCRMFS_ERR_IO;
    }

    return rc;
}

/* truncate file id to given length, frees resources if length is
 * less than size and allocates and zero-fills new bytes if length
 * is more than size */
int scrmfs_fid_truncate(int fid, off_t length)
{
    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* get current size of file */
    off_t size = meta->size;

    /* drop data if length is less than current size,
     * allocate new space and zero fill it if bigger */
    if (length < size) {
        /* determine the number of chunks to leave after truncating */
        int shrink_rc = scrmfs_fid_shrink(fid, length);
        if (shrink_rc != SCRMFS_SUCCESS) {
            return shrink_rc;
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
int scrmfs_fid_open(const char* path, int flags, mode_t mode, int* outfid, off_t* outpos)
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

            /* initialize the storage for the file */
            int store_rc = scrmfs_fid_store_alloc(fid);
            if (store_rc != SCRMFS_SUCCESS) {
                debug("Failed to create storage for file %s\n", path);
                return SCRMFS_ERR_IO;
            }
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

int scrmfs_fid_close(int fid)
{
    /* TODO: clear any held locks */

    /* nothing to do here, just a place holder */
    return SCRMFS_SUCCESS;
}

/* delete a file id and return file its resources to free pools */
int scrmfs_fid_unlink(int fid)
{
    /* return data to free pools */
    scrmfs_fid_truncate(fid, 0);

    /* finalize the storage we're using for this file */
    scrmfs_fid_store_free(fid);

    /* set this file id as not in use */
    scrmfs_filelist[fid].in_use = 0;

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

    //MAP_OR_FAIL(open);
    spillblock_fd = open(path, O_RDWR | O_CREAT | O_EXCL, perms);
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
    int scr_shmblock_shmid;

    debug("Key for superblock = %x\n", key);

#ifdef ENABLE_NUMA_POLICY
    /* if user requested to use 1 shm/process along with NUMA optimizations */
    if ( key != IPC_PRIVATE ) {
        numa_exit_on_error = 1;
        /* check to see if NUMA control capability is available */
        if (numa_available() >= 0 ) {
            int max_numa_nodes = numa_max_node() + 1;
            debug("Max. number of NUMA nodes = %d\n",max_numa_nodes);
            int num_cores = sysconf(_SC_NPROCESSORS_CONF);
            int my_core, i, pref_numa_bank = -1;

            /* scan through the CPU set to see which core the current process is bound to */
            /* TODO: can alternatively read from the proc filesystem (/proc/self*) */
            cpu_set_t myset;
            CPU_ZERO(&myset);
            sched_getaffinity(0, sizeof(myset), &myset);
            for(  i = 0; i < num_cores; i++ ) {
                if ( CPU_ISSET(i, &myset) ) {
                    my_core = i;
                }
            }
            if( my_core < 0 ) {
                debug("Not able to get current core affinity\n");
                return NULL;
            }
            debug("Process running on core %d\n",my_core);

            /* find out which NUMA bank this core belongs to */
            // numa_preferred doesn't work as needed, returns 0 always. placeholder only.
            pref_numa_bank = numa_preferred();
            debug("Preferred NUMA bank for core %d is %d\n", my_core, pref_numa_bank);

            /* create/attach to respective shmblock*/
            scr_shmblock_shmid = shmget( (key+pref_numa_bank), size, IPC_CREAT | IPC_EXCL | S_IRWXU);
            
        } else {
            debug("NUMA support unavailable!\n");
            return NULL;
        }
    } else {
        /* each process has its own block. use one of the other NUMA policies (scrmfs_numa_policy) instead */
        scr_shmblock_shmid = shmget( key, size, IPC_CREAT | IPC_EXCL | S_IRWXU);
    }
#else
    /* when NUMA optimizations are turned off, just let the kernel allocate pages as it desires */
    /* TODO: Add Huge-Pages support */
    scr_shmblock_shmid = shmget(key, size, IPC_CREAT | IPC_EXCL | S_IRWXU);
#endif

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
        /* set NUMA policy for scr_shmblock */
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
#endif /* HAVE_CONTAINER_LIB */

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

/* ---------------------------------------
 * APIs exposed to external libraries
 * --------------------------------------- */

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
