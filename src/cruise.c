/*
 * Copyright (c) 2014, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by
 *   Raghunath Rajachandrasekar <rajachan@cse.ohio-state.edu>
 *   Kathryn Mohror <kathryn@llnl.gov>
 *   Adam Moody <moody20@llnl.gov>
 * LLNL-CODE-642432.
 * All rights reserved.
 * This file is part of CRUISE.
 * For details, see https://github.com/hpc/cruise
 * Please also read this file COPYRIGHT
*/

#include "cruise-runtime-config.h"
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

#ifdef ENABLE_NUMA_POLICY
#include <numa.h>
#endif

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sched.h>

#include "cruise-internal.h"

#ifdef MACHINE_BGQ
/* BG/Q to get personality and persistent memory */
#include <sys/mman.h>
#include <hwi/include/common/uci.h>
#include <firmware/include/personality.h>
#include <spi/include/kernel/memory.h>
#include "mpi.h"
#include <mpix.h>
#endif /* MACHINE_BGQ */

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

static int cruise_fpos_enabled   = 1;  /* whether we can use fgetpos/fsetpos */

int cruise_use_memfs      = 1;
int cruise_use_spillover;
static int cruise_use_single_shm = 0;
static int cruise_page_size      = 0;

static off_t cruise_max_offt;
static off_t cruise_min_offt;
static off_t cruise_max_long;
static off_t cruise_min_long;

/* TODO: moved these to fixed file */
int    cruise_max_files;  /* maximum number of files to store */
size_t cruise_chunk_mem;  /* number of bytes in memory to be used for chunk storage */
int    cruise_chunk_bits; /* we set chunk size = 2^cruise_chunk_bits */
off_t  cruise_chunk_size; /* chunk size in bytes */
off_t  cruise_chunk_mask; /* mask applied to logical offset to determine physical offset within chunk */
int    cruise_max_chunks; /* maximum number of chunks that fit in memory */

static size_t cruise_spillover_size;  /* number of bytes in spillover to be used for chunk storage */
int  cruise_spillover_max_chunks; /* maximum number of chunks that fit in spillover storage */

#ifdef ENABLE_NUMA_POLICY
static char cruise_numa_policy[10];
static int cruise_numa_bank = -1;
#endif

extern pthread_mutex_t cruise_stack_mutex;

/* keep track of what we've initialized */
int cruise_initialized = 0;

/* global persistent memory block (metadata + data) */
static void* cruise_superblock = NULL;
static void* free_fid_stack = NULL;
void* free_chunk_stack = NULL;
void* free_spillchunk_stack = NULL;
cruise_filename_t* cruise_filelist    = NULL;
static cruise_filemeta_t* cruise_filemetas   = NULL;
static cruise_chunkmeta_t* cruise_chunkmetas = NULL;
char* cruise_chunks = NULL;
char external_data_dir[1024] = {0};
int cruise_spilloverblock = 0;

/* array of file descriptors */
cruise_fd_t cruise_fds[CRUISE_MAX_FILEDESCS];
rlim_t cruise_fd_limit;

/* array of file streams */
cruise_stream_t cruise_streams[CRUISE_MAX_FILEDESCS];

/* mount point information */
char*  cruise_mount_prefix = NULL;
size_t cruise_mount_prefixlen = 0;
static key_t  cruise_mount_shmget_key = 0;

/* mutex to lock stack operations */
pthread_mutex_t cruise_stack_mutex = PTHREAD_MUTEX_INITIALIZER;

/* single function to route all unsupported wrapper calls through */
int cruise_vunsupported(
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

    return CRUISE_SUCCESS;
}

/* single function to route all unsupported wrapper calls through */
int cruise_unsupported(
  const char* fn_name,
  const char* file,
  int line,
  const char* fmt,
  ...)
{
    /* print string with more info about call, e.g., param values */
    va_list args;
    va_start(args, fmt);
    int rc = cruise_vunsupported(fn_name, file, line, fmt, args);
    va_end(args);
    return rc;
}

/* given an CRUISE error code, return corresponding errno code */
int cruise_err_map_to_errno(int rc)
{
    switch(rc) {
    case CRUISE_SUCCESS:     return 0;
    case CRUISE_FAILURE:     return EIO;
    case CRUISE_ERR_NOSPC:   return ENOSPC;
    case CRUISE_ERR_IO:      return EIO;
    case CRUISE_ERR_NAMETOOLONG: return ENAMETOOLONG;
    case CRUISE_ERR_NOENT:   return ENOENT;
    case CRUISE_ERR_EXIST:   return EEXIST;
    case CRUISE_ERR_NOTDIR:  return ENOTDIR;
    case CRUISE_ERR_NFILE:   return ENFILE;
    case CRUISE_ERR_INVAL:   return EINVAL;
    case CRUISE_ERR_OVERFLOW: return EOVERFLOW;
    case CRUISE_ERR_FBIG:    return EFBIG;
    case CRUISE_ERR_BADF:    return EBADF;
    case CRUISE_ERR_ISDIR:   return EISDIR;
    default:                 return EIO;
    }
}

/* returns 1 if two input parameters will overflow their type when
 * added together */
inline int cruise_would_overflow_offt(off_t a, off_t b)
{
    /* if both parameters are positive, they could overflow when
     * added together */
    if (a > 0 && b > 0) {
        /* if the distance between a and max is greater than or equal to
         * b, then we could add a and b and still not exceed max */
        if (cruise_max_offt - a >= b) {
            return 0;
        }
        return 1;
    }

    /* if both parameters are negative, they could underflow when
     * added together */
    if (a < 0 && b < 0) {
        /* if the distance between min and a is less than or equal to
         * b, then we could add a and b and still not exceed min */
        if (cruise_min_offt - a <= b) {
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
inline int cruise_would_overflow_long(long a, long b)
{
    /* if both parameters are positive, they could overflow when
     * added together */
    if (a > 0 && b > 0) {
        /* if the distance between a and max is greater than or equal to
         * b, then we could add a and b and still not exceed max */
        if (cruise_max_long - a >= b) {
            return 0;
        }
        return 1;
    }

    /* if both parameters are negative, they could underflow when
     * added together */
    if (a < 0 && b < 0) {
        /* if the distance between min and a is less than or equal to
         * b, then we could add a and b and still not exceed min */
        if (cruise_min_long - a <= b) {
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
static inline void* cruise_memcpy(void *restrict b, const void *restrict a, size_t n)
{
    char *s1 = b;
    const char *s2 = a;
    for(; 0<n; --n)*s1++ = *s2++;
    return b;
}
#endif

/* given an input mode, mask it with umask and return, can specify
 * an input mode==0 to specify all read/write bits */
mode_t cruise_getmode(mode_t perms)
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

inline int cruise_stack_lock()
{
    if (cruise_use_single_shm) {
        return pthread_mutex_lock(&cruise_stack_mutex);
    }
    return 0;
}

inline int cruise_stack_unlock()
{
    if (cruise_use_single_shm) {
        return pthread_mutex_unlock(&cruise_stack_mutex);
    }
    return 0;
}

/* sets flag if the path is a special path */
inline int cruise_intercept_path(const char* path)
{
    /* don't intecept anything until we're initialized */
    if (! cruise_initialized) {
      return 0;
    }

    /* if the path starts with our mount point, intercept it */
    if (strncmp(path, cruise_mount_prefix, cruise_mount_prefixlen) == 0) {
        return 1;
    }
    return 0;
}

/* given an fd, return 1 if we should intercept this file, 0 otherwise,
 * convert fd to new fd value if needed */
inline int cruise_intercept_fd(int* fd)
{
    int oldfd = *fd;

    /* don't intecept anything until we're initialized */
    if (! cruise_initialized) {
        return 0;
    }

    if (oldfd < cruise_fd_limit) {
        /* this fd is a real system fd, so leave it as is */
        return 0;
    } else if (oldfd < 0) {
        /* this is an invalid fd, so we should not intercept it */
        return 0;
    } else {
        /* this is an fd we generated and returned to the user,
         * so intercept the call and shift the fd */
        int newfd = oldfd - cruise_fd_limit;
        *fd = newfd;
        debug("Changing fd from exposed %d to internal %d\n", oldfd, newfd);
        return 1;
    }
}

/* given an fd, return 1 if we should intercept this file, 0 otherwise,
 * convert fd to new fd value if needed */
inline int cruise_intercept_stream(FILE* stream)
{
    /* don't intecept anything until we're initialized */
    if (! cruise_initialized) {
        return 0;
    }

    /* check whether this pointer lies within range of our
     * file stream array */
    cruise_stream_t* ptr   = (cruise_stream_t*) stream;
    cruise_stream_t* start = &(cruise_streams[0]);
    cruise_stream_t* end   = &(cruise_streams[CRUISE_MAX_FILEDESCS]);
    if (ptr >= start && ptr < end) {
        return 1;
    }

    return 0;
}

/* given a path, return the file id */
inline int cruise_get_fid_from_path(const char* path)
{
    int i = 0;
    while (i < cruise_max_files)
    {
        if(cruise_filelist[i].in_use &&
           strcmp((void *)&cruise_filelist[i].filename, path) == 0)
        {
            debug("File found: cruise_filelist[%d].filename = %s\n",
                  i, (char*)&cruise_filelist[i].filename
            );
            return i;
        }
        i++;
    }

    /* couldn't find specified path */
    return -1;
}

/* given a file descriptor, return the file id */
inline int cruise_get_fid_from_fd(int fd)
{
  /* check that file descriptor is within range */
  if (fd < 0 || fd >= CRUISE_MAX_FILEDESCS) {
    return -1;
  }

  /* right now, the file descriptor is equal to the file id */
  return fd;
}

/* return address of file descriptor structure or NULL if fd is out
 * of range */
inline cruise_fd_t* cruise_get_filedesc_from_fd(int fd)
{
    if (fd >= 0 && fd < CRUISE_MAX_FILEDESCS) {
        cruise_fd_t* filedesc = &(cruise_fds[fd]);
        return filedesc;
    }
    return NULL;
}

/* given a file id, return a pointer to the meta data,
 * otherwise return NULL */
cruise_filemeta_t* cruise_get_meta_from_fid(int fid)
{
    /* check that the file id is within range of our array */
    if (fid >= 0 && fid < cruise_max_files) {
        /* get a pointer to the file meta data structure */
        cruise_filemeta_t* meta = &cruise_filemetas[fid];
        return meta;
    }
    return NULL;
}

/* ---------------------------------------
 * Operations on file storage
 * --------------------------------------- */

/* allocate and initialize data management resource for file */
static int cruise_fid_store_alloc(int fid)
{
    /* get meta data for this file */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);

    if (cruise_use_memfs || cruise_use_spillover) {
        /* we used fixed-size chunk storage for memfs and spillover */
        meta->storage = FILE_STORAGE_FIXED_CHUNK;
    }

    return CRUISE_SUCCESS;
}

/* free data management resource for file */
static int cruise_fid_store_free(int fid)
{
    /* get meta data for this file */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);

    return CRUISE_SUCCESS;
}

/* ---------------------------------------
 * Operations on file ids
 * --------------------------------------- */

/* checks to see if fid is a directory
 * returns 1 for yes
 * returns 0 for no */
int cruise_fid_is_dir(int fid)
{
   cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);
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
int cruise_fid_is_dir_empty(const char * path)
{
    int i = 0;
    while (i < cruise_max_files) {
       if(cruise_filelist[i].in_use) {
           /* if the file starts with the path, it is inside of that directory 
            * also check to make sure that it's not the directory entry itself */
           char * strptr = strstr(path, cruise_filelist[i].filename);
           if (strptr == cruise_filelist[i].filename && strcmp(path,cruise_filelist[i].filename)) {
              debug("File found: cruise_filelist[%d].filename = %s\n",
                    i, (char*)&cruise_filelist[i].filename
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
off_t cruise_fid_size(int fid)
{
    /* get meta data for this file */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);
    return meta->size;
}

/* fill in limited amount of stat information */
int cruise_fid_stat(int fid, struct stat* buf)
{
   cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);
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
   if(cruise_fid_is_dir(fid)) {
      buf->st_mode |= S_IFDIR;
   } else {
      buf->st_mode |= S_IFREG;
   }

   return 0;
}

/* allocate a file id slot for a new file 
 * return the fid or -1 on error */
int cruise_fid_alloc()
{
    cruise_stack_lock();
    int fid = cruise_stack_pop(free_fid_stack);
    cruise_stack_unlock();
    debug("cruise_stack_pop() gave %d\n", fid);
    if (fid < 0) {
        /* need to create a new file, but we can't */
        debug("cruise_stack_pop() failed (%d)\n", fid);
        return -1;
    }
    return fid;
}

/* return the file id back to the free pool */
int cruise_fid_free(int fid)
{
    cruise_stack_lock();
    cruise_stack_push(free_fid_stack, fid);
    cruise_stack_unlock();
    return CRUISE_SUCCESS;
}

/* add a new file and initialize metadata
 * returns the new fid, or negative value on error */
int cruise_fid_create_file(const char * path)
{
    int fid = cruise_fid_alloc();
    if (fid < 0)  {
        /* was there an error? if so, return it */
        errno = ENOSPC;
        return fid;
    }

    /* mark this slot as in use and copy the filename */
    cruise_filelist[fid].in_use = 1;
    /* TODO: check path length to see if it is < 128 bytes
     * and return appropriate error if it is greater
     */
    strcpy((void *)&cruise_filelist[fid].filename, path);
    debug("Filename %s got cruise fd %d\n",cruise_filelist[fid].filename,fid);

    /* initialize meta data */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);
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
int cruise_fid_create_directory(const char * path)
{
   /* set everything we do for a file... */
   int fid = cruise_fid_create_file(path);
   if (fid < 0) {
       /* was there an error? if so, return it */
       errno = ENOSPC;
       return fid;
   }

   /* ...and a little more */
   cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);
   meta->is_dir = 1;
   return fid;
}

/* read count bytes from file starting from pos and store into buf,
 * all bytes are assumed to exist, so checks on file size should be
 * done before calling this routine */
int cruise_fid_read(int fid, off_t pos, void* buf, size_t count)
{
    int rc;

    /* short-circuit a 0-byte read */
    if (count == 0) {
        return CRUISE_SUCCESS;
    }

    /* get meta for this file id */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);

    /* determine storage type to read file data */
    if (meta->storage == FILE_STORAGE_FIXED_CHUNK) {
        /* file stored in fixed-size chunks */
        rc = cruise_fid_store_fixed_read(fid, meta, pos, buf, count);
    } else {
        /* unknown storage type */
        rc = CRUISE_ERR_IO;
    }

    return rc;
}

/* write count bytes from buf into file starting at offset pos,
 * all bytes are assumed to be allocated to file, so file should
 * be extended before calling this routine */
int cruise_fid_write(int fid, off_t pos, const void* buf, size_t count)
{
    int rc;

    /* short-circuit a 0-byte write */
    if (count == 0) {
        return CRUISE_SUCCESS;
    }

    /* get meta for this file id */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);

    /* determine storage type to write file data */
    if (meta->storage == FILE_STORAGE_FIXED_CHUNK) {
        /* file stored in fixed-size chunks */
        rc = cruise_fid_store_fixed_write(fid, meta, pos, buf, count);
    } else {
        /* unknown storage type */
        rc = CRUISE_ERR_IO;
    }

    return rc;
}

/* given a file id, write zero bytes to region of specified offset
 * and length, assumes space is already reserved */
int cruise_fid_write_zero(int fid, off_t pos, off_t count)
{
    int rc = CRUISE_SUCCESS;

    /* allocate an aligned chunk of memory */
    size_t buf_size = 1024 * 1024;
    void* buf = (void*) malloc(buf_size);
    if (buf == NULL) {
        return CRUISE_ERR_IO;
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
        int write_rc = cruise_fid_write(fid, curpos, buf, num);
        if (write_rc != CRUISE_SUCCESS) {
            rc = CRUISE_ERR_IO;
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
int cruise_fid_extend(int fid, off_t length)
{
    int rc;

    /* get meta data for this file */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);

    /* determine file storage type */
    if (meta->storage == FILE_STORAGE_FIXED_CHUNK) {
        /* file stored in fixed-size chunks */
        rc = cruise_fid_store_fixed_extend(fid, meta, length);
    } else {
        /* unknown storage type */
        rc = CRUISE_ERR_IO;
    }

    /* TODO: move this statement elsewhere */
    /* increase file size up to length */
    if (length > meta->size) {
        meta->size = length;
    }

    return rc;
}

/* if length is less than reserved space, give back space down to length */
int cruise_fid_shrink(int fid, off_t length)
{
    int rc;

    /* get meta data for this file */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);

    /* determine file storage type */
    if (meta->storage == FILE_STORAGE_FIXED_CHUNK) {
        /* file stored in fixed-size chunks */
        rc = cruise_fid_store_fixed_shrink(fid, meta, length);
    } else {
        /* unknown storage type */
        rc = CRUISE_ERR_IO;
    }

    return rc;
}

/* truncate file id to given length, frees resources if length is
 * less than size and allocates and zero-fills new bytes if length
 * is more than size */
int cruise_fid_truncate(int fid, off_t length)
{
    /* get meta data for this file */
    cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);

    /* get current size of file */
    off_t size = meta->size;

    /* drop data if length is less than current size,
     * allocate new space and zero fill it if bigger */
    if (length < size) {
        /* determine the number of chunks to leave after truncating */
        int shrink_rc = cruise_fid_shrink(fid, length);
        if (shrink_rc != CRUISE_SUCCESS) {
            return shrink_rc;
        }
    } else if (length > size) {
        /* file size has been extended, allocate space */
        int extend_rc = cruise_fid_extend(fid, length);
        if (extend_rc != CRUISE_SUCCESS) {
            return CRUISE_ERR_NOSPC;
        }

        /* write zero values to new bytes */
        off_t gap_size = length - size;
        int zero_rc = cruise_fid_write_zero(fid, size, gap_size);
        if (zero_rc != CRUISE_SUCCESS) {
            return CRUISE_ERR_IO;
        }
    }

    /* set the new size */
    meta->size = length;

    return CRUISE_SUCCESS;
}

/* opens a new file id with specified path, access flags, and permissions,
 * fills outfid with file id and outpos with position for current file pointer,
 * returns CRUISE error code */
int cruise_fid_open(const char* path, int flags, mode_t mode, int* outfid, off_t* outpos)
{
    /* check that path is short enough */
    size_t pathlen = strlen(path) + 1;
    if (pathlen > CRUISE_MAX_FILENAME) {
        return CRUISE_ERR_NAMETOOLONG;
    }

    /* assume that we'll place the file pointer at the start of the file */
    off_t pos = 0;

    /* check whether this file already exists */
    int fid = cruise_get_fid_from_path(path);
    debug("cruise_get_fid_from_path() gave %d\n", fid);
    if (fid < 0) {
        /* file does not exist */

        /* create file if O_CREAT is set */
        if (flags & O_CREAT) {
            debug("Couldn't find entry for %s in CRUISE\n", path);
            debug("cruise_superblock = %p; free_fid_stack = %p; free_chunk_stack = %p; cruise_filelist = %p; chunks = %p\n",
                  cruise_superblock, free_fid_stack, free_chunk_stack, cruise_filelist, cruise_chunks
            );

            /* allocate a file id slot for this new file */
            fid = cruise_fid_create_file(path);
            if (fid < 0){
               debug("Failed to create new file %s\n", path);
               return CRUISE_ERR_NFILE;
            }

            /* initialize the storage for the file */
            int store_rc = cruise_fid_store_alloc(fid);
            if (store_rc != CRUISE_SUCCESS) {
                debug("Failed to create storage for file %s\n", path);
                return CRUISE_ERR_IO;
            }
        } else {
            /* ERROR: trying to open a file that does not exist without O_CREATE */
            debug("Couldn't find entry for %s in CRUISE\n", path);
            return CRUISE_ERR_NOENT;
        }
    } else {
        /* file already exists */

        /* if O_CREAT and O_EXCL are set, this is an error */
        if ((flags & O_CREAT) && (flags & O_EXCL)) {
            /* ERROR: trying to open a file that exists with O_CREATE and O_EXCL */
            return CRUISE_ERR_EXIST;
        }

        /* if O_DIRECTORY is set and fid is not a directory, error */
        if ((flags & O_DIRECTORY) && !cruise_fid_is_dir(fid)) {
            return CRUISE_ERR_NOTDIR;
        }

        /* if O_DIRECTORY is not set and fid is a directory, error */ 
        if (!(flags & O_DIRECTORY) && cruise_fid_is_dir(fid)) {
            return CRUISE_ERR_NOTDIR;
        }

        /* if O_TRUNC is set with RDWR or WRONLY, need to truncate file */
        if ((flags & O_TRUNC) && (flags & (O_RDWR | O_WRONLY))) {
            cruise_fid_truncate(fid, 0);
        }

        /* if O_APPEND is set, we need to place file pointer at end of file */
        if (flags & O_APPEND) {
            cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);
            pos = meta->size;
        }
    }

    /* TODO: allocate a free file descriptor and associate it with fid */
    /* set in_use flag and file pointer */
    *outfid = fid;
    *outpos = pos;
    debug("CRUISE_open generated fd %d for file %s\n", fid, path);    

    /* don't conflict with active system fds that range from 0 - (fd_limit) */
    return CRUISE_SUCCESS;
}

int cruise_fid_close(int fid)
{
    /* TODO: clear any held locks */

    /* nothing to do here, just a place holder */
    return CRUISE_SUCCESS;
}

/* delete a file id and return file its resources to free pools */
int cruise_fid_unlink(int fid)
{
    /* return data to free pools */
    cruise_fid_truncate(fid, 0);

    /* finalize the storage we're using for this file */
    cruise_fid_store_free(fid);

    /* set this file id as not in use */
    cruise_filelist[fid].in_use = 0;

    /* add this id back to the free stack */
    cruise_fid_free(fid);

    return CRUISE_SUCCESS;
}

/* ---------------------------------------
 * Operations to mount file system
 * --------------------------------------- */

/* initialize our global pointers into the given superblock */
static void* cruise_init_pointers(void* superblock)
{
    char* ptr = (char*) superblock;

    /* jump over header (right now just a uint32_t to record
     * magic value of 0xdeadbeef if initialized */
    ptr += sizeof(uint32_t);

    /* stack to manage free file ids */
    free_fid_stack = ptr;
    ptr += cruise_stack_bytes(cruise_max_files);

    /* record list of file names */
    cruise_filelist = (cruise_filename_t*) ptr;
    ptr += cruise_max_files * sizeof(cruise_filename_t);

    /* array of file meta data structures */
    cruise_filemetas = (cruise_filemeta_t*) ptr;
    ptr += cruise_max_files * sizeof(cruise_filemeta_t);

    /* array of chunk meta data strucutres for each file */
    cruise_chunkmetas = (cruise_chunkmeta_t*) ptr;
    ptr += cruise_max_files * cruise_max_chunks * sizeof(cruise_chunkmeta_t);

    /* Teng: array of chunk meta data for spillover chunks of each file*/
    if (cruise_use_spillover) {
    	ptr += cruise_max_files * cruise_spillover_max_chunks * sizeof(cruise_chunkmeta_t);
    }

    /* stack to manage free memory data chunks */
    free_chunk_stack = ptr;
    ptr += cruise_stack_bytes(cruise_max_chunks);

    if (cruise_use_spillover) {
        /* stack to manage free spill-over data chunks */
        free_spillchunk_stack = ptr;
        ptr += cruise_stack_bytes(cruise_spillover_max_chunks);
    }

    /* Only set this up if we're using memfs */
    if (cruise_use_memfs) {
      /* round ptr up to start of next page */
      unsigned long long ull_ptr  = (unsigned long long) ptr;
      unsigned long long ull_page = (unsigned long long) cruise_page_size;
      unsigned long long num_pages = ull_ptr / ull_page;
      if (ull_ptr > num_pages * ull_page) {
        ptr = (char*)((num_pages + 1) * ull_page);
      }

      /* pointer to start of memory data chunks */
      cruise_chunks = ptr;
      ptr += cruise_max_chunks * cruise_chunk_size;
    } else{
      cruise_chunks = NULL;
    }

    return ptr;
}

/* initialize data structures for first use */
static int cruise_init_structures()
{
    int i;
    for (i = 0; i < cruise_max_files; i++) {
        /* indicate that file id is not in use by setting flag to 0 */
        cruise_filelist[i].in_use = 0;

        /* set pointer to array of chunkmeta data structures */
        cruise_filemeta_t* filemeta = &cruise_filemetas[i];
        cruise_chunkmeta_t* chunkmetas = &(cruise_chunkmetas[cruise_max_chunks * i]);
        filemeta->chunk_meta = chunkmetas;

        /* Teng: add support for spillover*/
        if (!cruise_use_spillover)
        	chunkmetas = &(cruise_chunkmetas[cruise_max_chunks * i]);
        else
        	chunkmetas = &(cruise_chunkmetas[ (cruise_max_chunks + \
        			cruise_spillover_max_chunks)*i]);
    }

    cruise_stack_init(free_fid_stack, cruise_max_files);

    cruise_stack_init(free_chunk_stack, cruise_max_chunks);

    if (cruise_use_spillover) {
        cruise_stack_init(free_spillchunk_stack, cruise_spillover_max_chunks);
    }

    debug("Meta-stacks initialized!\n");

    return CRUISE_SUCCESS;
}

static int cruise_get_spillblock(size_t size, const char *path)
{
    void *scr_spillblock = NULL;
    int spillblock_fd;

    mode_t perms = cruise_getmode(0);

    //MAP_OR_FAIL(open);
    spillblock_fd = open(path, O_RDWR | O_CREAT | O_EXCL, perms);
    if (spillblock_fd < 0) {
        if (errno == EEXIST) {
            /* spillover block exists; attach and return */
            spillblock_fd = __real_open(path, O_RDWR);
        } else {
            perror("open() in cruise_get_spillblock() failed");
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
static void* cruise_superblock_shmget(size_t size, key_t key)
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
        /* each process has its own block. use one of the other NUMA policies (cruise_numa_policy) instead */
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
            cruise_init_pointers(scr_shmblock);
        } else {
            perror("shmget() failed");
            return NULL;
        }
    } else {
        /* brand new superblock created, attach to it */
        scr_shmblock = shmat(scr_shmblock_shmid, NULL, 0);
        if(scr_shmblock == (void *) -1) {
            perror("shmat() failed");
        }
        debug("Superblock created at %p!\n",scr_shmblock);


#ifdef ENABLE_NUMA_POLICY
        /* set NUMA policy for scr_shmblock */
        if ( cruise_numa_bank >= 0 ) {
            /* specifically allocate pages from user-set bank */
            numa_tonode_memory(scr_shmblock, size, cruise_numa_bank);
        } else if ( strcmp(cruise_numa_policy,"interleaved") == 0) {
            /* interleave the shared-memory segment
             * across all memory banks when all process share 1-superblock */
            debug("Interleaving superblock across all memory banks\n");
            numa_interleave_memory(scr_shmblock, size, numa_all_nodes_ptr);
        } else if( strcmp(cruise_numa_policy,"local") == 0) {
            /* each process has its own superblock, let it be allocated from
             * the closest memory bank */
            debug("Assigning memory from closest bank\n");
            numa_setlocal_memory(scr_shmblock, size);
        }
#endif
        /* init our global variables to point to spots in superblock */
        cruise_init_pointers(scr_shmblock);

        /* initialize data structures within block */
        cruise_init_structures();
    }
    
    return scr_shmblock;
}

#ifdef MACHINE_BGQ
static void* cruise_superblock_bgq(size_t size, const char* name)
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
    cruise_init_pointers(shmptr);

    /* initialize data structures within block if we haven't already */
    uint32_t* header = (uint32_t*) shmptr;
    uint32_t magic = *header;
    if (magic != 0xdeadbeef) {
      cruise_init_structures();
      *header = 0xdeadbeef;
    }

    return shmptr;
}
#endif /* MACHINE_BGQ */

/* converts string like 10mb to unsigned long long integer value of 10*1024*1024 */
static int cruise_abtoull(char* str, unsigned long long* val)
{
    /* check that we have a string */
    if (str == NULL) {
        debug("scr_abtoull: Can't convert NULL string to bytes @ %s:%d",
            __FILE__, __LINE__
        );
        return CRUISE_FAILURE;
    }

    /* check that we have a value to write to */
    if (val == NULL) {
        debug("scr_abtoull: NULL address to store value @ %s:%d",
            __FILE__, __LINE__
        );
        return CRUISE_FAILURE;
    }

    /* pull the floating point portion of our byte string off */
    errno = 0;
    char* next = NULL;
    double num = strtod(str, &next);
    if (errno != 0) {
        debug("scr_abtoull: Invalid double: %s @ %s:%d",
            str, __FILE__, __LINE__
        );
        return CRUISE_FAILURE;
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
            return CRUISE_FAILURE;
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
            return CRUISE_FAILURE;
        }
    }

    /* check that we got a positive value */
    if (num < 0) {
        debug("scr_abtoull: Byte string must be positive: %s @ %s:%d",
            str, __FILE__, __LINE__
        );
        return CRUISE_FAILURE;
    }

    /* multiply by our units and set out return value */
    *val = (unsigned long long) (num * (double) units);

    return CRUISE_SUCCESS;
}

static int cruise_init(int rank)
{
    if (! cruise_initialized) {
        char* env;
        unsigned long long bytes;

        /* as a hack to support fgetpos/fsetpos, we store the value of
         * a void* in an fpos_t so check that there's room and at least
         * print a message if this won't work */
        if (sizeof(fpos_t) < sizeof(void*)) {
            fprintf(stderr, "ERROR: fgetpos/fsetpos will not work correctly.\n");
            cruise_fpos_enabled = 0;
        }

        /* look up page size for buffer alignment */
        cruise_page_size = getpagesize();

        /* compute min and max off_t values */
        unsigned long long bits;
        bits = sizeof(off_t) * 8;
        cruise_max_offt = (off_t) ( (1ULL << (bits-1ULL)) - 1ULL);
        cruise_min_offt = (off_t) (-(1ULL << (bits-1ULL))       );

        /* compute min and max long values */
        cruise_max_long = LONG_MAX;
        cruise_min_long = LONG_MIN;

        /* will we use spillover to store the files? */
        cruise_use_spillover = 0;

        env = getenv("CRUISE_USE_SPILLOVER");
        if (env) {
            int val = atoi(env);
            if (val != 0) {
                cruise_use_spillover = 1;
            }
        }

        debug("are we using spillover? %d\n", cruise_use_spillover);

        /* determine max number of files to store in file system */
        cruise_max_files = CRUISE_MAX_FILES;
        env = getenv("CRUISE_MAX_FILES");
        if (env) {
            int val = atoi(env);
            cruise_max_files = val;
        }

        /* determine number of bits for chunk size */
        cruise_chunk_bits = CRUISE_CHUNK_BITS;
        env = getenv("CRUISE_CHUNK_BITS");
        if (env) {
            int val = atoi(env);
            cruise_chunk_bits = val;
        }

        /* determine maximum number of bytes of memory for chunk storage */
        cruise_chunk_mem = CRUISE_CHUNK_MEM;
        env = getenv("CRUISE_CHUNK_MEM");
        if (env) {
            cruise_abtoull(env, &bytes);
            cruise_chunk_mem = (size_t) bytes;
        }

        /* set chunk size, set chunk offset mask, and set total number
         * of chunks */
        cruise_chunk_size = 1 << cruise_chunk_bits;
        cruise_chunk_mask = cruise_chunk_size - 1;
        cruise_max_chunks = cruise_chunk_mem >> cruise_chunk_bits;

        /* determine maximum number of bytes of spillover for chunk storage */
        cruise_spillover_size = CRUISE_SPILLOVER_SIZE;
        env = getenv("CRUISE_SPILLOVER_SIZE");
        if (env) {
            cruise_abtoull(env, &bytes);
            cruise_spillover_size = (size_t) bytes;
        }

        /* set number of chunks in spillover device */
        cruise_spillover_max_chunks = cruise_spillover_size >> cruise_chunk_bits;

#ifdef ENABLE_NUMA_POLICY
        env = getenv("CRUISE_NUMA_POLICY");
        if ( env ) {
            sprintf(cruise_numa_policy, env);
            debug("NUMA policy used: %s\n", cruise_numa_policy);
        } else {
            sprintf(cruise_numa_policy, "default");
        }

        env = getenv("CRUISE_USE_NUMA_BANK");
        if (env) {
            int val = atoi(env);
            if (val >= 0) {
                cruise_numa_bank = val;
            } else {
                fprintf(stderr,"Incorrect NUMA bank specified in CRUISE_USE_NUMA_BANK."
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
            return CRUISE_FAILURE;
        }
        if (getrlimit(RLIMIT_NOFILE, r_limit) < 0) {
            perror("rlimit failed");
            free(r_limit);
            return CRUISE_FAILURE;
        }
        cruise_fd_limit = r_limit->rlim_cur;
        free(r_limit);
        debug("FD limit for system = %ld\n", cruise_fd_limit);

        /* determine the size of the superblock */
        /* generous allocation for chunk map (one file can take entire space)*/
        size_t superblock_size = 0;
        superblock_size += sizeof(uint32_t); /* header: single uint32_t to hold 0xdeadbeef number of initialization */
        superblock_size += cruise_stack_bytes(cruise_max_files);         /* free file id stack */
        superblock_size += cruise_max_files * sizeof(cruise_filename_t); /* file name struct array */
        superblock_size += cruise_max_files * sizeof(cruise_filemeta_t); /* file meta data struct array */
        superblock_size += cruise_max_files * cruise_max_chunks * sizeof(cruise_chunkmeta_t);
                                                                         /* chunk meta data struct array for each file */
		if (cruise_use_spillover) {
			superblock_size += cruise_max_files * cruise_spillover_max_chunks * sizeof(cruise_chunkmeta_t);
																		/*  Teng: spillover chunk meta data struct array for each file */
		}
        superblock_size += cruise_stack_bytes(cruise_max_chunks);        /* free chunk stack */
        if (cruise_use_memfs) {
           superblock_size += cruise_page_size + 
               (cruise_max_chunks * cruise_chunk_size);         /* memory chunks */
        }
        if (cruise_use_spillover) {
           superblock_size +=
               cruise_stack_bytes(cruise_spillover_max_chunks);     /* free spill over chunk stack */
        }

        /* get a superblock of persistent memory and initialize our
         * global variables for this block */
      #ifdef MACHINE_BGQ
        char bgqname[100];
        snprintf(bgqname, sizeof(bgqname), "memory_rank_%d", rank);
        cruise_superblock = cruise_superblock_bgq(superblock_size, bgqname);
      #else /* MACHINE_BGQ */
        cruise_superblock = cruise_superblock_shmget(superblock_size, cruise_mount_shmget_key);
      #endif /* MACHINE_BGQ */
        if (cruise_superblock == NULL) {
            debug("cruise_superblock_shmget() failed\n");
            return CRUISE_FAILURE;
        }

        /* Teng: make the spillover directory adjustable by users*/
        env = getenv("CRUISE_EXTERNAL_DATA_DIR");
        if (env) {
					strcpy(external_data_dir, env);
        }
				else
					strcpy(external_data_dir, EXTERNAL_DATA_DIR);

        char spillfile_prefix[100];
		sprintf(spillfile_prefix,"%s/spill_file_%d", external_data_dir, rank);

        /* initialize spillover store */
        if (cruise_use_spillover) {
            size_t spillover_size = cruise_max_chunks * cruise_chunk_size;
            cruise_spilloverblock = cruise_get_spillblock(spillover_size, spillfile_prefix);

            if(cruise_spilloverblock < 0) {
                debug("cruise_get_spillblock() failed!\n");
                return CRUISE_FAILURE;
            }
        }
      
        /* remember that we've now initialized the library */
        cruise_initialized = 1;
    }
    return CRUISE_SUCCESS;
}

/* ---------------------------------------
 * APIs exposed to external libraries
 * --------------------------------------- */

/* mount memfs at some prefix location */
int cruise_mount(const char prefix[], size_t size, int rank)
{
    cruise_mount_prefix = strdup(prefix);
    cruise_mount_prefixlen = strlen(cruise_mount_prefix);

    /* KMM commented out because we're just using a single rank, so use PRIVATE
     * downside, can't attach to this in another srun (PRIVATE, that is) */
    //cruise_mount_shmget_key = CRUISE_SUPERBLOCK_KEY + rank;

    char * env = getenv("CRUISE_USE_SINGLE_SHM");
    if (env) {
        int val = atoi(env);
        if (val != 0) {
            cruise_use_single_shm = 1;
        }
    }

    if (cruise_use_single_shm) {
        cruise_mount_shmget_key = CRUISE_SUPERBLOCK_KEY + rank;
    } else {
        cruise_mount_shmget_key = IPC_PRIVATE;
    }

    /* initialize our library */
    cruise_init(rank);

    /* add mount point as a new directory in the file list */
    if (cruise_get_fid_from_path(prefix) >= 0) {
        /* we can't mount this location, because it already exists */
        errno = EEXIST;
        return -1;
    } else {
        /* claim an entry in our file list */
        int fid = cruise_fid_create_directory(prefix);
        if (fid < 0) {
            /* if there was an error, return it */
            return fid;
        }
    }

    return 0;
}

/* get information about the chunk data region
 * for external async libraries to register during their init */
size_t cruise_get_data_region(void **ptr)
{
    *ptr = cruise_chunks;
    return cruise_chunk_mem;
}

/* get a list of chunks for a given file (useful for RDMA, etc.) */
chunk_list_t* cruise_get_chunk_list(char* path)
{
#if 0
    if (cruise_intercept_path(path)) {
        int i = 0;
        chunk_list_t *chunk_list = NULL;
        chunk_list_t *chunk_list_elem;
    
        /* get the file id for this file descriptor */
        /* Rag: We decided to use the path instead.. Can add flexibility to support both */
        //int fid = cruise_get_fid_from_fd(fd);
        int fid = cruise_get_fid_from_path(path);
        if ( fid < 0 ) {
            errno = EACCES;
            return NULL;
        }

        /* get meta data for this file */
        cruise_filemeta_t* meta = cruise_get_meta_from_fid(fid);
        if (meta) {
        
            while ( i < meta->chunks ) {
                chunk_list_elem = (chunk_list_t*)malloc(sizeof(chunk_list_t));

                /* get the chunk id for the i-th chunk and
                 * add it to the chunk_list */
                cruise_chunkmeta_t* chunk_meta = &(meta->chunk_meta[i]);
                chunk_list_elem->chunk_id = chunk_meta->id;
                chunk_list_elem->location = chunk_meta->location;

                if ( chunk_meta->location == CHUNK_LOCATION_MEMFS ) {
                    /* update the list_elem with the memory address of this chunk */
                    chunk_list_elem->chunk_offset = cruise_compute_chunk_buf( meta, chunk_meta->id, 0);
                    chunk_list_elem->spillover_offset = 0;
                } else if ( chunk_meta->location == CHUNK_LOCATION_SPILLOVER ) {
                    /* update the list_elem with the offset of this chunk in the spillover file*/
                    chunk_list_elem->spillover_offset = cruise_compute_spill_offset( meta, chunk_meta->id, 0);
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
        /* file not managed by CRUISE */
        errno = EACCES;
        return NULL;
    }
#endif
    return NULL;
}

/* debug function to print list of chunks constituting a file
 * and to test above function*/
void cruise_print_chunk_list(char* path)
{
#if 0
    chunk_list_t *chunk_list;
    chunk_list_t *chunk_element;

    chunk_list = cruise_get_chunk_list(path);

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
