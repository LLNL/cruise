/*
 *  (C) 2009 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

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

#include <sys/ipc.h>
#include <sys/shm.h>

#include "scrmfs.h"
#include "scrmfs-file.h"

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

extern char* __progname_full;

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
  ret (*__real_ ## name)args = NULL;

#define SCRMFS_DECL(__name) __name

#define SCRMFS_MPI_CALL(func) __real_ ## func

#define MAP_OR_FAIL(func) \
    if (!(__real_ ## func)) \
    { \
        __real_ ## func = dlsym(RTLD_NEXT, #func); \
        if(!(__real_ ## func)) { \
           fprintf(stderr, "SCRMFS failed to map symbol: %s\n", #func); \
           exit(1); \
       } \
    }


extern double (*__real_PMPI_Wtime)(void);

#else

#define SCRMFS_FORWARD_DECL(name,ret,args) \
  extern ret __real_ ## name args;

#define SCRMFS_DECL(__name) __wrap_ ## __name

#define MAP_OR_FAIL(func)

#define SCRMFS_MPI_CALL(func) func

#endif

SCRMFS_FORWARD_DECL(creat, int, (const char* path, mode_t mode));
SCRMFS_FORWARD_DECL(creat64, int, (const char* path, mode_t mode));
SCRMFS_FORWARD_DECL(open, int, (const char *path, int flags, ...));
SCRMFS_FORWARD_DECL(open64, int, (const char *path, int flags, ...));
SCRMFS_FORWARD_DECL(close, int, (int fd));
SCRMFS_FORWARD_DECL(write, ssize_t, (int fd, const void *buf, size_t count));
SCRMFS_FORWARD_DECL(read, ssize_t, (int fd, void *buf, size_t count));
SCRMFS_FORWARD_DECL(lseek, off_t, (int fd, off_t offset, int whence));
SCRMFS_FORWARD_DECL(lseek64, off64_t, (int fd, off64_t offset, int whence));
SCRMFS_FORWARD_DECL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
SCRMFS_FORWARD_DECL(pread64, ssize_t, (int fd, void *buf, size_t count, off64_t offset));
SCRMFS_FORWARD_DECL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
SCRMFS_FORWARD_DECL(pwrite64, ssize_t, (int fd, const void *buf, size_t count, off64_t offset
));
SCRMFS_FORWARD_DECL(readv, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
SCRMFS_FORWARD_DECL(writev, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
SCRMFS_FORWARD_DECL(__fxstat, int, (int vers, int fd, struct stat *buf));
SCRMFS_FORWARD_DECL(__fxstat64, int, (int vers, int fd, struct stat64 *buf));
SCRMFS_FORWARD_DECL(__lxstat, int, (int vers, const char* path, struct stat *buf));
SCRMFS_FORWARD_DECL(__lxstat64, int, (int vers, const char* path, struct stat64 *buf));
SCRMFS_FORWARD_DECL(__xstat, int, (int vers, const char* path, struct stat *buf));
SCRMFS_FORWARD_DECL(__xstat64, int, (int vers, const char* path, struct stat64 *buf));
SCRMFS_FORWARD_DECL(mmap, void*, (void *addr, size_t length, int prot, int flags, int fd, off_t offset));
SCRMFS_FORWARD_DECL(mmap64, void*, (void *addr, size_t length, int prot, int flags, int fd, off64_t offset));
SCRMFS_FORWARD_DECL(fopen, FILE*, (const char *path, const char *mode));
SCRMFS_FORWARD_DECL(fopen64, FILE*, (const char *path, const char *mode));
SCRMFS_FORWARD_DECL(fclose, int, (FILE *fp));
SCRMFS_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nmemb, FILE *stream));
SCRMFS_FORWARD_DECL(fwrite, size_t, (const void *ptr, size_t size, size_t nmemb, FILE *stream));
SCRMFS_FORWARD_DECL(fseek, int, (FILE *stream, long offset, int whence));
SCRMFS_FORWARD_DECL(fsync, int, (int fd));
SCRMFS_FORWARD_DECL(fdatasync, int, (int fd));
SCRMFS_FORWARD_DECL(unlink, int, (const char *path));
SCRMFS_FORWARD_DECL(rename, int, (const char *oldpath, const char *newpath));
SCRMFS_FORWARD_DECL(truncate, int, (const char *path, off_t length));
//SCRMFS_FORWARD_DECL(stat, int,( const char *path, struct stat *buf));

pthread_mutex_t cp_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP;
struct scrmfs_job_runtime* scrmfs_global_job = NULL;
static int my_rank = -1;
static struct stat64 cp_stat_buf;
static int scrmfs_mem_alignment = 1;

static int scrmfs_initialized = 0;
static int scrmfs_stack_init_done = 0;

/* these are paths that we will not trace */
static char* exclusions[] = {
"/etc/",
"/dev/",
"/usr/",
"/bin/",
"/boot/",
"/lib/",
"/opt/",
"/sbin/",
"/sys/",
"/proc/",
NULL
};

static double posix_wtime(void);

static void cp_access_counter(struct scrmfs_file_runtime* file, ssize_t size,     enum cp_counter_type type);

static struct scrmfs_file_ref* ref_by_handle(
    const void* handle,
    int handle_sz,
    enum scrmfs_handle_type handle_type);

static struct scrmfs_file_runtime* scrmfs_file_by_fd(int fd);
static void scrmfs_file_close_fd(int fd);
static struct scrmfs_file_runtime* scrmfs_file_by_name_setfd(const char* name, int fd);

#define CP_RECORD_WRITE(__ret, __fd, __count, __pwrite_flag, __pwrite_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    size_t stride; \
    int64_t this_offset; \
    int64_t file_alignment; \
    struct scrmfs_file_runtime* file; \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = scrmfs_file_by_fd(__fd); \
    if(!file) break; \
    if(__pwrite_flag) \
        this_offset = __pwrite_offset; \
    else \
        this_offset = file->offset; \
    file_alignment = CP_VALUE(file, CP_FILE_ALIGNMENT); \
    if(this_offset > file->last_byte_written) \
        CP_INC(file, CP_SEQ_WRITES, 1); \
    if(this_offset == (file->last_byte_written + 1)) \
        CP_INC(file, CP_CONSEC_WRITES, 1); \
    if(this_offset > 0 && this_offset > file->last_byte_written \
        && file->last_byte_written != 0) \
        stride = this_offset - file->last_byte_written - 1; \
    else \
        stride = 0; \
    file->last_byte_written = this_offset + __ret - 1; \
    file->offset = this_offset + __ret; \
    CP_MAX(file, CP_MAX_BYTE_WRITTEN, (this_offset + __ret -1)); \
    CP_INC(file, CP_BYTES_WRITTEN, __ret); \
    if(__stream_flag) \
        CP_INC(file, CP_POSIX_FWRITES, 1); \
    else \
        CP_INC(file, CP_POSIX_WRITES, 1); \
    CP_BUCKET_INC(file, CP_SIZE_WRITE_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment && (this_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); \
    if(file->last_io_type == CP_READ) \
        CP_INC(file, CP_RW_SWITCHES, 1); \
    file->last_io_type = CP_WRITE; \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_write_end, CP_F_POSIX_WRITE_TIME); \
    if(CP_F_VALUE(file, CP_F_WRITE_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_WRITE_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_WRITE_END_TIMESTAMP, __tm2); \
    if(CP_F_VALUE(file, CP_F_MAX_WRITE_TIME) < __elapsed){ \
        CP_F_SET(file, CP_F_MAX_WRITE_TIME, __elapsed); \
        CP_SET(file, CP_MAX_WRITE_TIME_SIZE, __ret); } \
} while(0)

#define CP_RECORD_READ(__ret, __fd, __count, __pread_flag, __pread_offset, __aligned, __stream_flag, __tm1, __tm2) do{ \
    size_t stride; \
    int64_t this_offset; \
    struct scrmfs_file_runtime* file; \
    int64_t file_alignment; \
    double __elapsed = __tm2-__tm1; \
    if(__ret < 0) break; \
    file = scrmfs_file_by_fd(__fd); \
    if(!file) break; \
    if(__pread_flag)\
        this_offset = __pread_offset; \
    else \
        this_offset = file->offset; \
    file_alignment = CP_VALUE(file, CP_FILE_ALIGNMENT); \
    if(this_offset > file->last_byte_read) \
        CP_INC(file, CP_SEQ_READS, 1); \
    if(this_offset == (file->last_byte_read + 1)) \
        CP_INC(file, CP_CONSEC_READS, 1); \
    if(this_offset > 0 && this_offset > file->last_byte_read \
        && file->last_byte_read != 0) \
        stride = this_offset - file->last_byte_read - 1; \
    else \
        stride = 0; \
    file->last_byte_read = this_offset + __ret - 1; \
    CP_MAX(file, CP_MAX_BYTE_READ, (this_offset + __ret -1)); \
    file->offset = this_offset + __ret; \
    CP_INC(file, CP_BYTES_READ, __ret); \
    if(__stream_flag)\
        CP_INC(file, CP_POSIX_FREADS, 1); \
    else\
        CP_INC(file, CP_POSIX_READS, 1); \
    CP_BUCKET_INC(file, CP_SIZE_READ_0_100, __ret); \
    cp_access_counter(file, stride, CP_COUNTER_STRIDE); \
    if(!__aligned) \
        CP_INC(file, CP_MEM_NOT_ALIGNED, 1); \
    if(file_alignment && (this_offset % file_alignment) != 0) \
        CP_INC(file, CP_FILE_NOT_ALIGNED, 1); \
    cp_access_counter(file, __ret, CP_COUNTER_ACCESS); \
    if(file->last_io_type == CP_WRITE) \
        CP_INC(file, CP_RW_SWITCHES, 1); \
    file->last_io_type = CP_READ; \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_read_end, CP_F_POSIX_READ_TIME); \
    if(CP_F_VALUE(file, CP_F_READ_START_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_READ_START_TIMESTAMP, __tm1); \
    CP_F_SET(file, CP_F_READ_END_TIMESTAMP, __tm2); \
    if(CP_F_VALUE(file, CP_F_MAX_READ_TIME) < __elapsed){ \
        CP_F_SET(file, CP_F_MAX_READ_TIME, __elapsed); \
        CP_SET(file, CP_MAX_READ_TIME_SIZE, __ret); } \
} while(0)

#define CP_LOOKUP_RECORD_STAT(__path, __statbuf, __tm1, __tm2) do { \
    char* exclude; \
    int tmp_index = 0; \
    struct scrmfs_file_runtime* file; \
    while((exclude = exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = scrmfs_file_by_name(__path); \
    if (file) \
    { \
        CP_RECORD_STAT(file, __statbuf, __tm1, __tm2); \
    } \
} while(0)

    
#define CP_RECORD_STAT(__file, __statbuf, __tm1, __tm2) do { \
    if(!CP_VALUE((__file), CP_FILE_ALIGNMENT)){ \
        CP_SET((__file), CP_DEVICE, (__statbuf)->st_dev); \
        CP_SET((__file), CP_FILE_ALIGNMENT, (__statbuf)->st_blksize); \
        CP_SET((__file), CP_SIZE_AT_OPEN, (__statbuf)->st_size); \
    }\
    (__file)->log_file->rank = my_rank; \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME); \
    CP_INC(__file, CP_POSIX_STATS, 1); \
} while(0)

static inline dev_t get_device(const char* path, struct stat64* statbuf)
{
    dev_t device = 0;

#ifdef __CP_ST_DEV_WORKAROUND
    struct stat64 dirstat;
    char* tmp_path = strdup(path);
    char* parent = dirname(tmp_path);
    if(parent && stat64(parent, &dirstat) == 0)
        device = dirstat.st_dev;
    if(tmp_path) free(tmp_path);
#else
    device = statbuf->st_dev;
#endif

    return(device);
}

#define CP_RECORD_OPEN(__ret, __path, __mode, __stream_flag, __tm1, __tm2) do { \
    struct scrmfs_file_runtime* file; \
    char* exclude; \
    int tmp_index = 0; \
    if(__ret < 0) break; \
    while((exclude = exclusions[tmp_index])) { \
        if(!(strncmp(exclude, __path, strlen(exclude)))) \
            break; \
        tmp_index++; \
    } \
    if(exclude) break; \
    file = scrmfs_file_by_name_setfd(__path, __ret); \
    if(!file) break; \
    if(!CP_VALUE(file, CP_FILE_ALIGNMENT)){ \
        if(stat64(__path, &cp_stat_buf) == 0) { \
            CP_SET(file, CP_DEVICE, get_device(__path, &cp_stat_buf)); \
            CP_SET(file, CP_FILE_ALIGNMENT, cp_stat_buf.st_blksize); \
            CP_SET(file, CP_SIZE_AT_OPEN, cp_stat_buf.st_size); \
        }\
    }\
    file->log_file->rank = my_rank; \
    if(__mode) \
        CP_SET(file, CP_MODE, __mode); \
    file->offset = 0; \
    file->last_byte_written = 0; \
    file->last_byte_read = 0; \
    if(__stream_flag)\
        CP_INC(file, CP_POSIX_FOPENS, 1); \
    else \
        CP_INC(file, CP_POSIX_OPENS, 1); \
    if(CP_F_VALUE(file, CP_F_OPEN_TIMESTAMP) == 0) \
        CP_F_SET(file, CP_F_OPEN_TIMESTAMP, posix_wtime()); \
    CP_F_INC_NO_OVERLAP(file, __tm1, __tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME); \
} while (0)

/* global persistent memory block (metadata + data) */
static void* scrmfs_superblock = NULL;
static void* free_fid_stack = NULL;
static void* free_chunk_stack = NULL;
static scrmfs_filename_t* scrmfs_filelist = NULL;
static scrmfs_filemeta_t* scrmfs_filemetas = NULL;
static chunk_t *chunk = NULL;
static active_fds_t scrmfs_active_fds[SCRMFS_MAX_FILEDESCS];
static rlim_t scrmfs_fd_limit;
static char*  scrmfs_mount_prefix = NULL;
static size_t scrmfs_mount_prefixlen = 0;
static key_t  scrmfs_mount_key = 0;

/* mount memfs at some prefix location */
int scrmfs_mount(const char prefix[], int rank)
{
    scrmfs_mount_prefix = strdup(prefix);
    scrmfs_mount_prefixlen = strlen(scrmfs_mount_prefix);
    //scrmfs_mount_key = SCRMFS_SUPERBLOCK_KEY + rank;
    scrmfs_mount_key = IPC_PRIVATE;
    return 0;
}

/* initialize our global pointers into the given superblock */
static void* scrmfs_init_globals(void* superblock)
{
    void* ptr = NULL;

    ptr = superblock;

    /* SCR stack to manage metadata structures */
    free_fid_stack = ptr;
    ptr += scrmfs_stack_bytes(SCRMFS_MAX_FILES);

    /* record list of file names in persistent mem */
    scrmfs_filelist = (scrmfs_filename_t*) ptr;
    ptr += SCRMFS_MAX_FILES * sizeof(scrmfs_filename_t);

    /* chunk offset map */
    scrmfs_filemetas = (scrmfs_filemeta_t*) ptr;
    ptr += SCRMFS_MAX_FILES * sizeof(scrmfs_filemeta_t);

    /* SCR stack to manage actual data chunks */
    free_chunk_stack = ptr;
    ptr += scrmfs_stack_bytes(SCRMFS_MAX_CHUNKS);

    /* chunk repository */
    chunk = (chunk_t*) ptr;
    ptr += SCRMFS_MAX_CHUNKS * sizeof(chunk_t);

    return ptr;
}

/* grab a superblock shm segment from SCR_Init */
/* currently called from each open call */
static void* scrmfs_get_shmblock(size_t size, key_t key)
{
    void *scr_shmblock = NULL;

    /* TODO:improve error-propogation */
    //int scr_shmblock_shmid = shmget(key, size, IPC_CREAT | IPC_EXCL | S_IRWXU | SHM_HUGETLB);
    int scr_shmblock_shmid = shmget(key, size, IPC_CREAT | IPC_EXCL | S_IRWXU);
    if ( scr_shmblock_shmid < 0 ) {
        if ( errno == EEXIST ) {
            /* Superblock exists. Donot init, just get and attach */
            scr_shmblock_shmid = shmget(key, size, 0);
            scr_shmblock = shmat(scr_shmblock_shmid, NULL, 0);
            if(scr_shmblock < 0) {
                perror("shmat() failed");
                return NULL;
            }
            debug("Superblock exists at %p\n!",scr_shmblock);

            /* init our global variables to point to spots in superblock */
            scrmfs_init_globals(scr_shmblock);
        } else {
            perror("shmget() failed");
            return NULL;
        }
    } else {
        /* valid segment created, attach to proc and init structures */
        scr_shmblock = shmat(scr_shmblock_shmid, NULL, 0);
        if(scr_shmblock < 0) {
            perror("shmat() failed");
        }
        debug("Superblock created at %p\n!",scr_shmblock);

        /* init our global variables to point to spots in superblock */
        scrmfs_init_globals(scr_shmblock);

        /* initialize block allocators within block */
        if (! scrmfs_stack_init_done) {
            scrmfs_stack_init(free_fid_stack, SCRMFS_MAX_FILES);
            scrmfs_stack_init(free_chunk_stack, SCRMFS_MAX_CHUNKS);
            scrmfs_stack_init_done = 1;
            debug("Meta-stacks initialized!\n");
        }
    }
    
    return scr_shmblock;
}

static int scrmfs_init()
{
    /* TODO: expose API as a init call that can be called from SCR_Init */ 

    if (! scrmfs_initialized) {
        /* record the max fd for the system */
        /* RLIMIT_NOFILE specifies a value one greater than the maximum
         * file descriptor number that can be opened by this process */
        struct rlimit *r_limit = malloc(sizeof(r_limit));
        if (r_limit == NULL) {
            perror("failed to allocate memory for call to getrlimit");
            return 1;
        }
        if (getrlimit(RLIMIT_NOFILE, r_limit) < 0) {
            perror("rlimit failed:");
            return 1;
        }
        scrmfs_fd_limit = r_limit->rlim_cur;
        free(r_limit);
        debug("FD limit for system = %ld\n",scrmfs_fd_limit);

        scrmfs_mount("/tmp", 0);
        
        /* determine the size of the superblock */
        /* generous allocation for chunk map (one file can take entire space)*/
        size_t superblock_size = scrmfs_stack_bytes(SCRMFS_MAX_FILES) +
                                 (SCRMFS_MAX_FILES * sizeof(scrmfs_filename_t)) +
                                 (SCRMFS_MAX_FILES * sizeof(scrmfs_filemeta_t)) +
                                 scrmfs_stack_bytes(SCRMFS_MAX_CHUNKS) +
                                 (SCRMFS_MAX_CHUNKS * sizeof(chunk_t));

        /* get a superblock of persistent memory and initialize our
         * global variables for this block */
        scrmfs_superblock = scrmfs_get_shmblock (superblock_size, scrmfs_mount_key);
        if(scrmfs_superblock == NULL)
        {
            debug("scrmfs_get_shmblock() failed\n");
            return 1;
        }

        /* remember that we've now initialized the library */
        scrmfs_initialized = 1;
    }

    return 0;
}

/* sets flag if the path is a special path */
static inline int scrmfs_intercept_path(const char* path)
{
    /* initialize our globals if we haven't already */
    if (! scrmfs_initialized) {
      scrmfs_init();
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
      scrmfs_init();
    }

    if (oldfd < scrmfs_fd_limit) {
        /* this fd is a real system fd, so leave it as is */
        *intercept = 0;
    } else {
        /* this is an fd we generated and returned to the user,
         * so intercept the call and shift the fd */
        int newfd = oldfd - scrmfs_fd_limit;
        *intercept = 1;
        *fd = newfd;
        debug("Changing fd from exposed %d to internal %d\n",oldfd,newfd);
    }

    return;
}

/* given a file descriptor, return the file id */
static inline int scrmfs_get_fid_from_fd(int fd)
{
  /* right now, this is the same thing */
  return fd;
}

/* given a path, return an fd */
static int scrmfs_get_fid_from_path(const char* path)
{
    int i = 0;
    while (i < SCRMFS_MAX_FILES)
    {
        if(scrmfs_filelist[i].in_use &&
           strcmp((void *)&scrmfs_filelist[i].filename, path) == 0)
        {
            debug("File found: scrmfs_filelist[%d].filename = %s\n",i,(void *)&scrmfs_filelist[i].filename);
            return i;
        }
        i++;
    }
    return -1;
}

/* given a file descriptor, return a pointer to the meta data,
 * otherwise return NULL */
static inline scrmfs_filemeta_t* scrmfs_get_meta_from_fid(int fid)
{
    /* check that the file descriptor is within range of our array */
    if (fid >= 0 && fid < SCRMFS_MAX_FILES) {
        /* get a pointer to the file meta data structure */
        scrmfs_filemeta_t* meta = &scrmfs_filemetas[fid];
        return meta;
    }
    return NULL;
}

/* given a chunk id and an offset within that chunk, return the pointer
 * to the memory location corresponding to that location */
static inline void* scrmfs_compute_chunk_buf(const scrmfs_filemeta_t* meta, int id, off_t offset)
{
    /* identify physical chunk id, find start of its buffer and add the offset */
    int chunk_id = meta->chunk_ids[id];
    char* start = chunk[chunk_id].buf;
    char* buf = start + offset;
    return (void*)buf;
}

/* ---------------------------------------
 * File system operations
 * --------------------------------------- */

/* truncate file id to given length */
static int scrmfs_truncate_fid(int fid, off_t length)
{
    /* get meta data for this file */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

    /* determine the number of chunks to leave after truncating */
    off_t num_chunks = 0;
    if (length > 0) {
      num_chunks = (length >> SCRMFS_CHUNK_BITS) + 1;
    }

    /* clear off any extra chunks */
    while (meta->chunks > num_chunks) {
        meta->chunks--;
        debug("truncate chunk %d\n", meta->chunk_ids[meta->chunks]);
        scrmfs_stack_push(free_chunk_stack, meta->chunk_ids[meta->chunks]);
    }
    
    /* set the new size */
    meta->size = length;

    return 0;
}

/* return file resources to free pools */
static int scrmfs_unlink_fid(int fid)
{
    /* return data to free pools */
    scrmfs_truncate_fid(fid, 0);

    /* set this file id as not in use */
    scrmfs_filelist[fid].in_use = 0;

    /* add this id back to the free stack */
    scrmfs_stack_push(free_fid_stack, fid);

    return 0;
}

/* ---------------------------------------
 * POSIX wrappers
 * --------------------------------------- */

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
            debug("Changing %s to %s\n",(void *)&scrmfs_filelist[fid].filename, newpath);
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
            debug("Couldn't find entry for %s in SCRMFS\n",path);
            errno = ENOENT;
            return -1;
        }

        /* truncate the file */
        scrmfs_truncate_fid(fid, length);

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

        /* free up idx in free_fid_stack */
        scrmfs_unlink_fid(fid);

        return 0;
    } else {
        MAP_OR_FAIL(unlink);
        int ret = __real_unlink(path);
        return ret;
    }
}

int SCRMFS_DECL(close)(int fd)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* TODO: free file descriptor */
        debug("closing fd %d\n",fd);
        return 0;
    } else {
        MAP_OR_FAIL(close);
        int ret = __real_close(fd);
        return ret;
    }
}

int SCRMFS_DECL(fclose)(FILE *fp)
{
    struct scrmfs_file_runtime* file;
    int tmp_fd = fileno(fp);
    double tm1, tm2;
    int ret;

    MAP_OR_FAIL(fclose);

    tm1 = scrmfs_wtime();
    ret = __real_fclose(fp);
    tm2 = scrmfs_wtime();
    
    CP_LOCK();
    file = scrmfs_file_by_fd(tmp_fd);
    if(file)
    {
        file->last_byte_written = 0;
        file->last_byte_read = 0;
        CP_F_SET(file, CP_F_CLOSE_TIMESTAMP, posix_wtime());
        CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME);
        scrmfs_file_close_fd(tmp_fd);
    }
    CP_UNLOCK();

    return(ret);
}


int SCRMFS_DECL(fsync)(int fd)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
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
    int ret;
    struct scrmfs_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fdatasync);

    tm1 = scrmfs_wtime();
    ret = __real_fdatasync(fd);
    tm2 = scrmfs_wtime();
    if(ret < 0)
        return(ret);

    CP_LOCK();
    file = scrmfs_file_by_fd(fd);
    if(file)
    {
        CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_write_end, CP_F_POSIX_WRITE_TIME); \
        CP_INC(file, CP_POSIX_FDSYNCS, 1);
    }
    CP_UNLOCK();

    return(ret);
}


void* SCRMFS_DECL(mmap64)(void *addr, size_t length, int prot, int flags,
    int fd, off64_t offset)
{
    void* ret;
    struct scrmfs_file_runtime* file;

    MAP_OR_FAIL(mmap64);

    ret = __real_mmap64(addr, length, prot, flags, fd, offset);
    if(ret == MAP_FAILED)
        return(ret);

    CP_LOCK();
    file = scrmfs_file_by_fd(fd);
    if(file)
    {
        CP_INC(file, CP_POSIX_MMAPS, 1);
    }
    CP_UNLOCK();

    return(ret);
}


void* SCRMFS_DECL(mmap)(void *addr, size_t length, int prot, int flags,
    int fd, off_t offset)
{
    void* ret;
    struct scrmfs_file_runtime* file;

    MAP_OR_FAIL(mmap);

    ret = __real_mmap(addr, length, prot, flags, fd, offset);
    if(ret == MAP_FAILED)
        return(ret);

    CP_LOCK();
    file = scrmfs_file_by_fd(fd);
    if(file)
    {
        CP_INC(file, CP_POSIX_MMAPS, 1);
    }
    CP_UNLOCK();

    return(ret);
}

int SCRMFS_DECL(creat)(const char* path, mode_t mode)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(creat);

    tm1 = scrmfs_wtime();
    ret = __real_creat(path, mode);
    tm2 = scrmfs_wtime();

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int SCRMFS_DECL(creat64)(const char* path, mode_t mode)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(creat64);

    tm1 = scrmfs_wtime();
    ret = __real_creat64(path, mode);
    tm2 = scrmfs_wtime();

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int SCRMFS_DECL(open64)(const char* path, int flags, ...)
{
    int mode = 0;
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(open64);

    if (flags & O_CREAT) 
    {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);

        tm1 = scrmfs_wtime();
        ret = __real_open64(path, flags, mode);
        tm2 = scrmfs_wtime();
    }
    else
    {
        tm1 = scrmfs_wtime();
        ret = __real_open64(path, flags);
        tm2 = scrmfs_wtime();
    }

    CP_LOCK();
    CP_RECORD_OPEN(ret, path, mode, 0, tm1, tm2);
    CP_UNLOCK();

    return(ret);
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
    
    /* TODO: use the fd_limit to determine if this call should be by-passed*/
    /* if intercept, or if not a special path in "exclusions" */
    if (scrmfs_intercept_path(path)) {
        /* TODO: handle relative paths using current working directory */

        /* TODO: check that path is short enough */
        size_t pathlen = strlen(path) + 1;
        if (pathlen > SCRMFS_MAX_FILENAME) {
            errno = ENAMETOOLONG;
            return -1;
        }

        /* assume that we'll place the file pointer at the start of the file */
        off_t pos = 0;

        /* check whether this file already exists */
        int fid = scrmfs_get_fid_from_path(path);
        debug("scrmfs_get_fid_from_path() gave %d\n",fid);
        if (fid < 0) {
            /* file does not exist */

            /* create file if O_CREAT is set */
            if (flags & O_CREAT) {
                debug("Couldn't find entry for %s in SCRMFS\n",path);
                debug("scrmfs_superblock = %p; free_fid_stack = %p; free_chunk_stack = %p; scrmfs_filelist = %p; chunks = %p\n",
                                            scrmfs_superblock,free_fid_stack,free_chunk_stack,scrmfs_filelist,chunk);

                /* allocate a file id slot for this new file */
                fid = scrmfs_stack_pop(free_fid_stack);
                debug("scrmfs_stack_pop() gave %d\n",fid);
                if (fid < 0) {
                    /* need to create a new file, but we can't */
                    debug("scrmfs_stack_pop() failed (%d)\n",fid);
                    errno = ENOSPC;
                    return -1;
                }

                /* mark this slot as in use and copy the filename */
                scrmfs_filelist[fid].in_use = 1;
                strcpy((void *)&scrmfs_filelist[fid].filename, path);
                debug("Filename %s got scrmfs fd %d\n",scrmfs_filelist[fid].filename,fid);

                /* initialize meta data */
                scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
                meta->size = 0;
                meta->chunks = 0;
            } else {
                /* ERROR: trying to open a file that does not exist without O_CREATE */
                debug("Couldn't find entry for %s in SCRMFS\n",path);
                errno = ENOENT;
                return -1;
            }
        } else {
            /* file already exists */

            /* if O_CREAT and O_EXCL are set, this is an error */
            if ((flags & O_CREAT) && (flags & O_EXCL)) {
                /* ERROR: trying to open a file that exists with O_CREATE and O_EXCL */
                errno = EEXIST;
                return -1;
            }

            /* if O_TRUNC is set with RDWR or WRONLY, need to truncate file */
            if ((flags & O_TRUNC) && (flags & (O_RDWR | O_WRONLY))) {
                scrmfs_truncate_fid(fid, 0);
            }

            /* if O_APPEND is set, we need to place file pointer at end of file */
            if (flags & O_APPEND) {
                scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
                pos = meta->size;
            }
        }

        /* TODO: allocate a free file descriptor and associate it with fid */
        /* set file pointer */
        scrmfs_active_fds[fid].pos = pos;
        debug("SCRMFS_open generated fd %d for file %s\n",fid,path);    

        /* don't conflict with active system fds that range from 0 - (fd_limit) */
        ret = fid + scrmfs_fd_limit;
    } else {
        MAP_OR_FAIL(open);
        if (flags & O_CREAT) {
            ret = __real_open(path, flags, mode);
        } else {
            ret = __real_open(path, flags);
        }
    }

    return ret;
}

FILE* SCRMFS_DECL(fopen64)(const char *path, const char *mode)
{
    FILE* ret;
    int fd;
    double tm1, tm2;

    MAP_OR_FAIL(fopen64);

    tm1 = scrmfs_wtime();
    ret = __real_fopen64(path, mode);
    tm2 = scrmfs_wtime();
    if(ret == 0)
        fd = -1;
    else
        fd = fileno(ret);

    CP_LOCK();
    CP_RECORD_OPEN(fd, path, 0, 1, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

FILE* SCRMFS_DECL(fopen)(const char *path, const char *mode)
{
    FILE* ret;
    int fd;
    double tm1, tm2;

    MAP_OR_FAIL(fopen);

    tm1 = scrmfs_wtime();
    ret = __real_fopen(path, mode);
    tm2 = scrmfs_wtime();
    if(ret == 0)
        fd = -1;
    else
        fd = fileno(ret);

    CP_LOCK();
    CP_RECORD_OPEN(fd, path, 0, 1, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int SCRMFS_DECL(__xstat64)(int vers, const char *path, struct stat64 *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__xstat64);

    tm1 = scrmfs_wtime();
    ret = __real___xstat64(vers, path, buf);
    tm2 = scrmfs_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    CP_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int SCRMFS_DECL(__lxstat64)(int vers, const char *path, struct stat64 *buf)
{
    int ret;
    double tm1, tm2;

    MAP_OR_FAIL(__lxstat64);

    tm1 = scrmfs_wtime();
    ret = __real___lxstat64(vers, path, buf);
    tm2 = scrmfs_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    CP_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int SCRMFS_DECL(__fxstat64)(int vers, int fd, struct stat64 *buf)
{
    int ret;
    struct scrmfs_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(__fxstat64);

    tm1 = scrmfs_wtime();
    ret = __real___fxstat64(vers, fd, buf);
    tm2 = scrmfs_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    /* skip logging if this was triggered internally */
    if(buf == &cp_stat_buf)
        return(ret);

    CP_LOCK();
    file = scrmfs_file_by_fd(fd);
    if(file)
    {
        CP_RECORD_STAT(file, buf, tm1, tm2);
    }
    CP_UNLOCK();

    return(ret);
}

/*
int SCRMFS_DECL(__stat)( const char *path, struct stat *buf)
{

    int ret;
    debug("stat called..\n");

    MAP_OR_FAIL(__stat);
    ret = __real_stat(path,buf);
    
    return ret;

} */

int SCRMFS_DECL(__xstat)(int vers, const char *path, struct stat *buf)
{
    debug("xstat was called for %s....\n",path);
    if (scrmfs_intercept_path(path)) {
        return 0;
    } else { 
        MAP_OR_FAIL(__xstat);
        int ret = __real___xstat(vers, path, buf);
        return ret;
    }
}

int SCRMFS_DECL(__lxstat)(int vers, const char *path, struct stat *buf)
{
    int ret;
    double tm1, tm2;

    debug("lxstat was called....\n");
    MAP_OR_FAIL(__lxstat);

    tm1 = scrmfs_wtime();
    ret = __real___lxstat(vers, path, buf);
    tm2 = scrmfs_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    CP_LOCK();
    CP_LOOKUP_RECORD_STAT(path, buf, tm1, tm2);
    CP_UNLOCK();

    return(ret);
}

int SCRMFS_DECL(__fxstat)(int vers, int fd, struct stat *buf)
{
    int ret;
    struct scrmfs_file_runtime* file;
    double tm1, tm2;

    debug("fxstat was called....\n");
    MAP_OR_FAIL(__fxstat);

    tm1 = scrmfs_wtime();
    ret = __real___fxstat(vers, fd, buf);
    tm2 = scrmfs_wtime();
    if(ret < 0 || !S_ISREG(buf->st_mode))
        return(ret);

    /* skip logging if this was triggered internally */
    if((void*)buf == (void*)&cp_stat_buf)
        return(ret);

    CP_LOCK();
    file = scrmfs_file_by_fd(fd);
    if(file)
    {
        CP_RECORD_STAT(file, buf, tm1, tm2);
    }
    CP_UNLOCK();

    return(ret);
}

ssize_t SCRMFS_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pread64);

    if((unsigned long)buf % scrmfs_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = scrmfs_wtime();
    ret = __real_pread64(fd, buf, count, offset);
    tm2 = scrmfs_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 1, offset, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t SCRMFS_DECL(pread)(int fd, void *buf, size_t count, off_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pread);

    if((unsigned long)buf % scrmfs_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = scrmfs_wtime();
    ret = __real_pread(fd, buf, count, offset);
    tm2 = scrmfs_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 1, offset, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}


ssize_t SCRMFS_DECL(pwrite)(int fd, const void *buf, size_t count, off_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pwrite);

    if((unsigned long)buf % scrmfs_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = scrmfs_wtime();
    ret = __real_pwrite(fd, buf, count, offset);
    tm2 = scrmfs_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 1, offset, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t SCRMFS_DECL(pwrite64)(int fd, const void *buf, size_t count, off64_t offset)
{
    ssize_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(pwrite64);

    if((unsigned long)buf % scrmfs_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = scrmfs_wtime();
    ret = __real_pwrite64(fd, buf, count, offset);
    tm2 = scrmfs_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 1, offset, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t SCRMFS_DECL(readv)(int fd, const struct iovec *iov, int iovcnt)
{
    ssize_t ret;
    int aligned_flag = 1;
    int i;
    double tm1, tm2;

    MAP_OR_FAIL(readv);

    for(i=0; i<iovcnt; i++)
    {
        if(((unsigned long)iov[i].iov_base % scrmfs_mem_alignment) != 0)
            aligned_flag = 0;
    }

    tm1 = scrmfs_wtime();
    ret = __real_readv(fd, iov, iovcnt);
    tm2 = scrmfs_wtime();
    CP_LOCK();
    CP_RECORD_READ(ret, fd, count, 0, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

ssize_t SCRMFS_DECL(writev)(int fd, const struct iovec *iov, int iovcnt)
{
    ssize_t ret;
    int aligned_flag = 1;
    int i;
    double tm1, tm2;

    MAP_OR_FAIL(writev);

    for(i=0; i<iovcnt; i++)
    {
        if(!((unsigned long)iov[i].iov_base % scrmfs_mem_alignment == 0))
            aligned_flag = 0;
    }

    tm1 = scrmfs_wtime();
    ret = __real_writev(fd, iov, iovcnt);
    tm2 = scrmfs_wtime();
    CP_LOCK();
    CP_RECORD_WRITE(ret, fd, count, 0, 0, aligned_flag, 0, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

size_t SCRMFS_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;

    MAP_OR_FAIL(fread);

    ret = __real_fread(ptr, size, nmemb, stream);
    return(ret);
}

ssize_t SCRMFS_DECL(read)(int fd, void *buf, size_t count)
{
    ssize_t ret;

    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);

        /* get a pointer to the file meta data structure */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        if (meta == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return -1;
        }

        /* get the current file pointer position */
        off_t oldpos = scrmfs_active_fds[fd].pos;

        /* check that we don't read past the end of the file */
        off_t newpos = oldpos + count;
        if (newpos > meta->size) {
            /* trying to read past the end of the file, so
             * adjust new file pointer position and count */
            newpos = meta->size;
            count = (size_t) (newpos - oldpos);
        }

        /* update position */
        scrmfs_active_fds[fd].pos = newpos;

        /* assume that we'll read all data */
        ret = count;

        /* get pointer to position within current chunk */
        int chunk_id = oldpos >> SCRMFS_CHUNK_BITS;
        off_t chunk_offset = oldpos & SCRMFS_CHUNK_MASK;
        void* chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, chunk_offset);

        /* determine how many bytes remain in the current chunk */
        size_t remaining = SCRMFS_CHUNK_SIZE - chunk_offset;
        if (count <= remaining) {
            /* all bytes for this write fit within the current chunk */
            memcpy(buf, chunk_buf, count);
        } else {
            /* read what's left of current chunk */
            char* ptr = (char*) buf;
            memcpy(ptr, chunk_buf, remaining);
            ptr += remaining;

            /* read from the next chunk */
            size_t read = remaining;
            while (read < count) {
                /* get pointer to start of next chunk */
                chunk_id++;
                chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, 0);

                /* compute size to read from this chunk */
                size_t to_read = count - read;
                if (to_read > SCRMFS_CHUNK_SIZE) {
                    to_read = SCRMFS_CHUNK_SIZE;
                }

                /* write data */
                memcpy(ptr, chunk_buf, to_read);
                ptr += to_read;

                /* update number of bytes written */
                read += to_read;
            }
        }
    } else {
        MAP_OR_FAIL(read);
        ret = __real_read(fd, buf, count);
    }

    return ret;
}

ssize_t SCRMFS_DECL(write)(int fd, const void *buf, size_t count)
{
    ssize_t ret;

    /* check file descriptor to determine whether we should pick off this call */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* assume we'll succeed with the write,
         * so set return value to count */
        ret = count;

        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);

        /* get a pointer to the file meta data structure */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        if (meta == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return -1;
        }
 
        /* Raghu: Added a new struct to store positions of active fids.
         * This struct is not part of superblock - doesn't have to be persistent */
        off_t oldpos = scrmfs_active_fds[fd].pos;
        off_t newpos = oldpos + count;
        scrmfs_active_fds[fd].pos = newpos;

        /* if we write past the end of the file, we need to update the
         * file size, and we may need to allocate more chunks */
        if (newpos > meta->size) {
            /* update file size */
            meta->size = newpos;

            /* determine whether we need to allocate more chunks */
            off_t maxsize = meta->chunks << SCRMFS_CHUNK_BITS;
            if (newpos > maxsize) {
                /* compute number of additional bytes we need */
                off_t additional = newpos - maxsize;
                while (additional > 0)
                {
                  /* allocate a new chunk */
                  int id = scrmfs_stack_pop(free_chunk_stack);
                  if (id < 0)
                  {
                      debug("scrmfs_stack_pop() failed (%d)\n", id);

                      /* ERROR: device out of space */
                      errno = ENOSPC;
                      return -1;
                  }

                  /* add it to the chunk list of this file */
                  meta->chunk_ids[meta->chunks] = id;
                  meta->chunks++;

                  /* subtract bytes from the number we need */
                  additional -= SCRMFS_CHUNK_SIZE;
                }
            }
        }

        /* get pointer to position within current chunk */
        int chunk_id = oldpos >> SCRMFS_CHUNK_BITS;
        off_t chunk_offset = oldpos & SCRMFS_CHUNK_MASK;
        void* chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, chunk_offset);

        /* determine how many bytes remain in the current chunk */
        size_t remaining = SCRMFS_CHUNK_SIZE - chunk_offset;
        if (count <= remaining) {
            /* all bytes for this write fit within the current chunk */
            memcpy(chunk_buf, buf, count);
//            _intel_fast_memcpy(chunk_buf, buf, count);
        } else {
            /* otherwise, fill up the remainder of the current chunk */
            char* ptr = (char*) buf;
            memcpy(chunk_buf, ptr, remaining);
//            _intel_fast_memcpy(chunk_buf, ptr, remaining);
            ptr += remaining;

            /* then write the rest of the bytes starting from beginning
             * of chunks */
            size_t written = remaining;
            while (written < count) {
                /* get pointer to start of next chunk */
                chunk_id++;
                chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, 0);

                /* compute size to write to this chunk */
                size_t nwrite = count - written;
                if (nwrite > SCRMFS_CHUNK_SIZE) {
                  nwrite = SCRMFS_CHUNK_SIZE;
                }

                /* write data */
                memcpy(chunk_buf, ptr, nwrite);
//                _intel_fast_memcpy(chunk_buf, ptr, nwrite);
                ptr += nwrite;

                /* update number of bytes written */
                written += nwrite;
            }
        }
    } else {
        /* lookup and record address of real function */
        MAP_OR_FAIL(write);

        /* don't intercept, just pass the call on to the real write call */
        ret = __real_write(fd, buf, count);
    }

    return(ret);
}

size_t SCRMFS_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    size_t ret;
    int aligned_flag = 0;
    double tm1, tm2;

    MAP_OR_FAIL(fwrite);

    if((unsigned long)ptr % scrmfs_mem_alignment == 0)
        aligned_flag = 1;

    tm1 = scrmfs_wtime();
    ret = __real_fwrite(ptr, size, nmemb, stream);
    tm2 = scrmfs_wtime();
    CP_LOCK();
    if(ret > 0)
        CP_RECORD_WRITE(size*ret, fileno(stream), (size*nmemb), 0, 0, aligned_flag, 1, tm1, tm2);
    else
        CP_RECORD_WRITE(ret, fileno(stream), 0, 0, 0, aligned_flag, 1, tm1, tm2);
    CP_UNLOCK();
    return(ret);
}

off64_t SCRMFS_DECL(lseek64)(int fd, off64_t offset, int whence)
{
    off64_t ret;
    struct scrmfs_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(lseek64);

    tm1 = scrmfs_wtime();
    ret = __real_lseek64(fd, offset, whence);
    tm2 = scrmfs_wtime();
    if(ret >= 0)
    {
        CP_LOCK();
        file = scrmfs_file_by_fd(fd);
        if(file)
        {
            file->offset = ret;
            CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME);
            CP_INC(file, CP_POSIX_SEEKS, 1);
        }
        CP_UNLOCK();
    }
    return(ret);
}

off_t SCRMFS_DECL(lseek)(int fd, off_t offset, int whence)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);

        /* check that file descriptor is good */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        if (meta == NULL) {
            /* bad file descriptor */
            errno = EBADF;
            return (off_t)-1;
        }

        debug("seeking from %ld\n",scrmfs_active_fds[fd].pos);        
        off_t current_pos = scrmfs_active_fds[fd].pos;
        if (whence == SEEK_SET) {
            /* seek to offset */
            current_pos = offset;
        } else if (whence == SEEK_CUR) {
            /* seek to current position + offset */
            current_pos += offset;
        } else if (whence == SEEK_END) {
            /* seek to EOF + offset */
            current_pos = meta->size + offset;
        }
        scrmfs_active_fds[fd].pos = current_pos;

        debug("seeking to %ld\n",scrmfs_active_fds[fd].pos);        
        return current_pos;
    } else {
        MAP_OR_FAIL(lseek);
        int ret = __real_lseek(fd, offset, whence);
        return ret;
    }
}

int SCRMFS_DECL(fseek)(FILE *stream, long offset, int whence)
{
    int ret;
    struct scrmfs_file_runtime* file;
    double tm1, tm2;

    MAP_OR_FAIL(fseek);

    tm1 = scrmfs_wtime();
    ret = __real_fseek(stream, offset, whence);
    tm2 = scrmfs_wtime();
    if(ret >= 0)
    {
        CP_LOCK();
        file = scrmfs_file_by_fd(fileno(stream));
        if(file)
        {
            file->offset = ret;
            CP_F_INC_NO_OVERLAP(file, tm1, tm2, file->last_posix_meta_end, CP_F_POSIX_META_TIME);
            CP_INC(file, CP_POSIX_FSEEKS, 1);
        }
        CP_UNLOCK();
    }
    return(ret);
}

void scrmfs_finalize(struct scrmfs_job_runtime* job)
{
    if(!job)
    {
        return;
    }

    free(job);
}

void scrmfs_initialize(int argc, char** argv,  int nprocs, int rank)
{
    int i;
    char* disable;
    char* disable_timing;
    char* envstr;
    char* truncate_string = "<TRUNCATED>";
    int truncate_offset;
    int chars_left = 0;
    int ret;
    int tmpval;

    disable = getenv("SCRMFS_DISABLE");
    if(disable)
    {
        /* turn off tracing */
        return;
    }

    disable_timing = getenv("SCRMFS_DISABLE_TIMING");

    if(scrmfs_global_job != NULL)
    {
        return;
    }

    #if (__CP_MEM_ALIGNMENT < 1)
        #error SCRMFS must be configured with a positive value for --with-mem-align
    #endif
    envstr = getenv("SCRMFS_MEMALIGN");
    if (envstr)
    {
        ret = sscanf(envstr, "%d", &tmpval);
        /* silently ignore if the env variable is set poorly */
        if(ret == 1 && tmpval > 0)
        {
            scrmfs_mem_alignment = tmpval;
        }
    }
    else
    {
        scrmfs_mem_alignment = __CP_MEM_ALIGNMENT;
    }

    /* avoid floating point errors on faulty input */
    if (scrmfs_mem_alignment < 1)
    {
        scrmfs_mem_alignment = 1;
    }

    /* allocate structure to track scrmfs_global_job information */
    scrmfs_global_job = malloc(sizeof(*scrmfs_global_job));
    if(!scrmfs_global_job)
    {
        return;
    }
    memset(scrmfs_global_job, 0, sizeof(*scrmfs_global_job));

    if(disable_timing)
    {
        scrmfs_global_job->flags |= CP_FLAG_NOTIMING;
    }

    /* set up file records */
    for(i=0; i<CP_MAX_FILES; i++)
    {
        scrmfs_global_job->file_runtime_array[i].log_file = 
            &scrmfs_global_job->file_array[i];
    }

    strcpy(scrmfs_global_job->log_job.version_string, CP_VERSION);
    scrmfs_global_job->log_job.magic_nr = CP_MAGIC_NR;
    scrmfs_global_job->log_job.uid = getuid();
    scrmfs_global_job->log_job.start_time = time(NULL);
    scrmfs_global_job->log_job.nprocs = nprocs;
    my_rank = rank;

    /* record exe and arguments */
    for(i=0; i<argc; i++)
    {
        chars_left = CP_EXE_LEN-strlen(scrmfs_global_job->exe);
        strncat(scrmfs_global_job->exe, argv[i], chars_left);
        if(i < (argc-1))
        {
            chars_left = CP_EXE_LEN-strlen(scrmfs_global_job->exe);
            strncat(scrmfs_global_job->exe, " ", chars_left);
        }
    }

    /* if we don't see any arguments, then use glibc symbol to get
     * program name at least (this happens in fortran)
     */
    if(argc == 0)
    {
        chars_left = CP_EXE_LEN-strlen(scrmfs_global_job->exe);
        strncat(scrmfs_global_job->exe, __progname_full, chars_left);
        chars_left = CP_EXE_LEN-strlen(scrmfs_global_job->exe);
        strncat(scrmfs_global_job->exe, " <unknown args>", chars_left);
    }

    if(chars_left == 0)
    {
        /* we ran out of room; mark that string was truncated */
        truncate_offset = CP_EXE_LEN - strlen(truncate_string);
        sprintf(&scrmfs_global_job->exe[truncate_offset], "%s", 
            truncate_string);
    }
}

/* scrmfs_condense()
 *
 * collapses all file statistics into a single unified set of counters; used
 * when we have opened too many files to track independently
 */
void scrmfs_condense(void)
{
    struct scrmfs_file_runtime* base_file;
    struct scrmfs_file_runtime* iter_file;
    int i;
    int j;

    if(!scrmfs_global_job)
        return;

    base_file = &scrmfs_global_job->file_runtime_array[0];

    /* iterate through files */
    for(j=1; j<scrmfs_global_job->file_count; j++)
    {
        iter_file = &scrmfs_global_job->file_runtime_array[j];

        /* iterate through records */
        for(i=0; i<CP_NUM_INDICES; i++)
        {
            switch(i)
            {
                /* NOTE: several fields cease to make sense if the records
                 * have been condensed.  Just let them get summed anyway.
                 */
                /* TODO: double check this */

                /* keep up with global maxes in case they are helpful */
                case CP_MAX_BYTE_READ:
                case CP_MAX_BYTE_WRITTEN:
                    CP_MAX(base_file, i, CP_VALUE(iter_file, i));
                    break;

                /* do nothing with these; they are handled in the floating
                 * point loop 
                 */
                case CP_MAX_WRITE_TIME_SIZE:
                case CP_MAX_READ_TIME_SIZE:
                    break;

                /* pick one */
                case CP_DEVICE:
                case CP_SIZE_AT_OPEN:
                    CP_SET(base_file, i, CP_VALUE(iter_file, i));
                    break;

                /* most records can simply be added */
                default:
                    CP_INC(base_file, i, CP_VALUE(iter_file, i));
                    break;
            }
        }
        for(i=0; i<CP_F_NUM_INDICES; i++)
        {
            switch(i)
            {
                case CP_F_MAX_WRITE_TIME:
                    if(CP_F_VALUE(iter_file, i) > CP_F_VALUE(base_file, i))
                    {
                        CP_F_SET(base_file, i, CP_F_VALUE(iter_file, i));
                        CP_SET(base_file, CP_MAX_WRITE_TIME_SIZE, 
                            CP_VALUE(iter_file, CP_MAX_WRITE_TIME_SIZE));
                    }
                    break;
                case CP_F_MAX_READ_TIME:
                    if(CP_F_VALUE(iter_file, i) > CP_F_VALUE(base_file, i))
                    {
                        CP_F_SET(base_file, i, CP_F_VALUE(iter_file, i));
                        CP_SET(base_file, CP_MAX_READ_TIME_SIZE, 
                            CP_VALUE(iter_file, CP_MAX_READ_TIME_SIZE));
                    }
                    break;
                default:
                    CP_F_SET(base_file, i, CP_F_VALUE(iter_file, i) + CP_F_VALUE(base_file, i));
                    break;
            }
        }
    }
    
    base_file->log_file->hash = 0;
    
    scrmfs_global_job->flags |= CP_FLAG_CONDENSED;
    scrmfs_global_job->file_count = 1;

    /* clear hash tables for safety */
    memset(scrmfs_global_job->name_table, 0, CP_HASH_SIZE*sizeof(struct scrmfs_file_runtime*));
    memset(scrmfs_global_job->handle_table, 0, CP_HASH_SIZE*sizeof(*scrmfs_global_job->handle_table));
    
    return;
}

static struct scrmfs_file_runtime* scrmfs_file_by_name_setfd(const char* name, int fd)
{
    struct scrmfs_file_runtime* tmp_file;

    tmp_file = scrmfs_file_by_name_sethandle(name, &fd, sizeof(fd), SCRMFS_FD);
    return(tmp_file);
}

static void scrmfs_file_close_fd(int fd)
{
    scrmfs_file_closehandle(&fd, sizeof(fd), SCRMFS_FD);
    return;
}

static struct scrmfs_file_runtime* scrmfs_file_by_fd(int fd)
{
    struct scrmfs_file_runtime* tmp_file;

    tmp_file = scrmfs_file_by_handle(&fd, sizeof(fd), SCRMFS_FD);
    
    return(tmp_file);
}

static int access_comparison(const void* a_p, const void* b_p)
{
    const struct cp_access_counter* a = a_p;
    const struct cp_access_counter* b = b_p;

    if(a->size < b->size)
        return(-1);
    if(a->size > b->size)
        return(1);
    return(0);
}

/* cp_access_counter()
 *
 * records the occurance of a particular access size for a file,
 * current implementation uses glibc red black tree
 */
static void cp_access_counter(struct scrmfs_file_runtime* file, ssize_t size, enum cp_counter_type type)
{
    struct cp_access_counter* counter;
    struct cp_access_counter* found;
    void* tmp;
    void** root;
    int* count;
    struct cp_access_counter tmp_counter;

    /* don't count sizes or strides of 0 */
    if(size == 0)
        return;
    
    switch(type)
    {
        case CP_COUNTER_ACCESS:
            root = &file->access_root;
            count = &file->access_count;
            break;
        case CP_COUNTER_STRIDE:
            root = &file->stride_root;
            count = &file->stride_count;
            break;
        default:
            return;
    }

    /* check to see if this size is already recorded */
    tmp_counter.size = size;
    tmp_counter.freq = 1;
    tmp = tfind(&tmp_counter, root, access_comparison);
    if(tmp)
    {
        found = *(struct cp_access_counter**)tmp;
        found->freq++;
        return;
    }

    /* we can add a new one as long as we haven't hit the limit */
    if(*count < CP_MAX_ACCESS_COUNT_RUNTIME)
    {
        counter = malloc(sizeof(*counter));
        if(!counter)
        {
            return;
        }

        counter->size = size;
        counter->freq = 1;

        tmp = tsearch(counter, root, access_comparison);
        found = *(struct cp_access_counter**)tmp;
        /* if we get a new answer out here we are in trouble; this was
         * already checked with the tfind()
         */
        assert(found == counter);

        (*count)++;
    }

    return;
}

#if 0
void scrmfs_shutdown_bench(int argc, char** argv, int rank, int nprocs)
{
    int* fd_array;
    int64_t* size_array;
    int i;
    int nfiles;
    char path[256];
    int iters;
    
    /* combinations to build:
     * - 1 unique file per proc
     * - 1 shared file per proc
     * - 1024 unique file per proc
     * - 1024 shared per proc
     */

    srand(rank);
    fd_array = malloc(sizeof(int)*CP_MAX_FILES);
    size_array = malloc(sizeof(int64_t)*CP_MAX_ACCESS_COUNT_RUNTIME);

    assert(fd_array&&size_array);

    for(i=0; i<CP_MAX_FILES; i++)
        fd_array[i] = i;
    for(i=0; i<CP_MAX_ACCESS_COUNT_RUNTIME; i++)
        size_array[i] = rand();

    /* clear out existing stuff */
    scrmfs_walk_file_accesses(scrmfs_global_job);
    scrmfs_finalize(scrmfs_global_job);
    scrmfs_global_job = NULL;

    /***********************************************************/
    /* reset scrmfs to start clean */
    scrmfs_initialize(argc, argv, nprocs, rank);

    /* populate one unique file per proc */
    nfiles = 1;
    iters = 1;
    for(i=0; i<nfiles; i++)
    {
        sprintf(path, "%d-%d", i, rank);
        CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
    }

    for(i=0; i<iters; i++)
    {
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1 unique file per proc\n");
    scrmfs_shutdown(1);
    scrmfs_global_job = NULL;

    /***********************************************************/
    /* reset scrmfs to start clean */
    scrmfs_initialize(argc, argv, nprocs, rank);

    /* populate one shared file per proc */
    nfiles = 1;
    iters = 1;
    for(i=0; i<nfiles; i++)
    {
        sprintf(path, "%d", i);
        CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
    }

    for(i=0; i<iters; i++)
    {
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1 shared file across procs\n");
    scrmfs_shutdown(1);
    scrmfs_global_job = NULL;

    /***********************************************************/
    /* reset scrmfs to start clean */
    scrmfs_initialize(argc, argv, nprocs, rank);

    /* populate 1024 unique file per proc */
    nfiles = 1024;
    iters = 1024;
    for(i=0; i<nfiles; i++)
    {
        sprintf(path, "%d-%d", i, rank);
        CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
    }

    for(i=0; i<iters; i++)
    {
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1024 unique files per proc\n");
    scrmfs_shutdown(1);
    scrmfs_global_job = NULL;

    /***********************************************************/
    /* reset scrmfs to start clean */
    scrmfs_initialize(argc, argv, nprocs, rank);

    /* populate 1024 shared file per proc */
    nfiles = 1024;
    iters = 1024;
    for(i=0; i<nfiles; i++)
    {
        sprintf(path, "%d", i);
        CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
    }

    for(i=0; i<iters; i++)
    {
        CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
    }

    if(rank == 0)
        printf("# 1024 shared files across procs\n");
    scrmfs_shutdown(1);
    scrmfs_global_job = NULL;

    scrmfs_initialize(argc, argv, nprocs, rank);

    free(fd_array);
    free(size_array);

    return;
}

void scrmfs_search_bench(int argc, char** argv, int iters)
{
    int* fd_array;
    int64_t* size_array;
    int i,j;
    int skip = 32;
    int nfiles;
    char path[256];
    double tm1, tm2;
    
    fd_array = malloc(sizeof(int)*CP_MAX_FILES);
    size_array = malloc(sizeof(int64_t)*CP_MAX_ACCESS_COUNT_RUNTIME);

    assert(fd_array&&size_array);

    for(i=0; i<CP_MAX_FILES; i++)
        fd_array[i] = i;
    for(i=0; i<CP_MAX_ACCESS_COUNT_RUNTIME; i++)
        size_array[i] = rand();

    printf("#<iters>\t<numfiles>\t<numsizes>\t<total time>\t<per iter>\n");

    for(j=0; j<2; j++)
    {
        /* warm up */
        /* reset scrmfs to start clean */
        scrmfs_walk_file_accesses(scrmfs_global_job);
        scrmfs_finalize(scrmfs_global_job);
        scrmfs_global_job = NULL;
        scrmfs_initialize(argc, argv, 1, 0);

        nfiles = 1;
        /* populate entries for each file */
        for(i=0; i<nfiles; i++)
        {
            sprintf(path, "%d", i);
            CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
        }

        for(i=0; i<iters; i++)
        {
            if(j==0)
            {
                CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
            }
            else
            {
                CP_RECORD_WRITE(size_array[0], fd_array[i%nfiles], size_array[0], 0, 0, 1, 0, 1, 2);
            }
        }

        /* real timing */
        for(nfiles=0; nfiles<=CP_MAX_FILES; nfiles += skip)
        {
            if(nfiles == 0)
                nfiles = 1;

            /* reset scrmfs to start clean */
            scrmfs_walk_file_accesses(scrmfs_global_job);
            scrmfs_finalize(scrmfs_global_job);
            scrmfs_global_job = NULL;
            scrmfs_initialize(argc, argv, 1, 0);

            /* populate entries for each file */
            for(i=0; i<nfiles; i++)
            {
                sprintf(path, "%d", i);
                CP_RECORD_OPEN(fd_array[i], path, 777, 0, 0, 0);
            }

            tm1 = scrmfs_wtime();
            for(i=0; i<iters; i++)
            {
                if(j==0)
                {
                    CP_RECORD_WRITE(size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], fd_array[i%nfiles], size_array[(i/nfiles)%CP_MAX_ACCESS_COUNT_RUNTIME], 0, 0, 1, 0, 1, 2);
                }
                else
                {
                    CP_RECORD_WRITE(size_array[0], fd_array[i%nfiles], size_array[0], 0, 0, 1, 0, 1, 2);
                }
            }
            tm2 = scrmfs_wtime();

            /* printf("#<iters>\t<numfiles>\t<numsizes>\t<total time>\t<per iter>\n"); */
            printf("%d\t%d\t%d\t%f\t%.12f\n", iters, nfiles, (j==0?CP_MAX_ACCESS_COUNT_RUNTIME:1), tm2-tm1, (tm2-tm1)/iters);

            if(nfiles == 1)
                nfiles = 0;
        }
    }

    free(fd_array);
    free(size_array);
}
#endif

static double posix_wtime(void)
{
    return SCRMFS_MPI_CALL(PMPI_Wtime)();
}

double scrmfs_wtime(void)
{
    if(!scrmfs_global_job || scrmfs_global_job->flags & CP_FLAG_NOTIMING)
    {
        return(0);
    }
    
    return(posix_wtime());
}

struct scrmfs_file_runtime* scrmfs_file_by_name(const char* name)
{
    struct scrmfs_file_runtime* tmp_file;
    uint64_t tmp_hash = 0;
    char* suffix_pointer;
    int hash_index;

    if(!scrmfs_global_job)
        return(NULL);

    /* if we have already condensed the data, then just hand the first file
     * back
     */
    if(scrmfs_global_job->flags & CP_FLAG_CONDENSED)
    {
        return(&scrmfs_global_job->file_runtime_array[0]);
    }

    tmp_hash = scrmfs_hash((void*)name, strlen(name), 0);

    /* search hash table */
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_file = scrmfs_global_job->name_table[hash_index];
    while(tmp_file)
    {
        if(tmp_file->log_file->hash == tmp_hash)
        {
            return(tmp_file);
        }
        tmp_file = tmp_file->name_next;
    }

    /* see if we need to condense */
    if(scrmfs_global_job->file_count >= CP_MAX_FILES)
    {
        scrmfs_condense();
        return(&scrmfs_global_job->file_runtime_array[0]);
    }

    /* new, unique file */
    tmp_file = &scrmfs_global_job->file_runtime_array[scrmfs_global_job->file_count];

    CP_SET(tmp_file, CP_MEM_ALIGNMENT, scrmfs_mem_alignment);
    tmp_file->log_file->hash = tmp_hash;

    /* record last N characters of file name too */
    suffix_pointer = (char*)name;
    if(strlen(name) > CP_NAME_SUFFIX_LEN)
    {
        suffix_pointer += (strlen(name) - CP_NAME_SUFFIX_LEN);
    }
    strcpy(tmp_file->log_file->name_suffix, suffix_pointer); 

    scrmfs_global_job->file_count++;

    /* put into hash table, head of list at that index */
    tmp_file->name_prev = NULL;
    tmp_file->name_next = scrmfs_global_job->name_table[hash_index];
    if(tmp_file->name_next)
        tmp_file->name_next->name_prev = tmp_file;
    scrmfs_global_job->name_table[hash_index] = tmp_file;

    return(tmp_file);
}


struct scrmfs_file_runtime* scrmfs_file_by_name_sethandle(
    const char* name,
    const void* handle,
    int handle_sz,
    enum scrmfs_handle_type handle_type)
{
    struct scrmfs_file_runtime* file;
    uint64_t tmp_hash;
    int hash_index;
    struct scrmfs_file_ref* tmp_ref;

    if(!scrmfs_global_job)
    {
        return(NULL);
    }

    /* find file record by name first */
    file = scrmfs_file_by_name(name);

    if(!file)
        return(NULL);

    /* search hash table */
    tmp_ref = ref_by_handle(handle, handle_sz, handle_type);
    if(tmp_ref)
    {
        /* we have a reference.  Make sure it points to the correct file
         * and return it
         */
        tmp_ref->file = file;
        return(file);
    }

    /* if we hit this point, then we don't have a reference for this handle
     * in the table yet.  Add it.
     */
    tmp_hash = scrmfs_hash(handle, handle_sz, 0);
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_ref = malloc(sizeof(*tmp_ref));
    if(!tmp_ref)
        return(NULL);

    memset(tmp_ref, 0, sizeof(*tmp_ref));
    tmp_ref->file = file;
    memcpy(tmp_ref->handle, handle, handle_sz);
    tmp_ref->handle_sz = handle_sz;
    tmp_ref->handle_type = handle_type;
    tmp_ref->prev = NULL;
    tmp_ref->next = scrmfs_global_job->handle_table[hash_index];
    if(tmp_ref->next)
        tmp_ref->next->prev = tmp_ref;
    scrmfs_global_job->handle_table[hash_index] = tmp_ref;

    return(file);
}

struct scrmfs_file_runtime* scrmfs_file_by_handle(
    const void* handle,
    int handle_sz,
    enum scrmfs_handle_type handle_type)
{   
    struct scrmfs_file_ref* tmp_ref;

    if(!scrmfs_global_job)
    {
        return(NULL);
    }

    tmp_ref = ref_by_handle(handle, handle_sz, handle_type);
    if(tmp_ref)
        return(tmp_ref->file);
    else
        return(NULL);

    return(NULL);
}

void scrmfs_file_closehandle(
    const void* handle,
    int handle_sz,
    enum scrmfs_handle_type handle_type)
{
    struct scrmfs_file_ref* tmp_ref;
    uint64_t tmp_hash;
    int hash_index;
    
    if(!scrmfs_global_job)
    {
        return;
    }

    /* search hash table */
    tmp_hash = scrmfs_hash(handle, handle_sz, 0);
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_ref = scrmfs_global_job->handle_table[hash_index];
    while(tmp_ref)
    {
        if(tmp_ref->handle_sz == handle_sz &&
            tmp_ref->handle_type == handle_type &&
            memcmp(tmp_ref->handle, handle, handle_sz) == 0)
        {
            /* we have a reference. */ 
            if(!tmp_ref->prev)
            {
                /* head of list */
                scrmfs_global_job->handle_table[hash_index] = tmp_ref->next;
                if(tmp_ref->next)
                    tmp_ref->next->prev = NULL;
            }
            else
            {
                /* not head of list */
                if(tmp_ref->prev)
                    tmp_ref->prev->next = tmp_ref->next;
                if(tmp_ref->next)
                    tmp_ref->next->prev = tmp_ref->prev;
            }
            free(tmp_ref);
            return;
        }
        tmp_ref = tmp_ref->next;
    }

    return;
}

static struct scrmfs_file_ref* ref_by_handle(
    const void* handle,
    int handle_sz,
    enum scrmfs_handle_type handle_type)
{   
    uint64_t tmp_hash;
    int hash_index;
    struct scrmfs_file_ref* tmp_ref;

    if(!scrmfs_global_job)
    {
        return(NULL);
    }

    /* search hash table */
    tmp_hash = scrmfs_hash(handle, handle_sz, 0);
    hash_index = tmp_hash & CP_HASH_MASK;
    tmp_ref = scrmfs_global_job->handle_table[hash_index];
    while(tmp_ref)
    {
        if(tmp_ref->handle_sz == handle_sz &&
            tmp_ref->handle_type == handle_type &&
            memcmp(tmp_ref->handle, handle, handle_sz) == 0)
        {
            /* we have a reference. */ 
            return(tmp_ref);
        }
        tmp_ref = tmp_ref->next;
    }

    return(NULL);
}


/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
