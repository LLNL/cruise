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
#include "utlist.h"

#include "container/src/container.h"

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

static int scrmfs_use_memfs = 1;
static int scrmfs_use_spillover;
static int scrmfs_use_containers;         /* set by env var SCRMFS_USE_CONTAINERS=1 */
static char scrmfs_container_info[100];   /* not sure what this is for */
static cs_store_handle_t cs_store_handle; /* the container store handle */
static cs_set_handle_t cs_set_handle;     /* the container set handle */

#define SCRMFS_DEBUG
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
#define SCRMFS_STACK_LOCK() pthread_mutex_lock(&scrmfs_stack_mutex)
#define SCRMFS_STACK_UNLOCK() pthread_mutex_unlock(&scrmfs_stack_mutex)


/* ---------------------------------------
 * POSIX wrappers: paths
 * --------------------------------------- */

SCRMFS_FORWARD_DECL(access, int, (const char *pathname, int mode));
SCRMFS_FORWARD_DECL(mkdir, int, (const char *path, mode_t mode));
SCRMFS_FORWARD_DECL(rmdir, int, (const char *path));
SCRMFS_FORWARD_DECL(unlink, int, (const char *path));
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
SCRMFS_FORWARD_DECL(fclose, int, (FILE *fp));
SCRMFS_FORWARD_DECL(fread, size_t, (void *ptr, size_t size, size_t nmemb, FILE *stream));
SCRMFS_FORWARD_DECL(fwrite, size_t, (const void *ptr, size_t size, size_t nmemb, FILE *stream));
SCRMFS_FORWARD_DECL(fseek, int, (FILE *stream, long offset, int whence));
SCRMFS_FORWARD_DECL(fsync, int, (int fd));
SCRMFS_FORWARD_DECL(fdatasync, int, (int fd));

/* keep track of what we've initialized */
static int scrmfs_initialized = 0;

/* global persistent memory block (metadata + data) */
static void* scrmfs_superblock = NULL;
static void* free_fid_stack = NULL;
static void* free_chunk_stack = NULL;
static void* free_spillchunk_stack = NULL;
static scrmfs_filename_t* scrmfs_filelist = NULL;
static scrmfs_filemeta_t* scrmfs_filemetas = NULL;
static char* scrmfs_chunks = NULL;
static int scrmfs_spilloverblock = 0;

/* array of file descriptors */
static scrmfs_fd_t scrmfs_fds[SCRMFS_MAX_FILEDESCS];
static rlim_t scrmfs_fd_limit;

/* mount point information */
static char*  scrmfs_mount_prefix = NULL;
static size_t scrmfs_mount_prefixlen = 0;
static key_t  scrmfs_mount_key = 0;

/* mutex to lock stack operations */
pthread_mutex_t scrmfs_stack_mutex = PTHREAD_MUTEX_INITIALIZER;

static int scrmfs_init();
static int scrmfs_get_fid_from_path(const char* path);
static int scrmfs_add_new_directory(const char * path);
static inline scrmfs_filemeta_t* scrmfs_get_meta_from_fid(int fid);

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

/* mount memfs at some prefix location */
int scrmfs_mount(const char prefix[], int rank)
{
    scrmfs_mount_prefix = strdup(prefix);
    scrmfs_mount_prefixlen = strlen(scrmfs_mount_prefix);

    /* add mount point as a new directory in the file list */
    if (scrmfs_get_fid_from_path(prefix) >= 0) {
        /* we can't mount this location, because it already exists */
        errno = EEXIST;
        return -1;
    } else {
        /* claim an entry in our file list */
        int fid = scrmfs_add_new_directory(prefix);
        if (fid < 0) {
            /* if there was an error, return it */
            return fid;
        }
    }

    /* KMM commented out because we're just using a single rank, so use PRIVATE
     * downside, can't attach to this in another srun (PRIVATE, that is) */
    //scrmfs_mount_key = SCRMFS_SUPERBLOCK_KEY + rank;
    scrmfs_mount_key = IPC_PRIVATE;
    return 0;
}

/* initialize our global pointers into the given superblock */
static void* scrmfs_init_pointers(void* superblock)
{
    char* ptr = (char*) superblock;

    /* stack to manage free file ids */
    free_fid_stack = ptr;
    ptr += scrmfs_stack_bytes(SCRMFS_MAX_FILES);

    /* record list of file names */
    scrmfs_filelist = (scrmfs_filename_t*) ptr;
    ptr += SCRMFS_MAX_FILES * sizeof(scrmfs_filename_t);

    /* array of file meta data structures */
    scrmfs_filemetas = (scrmfs_filemeta_t*) ptr;
    ptr += SCRMFS_MAX_FILES * sizeof(scrmfs_filemeta_t);

    /* stack to manage free memory data chunks */
    free_chunk_stack = ptr;
    ptr += scrmfs_stack_bytes(SCRMFS_MAX_CHUNKS);

    if (scrmfs_use_spillover) {
        /* stack to manage free spill-over data chunks */
        free_spillchunk_stack = ptr;
        ptr += scrmfs_stack_bytes(SCRMFS_MAX_SPILL_CHUNKS);
    }

    /* Only set this up if we're using memfs */
    if (scrmfs_use_memfs) {
      /* pointer to start of memory data chunks */
      scrmfs_chunks = ptr;
      ptr += SCRMFS_MAX_CHUNKS * SCRMFS_CHUNK_SIZE;
    } else{
      scrmfs_chunks = NULL;
    }

    return ptr;
}


static int scrmfs_get_spillblock(size_t size, const char *path)
{
    void *scr_spillblock = NULL;
    int spillblock_fd;

    spillblock_fd = __real_open(path, O_RDWR | O_CREAT | O_EXCL);

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
        /*TODO: align to SSD block size*/
    }

    return spillblock_fd;
}


/* create superblock of specified size and name, or attach to existing
 * block if available */
static void* scrmfs_get_shmblock(size_t size, key_t key)
{
    void *scr_shmblock = NULL;

    /* TODO:improve error-propogation */
    //int scr_shmblock_shmid = shmget(key, size, IPC_CREAT | IPC_EXCL | S_IRWXU | SHM_HUGETLB);
    int scr_shmblock_shmid = shmget(key, size, IPC_CREAT | IPC_EXCL | S_IRWXU);
    if (scr_shmblock_shmid < 0) {
        if (errno == EEXIST) {
            /* Superblock exists. Do not initialize, just get and attach */
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
        /* valid segment created, attach to proc and init structures */
        scr_shmblock = shmat(scr_shmblock_shmid, NULL, 0);
        if(scr_shmblock < 0) {
            perror("shmat() failed");
        }
        debug("Superblock created at %p\n!",scr_shmblock);

        /* init our global variables to point to spots in superblock */
        scrmfs_init_pointers(scr_shmblock);

        /* initialize data structures within block */
        int i;
        for (i = 0; i < SCRMFS_MAX_FILES) {
            scrmfs_filelist[i].in_use = 0;
        }
        scrmfs_stack_init(free_fid_stack, SCRMFS_MAX_FILES);
        scrmfs_stack_init(free_chunk_stack, SCRMFS_MAX_CHUNKS);
        if (scrmfs_use_spillover) {
            scrmfs_stack_init(free_spillchunk_stack, SCRMFS_MAX_SPILL_CHUNKS);
        }
        debug("Meta-stacks initialized!\n");
    }
    
    return scr_shmblock;
}

static int scrmfs_init()
{
    /* TODO: expose API as a init call that can be called from SCR_Init */ 

    if (! scrmfs_initialized) {
        /* will we use containers or shared memory to store the files? */
        scrmfs_use_containers = 0;
        scrmfs_use_spillover = 0;

        char * env = getenv("SCRMFS_USE_CONTAINERS");
        if (env) {
            int val = atoi(env);
            if (val != 0) {
                scrmfs_use_containers = 1;
                scrmfs_use_memfs = 0;
            }
        }

        env = getenv("SCRMFS_USE_SPILLOVER");
        if (env) {
            int val = atoi(env);
            if (val != 0) {
                scrmfs_use_spillover = 1;
            }
        }

        debug("are we using containers? %d\n", scrmfs_use_containers);
        debug("are we using spillover? %d\n", scrmfs_use_spillover);

        /* record the max fd for the system */
        /* RLIMIT_NOFILE specifies a value one greater than the maximum
         * file descriptor number that can be opened by this process */
        struct rlimit *r_limit = malloc(sizeof(r_limit));
        if (r_limit == NULL) {
            perror("failed to allocate memory for call to getrlimit");
            return 1;
        }
        if (getrlimit(RLIMIT_NOFILE, r_limit) < 0) {
            perror("rlimit failed");
            return 1;
        }
        scrmfs_fd_limit = r_limit->rlim_cur;
        free(r_limit);
        debug("FD limit for system = %ld\n",scrmfs_fd_limit);

        /* determine the size of the superblock */
        /* generous allocation for chunk map (one file can take entire space)*/
        size_t superblock_size;
        if (scrmfs_use_memfs && !scrmfs_use_spillover) {
           superblock_size = 
               scrmfs_stack_bytes(SCRMFS_MAX_FILES) +           /* free file id stack */
               (SCRMFS_MAX_FILES * sizeof(scrmfs_filename_t)) + /* file name struct array */
               (SCRMFS_MAX_FILES * sizeof(scrmfs_filemeta_t)) + /* file meta data struct array */
               scrmfs_stack_bytes(SCRMFS_MAX_CHUNKS) +          /* free chunk stack */
               (SCRMFS_MAX_CHUNKS * SCRMFS_CHUNK_SIZE);         /* memory chunks */
        } else if (scrmfs_use_memfs && scrmfs_use_spillover) {
           superblock_size =
               scrmfs_stack_bytes(SCRMFS_MAX_FILES) +           /* free file id stack */
               (SCRMFS_MAX_FILES * sizeof(scrmfs_filename_t)) + /* file name struct array */
               (SCRMFS_MAX_FILES * sizeof(scrmfs_filemeta_t)) + /* file meta data struct array */
               scrmfs_stack_bytes(SCRMFS_MAX_CHUNKS) +          /* free chunk stack */
               scrmfs_stack_bytes(SCRMFS_MAX_SPILL_CHUNKS) +    /* free spill over chunk stack */
               (SCRMFS_MAX_CHUNKS * SCRMFS_CHUNK_SIZE);         /* memory chunks */
            
        } else {
           /* don't actually need the chunks if we're not storing in memory */
           superblock_size =
               scrmfs_stack_bytes(SCRMFS_MAX_FILES) +           /* free file id stack */
               (SCRMFS_MAX_FILES * sizeof(scrmfs_filename_t)) + /* file name struct array */
               (SCRMFS_MAX_FILES * sizeof(scrmfs_filemeta_t)) + /* file meta data struct array */
               scrmfs_stack_bytes(SCRMFS_MAX_CHUNKS);           /* free chunk stack */
        }

        /* get a superblock of persistent memory and initialize our
         * global variables for this block */
        scrmfs_superblock = scrmfs_get_shmblock(superblock_size, scrmfs_mount_key);
        if(scrmfs_superblock == NULL) {
            debug("scrmfs_get_shmblock() failed\n");
            return 1;
        }
      
        /* initialize the container store */
        if(scrmfs_use_containers) {
           int ret = cs_store_init(scrmfs_container_info, &cs_store_handle); 
           if (ret != CS_SUCCESS) {
              debug("failed to create container store\n");
              return -1;
           }
           debug("successfully created container store\n");
           char prefix[100];
           int exclusive = 0;
           size_t size = SCRMFS_MAX_CHUNKS * SCRMFS_CHUNK_SIZE;
           sprintf(prefix,"cs_set1");

           ret = cs_store_set_create (cs_store_handle, prefix, size, exclusive, &cs_set_handle);
           if (ret != CS_SUCCESS) {
              debug("creation of container set for %s failed: %d\n",prefix, ret);
              return -1;
           } else {
              debug("creation of container set for %s succeeded\n", prefix);
           }
        }

        /* initialize spillover store */
        if(scrmfs_use_spillover) {
            size_t spillover_size = SCRMFS_MAX_CHUNKS * SCRMFS_CHUNK_SIZE;
            scrmfs_spilloverblock = scrmfs_get_spillblock(spillover_size,"/data/spill_file");

            if(scrmfs_spilloverblock < 0) {
                debug("scrmfs_get_spillblock() failed!\n");
                return -1;
            }
        }

        /* remember that we've now initialized the library */
        scrmfs_initialized = 1;

        /* TODO: read mount point from env var */
        /* mount at /tmp if not instructed otherwise */
        scrmfs_mount("/tmp", 0);
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

/* given a path, return the file id */
static int scrmfs_get_fid_from_path(const char* path)
{
    int i = 0;
    while (i < SCRMFS_MAX_FILES)
    {
        if(scrmfs_filelist[i].in_use &&
           strcmp((void *)&scrmfs_filelist[i].filename, path) == 0)
        {
            debug("File found: scrmfs_filelist[%d].filename = %s\n",
                                i,(void *)&scrmfs_filelist[i].filename);
            return i;
        }
        i++;
    }

    /* couldn't find specified path */
    return -1;
}

/* checks to see if fid is a directory
 * returns 1 for yes
 * returns 0 for no */
static int scrmfs_is_dir(int fid)
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
static int scrmfs_is_dir_empty(const char * path)
{
    int i = 0;
    while (i < SCRMFS_MAX_FILES) {
       if(scrmfs_filelist[i].in_use) {
           /* if the file starts with the path, it is inside of that directory 
            * also check to make sure that it's not the directory entry itself */
           char * strptr = strstr(path, scrmfs_filelist[i].filename);
           if (strptr == scrmfs_filelist[i].filename && strcmp(path,scrmfs_filelist[i].filename)) {
              debug("File found: scrmfs_filelist[%d].filename = %s\n",
                                 i,(void *)&scrmfs_filelist[i].filename);
              return 0;
           }
       } 
       ++i;
    }

    /* couldn't find any files with this prefix, dir must be empty */
    return 1;
}

static int scrmfs_fill_in_stat_buf(int fid, struct stat * buf)
{
   scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
   if (!meta) {
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
   if(scrmfs_is_dir(fid)) {
      buf->st_mode |= S_IFDIR;
   } else {
      buf->st_mode |= S_IFREG;
   }

   return 0;
}

/* given a file id, return a pointer to the meta data,
 * otherwise return NULL */
static inline scrmfs_filemeta_t* scrmfs_get_meta_from_fid(int fid)
{
    /* check that the file id is within range of our array */
    if (fid >= 0 && fid < SCRMFS_MAX_FILES) {
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
        if (cid >= 0 && cid < SCRMFS_MAX_CHUNKS) {
           scrmfs_chunkmeta_t* cmeta = &(meta->chunk_meta[cid]);
           return cmeta;
        }
    }

    /* failed to find file or chunk id is out of range */
    return (scrmfs_chunkmeta_t *)NULL;
}

/* get a list of chunks for a given file (useful for RDMA, etc.);
 * to be exposed as API */
chunk_list_t* scrmfs_get_chunk_list(int fd)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        int i = 0;
        chunk_list_t *chunk_list = NULL;
        chunk_list_t *chunk_list_elem;
    
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);

        /* get meta data for this file */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        
        while ( i < meta->chunks ) {
            chunk_list_elem = (chunk_list_t*)malloc(sizeof(chunk_list_t));

            /* get the chunk id for the i-th chunk and
             * add it to the chunk_list */
            chunk_list_elem->chunk_id = meta->chunk_ids[i];

            /* currently using macros from utlist.h to
             * handle link-list operations */
            LL_APPEND( chunk_list, chunk_list_elem);
            i++;
        }

        return chunk_list;
    } else {
        /* file not managed by SCRMFS */
        errno = EBADF;
        return NULL;
    }
}

#if 0
/* debug function to print list of chunks constituting a file
 * and to test above function*/
void scrmfs_print_chunk_list(int fd)
{
    chunk_list_t *chunk_list;
    chunk_list_t *chunk_element;

    chunk_list = scrmfs_get_chunk_list(fd);

    LL_FOREACH(chunk_list,chunk_element) {
        printf("%d,",chunk_element->chunk_id);
    }

    LL_FOREACH(chunk_list,chunk_element) {
        free(chunk_element);
    }
    fprintf(stdout,"\n");
    
}
#endif

/* given a chunk id and an offset within that chunk, return the pointer
 * to the memory location corresponding to that location */
static inline void* scrmfs_compute_chunk_buf(const scrmfs_filemeta_t* meta, int id, off_t offset)
{
    /* identify physical chunk id, find start of its buffer and add the offset */
    int chunk_id = meta->chunk_ids[id];

    char *start = NULL;
    if (chunk_id < SCRMFS_MAX_CHUNKS) {
        start = scrmfs_chunks + (chunk_id << SCRMFS_CHUNK_BITS);
    } else {
        /* compute buffer loc within spillover device chunk */
        debug("wrong chunk ID\n");
        return NULL;
    }

    /* now add offset */
    char* buf = start + offset;
    return (void*)buf;
}

/* given a chunk id and an offset within that chunk, return the offset
 * in the spillover file corresponding to that location */
static inline off_t scrmfs_compute_spill_offset(const scrmfs_filemeta_t* meta, int id, off_t offset)
{
    /* identify physical chunk id, find start of its buffer and add the offset */
    int chunk_id = meta->chunk_ids[id];
    off_t start = 0;

    if (chunk_id < SCRMFS_MAX_CHUNKS) {
        debug("wrong spill-chunk ID\n");
        return -1;
    } else {
        /* compute buffer loc within spillover device chunk */
        /* account for the SCRMFS_MAX_CHUNKS added to identify location when
         * grabbing this chunk */
        start = ( (chunk_id - SCRMFS_MAX_CHUNKS ) << SCRMFS_CHUNK_BITS);
    }
    off_t buf = start + offset;
    return buf;
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
        SCRMFS_STACK_LOCK();
        scrmfs_stack_push(free_chunk_stack, meta->chunk_ids[meta->chunks]);
        SCRMFS_STACK_UNLOCK();

        if (scrmfs_use_containers) {
            char prefix[100];
            sprintf(prefix,"fid_%d_chunk_%d", fid, meta->chunk_ids[meta->chunks]);

            int ret = cs_set_container_remove(cs_set_handle, prefix);
            if (ret != CS_SUCCESS) {
               debug("removal of container for %s failed: %d\n",prefix, ret);
               /* removal of containers is not implemented in the container library, so 
                * this will always fail */
               //return -1;
            } else {
               debug("removal of container for %s succeeded\n", prefix);
            }
        }

        meta->chunk_meta[meta->chunks].location = -1;
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
    SCRMFS_STACK_LOCK();
    scrmfs_stack_push(free_fid_stack, fid);
    SCRMFS_STACK_UNLOCK();

    return 0;
}

/* allocate a file id slot for a new file 
 * return the fid or -1 on error */
static int scrmfs_get_slot_for_new_file()
{
    SCRMFS_STACK_LOCK();
    int fid = scrmfs_stack_pop(free_fid_stack);
    SCRMFS_STACK_UNLOCK();
    debug("scrmfs_stack_pop() gave %d\n",fid);
    if (fid < 0) {
        /* need to create a new file, but we can't */
        debug("scrmfs_stack_pop() failed (%d)\n",fid);
        errno = ENOSPC;
        return -1;
    }
    return fid;
}

/* add a new file and initialize metadata
 * returns the new fid, or negative value on error */
static int scrmfs_add_new_file(const char * path)
{
    int fid = scrmfs_get_slot_for_new_file();
    if (fid < 0)  {
        /* was there an error? if so, return it */
        return fid;
    }

    /* mark this slot as in use and copy the filename */
    scrmfs_filelist[fid].in_use = 1;
    strcpy((void *)&scrmfs_filelist[fid].filename, path);
    debug("Filename %s got scrmfs fd %d\n",scrmfs_filelist[fid].filename,fid);

    /* initialize meta data */
    scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
    meta->size = 0;
    meta->chunks = 0;
    meta->is_dir = 0;
    meta->flock_status = UNLOCKED;
    /* PTHREAD_PROCESS_SHARED allows Process-Shared Synchronization*/
    pthread_spin_init(&meta->fspinlock, PTHREAD_PROCESS_SHARED);
   
    return fid;
}

/* add a new directory and initialize metadata
 * returns the new fid, or a negative value on error */
static int scrmfs_add_new_directory(const char * path)
{
   int fid = scrmfs_add_new_file(path);
   if (fid < 0) {
       /* was there an error? if so, return it */
       return fid;
   }

   scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
   meta->is_dir = 1;
   return fid;
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
        int fid = scrmfs_add_new_directory(path);
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
        if (!scrmfs_is_dir(fid)) {
            errno = ENOTDIR;
            return -1;
        }

        /* is it empty? */
        if (!scrmfs_is_dir_empty(path)) {
            errno = ENOTEMPTY;
            return -1;
        }

        /* remove the directory from the file list */ 
        int ret = scrmfs_unlink_fid(fid);
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

        /* check that it's not a directory */
        if (scrmfs_is_dir(fid)) {
            /* ERROR: is a directory */
            debug("Attempting to unlink a directory %s in SCRMFS\n",path);
            errno = EISDIR;
            return -1;
        }

        /* delete the file */
        scrmfs_unlink_fid(fid);

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

        scrmfs_fill_in_stat_buf(fid, buf);

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
        scrmfs_fill_in_stat_buf(fid, buf);

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

int SCRMFS_DECL(creat)(const char* path, mode_t mode)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        return -1;
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
                                            scrmfs_superblock,free_fid_stack,free_chunk_stack,scrmfs_filelist,scrmfs_chunks);

                /* allocate a file id slot for this new file */
                fid = scrmfs_add_new_file(path);
                if (fid < 0){
                   // some error occured, return it
                   return fid;
                }

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

            /* if O_DIRECTORY is set and fid is not a directory, error */
            if ((flags & O_DIRECTORY) && !scrmfs_is_dir(fid)){
                errno = ENOTDIR;
                return -1;
            }
            /* if O_DIRECTORY is not set and fid is a directory, error */ 
            if (!(flags & O_DIRECTORY) && scrmfs_is_dir(fid)){
                errno = ENOTDIR;
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
        /* set in_use flag and file pointer */
        scrmfs_fds[fid].pos = pos;
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

        debug("seeking from %ld\n",scrmfs_fds[fd].pos);        
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

        debug("seeking to %ld\n",scrmfs_fds[fd].pos);        
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
    ssize_t ret;

    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);
       
        /* it's an error to read from a directory */
        if(scrmfs_is_dir(fid)){
           errno = EISDIR;
           return -1;
        }

        debug("Reading mfs file %d\n",fid);

        /* get a pointer to the file meta data structure */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        if (meta == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return -1;
        }

        /* get the current file pointer position */
        off_t oldpos = scrmfs_fds[fd].pos;

        /* check that we don't read past the end of the file */
        off_t newpos = oldpos + count;
        if (newpos > meta->size) {
            /* trying to read past the end of the file, so
             * adjust new file pointer position and count */
            newpos = meta->size;
            count = (size_t) (newpos - oldpos);
        }

        /* update position */
        scrmfs_fds[fd].pos = newpos;

        /* assume that we'll read all data */
        ret = count;

        /* get pointer to position within current chunk */
        int chunk_id = oldpos >> SCRMFS_CHUNK_BITS;
        off_t chunk_offset = oldpos & SCRMFS_CHUNK_MASK;

        void *chunk_buf = NULL;
        off_t spill_offset = 0;

        /* get offset in shmsegment or spillover */
        if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS) {
            chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, chunk_offset);
        } else {
            spill_offset = scrmfs_compute_spill_offset(meta, chunk_id, chunk_offset);
        }


        /* determine how many bytes remain in the current chunk */
        size_t remaining = SCRMFS_CHUNK_SIZE - chunk_offset;
        if (scrmfs_use_memfs){
            if (count <= remaining) {
                /* all bytes for this read fit within the current chunk */
                if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS) {
                    memcpy(buf, chunk_buf, count);
                } else {
                    __real_pread(scrmfs_spilloverblock, buf, count, spill_offset);
                }
            } else {
                /* read what's left of current chunk */
                char* ptr = (char*) buf;
                if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS) {
                    memcpy(ptr, chunk_buf, remaining);
                } else {
                    __real_pread(scrmfs_spilloverblock, ptr, remaining, spill_offset);
                }
                ptr += remaining;
   
                /* read from the next chunk */
                size_t read = remaining;
                while (read < count) {
                    /* get pointer to start of next chunk */
                    chunk_id++;

                    if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS) {
                        chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, 0);
                    } else {
                        spill_offset = scrmfs_compute_spill_offset(meta, chunk_id, 0);
                    }
   
                    /* compute size to read from this chunk */
                    size_t to_read = count - read;
                    if (to_read > SCRMFS_CHUNK_SIZE) {
                        to_read = SCRMFS_CHUNK_SIZE;
                    }
   
                    /* write data */
                    if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS) {
                        memcpy(ptr, chunk_buf, to_read);
                    } else {
                        __real_pread(scrmfs_spilloverblock, ptr, to_read, spill_offset);
                    }
                    ptr += to_read;

                    /* update number of bytes written */
                    read += to_read;
                }
            }
        }
        else if(scrmfs_use_containers){
            if (count <= remaining) {
                /* all bytes for this read fit within the current chunk */
                cs_container_handle_t ch = 
                           meta->chunk_meta[chunk_id].container_data.cs_container_handle;
                size_t memcount = 1;
                size_t memsizes = count;
                size_t filecount = 1;
                cs_off_t fileofs = chunk_offset;
                cs_off_t filesizes = count;
                cs_off_t transferred = 0;
               
                int ret = cs_container_read (ch, memcount, &buf, &memsizes, filecount,
                               &fileofs, &filesizes, &transferred); 
                if (ret != CS_SUCCESS){
                   debug("container read failed\n");
                   return -1;
                }
                debug("container read succeeded\n");
            } else {
                /* read what's left of current chunk */
                char* ptr = (char*) buf;
                size_t memcount = 1;
                size_t memsizes = remaining;
                size_t filecount = 1;
                cs_off_t fileofs = chunk_offset;
                cs_off_t filesizes = remaining;
                cs_off_t transferred = 0;
                cs_container_handle_t ch = 
                           meta->chunk_meta[chunk_id].container_data.cs_container_handle;
               
                int ret = cs_container_read (ch, memcount, (void**)&ptr, &memsizes, filecount,
                               &fileofs, &filesizes, &transferred); 
                if (ret != CS_SUCCESS){
                   debug("container read failed\n");
                   return -1;
                }
                debug("container read succeeded\n");
                ptr += remaining;
                
                /* read from the next chunk */
                size_t read = remaining;
                while (read < count) {
                    /* get pointer to start of next chunk */
                    chunk_id++;

                    /* compute size to read from this chunk */
                    size_t to_read = count - read;
                    if (to_read > SCRMFS_CHUNK_SIZE) {
                        to_read = SCRMFS_CHUNK_SIZE;
                    }

                    /* read data */
                    ptr = (char*) buf;
                    memcount = 1;
                    memsizes = to_read;
                    filecount = 1;
                    fileofs = 0;
                    filesizes = to_read;
                    transferred = 0;
                   
                    ch = meta->chunk_meta[chunk_id].container_data.cs_container_handle;
                    ret = cs_container_read (ch, memcount, (void**)&ptr, &memsizes, filecount,
                                   &fileofs, &filesizes, &transferred); 
                    if (ret != CS_SUCCESS){
                       debug("container read failed\n");
                       return -1;
                    }
                    debug("container read succeeded\n");
                    ptr += to_read;
 
                    /* update number of bytes read */
                    read += to_read;
                }
            }
        }
    } else {
        MAP_OR_FAIL(read);
        ret = __real_read(fd, buf, count);
    }

    return ret;
}

/* TODO: find right place to msync spillover mapping */
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

        /* it's an error to write to a directory */
        if(scrmfs_is_dir(fid)){
           errno = EINVAL;
           return -1;
        }

        /* get a pointer to the file meta data structure */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        if (meta == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return -1;
        }
 
        /* Raghu: Added a new struct to store positions of active fids.
         * This struct is not part of superblock - doesn't have to be persistent */
        off_t oldpos = scrmfs_fds[fd].pos;
        off_t newpos = oldpos + count;
        scrmfs_fds[fd].pos = newpos;

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
                while (additional > 0) {
                    /* allocate a new chunk */
                    SCRMFS_STACK_LOCK();
                    int id = scrmfs_stack_pop(free_chunk_stack);
                    SCRMFS_STACK_UNLOCK();

                    /* shmsegment out of space, grab a block from spill-over device */
                    if (id < 0 && scrmfs_use_spillover) {
                        debug("getting blocks from spill-over device (%d)\n", id);

                        /* memfs out of chunks, get blocks from spillover dev */
                        /* add SCRMFS_MAX_CHUNKS to identify chunk location */
                        id = scrmfs_stack_pop(free_spillchunk_stack) + SCRMFS_MAX_CHUNKS;
                        if (id < SCRMFS_MAX_CHUNKS) {
                            debug("spill-over device out of space (%d)\n", id);
                            errno = ENOSPC;
                            return -1;
                        }
                    }
                    else if (id < 0 && !scrmfs_use_spillover) {
                        debug("memfs out of space (%d)\n", id);
                        errno = ENOSPC;
                        return -1;
                    }

                    /* add it to the chunk list of this file */
                    meta->chunk_ids[meta->chunks] = id;

                    if(scrmfs_use_containers){
                        char prefix[100];
                        int create = 1;
                        int created = 0;
                        size_t size = 1 << SCRMFS_CHUNK_BITS;
                        meta->chunk_meta[meta->chunks].location = CONTAINER;
                        cs_container_handle_t *ch = 
                             &(meta->chunk_meta[meta->chunks].container_data.cs_container_handle);
                        sprintf(prefix,"fid_%d_chunk_%d", fid, id);

                        int ret = cs_set_container_open(cs_set_handle, prefix, size, 
                                      create, &created, ch);
                        if (ret != CS_SUCCESS){
                           debug("creation of container for %s failed: %d\n",prefix, ret);
                           return -1;
                        }
                        debug("creation of container for %s succeeded\n", prefix);
                    }
                    else{
                        /* set location based on chunk ID obtained */
                        if (id >= SCRMFS_MAX_CHUNKS) {
                            meta->chunk_meta[meta->chunks].location = SPILLOVER_DEV;
                        }
                        else {
                            meta->chunk_meta[meta->chunks].location = MEMFS;
                        }
                    }

                    meta->chunks++;
                    /* subtract bytes from the number we need */
                    additional -= SCRMFS_CHUNK_SIZE;
                }
            }
        }

        /* get pointer to position within current chunk */
        int chunk_id = oldpos >> SCRMFS_CHUNK_BITS;
        off_t chunk_offset = oldpos & SCRMFS_CHUNK_MASK;

        /* determine how many bytes remain in the current chunk */
        size_t remaining = SCRMFS_CHUNK_SIZE - chunk_offset;
        if (scrmfs_use_memfs){

            void *chunk_buf = NULL;
            off_t spill_offset = 0;

            if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS) {
                chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, chunk_offset);
            } else {
                spill_offset = scrmfs_compute_spill_offset(meta, chunk_id, chunk_offset);
            }

            if (count <= remaining) {
                /* all bytes for this write fit within the current chunk */
                if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS ) {
                    memcpy(chunk_buf, buf, count);
                } else {
                    __real_pwrite(scrmfs_spilloverblock, buf, count, spill_offset);
                }
                 
//                _intel_fast_memcpy(chunk_buf, buf, count);
//                scrmfs_memcpy(chunk_buf, buf, count);
            } else {
                /* otherwise, fill up the remainder of the current chunk */
                char* ptr = (char*) buf;
                if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS ) {
                    memcpy(chunk_buf, ptr, remaining);
                } else {
                    __real_pwrite(scrmfs_spilloverblock, buf, remaining, spill_offset);
                }
                
//                _intel_fast_memcpy(chunk_buf, ptr, remaining);
//                scrmfs_memcpy(chunk_buf, ptr, remaining);
                ptr += remaining;
   
                /* then write the rest of the bytes starting from beginning
                 * of chunks */
                size_t written = remaining;
                while (written < count) {
                    /* get pointer to start of next chunk */
                    chunk_id++;

                    if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS ) {
                        chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, 0);
                    } else {
                        spill_offset = scrmfs_compute_spill_offset(meta, chunk_id, 0);
                    }
                    /* compute size to write to this chunk */
                    size_t nwrite = count - written;
                    if (nwrite > SCRMFS_CHUNK_SIZE) {
                      nwrite = SCRMFS_CHUNK_SIZE;
                    }
   
                    /* write data */

                    if ( meta->chunk_ids[chunk_id] < SCRMFS_MAX_CHUNKS ) {
                        memcpy(chunk_buf, ptr, nwrite);
                    } else {
                        __real_pwrite(scrmfs_spilloverblock, ptr, nwrite, spill_offset);
                    }
//                    _intel_fast_memcpy(chunk_buf, ptr, nwrite);
//                    scrmfs_memcpy(chunk_buf, ptr, nwrite);
                    ptr += nwrite;
   
                    /* update number of bytes written */
                    written += nwrite;
                }
            }
        }
        
        else if(scrmfs_use_containers){
           if (count <= remaining) {
               size_t memsizes = count;
               cs_off_t fileofs = chunk_offset;
               cs_off_t filesizes = count;
               cs_off_t transferred;
               cs_container_handle_t ch = 
                          meta->chunk_meta[chunk_id].container_data.cs_container_handle;
               /* all bytes for this write fit within the current container */
               int ret = cs_container_write (ch, 1, &buf, &memsizes, 1, 
                                  &fileofs, &filesizes, &transferred);

               if (ret != CS_SUCCESS){
                  debug("container write failed for single container write: %d\n");
                  return -1;
               }
               debug("container write was successful\n");
           }
           else {
               /* otherwise, fill up the remainder of the current container */
               char* ptr = (char*) buf;
               //memcpy(chunk_buf, ptr, remaining);
               size_t memsizes = remaining;
               cs_off_t fileofs = chunk_offset;
               cs_off_t filesizes = remaining;
               cs_off_t transferred;
               cs_container_handle_t ch = 
                          meta->chunk_meta[chunk_id].container_data.cs_container_handle;
               int ret = cs_container_write (ch, 1, (const void**)&ptr, &memsizes, 1, 
                                  &fileofs, &filesizes, &transferred);

               if (ret != CS_SUCCESS){
                  debug("container write failed for single container write: %d\n");
                  return -1;
               }
               ptr += remaining;
               debug("container write was successful\n");

               /* then write the rest of the bytes starting from beginning
                * of chunks */
               size_t written = remaining;
               while (written < count) {
                   chunk_id++;

                   /* compute size to write to this chunk */
                   size_t nwrite = count - written;
                   if (nwrite > SCRMFS_CHUNK_SIZE) {
                     nwrite = SCRMFS_CHUNK_SIZE;
                   }

                   /* write data */
                   //memcpy(chunk_buf, ptr, nwrite);
                   memsizes = nwrite;
                   fileofs = 0;
                   filesizes = nwrite;
                   ch = meta->chunk_meta[chunk_id].container_data.cs_container_handle;
                   int ret = cs_container_write (ch, 1, (const void**)&ptr, &memsizes, 1, 
                                  &fileofs, &filesizes, &transferred);
    
                   if (ret != CS_SUCCESS){
                      debug("container write failed for single container write: %d\n");
                      return -1;
                   }
                   ptr += nwrite;
                   debug("container write was successful\n");

                   /* update number of bytes written */
                   written += nwrite;

               }
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
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
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
    /* check whether we should intercept this path */
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
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

int SCRMFS_DECL(fsync)(int fd)
{
    int intercept;
    scrmfs_intercept_fd(&fd, &intercept);
    if (intercept) {

        /* TODO: check that fd is actually in use */

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

        /* get the file meta for this file descriptor */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);

        /* allocate memory required to mmap the data if addr is NULL;
         * using posix_memalign instead of malloc to align mmap'ed area
         * to page size */
        if ( ! addr ) {
            int ret = posix_memalign( &addr, sysconf(_SC_PAGE_SIZE), length);
            if ( ret ) {
                /* posix_memalign does not set errno */
                if ( ret == EINVAL ) {
                    errno = EINVAL;
                    return MAP_FAILED;
                }

                if ( ret = ENOMEM ) {
                    errno = ENOMEM;
                    return MAP_FAILED;
                }
            }
            
        }

        /* check that we don't copy past the end of the file */
        off_t total_length = offset + length;
 
        if (total_length > meta->size) {
            /* trying to copy past the end of the file, so
             * adjust the total amount to be copied */
            total_length = meta->size;
            length = (size_t) (total_length - offset);
        }

        /* get pointer to position within current chunk */
        int chunk_id = offset >> SCRMFS_CHUNK_BITS;
        off_t chunk_offset = offset & SCRMFS_CHUNK_MASK;
        void* chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, chunk_offset);

        /* determine how many bytes remain in the current chunk */
        size_t remaining = SCRMFS_CHUNK_SIZE - chunk_offset;
        if (length <= remaining) {
            /* all bytes for this write fit within the current chunk */
            memcpy(addr, chunk_buf, length);
        } else {
            /* copy what's left of current chunk */
            char* ptr = (char*) addr;
            memcpy(ptr, chunk_buf, remaining);
            ptr += remaining;

            /* read from the next chunk */
            size_t read = remaining;
            while (read < length) {
                /* get pointer to start of next chunk */
                chunk_id++;
                chunk_buf = scrmfs_compute_chunk_buf(meta, chunk_id, 0);

                /* compute size to read from this chunk */
                size_t to_read = length - read;
                if (to_read > SCRMFS_CHUNK_SIZE) {
                    to_read = SCRMFS_CHUNK_SIZE;
                }

                /* copy data */
                memcpy(ptr, chunk_buf, to_read);
                ptr += to_read;

                /* update number of bytes written */
                read += to_read;
            }
        }

        return addr;
    } else {
        MAP_OR_FAIL(mmap);
        void* ret = __real_mmap(addr, length, prot, flags, fd, offset);
        return ret;
    }
}

int munmap(void *addr, size_t length)
{
    fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
    errno = ENOSYS;
    return ENODEV;
}

int msync(void *addr, size_t length, int flags)
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
        /* TODO: what to do if underlying file has been deleted? */

        /* TODO: check that fd is actually in use */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            errno = EBADF;
            return -1;
        }

        /* TODO: free file descriptor */

        debug("closing fd %d\n",fd);
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

FILE* SCRMFS_DECL(fopen)(const char *path, const char *mode)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOENT;
        return NULL;
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

size_t SCRMFS_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    MAP_OR_FAIL(fread);
    size_t ret = __real_fread(ptr, size, nmemb, stream);
    return(ret);
}

size_t SCRMFS_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    MAP_OR_FAIL(fwrite);
    size_t ret = __real_fwrite(ptr, size, nmemb, stream);
    return ret;
}

int SCRMFS_DECL(fseek)(FILE *stream, long offset, int whence)
{
    MAP_OR_FAIL(fseek);
    int ret = __real_fseek(stream, offset, whence);
    return ret;
}

int SCRMFS_DECL(fclose)(FILE *fp)
{
    //int tmp_fd = fileno(fp);
    MAP_OR_FAIL(fclose);
    int ret = __real_fclose(fp);
    return ret;
}
