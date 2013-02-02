#ifndef SCRMFS_INTERNAL_H
#define SCRMFS_INTERNAL_H

/* this is overkill to include all of these here, but just to get things working... */
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

#include "scrmfs.h"
//#include "scrmfs-file.h"
#include "scrmfs-defs.h"
#include "scrmfs-sysio.h"
#include "scrmfs-stdio.h"
#include "scrmfs-stack.h"

#include "utlist.h"
#include "uthash.h"

//#define SCRMFS_DEBUG
#ifdef SCRMFS_DEBUG
    #define debug(fmt, args... )  printf("%s: "fmt, __func__, ##args)
#else
    #define debug(fmt, args... )
#endif

#ifdef SCRMFS_PRELOAD

    /* ===================================================================
     * Using LD_PRELOAD to intercept
     * ===================================================================
     * we need to use the same function names the application is calling,
     * and we then invoke the real library function after looking it up with
     * dlsym */

    /* we need the dlsym function */
    #define __USE_GNU
    #include <dlfcn.h>
    #include <stdlib.h>

    /* define a static variable called __real_open to record address of
     * real open call and initialize it to NULL */
    #define SCRMFS_FORWARD_DECL(name,ret,args) \
      static ret (*__real_ ## name)args = NULL;

    /* our open wrapper assumes the name of open() */
    #define SCRMFS_DECL(__name) __name

    /* if __real_open is still NULL, call dlsym to lookup address of real
     * function and record it */
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

    /* ===================================================================
     * Using ld -wrap option to intercept
     * ===================================================================
     * the linker will convert application calls from open --> __wrap_open,
     * so we define all of our functions as the __wrap variant and then
     * to call the real library, we call __real_open */

    /* we don't need a variable to record the address of the real function,
     * just declare the existence of __real_open so the compiler knows the
     * prototype of this function (linker will provide it) */
    #define SCRMFS_FORWARD_DECL(name,ret,args) \
      extern ret __real_ ## name args;

    /* we define our wrapper function as __wrap_open instead of open */
    #define SCRMFS_DECL(__name) __wrap_ ## __name

    /* no need to look up the address of the real function */
    #define MAP_OR_FAIL(func)

#endif

#define SCRMFS_SUCCESS     0
#define SCRMFS_FAILURE    -1
#define SCRMFS_ERR_NOSPC  -2
#define SCRMFS_ERR_IO     -3
#define SCRMFS_ERR_NAMETOOLONG -4
#define SCRMFS_ERR_NOENT  -5
#define SCRMFS_ERR_EXIST  -6
#define SCRMFS_ERR_NOTDIR -7
#define SCRMFS_ERR_NFILE  -8
#define SCRMFS_ERR_INVAL  -9
#define SCRMFS_ERR_OVERFLOW -10
#define SCRMFS_ERR_FBIG   -11
#define SCRMFS_ERR_BADF   -12
#define SCRMFS_ERR_ISDIR  -13
#define SCRMFS_ERR_NOMEM  -14

#ifndef HAVE_OFF64_T
typedef int64_t off64_t;
#endif

enum flock_enum {
    UNLOCKED,
    EX_LOCKED,
    SH_LOCKED
};

/* structure to represent file descriptors */
typedef struct {
    off_t pos;   /* current file pointer */
    int   read;  /* whether file is opened for read */
    int   write; /* whether file is opened for write */
} scrmfs_fd_t;

enum scrmfs_stream_orientation {
    SCRMFS_STREAM_ORIENTATION_NULL = 0,
    SCRMFS_STREAM_ORIENTATION_BYTE,
    SCRMFS_STREAM_ORIENTATION_WIDE,
};

/* structure to represent FILE* streams */
typedef struct {
    int    err;      /* stream error indicator flag */
    int    eof;      /* stream end-of-file indicator flag */
    int    fd;       /* file descriptor associated with stream */
    int    append;   /* whether file is opened in append mode */
    int    orient;   /* stream orientation, SCRMFS_STREAM_ORIENTATION_{NULL,BYTE,WIDE} */

    void*  buf;      /* pointer to buffer */
    int    buffree;  /* whether we need to free buffer */
    int    buftype;  /* _IOFBF fully buffered, _IOLBF line buffered, _IONBF unbuffered */
    size_t bufsize;  /* size of buffer in bytes */
    off_t  bufpos;   /* byte offset in file corresponding to start of buffer */
    size_t buflen;   /* number of bytes active in buffer */
    size_t bufdirty; /* whether data in buffer needs to be flushed */

    unsigned char* ubuf; /* ungetc buffer (we store bytes from end) */
    size_t ubufsize;     /* size of ungetc buffer in bytes */
    size_t ubuflen;      /* number of active bytes in buffer */

    unsigned char* _p; /* pointer to character in buffer */
    size_t         _r; /* number of bytes left at pointer */
} scrmfs_stream_t;

#ifdef HAVE_CONTAINER_LIB
typedef struct {
     cs_container_handle_t*  cs_container_handle;
     off_t container_size;
} scrmfs_container_t;
#endif /* HAVE_CONTAINER_LIB */

/* TODO: make this an enum */
#define CHUNK_LOCATION_NULL      0
#define CHUNK_LOCATION_MEMFS     1
#define CHUNK_LOCATION_CONTAINER 2
#define CHUNK_LOCATION_SPILLOVER 3

typedef struct {
    int location; /* CHUNK_LOCATION specifies how chunk is stored */
    off_t id;     /* physical id of chunk in its respective storage */
} scrmfs_chunkmeta_t;

#define FILE_STORAGE_NULL        0
#define FILE_STORAGE_FIXED_CHUNK 1
#define FILE_STORAGE_CONTAINER   2

typedef struct {
    off_t size;                     /* current file size */
    off_t chunks;                   /* number of chunks allocated to file */
    scrmfs_chunkmeta_t* chunk_meta; /* meta data for chunks */
    int is_dir;                     /* is this file a directory */
    int storage;                    /* FILE_STORAGE specifies file data management */
    pthread_spinlock_t fspinlock;
    enum flock_enum flock_status;
    #ifdef HAVE_CONTAINER_LIB
    scrmfs_container_t container_data;
    char * filename;
    #endif /* HAVE_CONTAINER_LIB */
} scrmfs_filemeta_t;

/* path to fid lookup struct */
typedef struct {
    int in_use; /* flag incidating whether slot is in use */
    const char filename[SCRMFS_MAX_FILENAME];
                /* full path and name of file */
} scrmfs_filename_t;



/* keep track of what we've initialized */
extern int scrmfs_initialized;

/* list of file names */
extern scrmfs_filename_t* scrmfs_filelist;

/* mount directory */
extern char*  scrmfs_mount_prefix;
extern size_t scrmfs_mount_prefixlen;

/* array of file descriptors */
extern scrmfs_fd_t scrmfs_fds[SCRMFS_MAX_FILEDESCS];
extern rlim_t scrmfs_fd_limit;

/* array of file streams */
extern scrmfs_stream_t scrmfs_streams[SCRMFS_MAX_FILEDESCS];

/* returns 1 if two input parameters will overflow their type when
 * added together */
int scrmfs_would_overflow_offt(off_t a, off_t b);

/* returns 1 if two input parameters will overflow their type when
 * added together */
int scrmfs_would_overflow_long(long a, long b);

/* given an input mode, mask it with umask and return, can specify
 * an input mode==0 to specify all read/write bits */
mode_t scrmfs_getmode(mode_t perms);

/* sets flag if the path is a special path */
int scrmfs_intercept_path(const char* path);

/* given an fd, return 1 if we should intercept this file, 0 otherwise,
 * convert fd to new fd value if needed */
void scrmfs_intercept_fd(int* fd, int* intercept);

/* given a FILE*, returns 1 if we should intercept this file,
 * 0 otherwise */
int scrmfs_intercept_stream(FILE* stream);

/* given a path, return the file id */
int scrmfs_get_fid_from_path(const char* path);

/* given a file descriptor, return the file id */
int scrmfs_get_fid_from_fd(int fd);

/* return address of file descriptor structure or NULL if fd is out
 * of range */
scrmfs_fd_t* scrmfs_get_filedesc_from_fd(int fd);

/* given a file id, return a pointer to the meta data,
 * otherwise return NULL */
inline scrmfs_filemeta_t* scrmfs_get_meta_from_fid(int fid);

scrmfs_chunkmeta_t* scrmfs_get_chunkmeta(int fid, int cid);

/* given an SCRMFS error code, return corresponding errno code */
int scrmfs_err_map_to_errno(int rc);



/* checks to see if fid is a directory
 * returns 1 for yes
 * returns 0 for no */
int scrmfs_fid_is_dir(int fid);

/* checks to see if a directory is empty
 * assumes that check for is_dir has already been made
 * only checks for full path matches, does not check relative paths,
 * e.g. ../dirname will not work
 * returns 1 for yes it is empty
 * returns 0 for no */
int scrmfs_fid_is_dir_empty(const char * path);

/* return current size of given file id */
off_t scrmfs_fid_size(int fid);

/* fill in limited amount of stat information */
int scrmfs_fid_stat(int fid, struct stat* buf);

/* allocate a file id slot for a new file 
 * return the fid or -1 on error */
int scrmfs_fid_alloc();

/* return the file id back to the free pool */
int scrmfs_fid_free(int fid);

/* add a new file and initialize metadata
 * returns the new fid, or negative value on error */
int scrmfs_fid_create_file(const char * path);

/* add a new directory and initialize metadata
 * returns the new fid, or a negative value on error */
int scrmfs_fid_create_directory(const char * path);

/* read count bytes from file starting from pos and store into buf,
 * all bytes are assumed to exist, so checks on file size should be
 * done before calling this routine */
int scrmfs_fid_read(int fid, off_t pos, void* buf, size_t count);

/* write count bytes from buf into file starting at offset pos,
 * all bytes are assumed to be allocated to file, so file should
 * be extended before calling this routine */
int scrmfs_fid_write(int fid, off_t pos, const void* buf, size_t count);

/* given a file id, write zero bytes to region of specified offset
 * and length, assumes space is already reserved */
int scrmfs_fid_write_zero(int fid, off_t pos, off_t count);

/* increase size of file if length is greater than current size,
 * and allocate additional chunks as needed to reserve space for
 * length bytes */
int scrmfs_fid_extend(int fid, off_t length);

/* truncate file id to given length, frees resources if length is
 * less than size and allocates and zero-fills new bytes if length
 * is more than size */
int scrmfs_fid_truncate(int fid, off_t length);

/* opens a new file id with specified path, access flags, and permissions,
 * fills outfid with file id and outpos with position for current file pointer,
 * returns SCRMFS error code */
int scrmfs_fid_open(const char* path, int flags, mode_t mode, int* outfid, off_t* outpos);

int scrmfs_fid_close(int fid);

/* delete a file id and return file its resources to free pools */
int scrmfs_fid_unlink(int fid);

#endif /* SCRMFS_INTERNAL_H */