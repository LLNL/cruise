#include "uthash.h"
#include "scrmfs-defs.h"

#ifdef HAVE_CONTAINER_LIB
#include<container.h>
#endif /* HAVE_CONTAINER_LIB */

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

enum flock_enum {
    UNLOCKED,
    EX_LOCKED,
    SH_LOCKED
};

/* structure to represent file descriptors */
typedef struct {
    off_t pos;     /* current file pointer */
} scrmfs_fd_t;

/* structure to represent FILE* streams */
typedef struct {
    int err;       /* stream error indicator flag */
    int eof;       /* stream end-of-file indicator flag */
    int fd;        /* file descriptor associated with stream */
} scrmfs_stream_t;

typedef struct {
    int chunk_id;
    struct chunk_list_t *next;
} chunk_list_t;

#ifdef HAVE_CONTAINER_LIB
typedef struct {
     cs_container_handle_t  cs_container_handle;
} scrmfs_container_t;
#endif /* HAVE_CONTAINER_LIB */

#define CHUNK_LOCATION_NULL      0
#define CHUNK_LOCATION_MEMFS     1
#define CHUNK_LOCATION_CONTAINER 2
#define CHUNK_LOCATION_SPILLOVER 3

typedef struct {
    int location;
    off_t id;
    #ifdef HAVE_CONTAINER_LIB
    scrmfs_container_t container_data;
    #endif /* HAVE_CONTAINER_LIB */
} scrmfs_chunkmeta_t;

typedef struct {
    off_t size;   /* current file size */
    off_t chunks; /* number of chunks currently allocated to file */
    scrmfs_chunkmeta_t chunk_meta[SCRMFS_MAX_CHUNKS]; /* meta data for chunks */
    int is_dir;  /* is this file a directory */
    pthread_spinlock_t fspinlock;
    enum flock_enum flock_status;
} scrmfs_filemeta_t;

/* path to fid lookup struct */
typedef struct {
    int in_use;
    const char filename[SCRMFS_MAX_FILENAME];
} scrmfs_filename_t;

typedef struct {
    char buf[SCRMFS_CHUNK_SIZE];
    /* location: memory or file */
    /* compression type / size */
} chunk_t;
