#include "uthash.h"
#include "scrmfs-defs.h"

#include<container.h>

enum flock_enum
{
    UNLOCKED,
    EX_LOCKED,
    SH_LOCKED
};

typedef struct
{
    off_t pos;
} scrmfs_fd_t;

typedef struct
{
    int chunk_id;
    struct chunk_list_t *next;
} chunk_list_t;


typedef struct{
     cs_container_handle_t  cs_container_handle;
} scrmfs_container_t;

#define MEMFS 0
#define CONTAINER 1 
typedef struct{
    int location;
    scrmfs_container_t container_data;
} scrmfs_chunkmeta_t;

typedef struct
{
    off_t size;   /* current file size */
    off_t chunks; /* number of chunks currently allocated to file */
    off_t chunk_ids[SCRMFS_MAX_CHUNKS]; /* offset to chunk in the mem pool */
    scrmfs_chunkmeta_t chunk_meta[SCRMFS_MAX_CHUNKS]; /* meta data for chunks */
    int is_dir;  /* is this file a directory */
    pthread_spinlock_t fspinlock;
    enum flock_enum flock_status;
} scrmfs_filemeta_t;


/* path to fid lookup struct */
typedef struct
{
    int in_use;
    const char filename[SCRMFS_MAX_FILENAME];
} scrmfs_filename_t;

typedef struct
{
    char buf[SCRMFS_CHUNK_SIZE];
    /* location: memory or file */
    /* compression type / size */
} chunk_t;


