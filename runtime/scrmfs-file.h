#include "uthash.h"
#include "scrmfs-defs.h"

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
    off_t size;   /* current file size */
    off_t chunks; /* number of chunks currently allocated to file */
    off_t chunk_ids[SCRMFS_MAX_CHUNKS]; /* chunk offset location in the mem pool */
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


