#include "uthash.h"
#include "scrmfs-defs.h"

typedef struct
{
    int fd;
    off_t pos;
} active_fds_t;

typedef struct
{
    off_t size;   /* current file size */
    off_t chunks; /* number of chunks currently allocated to file */
    off_t chunk_ids[SCRMFS_MAX_CHUNKS]; /* chunk offset location in the mem pool */
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
} chunk_t;

/* legacy structures from the dynamic mem design */

typedef struct
{
        struct scr_chunk_t* next;
        unsigned long offset; /* offset in file */
        unsigned long size;
        unsigned long written_size;
        unsigned long current_pos;
        void* buf;
}scr_chunk_t;

typedef struct
{
        int fd;
        int orig_fd;
        key_t shm_key;
        const char* filename;
        unsigned long size;
        unsigned long written_size; /* Size presently written - used to derive chunk offsets */ 
        unsigned long current_offset;
        UT_hash_handle hh;
        scr_chunk_t* chunk_list;
} scr_file_t;
