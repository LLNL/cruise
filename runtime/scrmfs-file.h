#include "uthash.h"
#include "scrmfs-defs.h"

/* path to fid lookup struct */
typedef struct
{
    int in_use;
    int real_fd;
    const char filename[SCRMFS_MAX_FILENAME];
    off_t chunk_map_offset;
} filename_to_chunk_map;

typedef struct
{
    off_t pos;    /* current file position: TODO: move this to a file descriptor */
    off_t size;   /* current file size */
    off_t chunks; /* number of chunks currently allocated to file */
    off_t current_chunk;
    int current_index;
    off_t chunk_offset[SCRMFS_MAX_CHUNKS]; /* chunk offset location in the mem pool */
} chunk_map_t;

typedef struct
{
    char buf[SCRMFS_CHUNK_SIZE];
    int in_use;
    off_t written_size;
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
}scr_file_t;

