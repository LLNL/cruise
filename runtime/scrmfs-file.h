#include "uthash.h"
#include "scrmfs-defs.h"

/* path to fid lookup struct */
typedef struct
{
    int in_use_flag;
    const char *filename;
    int datablock_index;
}filename_to_datablock_map;

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

