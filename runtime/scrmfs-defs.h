#define SCRMFS_MAX_FILES        ( 128 )

/* eventually could decouple these so there could be
 * more or less file descriptors than files, but for
 * now they're the same */
#define SCRMFS_MAX_FILEDESCS    ( SCRMFS_MAX_FILES )

#define SCRMFS_MAX_FILENAME     ( 128 )
#define SCRMFS_MAX_MEM          ( 1 * 1024 * 1024 * 1024 )

#define SCRMFS_CHUNK_BITS       ( 26 )
#define SCRMFS_CHUNK_SIZE       ( 1 << SCRMFS_CHUNK_BITS )
#define SCRMFS_CHUNK_MASK       ( SCRMFS_CHUNK_SIZE - 1 )
#define SCRMFS_MAX_CHUNKS       ( SCRMFS_MAX_MEM >> SCRMFS_CHUNK_BITS )

#define SCRMFS_SUPERBLOCK_KEY   ( 1234 )
