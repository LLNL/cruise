#define SCRMFS_MAX_FILES        ( 128 )

/* eventually could decouple these so there could be
 * more or less file descriptors than files, but for
 * now they're the same */
#define SCRMFS_MAX_FILEDESCS    ( SCRMFS_MAX_FILES )

#define SCRMFS_MAX_FILENAME     ( 128 )

#define SCRMFS_CHUNK_BITS       ( 24 )

#ifdef MACHINE_BGQ
  #define SCRMFS_CHUNK_MEM      ( 64 * 1024 * 1024 )
#else /* MACHINE_BGQ */
  #define SCRMFS_CHUNK_MEM      ( 256 * 1024 * 1024 )
#endif /* MACHINE_BGQ */

#define SCRMFS_SPILLOVER_SIZE   ( 1 * 1024 * 1024 * 1024 )

#define SCRMFS_SUPERBLOCK_KEY   ( 4321 )
