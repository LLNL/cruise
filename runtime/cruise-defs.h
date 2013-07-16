#define CRUISE_MAX_FILES        ( 128 )

/* eventually could decouple these so there could be
 * more or less file descriptors than files, but for
 * now they're the same */
#define CRUISE_MAX_FILEDESCS    ( CRUISE_MAX_FILES )

#define CRUISE_MAX_FILENAME     ( 128 )

#define CRUISE_STREAM_BUFSIZE   ( 1 * 1024 * 1024 )

#define CRUISE_CHUNK_BITS       ( 24 )

#ifdef MACHINE_BGQ
  #define CRUISE_CHUNK_MEM      ( 64 * 1024 * 1024 )
#else /* MACHINE_BGQ */
  #define CRUISE_CHUNK_MEM      ( 256 * 1024 * 1024 )
#endif /* MACHINE_BGQ */

#define CRUISE_SPILLOVER_SIZE   ( 1 * 1024 * 1024 * 1024 )

#define CRUISE_SUPERBLOCK_KEY   ( 4321 )
