#ifndef SCRMFS_FIXED_H
#define SCRMFS_FIXED_H

#include "scrmfs-internal.h"

/* if length is greater than reserved space,
 * reserve space up to length */
int scrmfs_fid_store_fixed_extend(
  int fid,                 /* file id to reserve space for */
  scrmfs_filemeta_t* meta, /* meta data for file */
  off_t length             /* number of bytes to reserve for file */
);

/* if length is shorter than reserved space,
 * give back space down to length */
int scrmfs_fid_store_fixed_shrink(
  int fid,                 /* file id to free space for */
  scrmfs_filemeta_t* meta, /* meta data for file */
  off_t length             /* number of bytes to reserve for file */
);

/* read data from file stored as fixed-size chunks,
 * returns SCRMFS error code */
int scrmfs_fid_store_fixed_read(
  int fid,                 /* file id to read from */
  scrmfs_filemeta_t* meta, /* meta data for file */
  off_t pos,               /* position within file to read from */
  void* buf,               /* user buffer to store data in */
  size_t count             /* number of bytes to read */
);

/* write data to file stored as fixed-size chunks,
 * returns SCRMFS error code */
int scrmfs_fid_store_fixed_write(
  int fid,                 /* file id to write to */
  scrmfs_filemeta_t* meta, /* meta data for file */
  off_t pos,               /* position within file to write to */
  const void* buf,         /* user buffer holding data */
  size_t count             /* number of bytes to write */
);

#endif /* SCRMFS_FIXED_H */
