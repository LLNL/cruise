#ifndef SCRMFS_SYSIO_H
#define SCRMFS_SYSIO_H

#include "scrmfs-internal.h"

/* read count bytes info buf from file starting at offset pos,
 * returns number of bytes actually read in retcount,
 * retcount will be less than count only if an error occurs
 * or end of file is reached */
int scrmfs_fd_read(int fd, off_t pos, void* buf, size_t count, size_t* retcount);

/* write count bytes from buf into file starting at offset pos,
 * allocates new bytes and updates file size as necessary,
 * fills any gaps with zeros */
int scrmfs_fd_write(int fd, off_t pos, const void* buf, size_t count);

#endif /* SCRMFS_SYSIO_H */
