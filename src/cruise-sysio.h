/*
 * Copyright (c) 2014, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by
 *   Raghunath Rajachandrasekar <rajachan@cse.ohio-state.edu>
 *   Kathryn Mohror <kathryn@llnl.gov>
 *   Adam Moody <moody20@llnl.gov>
 * LLNL-CODE-642432.
 * All rights reserved.
 * This file is part of CRUISE.
 * For details, see https://github.com/hpc/cruise
 * Please also read this file COPYRIGHT
*/

#ifndef CRUISE_SYSIO_H
#define CRUISE_SYSIO_H

#include "cruise-internal.h"

/* read count bytes info buf from file starting at offset pos,
 * returns number of bytes actually read in retcount,
 * retcount will be less than count only if an error occurs
 * or end of file is reached */
int cruise_fd_read(int fd, off_t pos, void* buf, size_t count, size_t* retcount);

/* write count bytes from buf into file starting at offset pos,
 * allocates new bytes and updates file size as necessary,
 * fills any gaps with zeros */
int cruise_fd_write(int fd, off_t pos, const void* buf, size_t count);

#endif /* CRUISE_SYSIO_H */
