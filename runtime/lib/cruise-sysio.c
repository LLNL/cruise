#include "scrmfs-runtime-config.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <search.h>
#include <assert.h>
#include <libgen.h>
#include <limits.h>
#define __USE_GNU
#include <pthread.h>

#include "scrmfs-internal.h"

/* ---------------------------------------
 * POSIX wrappers: paths
 * --------------------------------------- */

SCRMFS_DECL(access, int, (const char *pathname, int mode));
SCRMFS_DECL(mkdir, int, (const char *path, mode_t mode));
SCRMFS_DECL(rmdir, int, (const char *path));
SCRMFS_DECL(unlink, int, (const char *path));
SCRMFS_DECL(remove, int, (const char *path));
SCRMFS_DECL(rename, int, (const char *oldpath, const char *newpath));
SCRMFS_DECL(truncate, int, (const char *path, off_t length));
SCRMFS_DECL(stat, int,( const char *path, struct stat *buf));
SCRMFS_DECL(__lxstat, int, (int vers, const char* path, struct stat *buf));
SCRMFS_DECL(__lxstat64, int, (int vers, const char* path, struct stat64 *buf));
SCRMFS_DECL(__xstat, int, (int vers, const char* path, struct stat *buf));
SCRMFS_DECL(__xstat64, int, (int vers, const char* path, struct stat64 *buf));

/* ---------------------------------------
 * POSIX wrappers: file descriptors
 * --------------------------------------- */

SCRMFS_DECL(creat, int, (const char* path, mode_t mode));
SCRMFS_DECL(creat64, int, (const char* path, mode_t mode));
SCRMFS_DECL(open, int, (const char *path, int flags, ...));
SCRMFS_DECL(open64, int, (const char *path, int flags, ...));
SCRMFS_DECL(read, ssize_t, (int fd, void *buf, size_t count));
SCRMFS_DECL(write, ssize_t, (int fd, const void *buf, size_t count));
SCRMFS_DECL(readv, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
SCRMFS_DECL(writev, ssize_t, (int fd, const struct iovec *iov, int iovcnt));
SCRMFS_DECL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
SCRMFS_DECL(pread64, ssize_t, (int fd, void *buf, size_t count, off64_t offset));
SCRMFS_DECL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
SCRMFS_DECL(pwrite64, ssize_t, (int fd, const void *buf, size_t count, off64_t offset));
SCRMFS_DECL(posix_fadvise, int, (int fd, off_t offset, off_t len, int advice));
SCRMFS_DECL(lseek, off_t, (int fd, off_t offset, int whence));
SCRMFS_DECL(lseek64, off64_t, (int fd, off64_t offset, int whence));
SCRMFS_DECL(ftruncate, int, (int fd, off_t length));
SCRMFS_DECL(fsync, int, (int fd));
SCRMFS_DECL(fdatasync, int, (int fd));
SCRMFS_DECL(flock, int, (int fd, int operation));
SCRMFS_DECL(mmap, void*, (void *addr, size_t length, int prot, int flags, int fd, off_t offset));
SCRMFS_DECL(mmap64, void*, (void *addr, size_t length, int prot, int flags, int fd, off64_t offset));
SCRMFS_DECL(munmap, int,(void *addr, size_t length));
SCRMFS_DECL(msync, int, (void *addr, size_t length, int flags));
SCRMFS_DECL(__fxstat, int, (int vers, int fd, struct stat *buf));
SCRMFS_DECL(__fxstat64, int, (int vers, int fd, struct stat64 *buf));
SCRMFS_DECL(close, int, (int fd));

/* ---------------------------------------
 * POSIX wrappers: paths
 * --------------------------------------- */

int SCRMFS_WRAP(access)(const char *path, int mode)
{
    /* determine whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* check if path exists */
        if (scrmfs_get_fid_from_path(path) < 0) {
            debug("access: scrmfs_get_id_from path failed, returning -1, %s\n", path);
            errno = ENOENT;
            return -1;
        }

        /* currently a no-op */
        debug("access: path intercepted, returning 0, %s\n", path);
        return 0;
    } else {
        debug("access: calling MAP_OR_FAIL, %s\n", path);
        MAP_OR_FAIL(access);
        int ret = SCRMFS_REAL(access)(path, mode);
        debug("access: returning __real_access %d,%s\n", ret, path);
        return ret;
    }
}

int SCRMFS_WRAP(mkdir)(const char *path, mode_t mode)
{
    /* Support for directories is very limited at this time
     * mkdir simply puts an entry into the filelist for the
     * requested directory (assuming it does not exist)
     * It doesn't check to see if parent directory exists */

    /* determine whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* check if it already exists */
        if (scrmfs_get_fid_from_path(path) >= 0) {
            errno = EEXIST;
            return -1;
        }

        /* add directory to file list */
        int fid = scrmfs_fid_create_directory(path);
        return 0;
    } else {
        MAP_OR_FAIL(mkdir);
        int ret = SCRMFS_REAL(mkdir)(path, mode);
        return ret;
    }
}

int SCRMFS_WRAP(rmdir)(const char *path)
{
    /* determine whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* check if the mount point itself is being deleted */
        if (! strcmp(path, scrmfs_mount_prefix)) {
            errno = EBUSY;
            return -1;
        }

        /* check if path exists */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            errno = ENOENT;
            return -1;
        }

        /* is it a directory? */
        if (! scrmfs_fid_is_dir(fid)) {
            errno = ENOTDIR;
            return -1;
        }

        /* is it empty? */
        if (! scrmfs_fid_is_dir_empty(path)) {
            errno = ENOTEMPTY;
            return -1;
        }

        /* remove the directory from the file list */ 
        int ret = scrmfs_fid_unlink(fid);
        return 0;
    } else {
        MAP_OR_FAIL(rmdir);
        int ret = SCRMFS_REAL(rmdir)(path);
        return ret;
    }
}

int SCRMFS_WRAP(rename)(const char *oldpath, const char *newpath)
{
    /* TODO: allow oldpath / newpath to split across memfs and normal
     * linux fs, which means we'll need to do a read / write */

    /* check whether the old path is in our file system */
    if (scrmfs_intercept_path(oldpath)) {
        /* for now, we can only rename within our file system */
        if (! scrmfs_intercept_path(newpath)) {
            /* ERROR: can't yet rename across file systems */
            errno = EXDEV;
            return -1;
        }

        /* verify that we really have a file by the old name */
        int fid = scrmfs_get_fid_from_path(oldpath);
        debug("orig file in position %d\n",fid);
        if (fid < 0) {
            /* ERROR: oldname does not exist */
            debug("Couldn't find entry for %s in SCRMFS\n",oldpath);
            errno = ENOENT;
            return -1;
        }

        /* verify that we don't already have a file by the new name */
        if (scrmfs_get_fid_from_path(newpath) < 0) {
            /* check that new name is within bounds */
            size_t newpathlen = strlen(newpath) + 1;
            if (newpathlen > SCRMFS_MAX_FILENAME) {
                errno = ENAMETOOLONG;
                return -1;
            }

            /* finally overwrite the old name with the new name */
            debug("Changing %s to %s\n",(char*)&scrmfs_filelist[fid].filename, newpath);
            strcpy((void *)&scrmfs_filelist[fid].filename, newpath);
        } else {
            /* ERROR: new name already exists */
            debug("File %s exists\n",newpath);
            errno = EEXIST;
            return -1;
        }
        return 0;
    } else {
        /* for now, we can only rename within our file system */
        if (scrmfs_intercept_path(newpath)) {
            /* ERROR: can't yet rename across file systems */
            errno = EXDEV;
            return -1;
        }

        /* both files are normal linux files, delegate to system call */
        MAP_OR_FAIL(rename);
        int ret = SCRMFS_REAL(rename)(oldpath,newpath);
        return ret;
    }
}

int SCRMFS_WRAP(truncate)(const char* path, off_t length)
{
    /* determine whether we should intercept this path or not */
    if (scrmfs_intercept_path(path)) {
        /* lookup the fd for the path */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            /* ERROR: file does not exist */
            debug("Couldn't find entry for %s in SCRMFS\n", path);
            errno = ENOENT;
            return -1;
        }

        /* truncate the file */
        int rc = scrmfs_fid_truncate(fid, length);
        if (rc != SCRMFS_SUCCESS) {
            debug("scrmfs_fid_truncate failed for %s in SCRMFS\n", path);
            errno = EIO;
            return -1;
        }

        return 0;
    } else {
        MAP_OR_FAIL(truncate);
        int ret = SCRMFS_REAL(truncate)(path, length);
        return ret;
    }
}

int SCRMFS_WRAP(unlink)(const char *path)
{
    /* determine whether we should intercept this path or not */
    if (scrmfs_intercept_path(path)) {
        /* get file id for path name */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            /* ERROR: file does not exist */
            debug("Couldn't find entry for %s in SCRMFS\n",path);
            errno = ENOENT;
            return -1;
        }

        /* check that it's not a directory */
        if (scrmfs_fid_is_dir(fid)) {
            /* ERROR: is a directory */
            debug("Attempting to unlink a directory %s in SCRMFS\n",path);
            errno = EISDIR;
            return -1;
        }

        /* delete the file */
        scrmfs_fid_unlink(fid);

        return 0;
    } else {
        MAP_OR_FAIL(unlink);
        int ret = SCRMFS_REAL(unlink)(path);
        return ret;
    }
}

int SCRMFS_WRAP(remove)(const char *path)
{
    /* determine whether we should intercept this path or not */
    if (scrmfs_intercept_path(path)) {
        /* get file id for path name */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            /* ERROR: file does not exist */
            debug("Couldn't find entry for %s in SCRMFS\n",path);
            errno = ENOENT;
            return -1;
        }

        /* check that it's not a directory */
        if (scrmfs_fid_is_dir(fid)) {
            /* TODO: shall be equivalent to rmdir(path) */
            /* ERROR: is a directory */
            debug("Attempting to remove a directory %s in SCRMFS\n",path);
            errno = EISDIR;
            return -1;
        }

        /* shall be equivalent to unlink(path) */
        /* delete the file */
        scrmfs_fid_unlink(fid);

        return 0;
    } else {
        MAP_OR_FAIL(remove);
        int ret = SCRMFS_REAL(remove)(path);
        return ret;
    }
}

int SCRMFS_WRAP(stat)( const char *path, struct stat *buf)
{
    debug("stat was called for %s....\n",path);
    if (scrmfs_intercept_path(path)) {
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            errno = ENOENT;
            return -1;
        }

        scrmfs_fid_stat(fid, buf);

        return 0;
    } else {
        MAP_OR_FAIL(stat);
        int ret = SCRMFS_REAL(stat)(path, buf);
        return ret;
    }
} 

int SCRMFS_WRAP(__xstat)(int vers, const char *path, struct stat *buf)
{
    debug("xstat was called for %s....\n",path);
    if (scrmfs_intercept_path(path)) {
        /* get file id for path */
        int fid = scrmfs_get_fid_from_path(path);
        if (fid < 0) {
            /* file doesn't exist */
            errno = ENOENT;
            return -1;
        }

        /* get meta data for this file */
        scrmfs_fid_stat(fid, buf);

        return 0;
    } else { 
        MAP_OR_FAIL(__xstat);
        int ret = SCRMFS_REAL(__xstat)(vers, path, buf);
        return ret;
    }
}

int SCRMFS_WRAP(__xstat64)(int vers, const char *path, struct stat64 *buf)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOENT;
        return -1;
    } else {
        MAP_OR_FAIL(__xstat64);
        int ret = SCRMFS_REAL(__xstat64)(vers, path, buf);
        return ret;
    }
}

int SCRMFS_WRAP(__lxstat)(int vers, const char *path, struct stat *buf)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOENT;
        return -1;
    } else {
        MAP_OR_FAIL(__lxstat);
        int ret = SCRMFS_REAL(__lxstat)(vers, path, buf);
        return ret;
    }
}

int SCRMFS_WRAP(__lxstat64)(int vers, const char *path, struct stat64 *buf)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOENT;
        return -1;
    } else {
        MAP_OR_FAIL(__lxstat64);
        int ret = SCRMFS_REAL(__lxstat64)(vers, path, buf);
        return ret;
    }
}

/* ---------------------------------------
 * POSIX wrappers: file descriptors
 * --------------------------------------- */

/* read count bytes info buf from file starting at offset pos,
 * returns number of bytes actually read in retcount,
 * retcount will be less than count only if an error occurs
 * or end of file is reached */
int scrmfs_fd_read(int fd, off_t pos, void* buf, size_t count, size_t* retcount)
{
    /* get the file id for this file descriptor */
    int fid = scrmfs_get_fid_from_fd(fd);
    if (fid < 0) {
        return SCRMFS_ERR_BADF;
    }
       
    /* it's an error to read from a directory */
    if(scrmfs_fid_is_dir(fid)){
       /* TODO: note that read/pread can return this, but not fread */
       return SCRMFS_ERR_ISDIR;
    }

    /* check that file descriptor is open for read */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
    if (! filedesc->read) {
        return SCRMFS_ERR_BADF;
    }

    /* TODO: is it safe to assume that off_t is bigger than size_t? */
    /* check that we don't overflow the file length */
    if (scrmfs_would_overflow_offt(pos, (off_t) count)) {
        return SCRMFS_ERR_OVERFLOW;
    }

    /* TODO: check that file is open for reading */

    /* check that we don't try to read past the end of the file */
    off_t lastread = pos + (off_t) count;
    off_t filesize = scrmfs_fid_size(fid);
    if (filesize < lastread) {
        /* adjust count so we don't read past end of file */
        if (filesize > pos) {
            /* read all bytes until end of file */
            count = (size_t) (filesize - pos);
        } else {
            /* pos is already at or past the end of the file */
            count = 0;
        }
    }

    /* record number of bytes that we'll actually read */
    *retcount = count;

    /* if we don't read any bytes, return success */
    if (count == 0){
        return SCRMFS_SUCCESS;
    }

    /* read data from file */
    int read_rc = scrmfs_fid_read(fid, pos, buf, count);
    return read_rc;
}

/* write count bytes from buf into file starting at offset pos,
 * allocates new bytes and updates file size as necessary,
 * fills any gaps with zeros */
int scrmfs_fd_write(int fd, off_t pos, const void* buf, size_t count)
{
    /* get the file id for this file descriptor */
    int fid = scrmfs_get_fid_from_fd(fd);
    if (fid < 0) {
        return SCRMFS_ERR_BADF;
    }

    /* it's an error to write to a directory */
    if(scrmfs_fid_is_dir(fid)){
        return SCRMFS_ERR_INVAL;
    }

    /* check that file descriptor is open for write */
    scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
    if (! filedesc->write) {
        return SCRMFS_ERR_BADF;
    }

    /* TODO: is it safe to assume that off_t is bigger than size_t? */
    /* check that our write won't overflow the length */
    if (scrmfs_would_overflow_offt(pos, (off_t) count)) {
        /* TODO: want to return EFBIG here for streams */
        return SCRMFS_ERR_OVERFLOW;
    }

    /* TODO: check that file is open for writing */

    /* get current file size before extending the file */
    off_t filesize = scrmfs_fid_size(fid);

    /* extend file size and allocate chunks if needed */
    off_t newpos = pos + (off_t) count;
    int extend_rc = scrmfs_fid_extend(fid, newpos);
    if (extend_rc != SCRMFS_SUCCESS) {
        return extend_rc;
    }

    /* fill any new bytes between old size and pos with zero values */
    if (filesize < pos) {
        off_t gap_size = pos - filesize;
        int zero_rc = scrmfs_fid_write_zero(fid, filesize, gap_size);
        if (zero_rc != SCRMFS_SUCCESS) {
            return zero_rc;
        }
    }

    /* finally write specified data to file */
    debug("request to write %d bytes to position %d\n", count, pos);
    int write_rc = scrmfs_fid_write(fid, pos, buf, count);
    
    return write_rc;
}

int SCRMFS_WRAP(creat)(const char* path, mode_t mode)
{
    /* equivalent to open(path, O_WRONLY|O_CREAT|O_TRUNC, mode) */

    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* TODO: handle relative paths using current working directory */

        /* create the file */
        int fid;
        off_t pos;
        int rc = scrmfs_fid_open(path, O_WRONLY | O_CREAT | O_TRUNC, mode, &fid, &pos);
        if (rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(rc);
            return -1;
        }

        /* TODO: allocate a free file descriptor and associate it with fid */
        /* set in_use flag and file pointer, flags include O_WRONLY */
        scrmfs_fd_t* filedesc = &(scrmfs_fds[fid]);
        filedesc->pos   = pos;
        filedesc->read  = 0;
        filedesc->write = 1;
        debug("SCRMFS_open generated fd %d for file %s\n", fid, path);    

        /* don't conflict with active system fds that range from 0 - (fd_limit) */
        int ret = fid + scrmfs_fd_limit;
        return ret;
    } else {
        MAP_OR_FAIL(creat);
        int ret = SCRMFS_REAL(creat)(path, mode);
        return ret ;
    }
}

int SCRMFS_WRAP(creat64)(const char* path, mode_t mode)
{
    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        return -1;
    } else {
        MAP_OR_FAIL(creat64);
        int ret = SCRMFS_REAL(creat64)(path, mode);
        return ret;
    }
}

int SCRMFS_WRAP(open)(const char *path, int flags, ...)
{
    int ret;

    /* if O_CREAT is set, we should also have some mode flags */
    int mode = 0;
    if (flags & O_CREAT) {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);
    }
    
    /* determine whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* TODO: handle relative paths using current working directory */

        /* create the file */
        int fid;
        off_t pos;
        int rc = scrmfs_fid_open(path, flags, mode, &fid, &pos);
        if (rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(rc);
            return -1;
        }

        /* TODO: allocate a free file descriptor and associate it with fid */
        /* set in_use flag and file pointer */
        scrmfs_fd_t* filedesc = &(scrmfs_fds[fid]);
        filedesc->pos   = pos;
        filedesc->read  = ((flags & O_RDONLY) == O_RDONLY) || ((flags & O_RDWR) == O_RDWR);
        filedesc->write = ((flags & O_WRONLY) == O_WRONLY) || ((flags & O_RDWR) == O_RDWR);
        debug("SCRMFS_open generated fd %d for file %s\n", fid, path);    

        /* don't conflict with active system fds that range from 0 - (fd_limit) */
        ret = fid + scrmfs_fd_limit;
        return ret;
    } else {
        MAP_OR_FAIL(open);
        if (flags & O_CREAT) {
            ret = SCRMFS_REAL(open)(path, flags, mode);
        } else {
            ret = SCRMFS_REAL(open)(path, flags);
        }
        return ret;
    }
}

int SCRMFS_WRAP(open64)(const char* path, int flags, ...)
{
    int ret;

    /* if O_CREAT is set, we should also have some mode flags */
    int mode = 0;
    if (flags & O_CREAT) {
        va_list arg;
        va_start(arg, flags);
        mode = va_arg(arg, int);
        va_end(arg);
    }

    /* check whether we should intercept this path */
    if (scrmfs_intercept_path(path)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        return -1;
    } else {
        MAP_OR_FAIL(open64);
        if (flags & O_CREAT) {
            ret = SCRMFS_REAL(open64)(path, flags, mode);
        } else {
            ret = SCRMFS_REAL(open64)(path, flags);
        }
    }

    return ret;
}

off_t SCRMFS_WRAP(lseek)(int fd, off_t offset, int whence)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* TODO: check that fd is actually in use */

        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);

        /* check that file descriptor is good */
        scrmfs_filemeta_t* meta = scrmfs_get_meta_from_fid(fid);
        if (meta == NULL) {
            /* bad file descriptor */
            errno = EBADF;
            return (off_t)-1;
        }

        /* get file descriptor for fd */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);

        /* get current file position */
        off_t current_pos = filedesc->pos;

        /* compute final file position */
        debug("seeking from %ld\n", current_pos);        
        switch (whence)
        {
            case SEEK_SET:
                /* seek to offset */
                current_pos = offset;
                break;
            case SEEK_CUR:
                /* seek to current position + offset */
                current_pos += offset;
                break;
            case SEEK_END:
                /* seek to EOF + offset */
                current_pos = meta->size + offset;
                break;
            default:
                errno = EINVAL;
                return (off_t)-1;
        }
        debug("seeking to %ld\n", current_pos);        

        /* set and return final file position */
        filedesc->pos = current_pos;
        return current_pos;
    } else {
        MAP_OR_FAIL(lseek);
        off_t ret = SCRMFS_REAL(lseek)(fd, offset, whence);
        return ret;
    }
}

off64_t SCRMFS_WRAP(lseek64)(int fd, off64_t offset, int whence)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return (off64_t)-1;
    } else {
      MAP_OR_FAIL(lseek64);
      off64_t ret = SCRMFS_REAL(lseek64)(fd, offset, whence);
      return ret;
    }
}

int SCRMFS_WRAP(posix_fadvise)(int fd, off_t offset, off_t len, int advice)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* check that the file descriptor is valid */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            errno = EBADF;
            return errno;
        }

        /* process advice from caller */
        switch( advice ) {
            case POSIX_FADV_NORMAL:
            case POSIX_FADV_SEQUENTIAL:
                /* can use this hint for a better compression strategy */
            case POSIX_FADV_RANDOM:
            case POSIX_FADV_NOREUSE:
            case POSIX_FADV_WILLNEED:
                /* with the spill-over case, we can use this hint to
                 * to better manage the in-memory parts of a file. On
                 * getting this advice, move the chunks that are on the
                 * spill-over device to the in-memory portion
                 */
            case POSIX_FADV_DONTNEED:
                /* similar to the previous case, but move contents from memory
                 * to the spill-over device instead.
                 */

                /* ERROR: fn not yet supported */
                fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
                break;
            default:
                /* this function returns the errno itself, not -1 */
                errno = EINVAL;
                return errno;
        }

        /* just a hint so return success even if we don't do anything */
        return 0;
    } else {
      MAP_OR_FAIL(posix_fadvise);
      int ret = SCRMFS_REAL(posix_fadvise)(fd, offset, len, advice);
      return ret;
    }
}

ssize_t SCRMFS_WRAP(read)(int fd, void *buf, size_t count)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return (ssize_t)-1;
        }

        /* read data from file */
        size_t retcount;
        int read_rc = scrmfs_fd_read(fd, filedesc->pos, buf, count, &retcount);
        if (read_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(read_rc);
            return (ssize_t)-1;
        }

        /* update position */
        filedesc->pos += (off_t) retcount;

        /* return number of bytes read */
        return (ssize_t) retcount;
    } else {
        MAP_OR_FAIL(read);
        ssize_t ret = SCRMFS_REAL(read)(fd, buf, count);
        return ret;
    }
}

/* TODO: find right place to msync spillover mapping */
ssize_t SCRMFS_WRAP(write)(int fd, const void *buf, size_t count)
{
    ssize_t ret;

    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return (ssize_t)-1;
        }

        /* write data to file */
        int write_rc = scrmfs_fd_write(fd, filedesc->pos, buf, count);
        if (write_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(write_rc);
            return (ssize_t)-1;
        }

        /* update file position */
        filedesc->pos += count;

        /* return number of bytes read */
        ret = count;
    } else {
        MAP_OR_FAIL(write);
        ret = SCRMFS_REAL(write)(fd, buf, count);
    }

    return ret;
}

ssize_t SCRMFS_WRAP(readv)(int fd, const struct iovec *iov, int iovcnt)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(readv);
        ssize_t ret = SCRMFS_REAL(readv)(fd, iov, iovcnt);
        return ret;
    }
}

ssize_t SCRMFS_WRAP(writev)(int fd, const struct iovec *iov, int iovcnt)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(writev);
        ssize_t ret = SCRMFS_REAL(writev)(fd, iov, iovcnt);
        return ret;
    }
}

ssize_t SCRMFS_WRAP(pread)(int fd, void *buf, size_t count, off_t offset)
{
    /* equivalent to read(), except that it shall read from a given
     * position in the file without changing the file pointer */

    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return (ssize_t)-1;
        }

        /* read data from file */
        size_t retcount;
        int read_rc = scrmfs_fd_read(fd, offset, buf, count, &retcount);
        if (read_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(read_rc);
            return (ssize_t)-1;
        }

        /* return number of bytes read */
        return (ssize_t) retcount;
    } else {
        MAP_OR_FAIL(pread);
        ssize_t ret = SCRMFS_REAL(pread)(fd, buf, count, offset);
        return ret;
    }
}

ssize_t SCRMFS_WRAP(pread64)(int fd, void *buf, size_t count, off64_t offset)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(pread64);
        ssize_t ret = SCRMFS_REAL(pread64)(fd, buf, count, offset);
        return ret;
    }
}

ssize_t SCRMFS_WRAP(pwrite)(int fd, const void *buf, size_t count, off_t offset)
{
    /* equivalent to write(), except that it writes into a given
     * position without changing the file pointer */

    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* get pointer to file descriptor structure */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (filedesc == NULL) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return (ssize_t)-1;
        }

        /* write data to file */
        int write_rc = scrmfs_fd_write(fd, offset, buf, count);
        if (write_rc != SCRMFS_SUCCESS) {
            errno = scrmfs_err_map_to_errno(write_rc);
            return (ssize_t)-1;
        }

        /* return number of bytes read */
        return (ssize_t) count;
    } else {
        MAP_OR_FAIL(pwrite);
        ssize_t ret = SCRMFS_REAL(pwrite)(fd, buf, count, offset);
        return ret;
    }
}

ssize_t SCRMFS_WRAP(pwrite64)(int fd, const void *buf, size_t count, off64_t offset)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(pwrite64);
        ssize_t ret = SCRMFS_REAL(pwrite64)(fd, buf, count, offset);
        return ret;
    }
}

int SCRMFS_WRAP(ftruncate)(int fd, off_t length)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return -1;
        }

        /* check that file descriptor is open for write */
        scrmfs_fd_t* filedesc = scrmfs_get_filedesc_from_fd(fd);
        if (! filedesc->write) {
            errno = EBADF;
            return -1;
        }

        /* truncate the file */
        int rc = scrmfs_fid_truncate(fid, length);
        if (rc != SCRMFS_SUCCESS) {
            errno = EIO;
            return -1;
        }

        return 0;
    } else {
        MAP_OR_FAIL(ftruncate);
        int ret = SCRMFS_REAL(ftruncate)(fd, length);
        return ret;
    }
}

int SCRMFS_WRAP(fsync)(int fd)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            /* ERROR: invalid file descriptor */
            errno = EBADF;
            return -1;
        }

        /* TODO: if using spill over we may have some fsyncing to do */

        /* nothing to do in our case */
        return 0;
    } else {
        MAP_OR_FAIL(fsync);
        int ret = SCRMFS_REAL(fsync)(fd);
        return ret;
    }
}

int SCRMFS_WRAP(fdatasync)(int fd)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(fdatasync);
        int ret = SCRMFS_REAL(fdatasync)(fd);
        return ret;
    }
}

int SCRMFS_WRAP(flock)(int fd, int operation)
{
    int ret;

    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
     // KMM I removed the locking code because it was causing
     // hangs
      /*
        -- currently handling the blocking variants only 
        switch (operation)
        {
            case LOCK_EX:
                debug("locking file %d..\n",fid);
                ret = pthread_spin_lock(&meta->fspinlock);
                if ( ret ) {
                    perror("pthread_spin_lock() failed");
                    return -1;
                }
                meta->flock_status = EX_LOCKED;
                break;
            case LOCK_SH:
                -- not needed for CR; will not be supported,
                --  update flock_status anyway 
                meta->flock_status = SH_LOCKED;
                break;
            case LOCK_UN:
                ret = pthread_spin_unlock(&meta->fspinlock);
                debug("unlocking file %d..\n",fid);
                meta->flock_status = UNLOCKED;
                break;
            default:
                errno = EINVAL;
                return -1;
        }
       */

        return 0;
    } else {
        MAP_OR_FAIL(flock);
        ret = SCRMFS_REAL(flock)(fd,operation);
        return ret;
    }
}

/* TODO: handle different flags */
void* SCRMFS_WRAP(mmap)(void *addr, size_t length, int prot, int flags,
    int fd, off_t offset)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* get the file id for this file descriptor */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            errno = EBADF;
            return MAP_FAILED;
        }

        /* TODO: handle addr properly based on flags */

        /* allocate memory required to mmap the data if addr is NULL;
         * using posix_memalign instead of malloc to align mmap'ed area
         * to page size */
        if (! addr) {
            int ret = posix_memalign(&addr, sysconf(_SC_PAGE_SIZE), length);
            if (ret) {
                /* posix_memalign does not set errno */
                if (ret == EINVAL) {
                    errno = EINVAL;
                    return MAP_FAILED;
                }

                if (ret == ENOMEM) {
                    errno = ENOMEM;
                    return MAP_FAILED;
                }
            }
        }

        /* TODO: do we need to extend file if offset+length goes past current end? */

        /* check that we don't copy past the end of the file */
        off_t last_byte = offset + length;
        off_t file_size = scrmfs_fid_size(fid);
        if (last_byte > file_size) {
            /* trying to copy past the end of the file, so
             * adjust the total amount to be copied */
            length = (size_t) (file_size - offset);
        }

        /* read data from file */
        int rc = scrmfs_fid_read(fid, offset, addr, length);
        if (rc != SCRMFS_SUCCESS) {
            /* TODO: need to free memory in this case? */
            errno = ENOMEM;
            return MAP_FAILED;
        }

        return addr;
    } else {
        MAP_OR_FAIL(mmap);
        void* ret = SCRMFS_REAL(mmap)(addr, length, prot, flags, fd, offset);
        return ret;
    }
}

int SCRMFS_WRAP(munmap)(void *addr, size_t length)
{
    fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
    errno = ENOSYS;
    return ENODEV;
}

int SCRMFS_WRAP(msync)(void *addr, size_t length, int flags)
{
    /* TODO: need to keep track of all the mmaps that are linked to
     * a given file before this function can be implemented*/
    fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
    errno = ENOSYS;
    return ENOMEM;
}

void* SCRMFS_WRAP(mmap64)(void *addr, size_t length, int prot, int flags,
    int fd, off64_t offset)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = ENOSYS;
        return MAP_FAILED;
    } else {
        MAP_OR_FAIL(mmap64);
        void* ret = SCRMFS_REAL(mmap64)(addr, length, prot, flags, fd, offset);
        return ret;
    }
}

int SCRMFS_WRAP(__fxstat)(int vers, int fd, struct stat *buf)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(__fxstat);
        int ret = SCRMFS_REAL(__fxstat)(vers, fd, buf);
        return ret;
    }
}

int SCRMFS_WRAP(__fxstat64)(int vers, int fd, struct stat64 *buf)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        /* ERROR: fn not yet supported */
        fprintf(stderr, "Function not yet supported @ %s:%d\n", __FILE__, __LINE__);
        errno = EBADF;
        return -1;
    } else {
        MAP_OR_FAIL(__fxstat64);
        int ret = SCRMFS_REAL(__fxstat64)(vers, fd, buf);
        return ret;
    }
}

int SCRMFS_WRAP(close)(int fd)
{
    /* check whether we should intercept this file descriptor */
    if (scrmfs_intercept_fd(&fd)) {
        debug("closing fd %d\n", fd);

        /* TODO: what to do if underlying file has been deleted? */

        /* check that fd is actually in use */
        int fid = scrmfs_get_fid_from_fd(fd);
        if (fid < 0) {
            errno = EBADF;
            return -1;
        }

        /* close the file id */
        int close_rc = scrmfs_fid_close(fid);
        if (close_rc != SCRMFS_SUCCESS) {
            errno = EIO;
            return -1;
        }

        /* TODO: free file descriptor */

        return 0;
    } else {
        MAP_OR_FAIL(close);
        int ret = SCRMFS_REAL(close)(fd);
        return ret;
    }
}
