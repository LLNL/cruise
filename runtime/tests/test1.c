#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

#define UNUSEDFD 5000

#define CRUISE_STATUS
#ifdef CRUISE_STATUS
    #define status(fmt, args... )  printf("STATUS: %d %s, line %d: "fmt, testnum, \
                                          __func__, __LINE__,##args)
#else
    #define status(fmt, args... )
#endif

#define CRUISE_ERROR
#ifdef CRUISE_ERROR
    #define error(fmt, args... )  printf("ERROR: %d %s, line %d: "fmt, testnum, \
                                         __func__, __LINE__, ##args)
#else
    #define error(fmt, args... )
#endif

#define CHECK(a)  a; if (rc < 0) return rc;

#define TEST(a) ++testnum; a;

#define TESTFAIL(f,a) ++testnum; f = a; \
                   if (f >= 0){ error("test %d failed\n", testnum); return -1;} \
                   else {status("test %d succeeded\n", testnum);}

#define TESTFAILERR(f,a,err) ++testnum; f = a; \
                   if (f >= 0 || err != errno){ error("test %d failed\n", testnum); return -1;} \
                   else {status("test %d succeeded\n", testnum);}

#define TESTSUCC(f,a) ++testnum; f = a; \
                   if (f < 0) { error("test %d failed\n", testnum); return -1;} \
                   else {status("test %d succeeded\n", testnum);}

int testnum = 0;

int test1();

int main(int argc, char ** argv){
  int rc;
  cruise_mount("/tmp", 4096 * 4096, 0);
  CHECK(rc = test_open()); 
  CHECK(rc = test_close());
  CHECK(rc = test_unlink());
  CHECK(rc = test_mkdir());
  CHECK(rc = test_rmdir());
  CHECK(rc = test_stat());
  CHECK(rc = test_access());
  CHECK(rc = test_write());
  CHECK(rc = test_read());
  //seek
  return 0;
}



int test_open(){
   char afile[20] = "/tmp/file1.txt";
   char adir[20] = "/tmp/somewhere";
   int fd;
   int fd1;


   /* open a file that does not exist without create flag
    * should fail */
   TESTFAILERR(fd,open(afile, O_WRONLY), ENOENT);
   //TESTFAILERR(fd,open(afile, O_WRONLY), -1);

   /* open a file that does not exist with create flag
    * should succeed */
   TESTSUCC(fd1,open(afile, O_CREAT));

   /* open a file that already exists with create and exlusive flags
    * should fail */
   TESTFAILERR(fd,open(afile, O_CREAT|O_EXCL), EEXIST);

   /* open a file that already exists with create flag
    * should succeed */
   TESTSUCC(fd,open(afile, O_CREAT));
   if (fd != fd1){
         error("open of existing file did not return right file desc, fd=%d\n", fd);
         return -1;
   }

   /* test opening directory without O_DIRECTORY
    * should fail */
   mkdir(adir,S_IRWXU);
   TESTFAILERR(fd, open(adir, O_WRONLY), ENOTDIR);

   /* test opening directory with O_DIRECTORY
    * should succeed */
   TESTSUCC(fd, open(adir, O_WRONLY|O_DIRECTORY));

   /* test opening regular file with O_DIRECTORY
    * should fail */
   TESTFAILERR(fd, open(afile, O_WRONLY|O_DIRECTORY), ENOTDIR);

   close(fd);
   close(fd1);
   unlink(afile);
   rmdir(adir);

   return 1;
}

int test_close(){
   char afile[20] = "/tmp/file1.txt";
   int fd;
   int ret;

   /* close a file that does not exist
    * should fail */
   TESTFAILERR(ret, close(UNUSEDFD), EBADF);
  
   /* test that closing an existing file works
    * should succeed */ 
   TESTSUCC(fd, open(afile, O_CREAT));
   TESTSUCC(ret, close(fd));

   unlink(afile);

   /* TODO: test that close of deleted file fails
    * not in here now because not implemented in library */
   return 1; 
}


int test_unlink(){
   char afile[20] = "/tmp/file1.txt";
   char adir[20] = "/tmp/somewhere";
   int fd;
   int ret;

   /* try to remove non-existent file
    * should fail */
   TESTFAILERR(ret, unlink(afile), ENOENT);

   /* try to remove an existing file
    * we're not checking to see if the file is busy/open, so
    * should succeed */
   fd = open(afile, O_CREAT);
   TESTSUCC(ret, unlink(afile));

   close(fd);
   unlink(afile);

   /* try to remove a directory
    * should fail */
   mkdir(adir, S_IRWXU);
   TESTFAILERR(ret, unlink(adir), EISDIR);
   rmdir(adir);

   return 1;
}

int test_mkdir(){

   char adir[20] = "/tmp/somewhere";
   int ret;

   /* test making a directory
    * should succeed */
   TESTSUCC(ret,mkdir(adir,S_IRWXU));

   /* try to make it again
    * should fail */
   TESTFAILERR(ret,mkdir(adir,S_IRWXU), EEXIST);
  
   rmdir(adir);

   return 1;
}

int test_rmdir(){
 
   char adir[20] = "/tmp/somewhere";
   int ret;

   /* try to delete a directory that doesn't exist
    * should fail */
   TESTFAILERR(ret,rmdir(adir), ENOENT);

   /* try to remove a direcotry that exists
    * should succeed */
   mkdir(adir, S_IRWXU);
   TESTSUCC(ret, rmdir(adir));

   return 1;
}

int test_stat(){
   char afile[20] = "/tmp/afile";
   char adir[20] = "/tmp/somewhere";
   char buf[1000];
   int count = 1000;
   int fd;
   int ret;
   struct stat statbuf;

   /* call stat for non-existent file
    * should fail */
   TESTFAILERR(ret, stat(afile, &statbuf), ENOENT);

   /* open a file, stat it, size should be 0 
    * should succeed */
   fd = open(afile, O_CREAT);
   TESTSUCC(ret, stat(afile, &statbuf)); 
   TESTSUCC(ret, statbuf.st_size == 0? 1: -1);

   /* write to file, stat, size should be count
    * should succeed */
   write(fd, buf, count);
   TESTSUCC(ret, stat(afile, &statbuf)); 
   TESTSUCC(ret, statbuf.st_size == count? 1: -1);
 
   /* check to see that this is a regular file */
   TESTSUCC(ret, S_ISREG(statbuf.st_mode)==1? 1:-1);
   /* check that this is not a directory */
   TESTSUCC(ret, S_ISDIR(statbuf.st_mode)==0? 1:-1);

   /* make a directory and make sure that ISDIR 
    * reports is a directory */
   mkdir(adir,S_IRWXU);
   TESTSUCC(ret, stat(adir, &statbuf)); 
   TESTSUCC(ret, S_ISDIR(statbuf.st_mode)==1? 1:-1);
   TESTSUCC(ret, S_ISREG(statbuf.st_mode)==0? 1:-1);
  
   
   close(fd);
   unlink(afile);
   rmdir(adir);
   return 1;
}

int test_access(){
   char afile[20] = "/tmp/afile";
   int fd;

   /* test if we can access a non-existent file
    * should fail */
   TESTFAILERR(fd, access(afile, R_OK), ENOENT);
   TESTFAILERR(fd, access(afile, W_OK), ENOENT);
   TESTFAILERR(fd, access(afile, X_OK), ENOENT);
   TESTFAILERR(fd, access(afile, F_OK), ENOENT);

   /* create the file, see if we can access it
    * since we are not checking permissions at this time, all these
    * should succeed */
   fd = open(afile, O_CREAT);
   TESTSUCC(fd, access(afile, F_OK));
   TESTSUCC(fd, access(afile, R_OK));
   TESTSUCC(fd, access(afile, W_OK));
   TESTSUCC(fd, access(afile, X_OK));

   close(fd);
   unlink(afile);

   return 1;
}


int test_write(){
   char afile[20] = "/tmp/writefile";
   char adir[20] = "/tmp/somewhere";
   char buf[1000];
   int count = 1000;
   int fd;
   int ret;

   /* try to write to a file that doesn't exist
    * should fail */
   TESTFAILERR(ret, write(UNUSEDFD, buf, count), EBADF);

   /* write to an existing file
    * should succeed */
   TESTSUCC(fd, open(afile, O_CREAT));
   TESTSUCC(ret, write(fd, buf, count));
   /* should write count bytes */
   TESTSUCC(ret, ret == count? 1: -1);
   close(fd);
   unlink(afile);

   /* try to write to a directory
    * should fail */
   mkdir(adir,S_IRWXU);
   fd = open(adir, O_DIRECTORY);
   TESTFAILERR(ret, write(fd, buf, count), EINVAL);

   close(fd);
   rmdir(adir);

   return 1;
}

int test_read(){
   char afile[20] = "/tmp/afile";
   char adir[20] = "/tmp/somewhere";
   char buf[1000];
   int count = 1000;
   int fd;
   int ret;


   /* try to read a non-existent file
    * should fail */
   TESTFAILERR(ret, read(UNUSEDFD, buf, count), EBADF);

   /* try to read from an existent file with 0 bytes
    * should succeed, should read 0 bytes */
   fd = open(afile, O_CREAT);
   ret = read(fd, buf, count);
   TESTSUCC(ret, read(fd, buf, count));
   TESTSUCC(ret, 0==ret? 1:-1);
 
   /* test reading from a file with bytes
    * should succeed */
   TESTSUCC(ret, write(fd, buf, count));
   /* should write count bytes */
   TESTSUCC(ret, ret == count? 1: -1);
   TESTSUCC(ret, read(fd, buf, count));
   /* should read 0 bytes since at the end of the file */
   TESTSUCC(ret, 0==ret? 1:-1);
   /* seek to beginning and read, should get count bytes */
   lseek(fd,0, SEEK_SET);
   TESTSUCC(ret, read(fd, buf, count));
   TESTSUCC(ret, count==ret? 1:-1);

   /* try to read a directory
    * should fail */
   mkdir(adir,S_IRWXU);
   fd = open(adir, O_DIRECTORY);
   TESTFAILERR(ret, read(fd, buf, count), EISDIR);

   close(fd);
   rmdir(adir);

   return 1;
}
