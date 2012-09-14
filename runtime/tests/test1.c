#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

#define SCRMFS_STATUS
#ifdef SCRMFS_STATUS
    #define status(fmt, args... )  printf("STATUS: %d %s: "fmt, testnum, __func__, ##args)
#else
    #define status(fmt, args... )
#endif

#define SCRMFS_ERROR
#ifdef SCRMFS_ERROR
    #define error(fmt, args... )  printf("ERROR: %d %s: "fmt, testnum, __func__, ##args)
#else
    #define error(fmt, args... )
#endif

#define CHECK(a)  a; if (rc < 0) return rc;

#define TEST(a) ++testnum; a;

#define TESTFAIL(f,a) ++testnum; f = a; \
                   if (f >= 0){ error("test %d failed\n", testnum); return -1;} \
                   else {status("test %d succeeded\n", testnum);}

#define TESTSUCC(f,a) ++testnum; f = a; \
                   if (f < 0) { error("test %d failed\n", testnum); return -1;} \
                   else {status("test %d succeeded\n", testnum);}

int testnum = 0;

int test1();

int main(int argc, char ** argv){
  int rc;

  CHECK(rc = test_open()); 
  if (rc < 0)
     return rc; 
  CHECK(rc = test_close());

  return 0;
}



int test_open(){
   char afile[20] = "/tmp/file1.txt";
   int fd;
   int fd1;


   /* open a file that does not exist without create flag
    * should fail */
   TESTFAIL(fd,open(afile, O_WRONLY));

   /* open a file that does not exist with create flag
    * should succeed */
   TESTSUCC(fd1,open(afile, O_CREAT));

   /* open a file that already exists with create and exlusive flags
    * should fail */
   TESTFAIL(fd,open(afile, O_CREAT|O_EXCL));

   /* open a file that already exists with create flag
    * should succeed */
   TESTSUCC(fd,open(afile, O_CREAT));
   if (fd != fd1){
         error("open of existing file did not return right file desc, fd=%d\n", fd);
         return -1;
   }

   return 1;
}

int test_close(){
   printf("in test close\n");
}
