// build:  mpigcc -g -O3 -o test_ramdisk test_ramdisk.c
// run:    srun -n64 -N4 ./test_ramdisk

#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

int rank  = -1;
int ranks = 0;

int main (int argc, char* argv[])
{
  /* check that we got an appropriate number of arguments */
  if (argc != 1 && argc != 4) {
    printf("Usage: test_correctness [filesize times sleep_secs]\n");
    return 1;
  }

  char name[256];
  sprintf(name, "/tmp/rank.%d", rank);

  cruise_mount("/tmp", 100*1024*1024, rank);

  FILE* fp;
  int c, rc;
  off_t pos;

  rc = remove(name);

  fp = fopen(name, "w");
  if (fp == NULL) {
    printf("Failed to open %s\n", name);
    return 1;
  }
  rc = fprintf(fp, "%d %f %lf %x %u %s %s\n", 143, 25.634, 2.5e3, 345, 1, "hi", "there_world");
  rc = fclose(fp);
 
  fp = fopen(name, "r");
  while (1) {
    c = fgetc(fp);
    if (feof(fp)) {
      break;
    }
    printf("%d %c\n", c, (char)c);
  }
  fclose(fp);

  int i1, i2;
  float f1;
  double d1;
  unsigned long ul1;
  char s1[256], s2[256];
  fp = fopen(name, "r");
  rc = fscanf(fp, "%d %f %lf  %x %lu %s %s\n", &i1, &f1, &d1, &i2, &ul1, s1, s2);
  fclose(fp);

  fp = fopen(name, "a+");
  if (fp == NULL) {
    printf("Failed to open %s\n", name);
    return 1;
  }
  rc = fputc((int)'t', fp);
  pos = ftello(fp);
  rc = fseek(fp, 1, SEEK_SET);
  pos = ftello(fp);
  c = fgetc(fp);
  pos = ftello(fp);
  rc = fclose(fp);

  fp = fopen(name, "r+");
  if (fp == NULL) {
    printf("Failed to open %s\n", name);
    return 1;
  }
  rc = fseek(fp, 4, SEEK_SET);
  rc = ungetc((int)'c', fp);
  rc = ungetc((int)'b', fp);
  rc = ungetc((int)'a', fp);
  while (1) {
    c = fgetc(fp);
    if (feof(fp)) {
      break;
    }
    if (c == EOF) {
        printf("ERROR: %s\n", strerror(errno));
        break;
    }
    printf("%d %c\n", c, (char)c);
  }
  rc = fclose(fp);

  fp = fopen(name, "r");
  while (1) {
    c = fgetc(fp);
    if (feof(fp)) {
      break;
    }
    printf("%d %c\n", c, (char)c);
  }
  fclose(fp);

  fp = fopen(name, "r+");
  rc = fseek(fp, 5, SEEK_END);
  rc = fputc((int)'2', fp);
  rc = ungetc((int)'x', fp);
  rc = ungetc((int)'y', fp);
  rc = ungetc((int)'z', fp);
  while (1) {
    c = fgetc(fp);
    if (feof(fp)) {
      break;
    }
    if (c == EOF) {
        printf("ERROR: %s\n", strerror(errno));
        break;
    }
    printf("%d %c\n", c, (char)c);
  }
  rc = fclose(fp);
  
  fp = fopen(name, "r");
  while (1) {
    c = fgetc(fp);
    if (feof(fp)) {
      break;
    }
    printf("%d %c\n", c, (char)c);
  }
  fclose(fp);

  return 0;
}
