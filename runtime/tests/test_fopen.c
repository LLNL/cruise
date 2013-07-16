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
#include "mpi.h"

size_t filesize = 100*1024*1024;
int times = 5;
int seconds = 0;
int rank  = -1;
int ranks = 0;

/* reliable read from file descriptor (retries, if necessary, until hard error) */
int reliable_read(FILE* fp, void* buf, size_t size)
{
  size_t n = 0;
  int retries = 10;
  int rank;
  char host[128];
  while (n < size)
  {
    int rc = fread((char*) buf + n, 1, size - n, fp);
    if (rc > 0) {
      n += rc;
    } else {
      if (feof(fp)) {
        /* EOF */
        return n;
      }

      /* got an error, check whether it was serious */
      if(errno == EINTR || errno == EAGAIN) {
        continue;
      }

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Error reading errno=%d @ %s:%d\n",
                rank, host, errno, __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Giving up read errno=%d @ %s:%d\n",
                rank, host, errno, __FILE__, __LINE__
        );
        MPI_Abort(MPI_COMM_WORLD, 0);
      }
    }
  }
  return size;
}

/* reliable write to file descriptor (retries, if necessary, until hard error) */
int reliable_write(FILE* fp, const void* buf, size_t size)
{
  size_t n = 0;
  int retries = 10;
  int rank;
  char host[128];
  while (n < size)
  {
    size_t rc = fwrite((char*) buf + n, 1, size - n, fp);
    if (rc > 0) {
      n += rc;
    } else {
      /* got an error, check whether it was serious */
      if(errno == EINTR || errno == EAGAIN) {
        continue;
      }

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Error writing: errno=%d @ %s:%d\n",
                rank, host, errno, __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Giving up write: errno=%d @ %s:%d\n",
                rank, host, errno, __FILE__, __LINE__
        );
        MPI_Abort(MPI_COMM_WORLD, 0);
      }
    }
  }
  return size;
}

/* initialize buffer with some well-known value based on rank */
int init_buffer(char* buf, size_t size, int rank, int ckpt)
{
  size_t i;
  for(i=0; i < size; i++) {
    char c = 'a' + (char)((rank + ckpt + i) & 32);
    buf[i] = c;
  }
  return 0;
}

/* checks buffer for expected value */
int check_buffer(char* buf, size_t size, int rank, int ckpt)
{
  size_t i;
  for(i=0; i < size; i++) {
    char c = 'a' + (char)((rank + ckpt + i) & 32);
    if (buf[i] != c) {
      return 0;
    }
  }
  return 1;
}

/* read the checkpoint data from file into buf, and return whether the read was successful */
int read_checkpoint(FILE* fp, int* rank, int* ckpt, char* buf, size_t size)
{
  unsigned long n;
  char rank_buf[7];
  char ckpt_buf[7];
  size_t field_size = 6;

  /* read the rank id */
  n = reliable_read(fp, rank_buf, field_size);
  if (n != field_size) {
    printf("Failed to read rank\n");
    return 0;
  }
  rank_buf[6] = '\0';

  /* read the checkpoint id */
  n = reliable_read(fp, ckpt_buf, field_size);
  if (n != field_size) {
    printf("Failed to read timestep\n");
    return 0;
  }
  ckpt_buf[6] = '\0';

  /* read the checkpoint data, and check the file size */
  n = reliable_read(fp, buf, size+1);
  if (n != size) {
    printf("Filesize not correct\n");
    return 0;
  }

  /* if the file looks good, set the timestep and return */
  sscanf(rank_buf, "%06d", rank);
  sscanf(ckpt_buf, "%06d", ckpt);

  return 0;
}

/* write the checkpoint data to fp, and return whether the write was successful */
int write_checkpoint(FILE* fp, int rank, int ckpt, char* buf, size_t size)
{
  int rc;
  int valid = 0;
  char rank_buf[7];
  char ckpt_buf[7];
  size_t field_size = 6;

  /* write the rank id */
  sprintf(rank_buf, "%06d", rank);
  rc = reliable_write(fp, rank_buf, field_size);
  if (rc < 0) {
    valid = 0;
  }

  /* write the checkpoint id (application timestep) */
  sprintf(ckpt_buf, "%06d", ckpt);
  rc = reliable_write(fp, ckpt_buf, field_size);
  if (rc < 0) {
    valid = 0;
  }

  /* write the checkpoint data */
  rc = reliable_write(fp, buf, size);
  if (rc < 0) {
    valid = 0;
  }

  return valid;
}

void checkdata(char* file, size_t size, int times)
{
  char* buf = malloc(size);

  MPI_Barrier(MPI_COMM_WORLD);

  if (times > 0) {
    /* write the checkpoint file */
    int i, j;
    for(i=0; i < times; i++) {
      int rc;
      int valid = 0;

      rc = init_buffer(buf, size, rank, i);

      if (rank == 0) {
        printf("Writing checkpoint %d.\n", i);  fflush(stdout);
      }

      /* open the file and write the checkpoint */
      FILE* fp = fopen(file, "w");
      if (fp != NULL) {
        valid = 1;

        /* write the checkpoint data */
        rc = write_checkpoint(fp, rank, i, buf, size);
        if (rc < 0) {
          valid = 0;
        }

        /* force the data to storage */
        rc = fflush(fp);
        if (rc != 0) {
          valid = 0;
        }

        /* make sure the close is without error */
        rc = fclose(fp);
        if (rc != 0) {
          valid = 0;
        }
      }

      if (rank == 0) {
        printf("Completed checkpoint %d.\n", i);  fflush(stdout);
      }

      if (rank == 0) {
        printf("Reading checkpoint %d.\n", i);  fflush(stdout);
      }

      memset(buf, 0, size);

      /* open the file and write the checkpoint */
      int read_rank, read_timestep;
      fp = fopen(file, "r");
      if (fp != NULL) {
        valid = 1;

        /* write the checkpoint data */
        rc = read_checkpoint(fp, &read_rank, &read_timestep, buf, size);
        if (rc < 0) {
          valid = 0;
        }

        /* make sure the close is without error */
        rc = fclose(fp);
        if (rc != 0) {
          valid = 0;
        }

      }

      if (read_rank != rank || read_timestep != i) {
        printf("INVALID HEADER on rank %d in step %d\n", rank, i);  fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 0);
      }

      rc = check_buffer(buf, size, rank, i);
      if (! rc) {
        printf("INVALID DATA on rank %d in step %d\n", rank, i);  fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 0);
      }

      if (rank == 0) {
        printf("Verified checkpoint %d.\n", read_timestep);  fflush(stdout);
      }

      /* optionally sleep for some time */
      if (seconds > 0) {
        if (rank == 0) {
          printf("Sleeping for %d seconds... \n", seconds); fflush(stdout);
        }
        sleep(seconds);
      }

      remove(file);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

  return;
}

int main (int argc, char* argv[])
{
  /* check that we got an appropriate number of arguments */
  if (argc != 1 && argc != 4) {
    printf("Usage: test_correctness [filesize times sleep_secs]\n");
    return 1;
  }

  /* read parameters from command line, if any */
  if (argc > 1) {
    filesize = (size_t) atol(argv[1]);
    times = atoi(argv[2]);
    seconds = atoi(argv[3]);
  }

  MPI_Init(&argc, &argv);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  char name[256];
  sprintf(name, "/tmp/rank.%d", rank);

  /* allocate space for the checkpoint data (make filesize a function of rank for some variation) */
  filesize = filesize + rank;

  cruise_mount("/tmp", filesize, rank);

  /* verify data integrity in file */
  checkdata(name, filesize, times);

  MPI_Finalize();

  return 0;
}
