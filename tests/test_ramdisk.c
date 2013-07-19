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

int  timestep = 0;

/* reliable write to file descriptor (retries, if necessary, until hard error) */
int reliable_write(int fd, const void* buf, size_t size)
{
  size_t n = 0;
  int retries = 10;
  int rank;
  char host[128];
  while (n < size)
  {
    int rc = write(fd, (char*) buf + n, size - n);
    if (rc > 0) {
      n += rc;
    } else if (rc == 0) {
      /* something bad happened, print an error and abort */
      MPI_Comm_rank(MPI_COMM_WORLD, &rank);
      gethostname(host, sizeof(host));
      printf("%d on %s: ERROR: Error writing: write(%d, %p, %ld) returned 0 @ %s:%d\n",
              rank, host, fd, (char*) buf + n, size - n, __FILE__, __LINE__
      );
      MPI_Abort(MPI_COMM_WORLD, 0);
    } else { /* (rc < 0) */
      /* got an error, check whether it was serious */
      if(errno == EINTR || errno == EAGAIN) continue;

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Error writing: write(%d, %p, %ld) errno=%d %s @ %s:%d\n",
                rank, host, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Giving up write: write(%d, %p, %ld) errno=%d %s @ %s:%d\n",
                rank, host, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
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
    /*char c = 'a' + (rank+i) % 26;*/
    char c = (char) ((size_t)rank + i) % 256;
    buf[i] = c;
  }
  return 0;
}

/* write the checkpoint data to fd, and return whether the write was successful */
int write_checkpoint(int fd, int ckpt, char* buf, size_t size)
{
  int rc;
  int valid = 0;

  /* write the checkpoint id (application timestep) */
  char ckpt_buf[7];
  sprintf(ckpt_buf, "%06d", ckpt);
  rc = reliable_write(fd, ckpt_buf, sizeof(ckpt_buf)-1);
  if (rc < 0) { valid = 0; }

  /* write the checkpoint data */
  rc = reliable_write(fd, buf, size);
  if (rc < 0) { valid = 0; }

  return valid;
}

double getbw(char* file, char* buf, size_t size, int times)
{
  double bw = 0.0;

  MPI_Barrier(MPI_COMM_WORLD);

  if (times > 0) {
    /* start the timer */
    double time_start = MPI_Wtime();

    /* write the checkpoint file */
    int i, count = 0;
    for(i=0; i < times; i++) {
      int rc;
      int valid = 0;

      /* open the file and write the checkpoint */
      int fd_me = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fd_me > 0) {
        count++;
        valid = 1;

        /* write the checkpoint data */
        rc = write_checkpoint(fd_me, timestep, buf, size);
        if (rc < 0) {
          valid = 0;
        }

        /* force the data to storage */
        rc = fsync(fd_me);
        if (rc < 0) {
          valid = 0;
        }

        /* make sure the close is without error */
        rc = close(fd_me);
        if (rc < 0) {
          valid = 0;
        }
      }

      if (rank == 0) {
        printf("Completed checkpoint %d.\n", timestep);
        fflush(stdout);
      }

      /* increase the timestep counter */
      timestep++;

      /* optionally sleep for some time */
      if (seconds > 0) {
        if (rank == 0) { printf("Sleeping for %d seconds... \n", seconds); fflush(stdout); }
        sleep(seconds);
      }

      unlink(file);
    }

    /* stop the timer and compute the bandwidth */
    double time_end = MPI_Wtime();
    bw = ((size * count) / (1024*1024)) / (time_end - time_start);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  return bw;
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
  char* buf = (char*) malloc(filesize);
  
  /* make up some data for the next checkpoint */
  init_buffer(buf, filesize, rank, timestep);

  timestep++;

  /* prime system once before timing */
  getbw(name, buf, filesize, 1);

  /* now compute the bandwidth and print stats */
  if (times > 0) {
    double bw = getbw(name, buf, filesize, times);

    /* compute stats and print them to the screen */
    double bwmin, bwmax, bwsum;
    MPI_Reduce(&bw, &bwmin, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&bw, &bwmax, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&bw, &bwsum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    if (rank == 0) {
      printf("FileIO: Min %7.2f MB/s\tMax %7.2f MB/s\tAvg %7.2f MB/s\tAgg %7.2f MB/s\n",
             bwmin, bwmax, bwsum/ranks, bwsum
      );
    }
  }

  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

  MPI_Finalize();

  return 0;
}
