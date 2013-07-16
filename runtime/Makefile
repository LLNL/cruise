all: lib/libscrmfs-posix.a

DESTDIR =
srcdir = .
prefix = /g/g92/rajachan/scrmfs/runtime/install
exec_prefix = ${prefix}
datarootdir = $(DESTDIR)${prefix}/share
includedir = $(DESTDIR)${prefix}/include
mandir = $(DESTDIR)${datarootdir}/man
sbindir = $(DESTDIR)${exec_prefix}/sbin
bindir = $(DESTDIR)${exec_prefix}/bin
libdir = $(DESTDIR)${exec_prefix}/lib
LDFLAGS =  -L/usr/lib
CC = mpicc
LD = @LD@

CONTAINER_HOME = ./container
CONTAINER_INCS = -I$(CONTAINER_HOME)/include
CONTAINER_LDFLAGS = -L$(CONTAINER_HOME)/lib
CONTAINER_LIBS = #-lcontainer
#CONTAINER_LIBNAME = $(CONTAINER_HOME)/lib/libcontainer.a

NUMA_LIBS = -lnuma


DISABLE_LDPRELOAD = 
SCRMFS_LOG_FORMAT = $(srcdir)/../scrmfs-log-format.h

ifndef DISABLE_LDPRELOAD
all: lib/libscrmfs.so 
endif



CFLAGS = -DSCRMFS_CONFIG_H=\"scrmfs-runtime-config.h\" -I . -I ../ -I $(srcdir) -I$(srcdir)/../ -O3  -I/usr/include -D_LARGEFILE64_SOURCE 

CFLAGS_SHARED = -DSCRMFS_CONFIG_H=\"scrmfs-runtime-config.h\" -I . -I$(srcdir) -I$(srcdir)/../ -O3  -I/usr/include -D_LARGEFILE64_SOURCE -shared -fpic -DPIC -DSCRMFS_PRELOAD

LIBS = -lz @LIBBZ2@ $(NUMA_LIBS)

lib::
	@mkdir -p $@

HEADERS = \
  lib/scrmfs-stack.h \
  lib/scrmfs-fixed.h \
  lib/scrmfs-container.h \
  lib/scrmfs-sysio.h \
  lib/scrmfs-stdio.h \
  lib/scrmfs-internal.h \
  lib/scrmfs.h

OBJS = \
  lib/lookup3.o \
  lib/lookup8.o \
  lib/scrmfs-stack.o \
  lib/scrmfs-fixed.o \
  lib/scrmfs-container.o \
  lib/scrmfs-sysio.o \
  lib/scrmfs-stdio.o \
  lib/scrmfs.o

POBJS = \
  lib/lookup3.po \
  lib/lookup8.po \
  lib/scrmfs-stack.po \
  lib/scrmfs-fixed.po \
  lib/scrmfs-container.po \
  lib/scrmfs-sysio.po \
  lib/scrmfs-stdio.po \
  lib/scrmfs.po

#lib/scrmfs-mpi-io.o: lib/scrmfs-mpi-io.c scrmfs.h scrmfs-dynamic.h $(SCRMFS_LOG_FORMAT) | lib
#	$(CC) $(CFLAGS) -c $< -o $@

#lib/scrmfs-mpi-io.po: lib/scrmfs-mpi-io.c scrmfs.h scrmfs-dynamic.h $(SCRMFS_LOG_FORMAT) | lib
#	$(CC) $(CFLAGS_SHARED) -c $< -o $@

#lib/scrmfs-mpi-init-finalize.o: lib/scrmfs-mpi-init-finalize.c scrmfs.h scrmfs-dynamic.h $(SCRMFS_LOG_FORMAT) | lib
#	$(CC) $(CFLAGS) -c $< -o $@

#lib/scrmfs-mpi-init-finalize.po: lib/scrmfs-mpi-init-finalize.c scrmfs.h scrmfs-dynamic.h $(SCRMFS_LOG_FORMAT) | lib
#	$(CC) $(CFLAGS_SHARED) -c $< -o $@

lib/scrmfs-fixed.o: lib/scrmfs-fixed.c $(HEADERS)
	$(CC) $(CFLAGS) $(CONTAINER_INCS) -c $< -o $@

lib/scrmfs-fixed.po: lib/scrmfs-fixed.c $(HEADERS)
	$(CC) $(CFLAGS_SHARED) $(CONTAINER_INCS) -c $< -o $@


lib/scrmfs-container.o: lib/scrmfs-container.c $(HEADERS)
	$(CC) $(CFLAGS) $(CONTAINER_INCS) -c $< -o $@

lib/scrmfs-container.po: lib/scrmfs-container.c $(HEADERS)
	$(CC) $(CFLAGS_SHARED) $(CONTAINER_INCS) -c $< -o $@


lib/scrmfs.o: lib/scrmfs.c $(HEADERS)
	$(CC) $(CFLAGS) $(CONTAINER_INCS) -c lib/scrmfs.c -o $@

lib/scrmfs.po: lib/scrmfs.c $(HEADERS)
	$(CC) $(CFLAGS_SHARED) $(CONTAINER_INCS) -c lib/scrmfs.c -o $@


lib/scrmfs-sysio.o: lib/scrmfs-sysio.c $(HEADERS)
	$(CC) $(CFLAGS) -c lib/scrmfs-sysio.c -o $@

lib/scrmfs-sysio.po: lib/scrmfs-sysio.c $(HEADERS)
	$(CC) $(CFLAGS_SHARED) $(CONTAINER_INCS) -c lib/scrmfs-sysio.c -o $@


lib/scrmfs-stdio.o: lib/scrmfs-stdio.c $(HEADERS)
	$(CC) $(CFLAGS) -c lib/scrmfs-stdio.c -o $@

lib/scrmfs-stdio.po: lib/scrmfs-stdio.c $(HEADERS)
	$(CC) $(CFLAGS_SHARED) $(CONTAINER_INCS) -c lib/scrmfs-stdio.c -o $@


lib/lookup3.o: lib/lookup3.c
	$(CC) $(CFLAGS) -c $< -o $@

lib/lookup3.po: lib/lookup3.c
	$(CC) $(CFLAGS_SHARED) -c $< -o $@


lib/lookup8.o: lib/lookup8.c
	$(CC) $(CFLAGS) -c $< -o $@

lib/lookup8.po: lib/lookup8.c
	$(CC) $(CFLAGS_SHARED) -c $< -o $@


lib/scrmfs-stack.o: lib/scrmfs-stack.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

lib/scrmfs-stack.po: lib/scrmfs-stack.c $(HEADERS)
	$(CC) $(CFLAGS_SHARED) -c $< -o $@


%.i: %.c
	$(CC) -E $(CFLAGS) -c $< -o $@

lib/libscrmfs-posix.a: $(OBJS)
	ar rcs $@ $^

lib/libscrmfs.so: $(POBJS)
	$(CC) $(CFLAGS_SHARED) $(LDFLAGS) $(CONTAINER_LDFLAGS) -ldl -o $@ $^ -lpthread -lrt -lz $(CONTAINER_LIBS)

install:: all
	install -d $(libdir)
	install -m 755 lib/libscrmfs-posix.a $(libdir)
#	install -m 755 lib/libscrmfs-mpi-io.a $(libdir)
ifndef DISABLE_LDPRELOAD
	install -m 755 lib/libscrmfs.so $(libdir)
endif
	install -d $(bindir)
	install -m 755 scrmfs-config $(bindir)

clean::
	rm -f *.o *.po *.a lib/*.o lib/*.po lib/*.a lib/*.so 

distclean:: clean
	rm -f scrmfs-runtime-config.h aclocal.m4 autom4te.cache/* config.status config.log Makefile 
	rm -rf autom4te.cache
