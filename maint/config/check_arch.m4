dnl @synopsis CHECK_NUMA()
dnl
dnl This macro searches for an installed numa library. If nothing was
dnl specified when calling configure, it searches first in /usr/local
dnl and then in /usr. If the --with-numa=DIR is specified, it will try
dnl to find it in DIR/include/numa.h and DIR/lib/libz.a. If
dnl --without-numa is specified, the library is not searched at all.
dnl
dnl If either the header file (numa.h) or the library (libz) is not
dnl found, the configuration exits on error, asking for a valid numa
dnl installation directory or --without-numa.
dnl
dnl The macro defines the symbol HAVE_LIBZ if the library is found. You
dnl should use autoheader to include a definition for this symbol in a
dnl config.h file. Sample usage in a C/C++ source is as follows:
dnl
dnl   #ifdef HAVE_LIBZ
dnl   #include <numa.h>
dnl   #endif /* HAVE_LIBZ */
dnl
dnl @category InstalledPackages
dnl @author Loic Dachary <loic@senga.org>
dnl @version 2004-09-20
dnl @license GPLWithACException

AC_DEFUN([CHECK_ARCH],

#
# Handle user hints
#
[AC_MSG_CHECKING(architecture type)
AC_ARG_WITH([arch],
[AS_HELP_STRING([--with-numa=ARCH],[specify the architecture as bgq or linux])],
[if test "$withval" != no ; then
  AC_MSG_RESULT(yes)
    ARCH=$withval
    if test "${ARCH}" = "bgq"
    then
        AC_DEFINE([MACHINE_BGQ], [1], [Define if architecture is BG/Q])
    fi
else
    AC_MSG_RESULT(no)
fi])

])
