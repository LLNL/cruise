
AC_PREREQ(2.61)
AC_INIT([container],[0.1],[Container Library for in-system storage],[],[url])

AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])

m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR(src/container.h)
AC_CONFIG_HEADER([container_config.h])

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_LIBTOOL

AC_LANG_PUSH([C++])

AC_TYPE_UINT64_T


dnl ======================================
dnl === try to find boost unit testing ===
dnl ======================================

AX_BOOST_BASE([1.34],[have_boost=1], [have_boost=])

if test -n "$have_boost"; then
   AX_BOOST_UNIT_TEST_FRAMEWORK
fi

AM_CONDITIONAL(ENABLE_UNIT_TESTS, [test -n "$have_boost"])


dnl pthreads
AC_CHECK_LIB(pthread, [pthread_rwlock_rdlock], ,
              [AC_MSG_ERROR([Could not find pthreads library!])])

dnl ======================================
dnl ======================================
dnl ======================================

AC_CONFIG_FILES([Makefile])


AC_OUTPUT

