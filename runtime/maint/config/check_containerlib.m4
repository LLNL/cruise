dnl @synopsis CHECK_CONTAINER_LIB()
dnl
dnl This macro searches for an installed container library. If nothing was
dnl specified when calling configure, it searches first in /usr/local
dnl and then in /usr. If the --with-container-lib=DIR is specified, it will try
dnl to find it in DIR/include/container.h and DIR/lib/libcontainer.a. If
dnl --without-continer-lib is specified, the library is not searched at all.
dnl
dnl If either the header file (container.h) or the library (libcontainer) is not
dnl found, the configuration exits on error, asking for a valid container 
dnl installation directory or --without-container-lib.
dnl
dnl The macro defines the symbol HAVE_CONTAINER_LIB if the library is found. You
dnl should use autoheader to include a definition for this symbol in a
dnl config.h file. Sample usage in a C/C++ source is as follows:
dnl
dnl   #ifdef HAVE_CONTAINER_LIB
dnl   #include <container-lib.h>
dnl   #endif /* HAVE_CONTAINER_LIB */
dnl

AC_DEFUN([CHECK_CONTAINER_LIB],

#
# Handle user hints
#
[AC_MSG_CHECKING(if container-lib is wanted)
AC_ARG_WITH(container-lib,
[  --with-container-lib=DIR root directory path of container-lib installation [defaults to
                    /usr/local or /usr if not found in /usr/local]
  --without-container-lib to disable container-lib usage completely],
[if test "$withval" != no ; then
  AC_MSG_RESULT(yes)
  if test -d "$withval"
  then
    CONTAINER_LIB_HOME="$withval"
    AC_DEFINE([HAVE_CONTAINER_LIB], [1], [Define if libcontainer is available])

  else
    AC_MSG_WARN([Sorry, $withval does not exist, checking usual places])
  fi
else
  AC_MSG_RESULT(no)
fi])

#
# Locate container-lib, if wanted
#
if test -n "${CONTAINER_LIB_HOME}"
then
        CONTAINER_LIB_OLD_LDFLAGS=$LDFLAGS
        CONTAINER_LIB_OLD_CPPFLAGS=$LDFLAGS
        LDFLAGS="$LDFLAGS -L${CONTAINER_LIB_HOME}/lib"
        CPPFLAGS="$CPPFLAGS -I${CONTAINER_LIB_HOME}/include"
        AC_LANG_SAVE
        AC_LANG_C
        AC_CHECK_LIB(container, cs_store_init, [container_lib_cv_libcontainer=yes], [container_lib_cv_libcontainer=no])
        AC_CHECK_HEADER(container.h, [container_lib_cv_container_h=yes], [container_lib_cv_container_h=no])
        AC_LANG_RESTORE
        if test "$container_lib_cv_libcontainer" = "yes" -a "$container_lib_cv_container_h" = "yes"
        then
                #
                # If both library and header were found, use them
                #
                AC_CHECK_LIB(container, cs_store_init)
                AC_MSG_CHECKING(container-lib in ${CONTAINER_LIB_HOME})
                AC_MSG_RESULT(ok)
        else
                #
                # If either header or library was not found, revert and bomb
                #
                AC_MSG_CHECKING(container-lib in ${CONTAINER_LIB_HOME})
                LDFLAGS="$CONTAINER_LIB_OLD_LDFLAGS"
                CPPFLAGS="$CONTAINER_LIB_OLD_CPPFLAGS"
                AC_MSG_RESULT(failed)
                AC_MSG_ERROR(either specify a valid container-lib installation with --with-container-lib=DIR or disable container-lib usage with --without-container-lib)
        fi
fi

])
