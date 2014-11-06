AC_INIT([InfluxDB], [0.3.0], [support@InfluxDB.com], [InfluxDB], [http://InfluxDB.org/])
AC_SUBST(PROTOC)
AC_SUBST(GO)
AC_SUBST(GOFMT)
AC_SUBST(GOROOT)
AC_SUBST(AUTORECONF)
AC_SUBST(with_bison)
AC_SUBST(with_flex)
AC_SUBST(LRT_LDFLAG)
AC_SUBST(ROCKSDB_LIB)
AC_SUBST(sed_i)
AC_SUBST(VALGRIND)

AC_DEFUN([INFLUXDB_SED], [
  AC_MSG_CHECKING([sed -i])
  AC_CACHE_VAL(ac_cv_influxdb_sed_i, [
    f=/tmp/sed_$RANDOM
    echo -n 'version' > $f
    if sed -i 's/version/0.1/g' $f; then
      if test `cat $f` == "0.1"; then
        sed_i="sed -i"
        AC_MSG_RESULT(using sed -i)
      else
        AC_MSG_ERROR([Invalid sed implementation detected])
      fi
    elif sed -i '' 's/version/0.1/g' $f; then
      if test `cat $f` == "0.1"; then
        sed_i="sed -i ''"
        AC_MSG_RESULT(using sed -i '')
      else
        AC_MSG_ERROR([Invalid sed implementation detected])
      fi
    else
      AC_MSG_ERROR([Cannot find a sed implementation])
    fi
  ])
])

INFLUXDB_SED

AC_PATH_PROG(PROTOC, protoc, "notfound")
if test x"${PROTOC}" == x"notfound"; then
    AC_MSG_ERROR([Please install protobuf before trying to build InfluxDB])
fi

AC_PATH_PROG(VALGRIND, valgrind, "notfound")

AC_ARG_WITH([goroot],
  [AS_HELP_STRING([--with-goroot],
    [Set goroot to use])],
  [GOROOT=$withval],
  [])

AC_PATH_PROG(GO, go, "notfound", [$PATH$PATH_SEPARATOR$withval/bin$PATH_SEPARATOR])
if test x"${GO}" == x"notfound"; then
    AC_MSG_ERROR([Please install GO (or make sure it's on your path) before trying to build InfluxDB])
fi

AC_PATH_PROG(GOFMT, gofmt, "notfound", [$PATH$PATH_SEPARATOR$withval/bin$PATH_SEPARATOR])
if test x"${GOFMT}" == x"notfound"; then
    AC_MSG_ERROR([Please install GO (or make sure it's on your path) before trying to build InfluxDB])
fi

AC_PATH_PROG(HG, hg, "notfound")
if test x"${HG}" == x"notfound"; then
    AC_MSG_ERROR([Please install mercurial before trying to build InfluxDB])
fi

AC_ARG_WITH([flex],
  [AS_HELP_STRING([--with-flex],
    [Use flex given at that path])],
  [],
  [with_flex=])

if test x"${with_flex}" == x""; then
   AC_PATH_PROG(with_flex, flex, "notfound")
fi
if test x"${with_flex}" == x"notfound"; then
   AC_MSG_ERROR([Please install flex before trying to build InfluxDB])
fi
AC_ARG_WITH([bison],
  [AS_HELP_STRING([--with-bison],
    [Use bison given at that path])],
  [],
  [with_bison=])

if test x"${with_bison}" == x""; then
   AC_PATH_PROG(with_bison, bison, "notfound")
fi
if test x"${with_bison}" == x"notfound"; then
   AC_MSG_ERROR([Please install bison before trying to build InfluxDB])
fi

AC_ARG_WITH([rocksdb],
  [AS_HELP_STRING([--with-rocksdb],
    [Force building InfluxDB with rocksdb support])],
  [ROCKSDB_LIB=yes])

AC_PATH_PROG(AUTORECONF, autoreconf, "notfound")
if test x"${AUTORECONF}" == x"notfound"; then
   AC_MSG_ERROR([Please install autoconf before trying to build InfluxDB])
fi
AC_CHECK_LIB(z, gzdopen,,AC_MSG_ERROR("cannot find libz"))
AC_CHECK_LIB(bz2, BZ2_bzReadOpen,,AC_MSG_ERROR("cannot find libbz2"))

AC_SEARCH_LIBS(clock_gettime,rt,,)

if test x"${ac_cv_search_clock_gettime}" != x"none required" -a x"${ac_cv_search_clock_gettime}" != x"no"; then
   LRT_LDFLAG=${ac_cv_search_clock_gettime}
fi

AC_OUTPUT([Makefile parser/Makefile])
