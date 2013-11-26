AC_INIT([Influxdb], [0.3.0], [support@influxdb.com], [influxdb], [http://influxdb.org/])
AC_SUBST(PROTOC)
AC_PATH_PROG(PROTOC, protoc, "notfound")
if test x"${PROTOC}" == x"notfound"; then
    AC_MSG_ERROR([Please install protobuf before trying to build Influxdb])
fi
AC_PATH_PROG(GO, go, "notfound")
if test x"${GO}" == x"notfound"; then
    AC_MSG_ERROR([Please install GO (or make sure it's on your path) before trying to build Influxdb])
fi
AC_PATH_PROG(BZR, bzr, "notfound")
if test x"${BZR}" == x"notfound"; then
    AC_MSG_ERROR([Please install bzr before trying to build Influxdb])
fi
AC_PATH_PROG(HG, hg, "notfound")
if test x"${HG}" == x"notfound"; then
    AC_MSG_ERROR([Please install mercurial before trying to build Influxdb])
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
   AC_MSG_ERROR([Please install flex before trying to build Influxdb])
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
   AC_MSG_ERROR([Please install bison before trying to build Influxdb])
fi
AC_OUTPUT([Makefile src/parser/Makefile])
