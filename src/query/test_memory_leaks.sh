#!/usr/bin/env bash

cat > test_memory.c <<EOF
#include "query_types.h"

int main(int argc, char **argv) {
  query q = parse_query("select value from t where c == '5';");
  close_query(&q);
  return 0;
}
EOF
gcc -g *.c
valgrind --error-exitcode=1 --leak-check=full ./a.out
valgrind_result=$?
rm ./a.out test_memory.c
exit $valgrind_result
