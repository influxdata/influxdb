#!/usr/bin/env bash

cat > test_memory.c <<EOF
#include "query_types.h"

int main(int argc, char **argv) {
  // test freeing on close
  query q = parse_query("select count(*) from users.events group_by user_email,time(1h) where time>now()-1d;");
  close_query(&q);

  // test freeing on error
  q = parse_query("select count(*) from users.events group_by user_email,time(1h) where time >> now()-1d;");
  close_query(&q);

  // test freeing where conditions
  q = parse_query("select value from t where c == 5 and b == 6;");
  close_query(&q);

  // test freeing simple query
  q = parse_query("select value from t where c == '5';");
  close_query(&q);

  // test freeing on error
  q = parse_query("select value from t where c = '5';");
  close_query(&q);

  q = parse_query("select value from cpu.idle where value > 90 and (time > now() - 1d or value > 80) and time < now() - 1w;");
  close_query(&q);

  q = parse_query("select value from cpu.idle where value > 90 and (time > now() - 1d or value > 80) and time < now() - 1w last 10;");
  close_query(&q);

  q = parse_query("select email from users.events where email =~ /gmail\\\\.com/i and time>now()-2d;");
  close_query(&q);

  q = parse_query("select email from users.events as events where email === /gmail\\\\.com/i and time>now()-2d;");
  close_query(&q);

  q = parse_query("select email from users.events where email in ('jvshahid@gmail.com')");
  close_query(&q);

  return 0;
}
EOF
gcc -g *.c
valgrind --error-exitcode=1 --leak-check=full ./a.out
valgrind_result=$?
rm ./a.out test_memory.c
exit $valgrind_result
