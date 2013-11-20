#!/usr/bin/env bash

cd `dirname $0`

brew_dir=/usr/local/Cellar
on_linux="no"
if [ `uname` = "Linux" ]; then
    on_linux=yes
fi

if [ $on_linux = no ]; then
    if ! ls -l $brew_dir/bison/* >/dev/null 2>&1; then
        echo "It looks like you don't have bison installed. Please run 'brew install bison'"
        exit 1
    fi
    if ! ls -l $brew_dir/flex/* >/dev/null 2>&1; then
        echo "It looks like you don't have flex installed. Please run 'brew install flex'"
        exit 1
    fi

    bison_path=`ls -d $brew_dir/bison/* | tail -1`
    flex_path=`ls -d $brew_dir/flex/* | tail -1`

    export PATH=$bison_path/bin:$flex_path/bin:$PATH
    echo $PATH
fi

bison -t -d query.yacc -o y.tab.c --defines=y.tab.h && flex -o lex.yy.c -i query.lex
