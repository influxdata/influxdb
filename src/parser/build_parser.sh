#!/usr/bin/env bash

cd `dirname $0`

source ../../exports.sh

brew_dir=/usr/local/Cellar
if [ $on_linux == no ]; then
    if ! ls -l $brew_dir/bison/* >/dev/null 2>&1; then
        echo "It looks like you don't have bison installed. Please run 'brew install bison'"
        exit 1
    fi
    if ! ls -l $brew_dir/flex/* >/dev/null 2>&1; then
        echo "It looks like you don't have flex installed. Please run 'brew install flex'"
        exit 1
    fi

    bison_version=`ls -l $brew_dir/bison/* | sort -Vr | head -1`
    flex_version=`ls -l $brew_dir/flex/* | sort -Vr | head -1`

    export PATH=$brew_dir/bison/$bison_version/bin:$brew_dir/flex/$flex_version/bin:$PATH
fi

bison -t -d query.yacc -o y.tab.c --defines=y.tab.h && flex -o lex.yy.c -i query.lex
