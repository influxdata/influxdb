#!/bin/bash

set -e

. ./exports.sh

influxdb_version=`cat VERSION`
rm -rf packages
mkdir packages

function setup_rvm {
    # Load RVM into a shell session *as a function*
    if [ -s "$HOME/.rvm/scripts/rvm" ]; then
        # First try to load from a user install
        source "$HOME/.rvm/scripts/rvm"
    elif [ -s "/usr/local/rvm/scripts/rvm" ]; then
        # Then try to load from a root install
        source "/usr/local/rvm/scripts/rvm"
    else
        printf "ERROR: An RVM installation was not found.\n"
    fi
    rvm use --create 1.9.3@errplane-agent
    gem install fpm
}

function package_files {
    if [ $# -ne 1 ]; then
        echo "Usage: $0 architecture"
        return 1
    fi

    rm -rf build
    mkdir build

    mv server build/influxdb

    cp config.json.sample build/config.json

    cp -R src/admin/site/ build/admin/

    tar_file=influxdb-$influxdb_version.$1.tar.gz

    tar -czf $tar_file build/*

    mv $tar_file packages/
}

function build_packages {
    if [ $# -ne 1 ]; then
        echo "Usage: $0 architecture"
        return 1
    fi

    if [ $1 == "386" ]; then
        rpm_args="setarch i386"
        deb_args="-a i386"
    fi

    rm -rf out_rpm
    mkdir -p out_rpm/opt/influxdb/versions/$influxdb_version
    cp -r build/* out_rpm/opt/influxdb/versions/$influxdb_version
    pushd out_rpm
    $rpm_args fpm  -s dir -t rpm -n influxdb -v $influxdb_version . || exit $?
    mv *.rpm ../packages/
    fpm  -s dir -t deb $deb_args -n influxdb -v $influxdb_version . || exit $?
    mv *.deb ../packages/
    popd
}

function setup_version {
    echo "Changing version from dev to $influxdb_version"
    sha1=`git rev-list --max-count=1 HEAD`
    sed -i.bak -e "s/var version = \"dev\"/var version = \"$influxdb_version\"/" -e "s/var gitSha = \"\"/var gitSha = \"$sha1\"/" src/server/server.go
}

function revert_version {
    if [ ! -e src/server/server.go.bak ]; then
        return
    fi

    rm src/server/server.go
    mv src/server/server.go.bak src/server/server.go
    echo "Changed version back to dev"
}

setup_rvm
setup_version
UPDATE=on ./build.sh && package_files amd64 && build_packages amd64
revert_version
[ $on_linux == yes ] && CGO_ENABLED=1 GOARCH=386 UPDATE=on ./build.sh && package_files 386 && build_packages 386
