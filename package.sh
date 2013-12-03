#!/bin/bash

set -e

. ./exports.sh

if [ $# -ne 1 ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

admin_dir=`mktemp -d`
influxdb_version=$1
rm -rf packages
mkdir packages
bundle install

function package_admin_interface {
    pushd $admin_dir
    git clone https://github.com/influxdb/influxdb-admin.git .
    rvm rvmrc trust ./.rvmrc

    gem install bundler
    bundle install
    bundle exec middleman build
    popd
}

function packae_source {
    # make sure we revert the changes we made to the levigo
    # source packages are used by MacOSX which should be using
    # dynamic linking
    pushd src/github.com/jmhodges/levigo/
    git checkout .
    popd

    rm -f influxd
    rm -f server
    git ls-files --others  | egrep -v 'github|launchpad|code.google' > /tmp/influxdb.ignored
    echo "pkg/*" >> /tmp/influxdb.ignored
    echo "packages/*" >> /tmp/influxdb.ignored
    echo "build/*" >> /tmp/influxdb.ignored
    echo "out_rpm/*" >> /tmp/influxdb.ignored
    tar_file=influxdb-$influxdb_version.src.tar.gz
    tar -czf packages/$tar_file --exclude-vcs -X /tmp/influxdb.ignored *
    pushd packages
    # put all files in influxdb
    mkdir influxdb
    tar -xzf $tar_file -C influxdb
    rm $tar_file
    tar -czf $tar_file influxdb
    popd
}

function package_files {
    if [ $# -ne 1 ]; then
        echo "Usage: $0 architecture"
        return 1
    fi

    rm -rf build
    mkdir build

    package_admin_interface

    mv daemon build/influxdb

    # cp -R src/admin/site/ build/admin/
    mkdir build/admin
    cp -R $admin_dir/build/* build/admin/

    cp -R scripts/ build/

    tar_file=influxdb-$influxdb_version.$1.tar.gz

    tar -czf $tar_file build/*

    mv $tar_file packages/

    # the tar file should use "./assets" but the deb and rpm packages should use "/opt/influxdb/current/admin"
    cat > build/config.json <<EOF
{
  "AdminHttpPort":  8083,
  "AdminAssetsDir": "/opt/influxdb/current/admin",
  "ApiHttpPort":    8086,
  "RaftServerPort": 8090,
  "SeedServers":    [],
  "DataDir":        "/opt/influxdb/shared/data/db",
  "RaftDir":        "/opt/influxdb/shared/data/raft"
}
EOF
    rm build/*.bak
    rm build/scripts/*.bak
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
    $rpm_args fpm  -s dir -t rpm --after-install ../scripts/post_install.sh -n influxdb -v $influxdb_version . || exit $?
    mv *.rpm ../packages/
    fpm  -s dir -t deb $deb_args --after-install ../scripts/post_install.sh -n influxdb -v $influxdb_version . || exit $?
    mv *.deb ../packages/
    popd
}

function setup_version {
    echo "Changing version from dev to $influxdb_version"
    sha1=`git rev-list --max-count=1 HEAD`
    sed -i.bak -e "s/version = \"dev\"/version = \"$influxdb_version\"/" -e "s/gitSha\s*=\s*\"HEAD\"/gitSha = \"$sha1\"/" src/daemon/influxd.go
    sed -i.bak -e "s/REPLACE_VERSION/$influxdb_version/" scripts/post_install.sh
}

function revert_version {
    if [ -e src/daemon/influxd.go.bak ]; then
        rm src/daemon/influxd.go
        mv src/daemon/influxd.go.bak src/daemon/influxd.go
    fi

    if [ -e scripts/post_install.sh ]; then
        rm scripts/post_install.sh
        mv scripts/post_install.sh.bak scripts/post_install.sh
    fi

    echo "Changed version back to dev"
}

setup_version
UPDATE=on ./build.sh && package_files amd64 && build_packages amd64
# we need to build to make sure all the dependencies are downloaded
[ $on_linux == yes ] && CGO_ENABLED=1 GOARCH=386 UPDATE=on ./build.sh && package_files 386 && build_packages 386
packae_source
revert_version
