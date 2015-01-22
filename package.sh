#!/bin/bash

INSTALL_ROOT_DIR=/opt/influxdb
CONFIG_ROOT_DIR=/etc/opt/influxdb

SAMPLE_CONFIGURATION=etc/config.sample.toml

TMP_WORK_DIR=`mktemp -d`
POST_INSTALL_PATH=`mktemp`
ARCH=`uname -i`

###########################################################################
# Helper functions.
###########################################################################

# usage prints simple usage information.
usage() {
    echo -e "$0 [<version>] [-h]\n"
    cleanup_exit $1
}

# cleanup_exit removes all resources created during the process and exits with
# the supplied returned code.
cleanup_exit() {
    rm -r $TMP_WORK_DIR
    rm $POST_INSTALL_PATH
    exit $1
}

# check_gopath sanity checks the value of the GOPATH env variable.
check_gopath() {
    [ -z "$GOPATH" ] && echo "GOPATH is not set." && cleanup_exit 1
    [ ! -d "$GOPATH" ] && echo "GOPATH is not a directory." && cleanup_exit 1
    echo "GOPATH ($GOPATH) looks sane."
}

# check_clean_tree ensures that no source file is locally modified.
check_clean_tree() {
    modified=$(git ls-files --modified | wc -l)
    if [ $modified -ne 0 ]; then
        echo "The source tree is not clean -- aborting."
        cleanup_exit 1
    fi
    echo "Git tree is clean."
}

# update_tree ensures the tree is in-sync with the repo.
update_tree() {
    git pull origin master
    if [ $? -ne 0 ]; then
        echo "Failed to pull latest code -- aborting."
        cleanup_exit 1
    fi
    git fetch --tags
    if [ $? -ne 0 ]; then
        echo "Failed to fetch tags -- aborting."
        cleanup_exit 1
    fi
    echo "Git tree updated successfully."
}

# check_tag_exists checks if the existing release already exists in the tags.
check_tag_exists () {
    version=$1
    git tag | grep -q "^v$version$"
    if [ $? -eq 0 ]; then
        echo "Proposed version $version already exists as a tag -- aborting."
        cleanup_exit 1
    fi
}

# make_dir_tree creates the directory structure within the packages.
make_dir_tree() {
    work_dir=$1
    version=$2
    mkdir -p $work_dir/$INSTALL_ROOT_DIR/versions/$version
    if [ $? -ne 0 ]; then
        echo "Failed to create installation directory -- aborting."
        cleanup_exit 1
    fi
    mkdir -p $work_dir/$CONFIG_ROOT_DIR
    if [ $? -ne 0 ]; then
        echo "Failed to create configuration directory -- aborting."
        cleanup_exit 1
    fi
}


# do_build builds the code.
do_build() {
    rm $GOPATH/bin/*
    go install -a ./...
    if [ $? -ne 0 ]; then
        echo "Build failed, unable to create package -- aborting"
        cleanup_exit 1
    fi
    echo "Build completed successfully."
}

# generate_postinstall_script creates the post-install script for the
# package. It must be passed the version.
generate_postinstall_script() {
    version=$1
    cat  <<EOF >$POST_INSTALL_PATH
rm -f $INSTALL_ROOT_DIR/influxd
ln -s  $INSTALL_ROOT_DIR/versions/$version/influxd $INSTALL_ROOT_DIR/influxd

if ! id influxdb >/dev/null 2>&1; then
        useradd --system -U -M influxdb
fi
chown -R -L influxdb:influxdb $INSTALL_ROOT_DIR
chmod -R a+rX $INSTALL_ROOT_DIR
EOF
    echo "Post-install script created successfully at $POST_INSTALL_PATH"
}

###########################################################################
# Start the packaging process.
###########################################################################

if [ $# -ne 1 ]; then
    usage 1
elif [ $1 == "-h" ]; then
    usage 0
else
    VERSION=$1
fi

echo -e "\nStarting package process...\n"

check_gopath
check_clean_tree
update_tree
check_tag_exists $VERSION
do_build
make_dir_tree $TMP_WORK_DIR $VERSION

cp $GOPATH/bin/* $TMP_WORK_DIR/$INSTALL_ROOT_DIR/versions/$VERSION
if [ $? -ne 0 ]; then
    echo "Failed to copy binaries to packaging directory -- aborting."
    cleanup_exit 1
fi
echo "Binaries in $GOPATH/bin copied to $TMP_WORK_DIR/$INSTALL_ROOT_DIR/versions/$VERSION"

cp $SAMPLE_CONFIGURATION $TMP_WORK_DIR/$CONFIG_ROOT_DIR
if [ $? -ne 0 ]; then
    echo "Failed to copy $SAMPLE_CONFIGURATION to packaging directory -- aborting."
    cleanup_exit 1
fi

generate_postinstall_script $VERSION

echo -n "Commence creation of $ARCH packages, version $VERSION? [Y/n] "
read response
response=`echo $response | tr 'A-Z' 'a-z'`
if [ "x$response" == "xn" ]; then
    echo "Packaging aborted."
    cleanup_exit 1
fi

if [ $ARCH == "i386" ]; then
    rpm_package=influxdb-$VERSION-1.i686.rpm
    debian_package=influxdb_${VERSION}_i686.deb
    deb_args="-a i686"
    rpm_args="setarch i686"
elif [ $ARCH == "arm" ]; then
    rpm_package=influxdb-$VERSION-1.armel.rpm
    debian_package=influxdb_${VERSION}_armel.deb
else
    rpm_package=influxdb-$VERSION-1.x86_64.rpm
    debian_package=influxdb_${VERSION}_amd64.deb
fi

COMMON_FPM_ARGS="--after-install $POST_INSTALL_PATH -n influxdb -v $VERSION -C $TMP_WORK_DIR ."
DESCRIPTION="Distributed time-series database"
$rpm_args fpm -s dir -t rpm --description "$DESCRIPTION" $COMMON_FPM_ARGS
if [ $? -ne 0 ]; then
    echo "Failed to create RPM package -- aborting."
    cleanup_exit 1
fi
echo "RPM package created successfully."

fpm -s dir -t deb $deb_args --description "$DESCRIPTION"  $COMMON_FPM_ARGS
if [ $? -ne 0 ]; then
    echo "Failed to create Debian package -- aborting."
    cleanup_exit 1
fi
echo "Debian package created successfully."

echo "Packaging process complete."
cleanup_exit 0
