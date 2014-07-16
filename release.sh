#!/usr/bin/env bash

set -e

cd `dirname $0`

git checkout .
git pull --rebase
make clean
git clean -dfx
./configure

modified=$(git ls-files --modified | wc -l)

if [ $modified -ne 0 ]; then
    echo "Please commit or stash all your changes and try to run this command again"
    exit 1
fi

git fetch --tags

if [ $# -lt 1 ]; then
    current_version=`git tag | grep -v rc | sort -V | tail -n1`
    current_version=${current_version#v}
    version=`echo $current_version | awk 'BEGIN {FS="."}; {print $1 "." $2 "." ++$3}'`
else
    version=$1
fi

echo -n "Release version $version ? [Y/n] "
read response
response=`echo $response | tr 'A-Z' 'a-z'`
if [ "x$response" == "xn" ]; then
    echo "Aborting"
    exit 1
fi

echo "Releasing version $version"

if ! which aws > /dev/null 2>&1; then
    echo "Please install awscli see https://github.com/aws/aws-cli for more details"
    exit 1
fi

make clean
make package version=$version
make package version=$version arch=386
# make arch=arm CROSS_COMPILE=arm-unknown-linux-gnueabi package version=$version PATH=$PATH:$HOME/x-tools/arm-unknown-linux-gnueabi/bin
# rpm convention is not to have dashes in the package, or at least
# that's what fpm is claiming
rpm_version=${version/-/_}

for filepath in `ls packages/*.{tar.gz,deb,rpm}`; do
    [ -e "$filepath" ] || continue
    echo "Uploading $filepath to S3"
    filename=`basename $filepath`
    latest_filename=`echo ${filename} | sed "s/${rpm_version}/latest/g" | sed "s/${version}/latest/g"`
    bucket=influxdb

    AWS_CONFIG_FILE=~/aws.conf aws s3 cp $filepath s3://influxdb/$filename --acl public-read --region us-east-1
    AWS_CONFIG_FILE=~/aws.conf aws s3 cp $filepath s3://get.influxdb.org/$filename --acl public-read --region us-east-1

    case "$version" in
        *rc*) continue;;        # don't do upload the latest file
        *)
            AWS_CONFIG_FILE=~/aws.conf aws s3 cp $filepath s3://influxdb/${latest_filename} --acl public-read --region us-east-1
            AWS_CONFIG_FILE=~/aws.conf aws s3 cp $filepath s3://get.influxdb.org/${latest_filename} --acl public-read --region us-east-1
            ;;
    esac
done

branch=`git rev-parse --abbrev-ref HEAD`
git tag v$version
git push origin --tags
