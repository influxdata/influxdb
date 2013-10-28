#!/usr/bin/env bash

cd `dirname $0`

modified=$(git ls-files --modified | wc -l)

if [ $modified -ne 0 ]; then
    echo "Please commit or stash all your changes and try to run this command again"
    exit 1
fi

git fetch --tags

if [ $# -ne 1 ]; then
    current_version=`git tag | sort -V | tail -n1`
    current_version=${current_version#v}
    version=`echo $current_version | awk 'BEGIN {FS="."}; {print $1 "." $2 "." ++$3}'`
else
    version=$1
fi

if [ "x$assume_yes" != "xtrue" ]; then
    echo -n "Release version $version ? [Y/n] "
    read response
    response=`echo $response | tr 'A-Z' 'a-z'`
    if [ "x$response" == "xn" ]; then
        echo "Aborting"
        exit 1
    fi
fi

echo "Releasing version $version"

if ! which aws > /dev/null 2>&1; then
    echo "Please install awscli see https://github.com/aws/aws-cli for more details"
    exit 1
fi

if ! ./package.sh $version; then
    echo "Build failed. Aborting the release"
    exit 1
fi

for filepath in `ls packages/*.{tar.gz,deb,rpm}`; do
    [ -e "$filepath" ] || continue
    echo "Uploading $filepath to S3"
    filename=`basename $filepath`
    latest_filename=`echo ${filename} | sed "s/${version}/latest/g"`
    bucket=influxdb
    AWS_CONFIG_FILE=~/aws.conf aws s3 put-object --bucket $bucket --key $filename --body $filepath --acl public-read
    AWS_CONFIG_FILE=~/aws.conf aws s3 put-object --bucket $bucket --key ${latest_filename} --body $filepath --acl public-read
done

branch=`git rev-parse --abbrev-ref HEAD`
git tag v$version
git push origin --tags
