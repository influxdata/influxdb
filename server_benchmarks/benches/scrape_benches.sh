#!/bin/bash
#
# Scrapes the most recent run of all criterion benchmark results into lineprotocol for analysis
#
# Generate data:
#  (cd ~/Codes/influxdb_iox && cargo bench)
#
# Scrape data:
#  ./scrape_benches.sh /Users/alamb/Software/influxdb_iox2
#
# To load to


function main {
    SOURCE_DIR=$1
    if [ -z "$SOURCE_DIR" ] ; then
        echo "Error: influxdb_iox source directory not specified"
        echo "Usage: $0 <influxdb_iox_dir>"
        exit 1
    fi

    GITSHA=`cd $SOURCE_DIR && git rev-parse HEAD`
    # pick timestamp of the commit
    TIMESTAMP=$((`git show -s --format="%ct" $GITSHA` * 1000000000)) # ct means seconds since epoch
    # note can use this to use the time this script ran
    # TIMESTAMP=$((`date +%s` * 1000000000)) # +%s means seconds since epoch


    # Criterion produces files named like this:
    #
    # target/criterion/float_encode_sequential/10000/new/estimates.json
    # target/criterion/float_encode_sequential/10000/base/estimates.json
    # target/criterion/float_encode_sequential/100000/change/estimates.json
    # target/criterion/float_encode_sequential/100000/sample1/estimates.json
    # target/criterion/float_encode_sequential/100000/new/estimates.json
    # target/criterion/float_encode_sequential/100000/base/estimates.json
    #
    # The new/estimates.json are the ones from the most recent run
    find "$SOURCE_DIR/target/criterion" -name 'estimates.json' | grep 'new/estimates.json'| while read estimates_file ;
    do
        process_file "$estimates_file"
    done
}



# Processes a criterion results file and produces line protocol out
#
# Input:
#  {
#    "mean":{
#      "confidence_interval":{"confidence_level":0.95,"lower_bound":92384.98456288037,"upper_bound":94127.8605349043},
#      "point_estimate":93193.31282952648,
#      "standard_error":444.9439871182596
#    },
#    "median":{
#       "confidence_interval":{"confidence_level":0.95,"lower_bound":91137.96363636364,"upper_bound":92769.5854020979},
#       "point_estimate":91426.08165568294,
#       "standard_error":505.4331525578268
#    },
#    "median_abs_dev": .. (same structure )
#    "slope": .. (same structure )
#    "std_dev": .. (same structure )
#  }
#
# Output: (line protocol)
#
# bench,gitsha=<gitsha>,hostname=trogdor,group_name=float_encode_sequential,bench_name=10000 mean=93193.31282952648,mean_standard_error=444.9439871182596,median=91426.08165568294,median_standard_error=505.4331525578268
function process_file {
    estimates_file=$1
    #echo "processing $estimates_file"

    # example estimates_file:
    # /path/target/criterion/float_encode_sequential/10000/new/estimates.json

    # find the benchmark name (encoded as a filename)
    [[ $estimates_file =~ ^.*target/criterion/(.*)/new/estimates.json$ ]] && dirname=${BASH_REMATCH[1]}

    # echo $dirname
    # float_encode_sequential/10000
    #echo "dirname: $dirname"

    # split on `/`
    # https://stackoverflow.com/questions/918886/how-do-i-split-a-string-on-a-delimiter-in-bash)
    IFS=/ read -a fields <<<"$dirname"

    #echo "fields[0]: ${fields[0]}"
    #echo "fields[1]: ${fields[1]}"
    # fields[0]: float_encode_sequential
    # fields[1]: 10000

    # some benchmark names have spaces in them (thumbs down, so replace them with _)
    group_name=${fields[0]/ /_}
    bench_name=${fields[1]/ /_}

    hostname=`hostname`
    echo -n "bench,gitsha=$GITSHA,hostname=${hostname},group_name=$group_name,bench_name=$bench_name "

    # use the jq command to pull out the various fields
    echo -n `jq  -j '"mean=" + (.mean.point_estimate | tostring), ",mean_standard_error=" + (.mean.standard_error | tostring), ",median=" + (.median.point_estimate | tostring), ",median_standard_error=" + (.median.standard_error | tostring)' "$estimates_file"`

    echo -n " $TIMESTAMP"
    echo
}

main $*
