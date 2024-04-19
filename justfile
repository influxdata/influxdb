default:
    @just --list

# Build the load generator
build-lg:
    cargo build -p influxdb3_load_generator --release

run-lg: build-lg
    #!/usr/bin/env sh
    for n_tags in $(seq 1 15); do
    for seed in $(seq 0 9); do
    for sid in true false; do
    for n_threads in 1 2 4; do
    echo "n_tags $n_tags seed $seed series_id $sid n_threads $n_threads";
    rm -rf .local;
    if $sid; then
        ./target/release/influxdb3_load_generator compact \
            --object-store file \
            --data-dir .local \
            --num-input-files 10 \
            --rows-per-file 1000000 \
            --cardinality 10000 \
            --seed $seed \
            --num-tags $n_tags \
            --num-threads $n_threads \
            --series-id \
            --results-file results/compact.csv
    else
        ./target/release/influxdb3_load_generator compact \
            --object-store file \
            --data-dir .local \
            --num-input-files 10 \
            --rows-per-file 1000000 \
            --cardinality 10000 \
            --seed $seed \
            --num-tags $n_tags \
            --num-threads $n_threads \
            --results-file results/compact.csv
    fi
    done
    done
    done
    done
