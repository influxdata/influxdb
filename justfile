default:
    @just --list

# Build the load generator
build-lg:
    cargo build -p influxdb3_load_generator --release

results_file := "results/compact_tag_len_high_card.csv"
rows_per_file := "1000000"

run-lg: build-lg
    #!/usr/bin/env sh
    for n_tags in 1 2 5 7 10 12 15; do
    for seed in $(seq 0 9); do
    for sid in true false; do
    for n_threads in 4; do
    for card in 100000 1000000; do
    for base_len in 15 30; do
    echo "################################################";
    echo "n_tags $n_tags | seed $seed | series_id $sid | base_len $base_len";
    echo "################################################";
    rm -rf .local;
    if $sid; then
        ./target/release/influxdb3_load_generator compact \
            --object-store file \
            --data-dir .local \
            --num-input-files 10 \
            --rows-per-file {{rows_per_file}} \
            --cardinality $card \
            --seed $seed \
            --num-tags $n_tags \
            --num-threads $n_threads \
            --series-id \
            --tag-base-len $base_len \
            --results-file {{results_file}}
    else
        ./target/release/influxdb3_load_generator compact \
            --object-store file \
            --data-dir .local \
            --num-input-files 10 \
            --rows-per-file {{rows_per_file}} \
            --cardinality $card \
            --seed $seed \
            --num-tags $n_tags \
            --num-threads $n_threads \
            --tag-base-len $base_len \
            --results-file {{results_file}}
    fi
    done
    done
    done
    done
    done
    done
