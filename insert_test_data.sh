#!/bin/bash

# InfluxDB configuration
INFLUX_HOST="localhost"
INFLUX_PORT="8086"
INFLUX_DB="bbb"

# Function to convert RFC3339 timestamp to Unix nanoseconds
to_nanoseconds() {
    local timestamp=$1
    # Convert to seconds, then multiply by 1000000000 for nanoseconds
    echo $(( $(date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "$timestamp" "+%s") * 1000000000 ))
}

# Create array of data points
declare -a data_points=(
    # 2023 - First day of year, Sunday, Q1
    "cpu,host=server01,a=b value=1 $(to_nanoseconds "2023-01-01T00:00:00Z")"
    # 2023 - Monday in Q1, week 3
    "cpu,host=server01,a=b value=2 $(to_nanoseconds "2023-01-16T10:30:45Z")"
    # 2023 - Saturday in Q2
    "cpu,host=server01,a=a value=3 $(to_nanoseconds "2023-04-15T14:20:30Z")"
    # 2023 - Wednesday in Q3
    "cpu,host=server01,a=a value=4 $(to_nanoseconds "2023-07-19T08:15:22Z")"
    # 2023 - Friday in Q4
    "cpu,host=server02,a=b value=5 $(to_nanoseconds "2023-10-27T16:45:10Z")"
    # 2023 - Last day of year, Sunday
    "cpu,host=server02,a=b value=6 $(to_nanoseconds "2023-12-31T23:59:59Z")"
    # 2024 - First day of year, Monday, Q1
    "cpu,host=server02,a=a value=7 $(to_nanoseconds "2024-01-01T00:00:00Z")"
    # 2024 - Leap year day (Feb 29), Thursday
    "cpu,host=server02,a=a value=8 $(to_nanoseconds "2024-02-29T12:00:00Z")"
    # 2024 - Sunday in Q2
    "cpu,host=server02,a=c value=9 $(to_nanoseconds "2024-05-19T06:30:15Z")"
    # 2024 - Tuesday in Q3
    "cpu,host=server02,a=c value=10 $(to_nanoseconds "2024-08-06T18:45:00Z")"
    # 2024 - Saturday in Q4
    "cpu,host=server02,a=a value=11 $(to_nanoseconds "2024-11-23T22:10:55Z")"
    # 2024 - Last day of year, Tuesday
    "cpu,host=server03,a=a value=12 $(to_nanoseconds "2024-12-31T23:59:59Z")"
    # 2025 - First day of year, Wednesday, Q1
    "cpu,host=server03,a=b value=13 $(to_nanoseconds "2025-01-01T00:00:00Z")"
    # 2025 - Thursday in Q2
    "cpu,host=server03,a=c value=14 $(to_nanoseconds "2025-06-12T11:20:30Z")"
    # 2025 - Different times of day for same date
    "cpu,host=server03,a=b value=15 $(to_nanoseconds "2025-09-15T00:00:00Z")"  # Midnight
    "cpu,host=server03,a=b value=16 $(to_nanoseconds "2025-09-15T06:00:00Z")"  # 6 AM
    "cpu,host=server03,a=c value=17 $(to_nanoseconds "2025-09-15T12:00:00Z")"  # Noon
    "cpu,host=server03,a=c value=18 $(to_nanoseconds "2025-09-15T18:00:00Z")"  # 6 PM
    "cpu,host=server03,a=c value=19 $(to_nanoseconds "2025-09-15T23:59:59Z")"  # End of day
)

# Join all data points with newlines
data=$(printf '%s\n' "${data_points[@]}")

# Insert data into InfluxDB
echo "Inserting data into InfluxDB..."
curl -i -XPOST "http://${INFLUX_HOST}:${INFLUX_PORT}/write?db=${INFLUX_DB}" \
    --data-binary "$data"

echo ""
echo "Data insertion complete!"