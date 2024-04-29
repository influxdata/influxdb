from flask import Flask, jsonify, render_template, request
import os
import csv
import math
import sys
import re

app = Flask(__name__)

# Directory path (provided as a command-line argument)
RESULTS_DIRECTORY = 'results/'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/test-names')
def get_test_names():
    test_names = [name for name in os.listdir(RESULTS_DIRECTORY) if os.path.isdir(os.path.join(RESULTS_DIRECTORY, name))]
    return jsonify(test_names)

@app.route('/api/config-names')
def get_config_names():
    test_name = request.args.get('test_name')
    test_path = os.path.join(RESULTS_DIRECTORY, test_name)
    config_names = {}

    for config_name in os.listdir(test_path):
        config_path = os.path.join(test_path, config_name)
        if os.path.isdir(config_path):
            run_times = set()
            for file_name in os.listdir(config_path):
                match = re.search(r'_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})', file_name)
                if match:
                    run_time = match.group(1)
                    run_times.add(run_time)
            config_names[config_name] = sorted(run_times)

    return jsonify(config_names)

@app.route('/api/aggregated-data')
def get_aggregated_data():
    test_name = request.args.get('test_name')
    config_name = request.args.get('config_name')
    run_time = request.args.get('run_time')

    config_path = os.path.join(RESULTS_DIRECTORY, test_name, config_name)
    write_file = os.path.join(config_path, f'write_{run_time}.csv')
    query_file = os.path.join(config_path, f'query_{run_time}.csv')
    system_file = os.path.join(config_path, f'system_{run_time}.csv')

    if not os.path.isfile(write_file) and not os.path.isfile(query_file) and not os.path.isfile(system_file):
        return jsonify({'error': 'Files not found for the specified configuration and run time'})

    write_data = None
    if os.path.isfile(write_file):
        write_data = aggregate_data(write_file, 'lines', 'latency_ms')
    query_data = None
    if os.path.isfile(query_file):
        query_data = aggregate_data(query_file, 'rows', 'response_ms')
    system_data = None
    if os.path.isfile(system_file):
        system_data = aggregate_system_data(system_file)
    

    aggregated_data = {
        'config_name': config_name,
        'run_time': run_time,
        'write_data': write_data,
        'query_data': query_data,
        'system_data': system_data
    }

    return jsonify(aggregated_data)

def aggregate_data(file_path, lines_field, latency_field):
    aggregated_data = []

    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        data = list(reader)

    for row in data:
        test_time = int(row['test_time_ms'])
        lines = int(row[lines_field])
        latency = int(row[latency_field])

        aggregated_data.append({
            'test_time': test_time,
            'lines': lines,
            'latency': latency
        })

    return aggregated_data

def aggregate_system_data(file_path):
    aggregated_data = []

    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        data = list(reader)

    for row in data:
        aggregated_data.append({
            'test_time': int(row['test_time_ms']),
            'cpu_usage': float(row['cpu_usage']),
            'memory_bytes': int(row['memory_bytes']) / 1024 / 1024
        })

    return aggregated_data

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python app.py <results_directory>')
        print('results directory not provided, defaulting to "results/"')
    else:
        RESULTS_DIRECTORY = sys.argv[1]

    app.run()
