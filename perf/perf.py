#!/usr/bin/env python3

import argparse
import random
import glob
import logging
import math
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import threading
import time

import docker
import grpc_requests
import minio
import requests
import toml
import urllib3

ioxperf_name = "ioxperf"
ioxperf_labels = {ioxperf_name: None}
ioxperf_filters = {'label': ioxperf_name}
org_name = 'myorg'
bucket_name = 'mybucket'
db_name = '%s_%s' % (org_name, bucket_name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-build', help='do not build IOx, execute existing binaries', action='store_true')
    parser.add_argument('--debug', help='build/execute debug IOx binaries instead of release', action='store_true')
    parser.add_argument('--object-store', help='object store type', default='s3', choices=('memory', 's3', 'file'))
    parser.add_argument('--kafka-zookeeper', help='use Kafka/ZooKeeper instead of Redpanda', action='store_true')
    parser.add_argument('--hold', help='keep all services running after tests complete', action='store_true')
    parser.add_argument('--cleanup', help='remove Docker assets and exit (TODO terminate IOx processes)',
                        action='store_true')
    parser.add_argument('--no-volumes', help='do not mount Docker volumes', action='store_true')
    parser.add_argument('--no-jaeger', help='do not collect traces in Jaeger', action='store_true')
    parser.add_argument('batteries', help='name of directories containing test batteries, or "all"', nargs='*')
    args = parser.parse_args()

    do_trace = args.hold and not args.no_jaeger

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    try:
        dc = docker.from_env()
    except docker.errors.DockerException as e:
        print('failed to communicate with Docker, is Docker running?')
        exit(1)

    if args.cleanup:
        docker_cleanup_resources(dc)
        return
    cleanup_logs_and_volumes(dc)

    batteries = args.batteries
    if batteries == ['all']:
        batteries = (
            p.relative_to(os.getcwd())
            for p
            in pathlib.Path(os.getcwd()).iterdir()
            if p.joinpath('datagen.toml').is_file()
        )
    else:
        for battery in batteries:
            p = pathlib.Path(os.getcwd()).joinpath(battery, 'datagen.toml')
            if not p.is_file():
                print('invalid battery "%s" - does not contain datagen.toml' % battery)
                exit(1)

    processes = {}
    tests_pass = True

    try:
        if not args.skip_build:
            build_with_aws = args.object_store == 's3'
            cargo_build_iox(args.debug, build_with_aws)

        docker_create_network(dc)
        if args.kafka_zookeeper:
            docker_run_zookeeper(dc, args.no_volumes)
            docker_run_kafka(dc, args.no_volumes)
        else:
            docker_run_redpanda(dc, args.no_volumes)
        if args.object_store == 's3':
            docker_run_minio(dc, args.no_volumes)
        if do_trace:
            docker_run_jaeger(dc)
        processes['iox_router'] = exec_iox(1, 'iox_router',
                                           debug=args.debug, object_store=args.object_store, do_trace=do_trace)
        processes['iox_writer'] = exec_iox(2, 'iox_writer',
                                           debug=args.debug, object_store=args.object_store, do_trace=do_trace)
        grpc_create_database(1, 2)

        print('-' * 40)
        for battery in batteries:
            if not run_test_battery(battery, 1, 2, debug=args.debug, do_trace=do_trace):
                tests_pass = False
        print('-' * 40)

    except Exception as e:
        print(e)
        tests_pass = False

    if args.hold:
        print('subprocesses are still running, ctrl-C to terminate and exit')
        try:
            signal.pause()
        except KeyboardInterrupt:
            pass
        print('-' * 40)

    for service_name, process in processes.items():
        if process is None:
            continue
        print('%s <- SIGTERM' % service_name)
        process.send_signal(signal.SIGTERM)
        exit_code = process.wait(1.0)
        if exit_code is None:
            print('%s <- SIGKILL' % service_name)
            process.send_signal(signal.SIGKILL)
        if exit_code != 0:
            print('%s exited with %d' % (service_name, exit_code))
    docker_cleanup_resources(dc)

    if not tests_pass:
        exit(1)


def docker_cleanup_resources(dc):
    containers = dc.containers.list(all=True, filters=ioxperf_filters)
    if len(containers) > 0:
        print('removing containers: %s' % ', '.join((c.name for c in containers)))
        for container in containers:
            container.remove(v=True, force=True)

    networks = dc.networks.list(filters=ioxperf_filters)
    if len(networks) > 0:
        print('removing networks: %s' % ', '.join((n.name for n in networks)))
        for network in networks:
            network.remove()


def cleanup_logs_and_volumes(dc):
    docker_cleanup_resources(dc)

    volume_paths = glob.glob(os.path.join(os.getcwd(), 'volumes', '*'))
    if len(volume_paths) > 0:
        print('removing volume contents: %s' % ', '.join((os.path.relpath(p) for p in volume_paths)))
        for path in volume_paths:
            shutil.rmtree(path)

    log_paths = glob.glob(os.path.join(os.getcwd(), 'logs', '*'))
    if len(log_paths) > 0:
        print('removing logs: %s' % ', '.join((os.path.relpath(p) for p in log_paths)))
        for path in log_paths:
            os.unlink(path)


def docker_create_network(dc):
    dc.networks.create(name=ioxperf_name, driver='bridge', check_duplicate=True, scope='local',
                       labels=ioxperf_labels)


def docker_pull_image_if_needed(dc, image):
    try:
        dc.images.get(image)
    except docker.errors.ImageNotFound:
        print("pulling image '%s'..." % image)
        dc.images.pull(image)


def docker_wait_container_running(container):
    while True:
        container.reload()
        if container.status == 'running':
            print("container '%s' has started" % container.name)
            return
        elif container.status == 'created':
            print("waiting for container '%s' to start" % container.name)
            time.sleep(0.1)
        raise Exception("container '%s' status '%s' unexpected" % (container.name, container.status))


def pipe_container_logs_to_file(container, log_filename):
    with pathlib.Path(os.path.join(os.getcwd(), 'logs')) as dir_path:
        if not dir_path.exists():
            os.mkdir(dir_path, mode=0o777)

    logs = container.logs(stdout=True, stderr=True, stream=True, follow=True)
    f = open(file=os.path.join(os.getcwd(), 'logs', log_filename), mode='wb', buffering=0)

    def thread_function():
        for entry in logs:
            f.write(entry)
        f.flush()
        f.close()

    threading.Thread(target=thread_function, daemon=True).start()


def check_port_open(addr, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port_open = sock.connect_ex((addr, port)) == 0
    sock.close()
    return port_open


def docker_run_redpanda(dc, no_volumes):
    image = 'vectorized/redpanda:v21.7.6'
    command = ['redpanda', 'start',
               '--overprovisioned', '--smp 1', '--memory 128M', '--reserve-memory', '0M', '--node-id', '0',
               '--check=false', '--kafka-addr', 'CLIENT://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093',
               '--advertise-kafka-addr', 'CLIENT://kafka:9092,EXTERNAL://localhost:9093']
    name = '%s-%s' % (ioxperf_name, 'redpanda')
    ports = {'9093/tcp': 9093}
    if no_volumes:
        volumes = None
    else:
        volumes = {os.path.join(os.getcwd(), 'volumes/redpanda'): {
            'bind': '/var/lib/redpanda/data',
            'mode': 'rw',
        }}
    docker_pull_image_if_needed(dc, image)
    container = dc.containers.run(image=image, command=command, detach=True, name=name, hostname='kafka',
                                  labels=ioxperf_labels, network=ioxperf_name, ports=ports, volumes=volumes)
    docker_wait_container_running(container)

    while True:
        if check_port_open('127.0.0.1', 9093):
            break
        print('waiting for Redpanda to become ready')
        time.sleep(0.1)

    pipe_container_logs_to_file(container, 'redpanda.log')
    print('Redpanda service is ready')

    return container


def docker_run_zookeeper(dc, no_volumes):
    image = 'docker.io/bitnami/zookeeper:3'
    name = '%s-%s' % (ioxperf_name, 'zookeeper')
    ports = {'2181/tcp': 2181}
    env = {
        'ALLOW_ANONYMOUS_LOGIN': 'yes',
    }
    if no_volumes:
        volumes = None
    else:
        volumes = {os.path.join(os.getcwd(), 'volumes/zookeeper'): {
            'bind': '/bitnami/zookeeper',
            'mode': 'rw',
        }}
    docker_pull_image_if_needed(dc, image)
    container = dc.containers.run(image=image, detach=True, environment=env, name=name, hostname='zookeeper',
                                  labels=ioxperf_labels, network=ioxperf_name, ports=ports, volumes=volumes)
    docker_wait_container_running(container)

    while True:
        if check_port_open('127.0.0.1', 2181):
            break
        print('waiting for ZooKeeper to become ready')
        time.sleep(0.1)

    pipe_container_logs_to_file(container, 'zookeeper.log')
    print('ZooKeeper service is ready')

    return container


def docker_run_kafka(dc, no_volumes):
    image = 'docker.io/bitnami/kafka:2'
    name = '%s-%s' % (ioxperf_name, 'kafka')
    ports = {'9093/tcp': 9093}
    env = {
        'KAFKA_CFG_ZOOKEEPER_CONNECT': 'zookeeper:2181',
        'ALLOW_PLAINTEXT_LISTENER': 'yes',
        'KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP': 'CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT',
        'KAFKA_CFG_LISTENERS': 'CLIENT://:9092,EXTERNAL://:9093',
        'KAFKA_CFG_ADVERTISED_LISTENERS': 'CLIENT://kafka:9092,EXTERNAL://localhost:9093',
        'KAFKA_INTER_BROKER_LISTENER_NAME': 'CLIENT',
        'KAFKA_CFG_LOG_RETENTION_CHECK_INTERVAL_MS': '100',
    }
    if no_volumes:
        volumes = None
    else:
        volumes = {os.path.join(os.getcwd(), 'volumes/kafka'): {
            'bind': '/bitname/kafka',
            'mode': 'rw',
        }}
    docker_pull_image_if_needed(dc, image)
    container = dc.containers.run(image=image, detach=True, environment=env, name=name, hostname='kafka',
                                  labels=ioxperf_labels, network=ioxperf_name, ports=ports, volumes=volumes)
    docker_wait_container_running(container)

    while True:
        if check_port_open('127.0.0.1', 9093):
            break
        print('waiting for Kafka to become ready')
        time.sleep(0.1)

    pipe_container_logs_to_file(container, 'kafka.log')
    print('Kafka service is ready')

    return container


def docker_run_minio(dc, no_volumes):
    image = 'minio/minio:RELEASE.2021-08-05T22-01-19Z'
    command = 'server --address 0.0.0.0:9000 --console-address 0.0.0.0:9001 /data'
    name = '%s-%s' % (ioxperf_name, 'minio')
    ports = {'9000/tcp': 9000, '9001/tcp': 9001}
    if no_volumes:
        volumes = None
    else:
        volumes = {os.path.join(os.getcwd(), 'volumes/minio'): {
            'bind': '/data',
            'mode': 'rw',
        }}
    env = {
        'MINIO_ROOT_USER': 'minio',
        'MINIO_ROOT_PASSWORD': 'miniominio',
        'MINIO_PROMETHEUS_AUTH_TYPE': 'public',
        'MINIO_HTTP_TRACE': '/dev/stdout',
    }
    docker_pull_image_if_needed(dc, image)
    container = dc.containers.run(image=image, command=command, detach=True, environment=env, name=name,
                                  hostname='minio', labels=ioxperf_labels, network=ioxperf_name, ports=ports,
                                  volumes=volumes)
    docker_wait_container_running(container)

    while True:
        timeout = urllib3.util.Timeout(connect=0.1, read=0.1)
        http_client = urllib3.PoolManager(num_pools=1, timeout=timeout, retries=False)
        try:
            mc = minio.Minio(endpoint='127.0.0.1:9000', access_key='minio', secret_key='miniominio', secure=False,
                             http_client=http_client)
            if not mc.bucket_exists('iox1'):
                mc.make_bucket('iox1')
            if not mc.bucket_exists('iox2'):
                mc.make_bucket('iox2')
            break
        except (urllib3.exceptions.ProtocolError, urllib3.exceptions.TimeoutError, minio.error.S3Error):
            pass
        print('waiting for Minio to become ready')
        time.sleep(0.5)

    pipe_container_logs_to_file(container, 'minio.log')
    print('Minio service ready')

    return container


def docker_run_jaeger(dc):
    image = 'jaegertracing/all-in-one:1.26'
    name = '%s-%s' % (ioxperf_name, 'jaeger')
    ports = {'16686/tcp': 16686, '6831/udp': 6831}
    docker_pull_image_if_needed(dc, image)
    container = dc.containers.run(image=image, detach=True, name=name, hostname='jaeger', labels=ioxperf_labels,
                                  network=ioxperf_name, ports=ports)
    docker_wait_container_running(container)

    while True:
        try:
            if requests.get(url='http://127.0.0.1:16686/search', timeout=0.1).status_code / 100 == 2:
                break
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pass
        print('waiting for Jaeger to become ready')
        time.sleep(0.1)

    pipe_container_logs_to_file(container, 'jaeger.log')
    print('Jaeger service ready')

    return container


def cargo_build_iox(debug=False, build_with_aws=True):
    t = time.time()
    print('building IOx')

    features = []
    if build_with_aws:
        features.append('aws')
    features = ','.join(features)

    env = os.environ.copy()
    args = ['cargo', 'build']
    if debug:
        env['RUSTFLAGS'] = '-C debuginfo=1'
        env['RUST_BACKTRACE'] = '1'
    else:
        args += ['--release']
    args += ['--package', 'influxdb_iox', '--features', features, '--bin', 'influxdb_iox']
    args += ['--package', 'iox_data_generator', '--bin', 'iox_data_generator']
    process = subprocess.run(args=args, env=env)
    if process.returncode != 0:
        raise ChildProcessError('cargo build returned %d' % process.returncode)

    print('building IOx finished in %.2fs' % (time.time() - t))


def exec_iox(id, service_name, debug=False, object_store='memory', print_only=False, do_trace=False):
    http_addr = 'localhost:%d' % (id * 10000 + 8080)
    grpc_addr = 'localhost:%d' % (id * 10000 + 8082)

    if debug:
        iox_path = os.path.abspath(os.path.join(os.getcwd(), '../target/debug/influxdb_iox'))
    else:
        iox_path = os.path.abspath(os.path.join(os.getcwd(), '../target/release/influxdb_iox'))
    args = [iox_path, 'run']
    env = {
        'INFLUXDB_IOX_ID': str(id),
        'INFLUXDB_IOX_BIND_ADDR': http_addr,
        'INFLUXDB_IOX_GRPC_BIND_ADDR': grpc_addr,
        'INFLUXDB_IOX_BUCKET': 'iox%d' % id,
        'LOG_DESTINATION': 'stdout',
        'LOG_FORMAT': 'full',
        'RUST_BACKTRACE': '1',
        'LOG_FILTER': 'debug,lifecycle=info,rusoto_core=warn,hyper=warn,h2=warn',
    }
    if do_trace:
        env['TRACES_EXPORTER'] = 'jaeger'
        env['TRACES_EXPORTER_JAEGER_AGENT_HOST'] = 'localhost'
        env['TRACES_EXPORTER_JAEGER_AGENT_PORT'] = '6831'
        env['TRACES_EXPORTER_JAEGER_SERVICE_NAME'] = service_name
        env['TRACES_SAMPLER'] = 'always_on'

    if object_store == 'memory':
        env['INFLUXDB_IOX_OBJECT_STORE'] = 'memory'
    elif object_store == 's3':
        env['INFLUXDB_IOX_OBJECT_STORE'] = 's3'
        env['AWS_ACCESS_KEY_ID'] = 'minio'
        env['AWS_SECRET_ACCESS_KEY'] = 'miniominio'
        env['AWS_ENDPOINT'] = 'http://localhost:9000'
    elif object_store == 'file':
        env['INFLUXDB_IOX_OBJECT_STORE'] = 'file'
        env['INFLUXDB_IOX_DB_DIR'] = 'volumes/%s' % service_name
    else:
        raise ValueError('invalid object_store value "%s"' % object_store)

    if print_only:
        print()
        for k in sorted(env.keys()):
            print('%s=%s' % (k, env[k]))
        print(' '.join(args))
        print()
        return None

    log_file = open('logs/%s.log' % service_name, mode='w')
    process = subprocess.Popen(args=args, env=env, stdout=log_file, stderr=log_file)

    while True:
        if process.poll() is not None:
            raise ChildProcessError('service %s stopped unexpectedly, check %s' % (service_name, log_file.name))
        router = grpc_requests.Client(grpc_addr, lazy=True)
        while True:
            try:
                router.register_service('influxdata.iox.management.v1.ManagementService')
                break
            except:
                # fall through to retry
                pass
        try:
            server_status_response = router.request('influxdata.iox.management.v1.ManagementService', 'GetServerStatus',
                                                    None)
            if 'server_status' in server_status_response and server_status_response['server_status'][
                'initialized'] is True:
                break
        except:
            # fall through to retry
            pass

        print('waiting for %s to become ready' % service_name)
        time.sleep(0.1)

    print('%s service ready' % service_name)

    return process


def grpc_create_database(router_id, writer_id):
    print('creating database "%s" on both IOx servers' % db_name)

    router_db_rules = {
        'rules': {
            'name': db_name,
            'partition_template': {
                'parts': [
                    {'time': '%Y-%m-%d %H:00:00'},
                ],
            },
            'lifecycle_rules': {
                'immutable': True,
                'worker_backoff_millis': '1000',
                'catalog_transactions_until_checkpoint': '100',
                'late_arrive_window_seconds': 300,
                'persist_row_threshold': '1000000',
                'persist_age_threshold_seconds': 1800,
                'mub_row_threshold': '100000',
                'max_active_compactions_cpu_fraction': 1.0,
            },
            'routing_config': {'sink': {'kafka': {}}},
            'worker_cleanup_avg_sleep': '500s',
            'write_buffer_connection': {
                'direction': 'DIRECTION_WRITE',
                'type': 'kafka',
                'connection': '127.0.0.1:9093',
                'connection_config': {},
                'creation_config': {
                    'n_sequencers': 1,
                    'options': {},
                },
            },
        },
    }

    writer_db_rules = {
        'rules': {
            'name': db_name,
            'partition_template': {
                'parts': [
                    {'time': '%Y-%m-%d %H:00:00'}
                ],
            },
            'lifecycle_rules': {
                'buffer_size_soft': 1024 * 1024 * 1024,
                'buffer_size_hard': 1024 * 1024 * 1024 * 2,
                'worker_backoff_millis': 100,
                'max_active_compactions': 1,
                'persist': True,
                'persist_row_threshold': 10000000,
                'catalog_transactions_until_checkpoint': 100,
                'late_arrive_window_seconds': 300,
                'persist_age_threshold_seconds': 1800,
                'mub_row_threshold': 100000,
            },
            'routing_config': {'sink': {'kafka': {}}},
            'worker_cleanup_avg_sleep': '500s',
            'write_buffer_connection': {
                'direction': 'DIRECTION_READ',
                'type': 'kafka',
                'connection': '127.0.0.1:9093',
                'connection_config': {},
                'creation_config': {
                    'n_sequencers': 1,
                    'options': {},
                },
            },
        },
    }

    if router_id is not None:
        router_grpc_addr = 'localhost:%d' % (router_id * 10000 + 8082)
        router = grpc_requests.Client(router_grpc_addr, lazy=True)
        router.register_service('influxdata.iox.management.v1.ManagementService')
        router.request('influxdata.iox.management.v1.ManagementService', 'CreateDatabase', router_db_rules)

        router_http_addr = 'localhost:%d' % (router_id * 10000 + 8080)
        router_write_url = 'http://%s/api/v2/write?org=%s&bucket=%s' % (router_http_addr, org_name, bucket_name)
        lp = "sentinel,source=perf.py f=1i"
        response = requests.post(url=router_write_url, data=lp, timeout=10)
        if not response.ok:
            print('failed to write to router')
            print(response.reason)
            print(response.content)
            return

    else:
        print()
        print(router_db_rules)
        print()

    if writer_id is not None:
        writer_grpc_addr = 'localhost:%d' % (writer_id * 10000 + 8082)
        writer = grpc_requests.Client(writer_grpc_addr, lazy=True)
        writer.register_service('influxdata.iox.management.v1.ManagementService')
        writer.request('influxdata.iox.management.v1.ManagementService', 'CreateDatabase', writer_db_rules)

        writer_http_addr = 'localhost:%d' % (writer_id * 10000 + 8080)
        writer_query_url = 'http://%s/api/v3/query' % writer_http_addr
        writer_query_params = {'q': 'select count(1) from sentinel', 'd': db_name}

        response = requests.get(url=writer_query_url, params=writer_query_params, timeout=10)
        for i in range(20):
            if response.ok:
                break
            print('waiting for round trip test to succeed')
            time.sleep(0.5)
            response = requests.get(url=writer_query_url, params=writer_query_params, timeout=10)

        if not response.ok:
            print(response.reason)
            print(response.content)
            return

    else:
        print()
        print(writer_db_rules)
        print()

    print('created database "%s" on both IOx servers' % db_name)


def run_test_battery(battery_name, router_id, writer_id, debug=False, do_trace=False):
    # TODO drop do_trace when IOx can be configured to always trace
    print('starting test battery "%s"' % battery_name)
    failed = False

    # Write

    battery_dir = os.path.join(os.getcwd(), battery_name)
    datagen_filename = os.path.join(battery_dir, 'datagen.toml')
    if debug:
        iox_data_generator_path = os.path.abspath(os.path.join(os.getcwd(), '../target/debug/iox_data_generator'))
    else:
        iox_data_generator_path = os.path.abspath(os.path.join(os.getcwd(), '../target/release/iox_data_generator'))

    router_http_addr = 'localhost:%d' % (router_id * 10000 + 8080)
    args = [iox_data_generator_path,
            '--host', router_http_addr, '--token', 'arbitrary',
            '--org', org_name, '--bucket', bucket_name,
            '--spec', datagen_filename]
    env = {
        'RUST_BACKTRACE': '0',
    }
    log_file = open('logs/test.log', mode='w')
    if subprocess.run(args=args, stdout=log_file, stderr=log_file, env=env).returncode != 0:
        raise ChildProcessError(
            'failed to run iox_data_generator for battery "%s", check %s' % (battery_name, log_file.name))

    # Query

    writer_http_addr = 'localhost:%d' % (writer_id * 10000 + 8080)
    query_url = 'http://%s/api/v3/query' % writer_http_addr
    queries_filename = os.path.join(battery_dir, 'queries.toml')
    queries = toml.load(open(queries_filename))

    for query in queries['queries']:
        if 'sql' not in query:
            print('query missing SQL query')
            print(query)
            print()
            failed = True
            continue
        sql = query['sql']
        name = query['name']
        if name is None:
            name = sql

        print('running test "%s"' % name)
        time_start = time.time()
        params = {'q': sql, 'format': 'csv', 'd': db_name}
        headers = {}
        if do_trace:
            # TODO remove this after IOx can be configured to sample 100% of traces
            headers['jaeger-debug-id'] = 'from-perf'
        response = requests.get(url=query_url, params=params, headers=headers)
        time_delta = '%dms' % math.floor((time.time() - time_start) * 1000)

        if not response.ok:
            print(response.reason)
            print(response.content.decode('UTF-8'))
            print()
            failed = True
            continue

        got = response.content.decode('UTF-8').strip()
        print('time: %s' % time_delta)
        if 'expect' in query:
            expect = query['expect'].strip()
            if expect != got:
                print('expected: %s' % expect)
                print('got: %s' % got)
                failed = True
            else:
                print('OK')

        elif 'expect_filename' in query:
            path = pathlib.Path(os.path.join(battery_dir, query['expect_filename']))
            if not path.is_file():
                print('file "%s" not found' % path)
                print()
                failed = True
                continue
            expect = open(path).read().strip()
            if expect != got:
                print('expected: %s' % expect)
                print('got: %s' % got)
                failed = True
            else:
                print('OK')
        else:
            print('OK')

        print()

    print('completed test battery "%s"' % battery_name)
    return not failed


if __name__ == "__main__":
    logging.getLogger('grpc_requests.client').setLevel(logging.ERROR)
    main()
