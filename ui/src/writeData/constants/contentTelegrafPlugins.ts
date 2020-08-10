// Constants
import {TELEGRAF_PLUGINS} from 'src/shared/constants/routes'

// Types
import {WriteDataItem, WriteDataSection} from 'src/writeData/constants'

// Markdown
import stackdriverMarkdown from 'src/writeData/components/telegrafPlugins/stackdriver.md'
import ntpqMarkdown from 'src/writeData/components/telegrafPlugins/ntpq.md'
import activemqMarkdown from 'src/writeData/components/telegrafPlugins/activemq.md'
import infinibandMarkdown from 'src/writeData/components/telegrafPlugins/infiniband.md'
import clickhouseMarkdown from 'src/writeData/components/telegrafPlugins/clickhouse.md'
import jolokia2Markdown from 'src/writeData/components/telegrafPlugins/jolokia2.md'
import postgresqlMarkdown from 'src/writeData/components/telegrafPlugins/postgresql.md'
import dmcacheMarkdown from 'src/writeData/components/telegrafPlugins/dmcache.md'
import sensorsMarkdown from 'src/writeData/components/telegrafPlugins/sensors.md'
import mqtt_consumerMarkdown from 'src/writeData/components/telegrafPlugins/mqtt_consumer.md'
import cloudwatchMarkdown from 'src/writeData/components/telegrafPlugins/cloudwatch.md'
import tempMarkdown from 'src/writeData/components/telegrafPlugins/temp.md'
import pingMarkdown from 'src/writeData/components/telegrafPlugins/ping.md'
import ipsetMarkdown from 'src/writeData/components/telegrafPlugins/ipset.md'
import cloud_pubsubMarkdown from 'src/writeData/components/telegrafPlugins/cloud_pubsub.md'
import docker_logMarkdown from 'src/writeData/components/telegrafPlugins/docker_log.md'
import diskioMarkdown from 'src/writeData/components/telegrafPlugins/diskio.md'
import graylogMarkdown from 'src/writeData/components/telegrafPlugins/graylog.md'
import dockerMarkdown from 'src/writeData/components/telegrafPlugins/docker.md'
import bcacheMarkdown from 'src/writeData/components/telegrafPlugins/bcache.md'
import syslogMarkdown from 'src/writeData/components/telegrafPlugins/syslog.md'
//import netMarkdown from 'src/writeData/components/telegrafPlugins/net.md'
//import netMarkdown from 'src/writeData/components/telegrafPlugins/net.md'
import logparserMarkdown from 'src/writeData/components/telegrafPlugins/logparser.md'
import fluentdMarkdown from 'src/writeData/components/telegrafPlugins/fluentd.md'
import couchdbMarkdown from 'src/writeData/components/telegrafPlugins/couchdb.md'
import udp_listenerMarkdown from 'src/writeData/components/telegrafPlugins/udp_listener.md'
import snmpMarkdown from 'src/writeData/components/telegrafPlugins/snmp.md'
import kernel_vmstatMarkdown from 'src/writeData/components/telegrafPlugins/kernel_vmstat.md'
import vsphereMarkdown from 'src/writeData/components/telegrafPlugins/vsphere.md'
import teamspeakMarkdown from 'src/writeData/components/telegrafPlugins/teamspeak.md'
import dovecotMarkdown from 'src/writeData/components/telegrafPlugins/dovecot.md'
import tomcatMarkdown from 'src/writeData/components/telegrafPlugins/tomcat.md'
import kibanaMarkdown from 'src/writeData/components/telegrafPlugins/kibana.md'
import nginx_vtsMarkdown from 'src/writeData/components/telegrafPlugins/nginx_vts.md'
import gnmiMarkdown from 'src/writeData/components/telegrafPlugins/gnmi.md'
import socket_listenerMarkdown from 'src/writeData/components/telegrafPlugins/socket_listener.md'
import fileMarkdown from 'src/writeData/components/telegrafPlugins/file.md'
import cpuMarkdown from 'src/writeData/components/telegrafPlugins/cpu.md'
import azure_storage_queueMarkdown from 'src/writeData/components/telegrafPlugins/azure_storage_queue.md'
import logstashMarkdown from 'src/writeData/components/telegrafPlugins/logstash.md'
import redisMarkdown from 'src/writeData/components/telegrafPlugins/redis.md'
import http_responseMarkdown from 'src/writeData/components/telegrafPlugins/http_response.md'
import cephMarkdown from 'src/writeData/components/telegrafPlugins/ceph.md'
import snmp_legacyMarkdown from 'src/writeData/components/telegrafPlugins/snmp_legacy.md'
import redfishMarkdown from 'src/writeData/components/telegrafPlugins/redfish.md'
import amqp_consumerMarkdown from 'src/writeData/components/telegrafPlugins/amqp_consumer.md'
import sflowMarkdown from 'src/writeData/components/telegrafPlugins/sflow.md'
import execdMarkdown from 'src/writeData/components/telegrafPlugins/execd.md'
import shimMarkdown from 'src/writeData/components/telegrafPlugins/shim.md'
import conntrackMarkdown from 'src/writeData/components/telegrafPlugins/conntrack.md'
import kapacitorMarkdown from 'src/writeData/components/telegrafPlugins/kapacitor.md'
import bondMarkdown from 'src/writeData/components/telegrafPlugins/bond.md'
import processesMarkdown from 'src/writeData/components/telegrafPlugins/processes.md'
import inputsMarkdown from 'src/writeData/components/telegrafPlugins/inputs.md'
import snmp_trapMarkdown from 'src/writeData/components/telegrafPlugins/snmp_trap.md'
import fail2banMarkdown from 'src/writeData/components/telegrafPlugins/fail2ban.md'
import multifileMarkdown from 'src/writeData/components/telegrafPlugins/multifile.md'
import smartMarkdown from 'src/writeData/components/telegrafPlugins/smart.md'
import synproxyMarkdown from 'src/writeData/components/telegrafPlugins/synproxy.md'
import swapMarkdown from 'src/writeData/components/telegrafPlugins/swap.md'
import fibaroMarkdown from 'src/writeData/components/telegrafPlugins/fibaro.md'
import suricataMarkdown from 'src/writeData/components/telegrafPlugins/suricata.md'
import burrowMarkdown from 'src/writeData/components/telegrafPlugins/burrow.md'
import powerdnsMarkdown from 'src/writeData/components/telegrafPlugins/powerdns.md'
import lustre2Markdown from 'src/writeData/components/telegrafPlugins/lustre2.md'
import kafka_consumerMarkdown from 'src/writeData/components/telegrafPlugins/kafka_consumer.md'
import nsq_consumerMarkdown from 'src/writeData/components/telegrafPlugins/nsq_consumer.md'
import marklogicMarkdown from 'src/writeData/components/telegrafPlugins/marklogic.md'
import internalMarkdown from 'src/writeData/components/telegrafPlugins/internal.md'
import cloud_pubsub_pushMarkdown from 'src/writeData/components/telegrafPlugins/cloud_pubsub_push.md'
import jti_openconfig_telemetryMarkdown from 'src/writeData/components/telegrafPlugins/jti_openconfig_telemetry.md'
import procstatMarkdown from 'src/writeData/components/telegrafPlugins/procstat.md'
import neptune_apexMarkdown from 'src/writeData/components/telegrafPlugins/neptune_apex.md'
import nginx_upstream_checkMarkdown from 'src/writeData/components/telegrafPlugins/nginx_upstream_check.md'
import x509_certMarkdown from 'src/writeData/components/telegrafPlugins/x509_cert.md'
import mailchimpMarkdown from 'src/writeData/components/telegrafPlugins/mailchimp.md'
import auroraMarkdown from 'src/writeData/components/telegrafPlugins/aurora.md'
import rabbitmqMarkdown from 'src/writeData/components/telegrafPlugins/rabbitmq.md'
import influxdb_listenerMarkdown from 'src/writeData/components/telegrafPlugins/influxdb_listener.md'
import ecsMarkdown from 'src/writeData/components/telegrafPlugins/ecs.md'
import statsdMarkdown from 'src/writeData/components/telegrafPlugins/statsd.md'
import cgroupMarkdown from 'src/writeData/components/telegrafPlugins/cgroup.md'
import disqueMarkdown from 'src/writeData/components/telegrafPlugins/disque.md'
import rethinkdbMarkdown from 'src/writeData/components/telegrafPlugins/rethinkdb.md'
import dcosMarkdown from 'src/writeData/components/telegrafPlugins/dcos.md'
import jolokiaMarkdown from 'src/writeData/components/telegrafPlugins/jolokia.md'
import varnishMarkdown from 'src/writeData/components/telegrafPlugins/varnish.md'
import tailMarkdown from 'src/writeData/components/telegrafPlugins/tail.md'
import mongodbMarkdown from 'src/writeData/components/telegrafPlugins/mongodb.md'
import zookeeperMarkdown from 'src/writeData/components/telegrafPlugins/zookeeper.md'
import solrMarkdown from 'src/writeData/components/telegrafPlugins/solr.md'
import systemd_unitsMarkdown from 'src/writeData/components/telegrafPlugins/systemd_units.md'
import influxdbMarkdown from 'src/writeData/components/telegrafPlugins/influxdb.md'
import memcachedMarkdown from 'src/writeData/components/telegrafPlugins/memcached.md'
import filestatMarkdown from 'src/writeData/components/telegrafPlugins/filestat.md'
import net_responseMarkdown from 'src/writeData/components/telegrafPlugins/net_response.md'
import cisco_telemetry_mdtMarkdown from 'src/writeData/components/telegrafPlugins/cisco_telemetry_mdt.md'
import systemMarkdown from 'src/writeData/components/telegrafPlugins/system.md'
import zfsMarkdown from 'src/writeData/components/telegrafPlugins/zfs.md'
import opensmtpdMarkdown from 'src/writeData/components/telegrafPlugins/opensmtpd.md'
import natsMarkdown from 'src/writeData/components/telegrafPlugins/nats.md'
import mcrouterMarkdown from 'src/writeData/components/telegrafPlugins/mcrouter.md'
import lanzMarkdown from 'src/writeData/components/telegrafPlugins/lanz.md'
import mesosMarkdown from 'src/writeData/components/telegrafPlugins/mesos.md'
//import githubMarkdown from 'src/writeData/components/telegrafPlugins/github.md'
import win_perf_countersMarkdown from 'src/writeData/components/telegrafPlugins/win_perf_counters.md'
import win_servicesMarkdown from 'src/writeData/components/telegrafPlugins/win_services.md'
import zipkinMarkdown from 'src/writeData/components/telegrafPlugins/zipkin.md'
import consulMarkdown from 'src/writeData/components/telegrafPlugins/consul.md'
import kube_inventoryMarkdown from 'src/writeData/components/telegrafPlugins/kube_inventory.md'
import pfMarkdown from 'src/writeData/components/telegrafPlugins/pf.md'
import nginxMarkdown from 'src/writeData/components/telegrafPlugins/nginx.md'
import openntpdMarkdown from 'src/writeData/components/telegrafPlugins/openntpd.md'
import httpMarkdown from 'src/writeData/components/telegrafPlugins/http.md'
import aerospikeMarkdown from 'src/writeData/components/telegrafPlugins/aerospike.md'
import cassandraMarkdown from 'src/writeData/components/telegrafPlugins/cassandra.md'
import wirelessMarkdown from 'src/writeData/components/telegrafPlugins/wireless.md'
import interruptsMarkdown from 'src/writeData/components/telegrafPlugins/interrupts.md'
import nginx_plusMarkdown from 'src/writeData/components/telegrafPlugins/nginx_plus.md'
import mysqlMarkdown from 'src/writeData/components/telegrafPlugins/mysql.md'
import apcupsdMarkdown from 'src/writeData/components/telegrafPlugins/apcupsd.md'
import sqlserverMarkdown from 'src/writeData/components/telegrafPlugins/sqlserver.md'
import httpjsonMarkdown from 'src/writeData/components/telegrafPlugins/httpjson.md'
import postgresql_extensibleMarkdown from 'src/writeData/components/telegrafPlugins/postgresql_extensible.md'
import chronyMarkdown from 'src/writeData/components/telegrafPlugins/chrony.md'
import execMarkdown from 'src/writeData/components/telegrafPlugins/exec.md'
import linux_sysctl_fsMarkdown from 'src/writeData/components/telegrafPlugins/linux_sysctl_fs.md'
import monitMarkdown from 'src/writeData/components/telegrafPlugins/monit.md'
import leofsMarkdown from 'src/writeData/components/telegrafPlugins/leofs.md'
import eventhub_consumerMarkdown from 'src/writeData/components/telegrafPlugins/eventhub_consumer.md'
import raindropsMarkdown from 'src/writeData/components/telegrafPlugins/raindrops.md'
import nvidia_smiMarkdown from 'src/writeData/components/telegrafPlugins/nvidia_smi.md'
import beanstalkdMarkdown from 'src/writeData/components/telegrafPlugins/beanstalkd.md'
import diskMarkdown from 'src/writeData/components/telegrafPlugins/disk.md'
import salesforceMarkdown from 'src/writeData/components/telegrafPlugins/salesforce.md'
import ipmi_sensorMarkdown from 'src/writeData/components/telegrafPlugins/ipmi_sensor.md'
import tengineMarkdown from 'src/writeData/components/telegrafPlugins/tengine.md'
import prometheusMarkdown from 'src/writeData/components/telegrafPlugins/prometheus.md'
import ethtoolMarkdown from 'src/writeData/components/telegrafPlugins/ethtool.md'
import phpfpmMarkdown from 'src/writeData/components/telegrafPlugins/phpfpm.md'
import nginx_plus_apiMarkdown from 'src/writeData/components/telegrafPlugins/nginx_plus_api.md'
import postfixMarkdown from 'src/writeData/components/telegrafPlugins/postfix.md'
import uwsgiMarkdown from 'src/writeData/components/telegrafPlugins/uwsgi.md'
import jenkinsMarkdown from 'src/writeData/components/telegrafPlugins/jenkins.md'
import kafka_consumer_legacyMarkdown from 'src/writeData/components/telegrafPlugins/kafka_consumer_legacy.md'
import sysstatMarkdown from 'src/writeData/components/telegrafPlugins/sysstat.md'
import iptablesMarkdown from 'src/writeData/components/telegrafPlugins/iptables.md'
import hddtempMarkdown from 'src/writeData/components/telegrafPlugins/hddtemp.md'
import unboundMarkdown from 'src/writeData/components/telegrafPlugins/unbound.md'
import pgbouncerMarkdown from 'src/writeData/components/telegrafPlugins/pgbouncer.md'
import kinesis_consumerMarkdown from 'src/writeData/components/telegrafPlugins/kinesis_consumer.md'
import kubernetesMarkdown from 'src/writeData/components/telegrafPlugins/kubernetes.md'
import icinga2Markdown from 'src/writeData/components/telegrafPlugins/icinga2.md'
import openldapMarkdown from 'src/writeData/components/telegrafPlugins/openldap.md'
import passengerMarkdown from 'src/writeData/components/telegrafPlugins/passenger.md'
import mandrillMarkdown from 'src/writeData/components/telegrafPlugins/mandrill.md'
import filestackMarkdown from 'src/writeData/components/telegrafPlugins/filestack.md'
import rollbarMarkdown from 'src/writeData/components/telegrafPlugins/rollbar.md'
import webhooksMarkdown from 'src/writeData/components/telegrafPlugins/webhooks.md'
import githubMarkdown from 'src/writeData/components/telegrafPlugins/github.md'
import papertrailMarkdown from 'src/writeData/components/telegrafPlugins/papertrail.md'
import particleMarkdown from 'src/writeData/components/telegrafPlugins/particle.md'
import wireguardMarkdown from 'src/writeData/components/telegrafPlugins/wireguard.md'
import riakMarkdown from 'src/writeData/components/telegrafPlugins/riak.md'
import bindMarkdown from 'src/writeData/components/telegrafPlugins/bind.md'
import modbusMarkdown from 'src/writeData/components/telegrafPlugins/modbus.md'
import minecraftMarkdown from 'src/writeData/components/telegrafPlugins/minecraft.md'
import tcp_listenerMarkdown from 'src/writeData/components/telegrafPlugins/tcp_listener.md'
import ipvsMarkdown from 'src/writeData/components/telegrafPlugins/ipvs.md'
import openweathermapMarkdown from 'src/writeData/components/telegrafPlugins/openweathermap.md'
import powerdns_recursorMarkdown from 'src/writeData/components/telegrafPlugins/powerdns_recursor.md'
import puppetagentMarkdown from 'src/writeData/components/telegrafPlugins/puppetagent.md'
import dns_queryMarkdown from 'src/writeData/components/telegrafPlugins/dns_query.md'
import elasticsearchMarkdown from 'src/writeData/components/telegrafPlugins/elasticsearch.md'
import apacheMarkdown from 'src/writeData/components/telegrafPlugins/apache.md'
import nginx_stsMarkdown from 'src/writeData/components/telegrafPlugins/nginx_sts.md'
import nstatMarkdown from 'src/writeData/components/telegrafPlugins/nstat.md'
import http_listener_v2Markdown from 'src/writeData/components/telegrafPlugins/http_listener_v2.md'
import kernelMarkdown from 'src/writeData/components/telegrafPlugins/kernel.md'
import filecountMarkdown from 'src/writeData/components/telegrafPlugins/filecount.md'
import nsqMarkdown from 'src/writeData/components/telegrafPlugins/nsq.md'
import memMarkdown from 'src/writeData/components/telegrafPlugins/mem.md'
import nats_consumerMarkdown from 'src/writeData/components/telegrafPlugins/nats_consumer.md'
import haproxyMarkdown from 'src/writeData/components/telegrafPlugins/haproxy.md'
import couchbaseMarkdown from 'src/writeData/components/telegrafPlugins/couchbase.md'
import fireboardMarkdown from 'src/writeData/components/telegrafPlugins/fireboard.md'

export const WRITE_DATA_TELEGRAF_PLUGINS: WriteDataItem[] = [
  {
    id: 'stackdriver',
    name: 'stackdriver',
    url: `${TELEGRAF_PLUGINS}/stackdriver`,
    markdown: stackdriverMarkdown,
    image: '/src/writeData/graphics/stackdriver.svg',
  },
  {
    id: 'ntpq',
    name: 'ntpq',
    url: `${TELEGRAF_PLUGINS}/ntpq`,
    markdown: ntpqMarkdown,
    image: '/src/writeData/graphics/ntpq.svg',
  },
  {
    id: 'activemq',
    name: 'activemq',
    url: `${TELEGRAF_PLUGINS}/activemq`,
    markdown: activemqMarkdown,
    image: '/src/writeData/graphics/activemq.svg',
  },
  {
    id: 'infiniband',
    name: 'infiniband',
    url: `${TELEGRAF_PLUGINS}/infiniband`,
    markdown: infinibandMarkdown,
    image: '/src/writeData/graphics/infiniband.svg',
  },
  {
    id: 'clickhouse',
    name: 'clickhouse',
    url: `${TELEGRAF_PLUGINS}/clickhouse`,
    markdown: clickhouseMarkdown,
    image: '/src/writeData/graphics/clickhouse.svg',
  },
  {
    id: 'jolokia2',
    name: 'jolokia2',
    url: `${TELEGRAF_PLUGINS}/jolokia2`,
    markdown: jolokia2Markdown,
    image: '/src/writeData/graphics/jolokia2.svg',
  },
  {
    id: 'postgresql',
    name: 'postgresql',
    url: `${TELEGRAF_PLUGINS}/postgresql`,
    markdown: postgresqlMarkdown,
    image: '/src/writeData/graphics/postgresql.svg',
  },
  {
    id: 'dmcache',
    name: 'dmcache',
    url: `${TELEGRAF_PLUGINS}/dmcache`,
    markdown: dmcacheMarkdown,
    image: '/src/writeData/graphics/dmcache.svg',
  },
  {
    id: 'sensors',
    name: 'sensors',
    url: `${TELEGRAF_PLUGINS}/sensors`,
    markdown: sensorsMarkdown,
    image: '/src/writeData/graphics/sensors.svg',
  },
  {
    id: 'mqtt_consumer',
    name: 'mqtt_consumer',
    url: `${TELEGRAF_PLUGINS}/mqtt_consumer`,
    markdown: mqtt_consumerMarkdown,
    image: '/src/writeData/graphics/mqtt_consumer.svg',
  },
  {
    id: 'cloudwatch',
    name: 'cloudwatch',
    url: `${TELEGRAF_PLUGINS}/cloudwatch`,
    markdown: cloudwatchMarkdown,
    image: '/src/writeData/graphics/cloudwatch.svg',
  },
  {
    id: 'temp',
    name: 'temp',
    url: `${TELEGRAF_PLUGINS}/temp`,
    markdown: tempMarkdown,
    image: '/src/writeData/graphics/temp.svg',
  },
  {
    id: 'ping',
    name: 'ping',
    url: `${TELEGRAF_PLUGINS}/ping`,
    markdown: pingMarkdown,
    image: '/src/writeData/graphics/ping.svg',
  },
  {
    id: 'ipset',
    name: 'ipset',
    url: `${TELEGRAF_PLUGINS}/ipset`,
    markdown: ipsetMarkdown,
    image: '/src/writeData/graphics/ipset.svg',
  },
  {
    id: 'cloud_pubsub',
    name: 'cloud_pubsub',
    url: `${TELEGRAF_PLUGINS}/cloud_pubsub`,
    markdown: cloud_pubsubMarkdown,
    image: '/src/writeData/graphics/cloud_pubsub.svg',
  },
  {
    id: 'docker_log',
    name: 'docker_log',
    url: `${TELEGRAF_PLUGINS}/docker_log`,
    markdown: docker_logMarkdown,
    image: '/src/writeData/graphics/docker_log.svg',
  },
  {
    id: 'diskio',
    name: 'diskio',
    url: `${TELEGRAF_PLUGINS}/diskio`,
    markdown: diskioMarkdown,
    image: '/src/writeData/graphics/diskio.svg',
  },
  {
    id: 'graylog',
    name: 'graylog',
    url: `${TELEGRAF_PLUGINS}/graylog`,
    markdown: graylogMarkdown,
    image: '/src/writeData/graphics/graylog.svg',
  },
  {
    id: 'docker',
    name: 'docker',
    url: `${TELEGRAF_PLUGINS}/docker`,
    markdown: dockerMarkdown,
    image: '/src/writeData/graphics/docker.svg',
  },
  {
    id: 'bcache',
    name: 'bcache',
    url: `${TELEGRAF_PLUGINS}/bcache`,
    markdown: bcacheMarkdown,
    image: '/src/writeData/graphics/bcache.svg',
  },
  {
    id: 'syslog',
    name: 'syslog',
    url: `${TELEGRAF_PLUGINS}/syslog`,
    markdown: syslogMarkdown,
    image: '/src/writeData/graphics/syslog.svg',
  },
  // {
  //     id: 'net',
  //     name: 'net',
  //     url: `${TELEGRAF_PLUGINS}/net`,
  //     markdown: netMarkdown,
  //     image: '/src/writeData/graphics/net.svg',
  // },
  // {
  //     id: 'net',
  //     name: 'net',
  //     url: `${TELEGRAF_PLUGINS}/net`,
  //     markdown: netMarkdown,
  //     image: '/src/writeData/graphics/net.svg',
  //},
  {
    id: 'logparser',
    name: 'logparser',
    url: `${TELEGRAF_PLUGINS}/logparser`,
    markdown: logparserMarkdown,
    image: '/src/writeData/graphics/logparser.svg',
  },
  {
    id: 'fluentd',
    name: 'fluentd',
    url: `${TELEGRAF_PLUGINS}/fluentd`,
    markdown: fluentdMarkdown,
    image: '/src/writeData/graphics/fluentd.svg',
  },
  {
    id: 'couchdb',
    name: 'couchdb',
    url: `${TELEGRAF_PLUGINS}/couchdb`,
    markdown: couchdbMarkdown,
    image: '/src/writeData/graphics/couchdb.svg',
  },
  {
    id: 'udp_listener',
    name: 'udp_listener',
    url: `${TELEGRAF_PLUGINS}/udp_listener`,
    markdown: udp_listenerMarkdown,
    image: '/src/writeData/graphics/udp_listener.svg',
  },
  {
    id: 'snmp',
    name: 'snmp',
    url: `${TELEGRAF_PLUGINS}/snmp`,
    markdown: snmpMarkdown,
    image: '/src/writeData/graphics/snmp.svg',
  },
  {
    id: 'kernel_vmstat',
    name: 'kernel_vmstat',
    url: `${TELEGRAF_PLUGINS}/kernel_vmstat`,
    markdown: kernel_vmstatMarkdown,
    image: '/src/writeData/graphics/kernel_vmstat.svg',
  },
  {
    id: 'vsphere',
    name: 'vsphere',
    url: `${TELEGRAF_PLUGINS}/vsphere`,
    markdown: vsphereMarkdown,
    image: '/src/writeData/graphics/vsphere.svg',
  },
  {
    id: 'teamspeak',
    name: 'teamspeak',
    url: `${TELEGRAF_PLUGINS}/teamspeak`,
    markdown: teamspeakMarkdown,
    image: '/src/writeData/graphics/teamspeak.svg',
  },
  {
    id: 'dovecot',
    name: 'dovecot',
    url: `${TELEGRAF_PLUGINS}/dovecot`,
    markdown: dovecotMarkdown,
    image: '/src/writeData/graphics/dovecot.svg',
  },
  {
    id: 'tomcat',
    name: 'tomcat',
    url: `${TELEGRAF_PLUGINS}/tomcat`,
    markdown: tomcatMarkdown,
    image: '/src/writeData/graphics/tomcat.svg',
  },
  {
    id: 'kibana',
    name: 'kibana',
    url: `${TELEGRAF_PLUGINS}/kibana`,
    markdown: kibanaMarkdown,
    image: '/src/writeData/graphics/kibana.svg',
  },
  {
    id: 'nginx_vts',
    name: 'nginx_vts',
    url: `${TELEGRAF_PLUGINS}/nginx_vts`,
    markdown: nginx_vtsMarkdown,
    image: '/src/writeData/graphics/nginx_vts.svg',
  },
  {
    id: 'gnmi',
    name: 'gnmi',
    url: `${TELEGRAF_PLUGINS}/gnmi`,
    markdown: gnmiMarkdown,
    image: '/src/writeData/graphics/gnmi.svg',
  },
  {
    id: 'socket_listener',
    name: 'socket_listener',
    url: `${TELEGRAF_PLUGINS}/socket_listener`,
    markdown: socket_listenerMarkdown,
    image: '/src/writeData/graphics/socket_listener.svg',
  },
  {
    id: 'file',
    name: 'file',
    url: `${TELEGRAF_PLUGINS}/file`,
    markdown: fileMarkdown,
    image: '/src/writeData/graphics/file.svg',
  },
  {
    id: 'cpu',
    name: 'cpu',
    url: `${TELEGRAF_PLUGINS}/cpu`,
    markdown: cpuMarkdown,
    image: '/src/writeData/graphics/cpu.svg',
  },
  {
    id: 'azure_storage_queue',
    name: 'azure_storage_queue',
    url: `${TELEGRAF_PLUGINS}/azure_storage_queue`,
    markdown: azure_storage_queueMarkdown,
    image: '/src/writeData/graphics/azure_storage_queue.svg',
  },
  {
    id: 'logstash',
    name: 'logstash',
    url: `${TELEGRAF_PLUGINS}/logstash`,
    markdown: logstashMarkdown,
    image: '/src/writeData/graphics/logstash.svg',
  },
  {
    id: 'redis',
    name: 'redis',
    url: `${TELEGRAF_PLUGINS}/redis`,
    markdown: redisMarkdown,
    image: '/src/writeData/graphics/redis.svg',
  },
  {
    id: 'http_response',
    name: 'http_response',
    url: `${TELEGRAF_PLUGINS}/http_response`,
    markdown: http_responseMarkdown,
    image: '/src/writeData/graphics/http_response.svg',
  },
  {
    id: 'ceph',
    name: 'ceph',
    url: `${TELEGRAF_PLUGINS}/ceph`,
    markdown: cephMarkdown,
    image: '/src/writeData/graphics/ceph.svg',
  },
  {
    id: 'snmp_legacy',
    name: 'snmp_legacy',
    url: `${TELEGRAF_PLUGINS}/snmp_legacy`,
    markdown: snmp_legacyMarkdown,
    image: '/src/writeData/graphics/snmp_legacy.svg',
  },
  {
    id: 'redfish',
    name: 'redfish',
    url: `${TELEGRAF_PLUGINS}/redfish`,
    markdown: redfishMarkdown,
    image: '/src/writeData/graphics/redfish.svg',
  },
  {
    id: 'amqp_consumer',
    name: 'amqp_consumer',
    url: `${TELEGRAF_PLUGINS}/amqp_consumer`,
    markdown: amqp_consumerMarkdown,
    image: '/src/writeData/graphics/amqp_consumer.svg',
  },
  {
    id: 'sflow',
    name: 'sflow',
    url: `${TELEGRAF_PLUGINS}/sflow`,
    markdown: sflowMarkdown,
    image: '/src/writeData/graphics/sflow.svg',
  },
  {
    id: 'execd',
    name: 'execd',
    url: `${TELEGRAF_PLUGINS}/execd`,
    markdown: execdMarkdown,
    image: '/src/writeData/graphics/execd.svg',
  },
  {
    id: 'shim',
    name: 'shim',
    url: `${TELEGRAF_PLUGINS}/shim`,
    markdown: shimMarkdown,
    image: '/src/writeData/graphics/shim.svg',
  },
  {
    id: 'conntrack',
    name: 'conntrack',
    url: `${TELEGRAF_PLUGINS}/conntrack`,
    markdown: conntrackMarkdown,
    image: '/src/writeData/graphics/conntrack.svg',
  },
  {
    id: 'kapacitor',
    name: 'kapacitor',
    url: `${TELEGRAF_PLUGINS}/kapacitor`,
    markdown: kapacitorMarkdown,
    image: '/src/writeData/graphics/kapacitor.svg',
  },
  {
    id: 'bond',
    name: 'bond',
    url: `${TELEGRAF_PLUGINS}/bond`,
    markdown: bondMarkdown,
    image: '/src/writeData/graphics/bond.svg',
  },
  {
    id: 'processes',
    name: 'processes',
    url: `${TELEGRAF_PLUGINS}/processes`,
    markdown: processesMarkdown,
    image: '/src/writeData/graphics/processes.svg',
  },
  {
    id: 'inputs',
    name: 'inputs',
    url: `${TELEGRAF_PLUGINS}/inputs`,
    markdown: inputsMarkdown,
    image: '/src/writeData/graphics/inputs.svg',
  },
  {
    id: 'snmp_trap',
    name: 'snmp_trap',
    url: `${TELEGRAF_PLUGINS}/snmp_trap`,
    markdown: snmp_trapMarkdown,
    image: '/src/writeData/graphics/snmp_trap.svg',
  },
  {
    id: 'fail2ban',
    name: 'fail2ban',
    url: `${TELEGRAF_PLUGINS}/fail2ban`,
    markdown: fail2banMarkdown,
    image: '/src/writeData/graphics/fail2ban.svg',
  },
  {
    id: 'multifile',
    name: 'multifile',
    url: `${TELEGRAF_PLUGINS}/multifile`,
    markdown: multifileMarkdown,
    image: '/src/writeData/graphics/multifile.svg',
  },
  {
    id: 'smart',
    name: 'smart',
    url: `${TELEGRAF_PLUGINS}/smart`,
    markdown: smartMarkdown,
    image: '/src/writeData/graphics/smart.svg',
  },
  {
    id: 'synproxy',
    name: 'synproxy',
    url: `${TELEGRAF_PLUGINS}/synproxy`,
    markdown: synproxyMarkdown,
    image: '/src/writeData/graphics/synproxy.svg',
  },
  {
    id: 'swap',
    name: 'swap',
    url: `${TELEGRAF_PLUGINS}/swap`,
    markdown: swapMarkdown,
    image: '/src/writeData/graphics/swap.svg',
  },
  {
    id: 'fibaro',
    name: 'fibaro',
    url: `${TELEGRAF_PLUGINS}/fibaro`,
    markdown: fibaroMarkdown,
    image: '/src/writeData/graphics/fibaro.svg',
  },
  {
    id: 'suricata',
    name: 'suricata',
    url: `${TELEGRAF_PLUGINS}/suricata`,
    markdown: suricataMarkdown,
    image: '/src/writeData/graphics/suricata.svg',
  },
  {
    id: 'burrow',
    name: 'burrow',
    url: `${TELEGRAF_PLUGINS}/burrow`,
    markdown: burrowMarkdown,
    image: '/src/writeData/graphics/burrow.svg',
  },
  {
    id: 'powerdns',
    name: 'powerdns',
    url: `${TELEGRAF_PLUGINS}/powerdns`,
    markdown: powerdnsMarkdown,
    image: '/src/writeData/graphics/powerdns.svg',
  },
  {
    id: 'lustre2',
    name: 'lustre2',
    url: `${TELEGRAF_PLUGINS}/lustre2`,
    markdown: lustre2Markdown,
    image: '/src/writeData/graphics/lustre2.svg',
  },
  {
    id: 'kafka_consumer',
    name: 'kafka_consumer',
    url: `${TELEGRAF_PLUGINS}/kafka_consumer`,
    markdown: kafka_consumerMarkdown,
    image: '/src/writeData/graphics/kafka_consumer.svg',
  },
  {
    id: 'nsq_consumer',
    name: 'nsq_consumer',
    url: `${TELEGRAF_PLUGINS}/nsq_consumer`,
    markdown: nsq_consumerMarkdown,
    image: '/src/writeData/graphics/nsq_consumer.svg',
  },
  {
    id: 'marklogic',
    name: 'marklogic',
    url: `${TELEGRAF_PLUGINS}/marklogic`,
    markdown: marklogicMarkdown,
    image: '/src/writeData/graphics/marklogic.svg',
  },
  {
    id: 'internal',
    name: 'internal',
    url: `${TELEGRAF_PLUGINS}/internal`,
    markdown: internalMarkdown,
    image: '/src/writeData/graphics/internal.svg',
  },
  {
    id: 'cloud_pubsub_push',
    name: 'cloud_pubsub_push',
    url: `${TELEGRAF_PLUGINS}/cloud_pubsub_push`,
    markdown: cloud_pubsub_pushMarkdown,
    image: '/src/writeData/graphics/cloud_pubsub_push.svg',
  },
  {
    id: 'jti_openconfig_telemetry',
    name: 'jti_openconfig_telemetry',
    url: `${TELEGRAF_PLUGINS}/jti_openconfig_telemetry`,
    markdown: jti_openconfig_telemetryMarkdown,
    image: '/src/writeData/graphics/jti_openconfig_telemetry.svg',
  },
  {
    id: 'procstat',
    name: 'procstat',
    url: `${TELEGRAF_PLUGINS}/procstat`,
    markdown: procstatMarkdown,
    image: '/src/writeData/graphics/procstat.svg',
  },
  {
    id: 'neptune_apex',
    name: 'neptune_apex',
    url: `${TELEGRAF_PLUGINS}/neptune_apex`,
    markdown: neptune_apexMarkdown,
    image: '/src/writeData/graphics/neptune_apex.svg',
  },
  {
    id: 'nginx_upstream_check',
    name: 'nginx_upstream_check',
    url: `${TELEGRAF_PLUGINS}/nginx_upstream_check`,
    markdown: nginx_upstream_checkMarkdown,
    image: '/src/writeData/graphics/nginx_upstream_check.svg',
  },
  {
    id: 'x509_cert',
    name: 'x509_cert',
    url: `${TELEGRAF_PLUGINS}/x509_cert`,
    markdown: x509_certMarkdown,
    image: '/src/writeData/graphics/x509_cert.svg',
  },
  {
    id: 'mailchimp',
    name: 'mailchimp',
    url: `${TELEGRAF_PLUGINS}/mailchimp`,
    markdown: mailchimpMarkdown,
    image: '/src/writeData/graphics/mailchimp.svg',
  },
  {
    id: 'aurora',
    name: 'aurora',
    url: `${TELEGRAF_PLUGINS}/aurora`,
    markdown: auroraMarkdown,
    image: '/src/writeData/graphics/aurora.svg',
  },
  {
    id: 'rabbitmq',
    name: 'rabbitmq',
    url: `${TELEGRAF_PLUGINS}/rabbitmq`,
    markdown: rabbitmqMarkdown,
    image: '/src/writeData/graphics/rabbitmq.svg',
  },
  {
    id: 'influxdb_listener',
    name: 'influxdb_listener',
    url: `${TELEGRAF_PLUGINS}/influxdb_listener`,
    markdown: influxdb_listenerMarkdown,
    image: '/src/writeData/graphics/influxdb_listener.svg',
  },
  {
    id: 'ecs',
    name: 'ecs',
    url: `${TELEGRAF_PLUGINS}/ecs`,
    markdown: ecsMarkdown,
    image: '/src/writeData/graphics/ecs.svg',
  },
  {
    id: 'statsd',
    name: 'statsd',
    url: `${TELEGRAF_PLUGINS}/statsd`,
    markdown: statsdMarkdown,
    image: '/src/writeData/graphics/statsd.svg',
  },
  {
    id: 'cgroup',
    name: 'cgroup',
    url: `${TELEGRAF_PLUGINS}/cgroup`,
    markdown: cgroupMarkdown,
    image: '/src/writeData/graphics/cgroup.svg',
  },
  {
    id: 'disque',
    name: 'disque',
    url: `${TELEGRAF_PLUGINS}/disque`,
    markdown: disqueMarkdown,
    image: '/src/writeData/graphics/disque.svg',
  },
  {
    id: 'rethinkdb',
    name: 'rethinkdb',
    url: `${TELEGRAF_PLUGINS}/rethinkdb`,
    markdown: rethinkdbMarkdown,
    image: '/src/writeData/graphics/rethinkdb.svg',
  },
  {
    id: 'dcos',
    name: 'dcos',
    url: `${TELEGRAF_PLUGINS}/dcos`,
    markdown: dcosMarkdown,
    image: '/src/writeData/graphics/dcos.svg',
  },
  {
    id: 'jolokia',
    name: 'jolokia',
    url: `${TELEGRAF_PLUGINS}/jolokia`,
    markdown: jolokiaMarkdown,
    image: '/src/writeData/graphics/jolokia.svg',
  },
  {
    id: 'varnish',
    name: 'varnish',
    url: `${TELEGRAF_PLUGINS}/varnish`,
    markdown: varnishMarkdown,
    image: '/src/writeData/graphics/varnish.svg',
  },
  {
    id: 'tail',
    name: 'tail',
    url: `${TELEGRAF_PLUGINS}/tail`,
    markdown: tailMarkdown,
    image: '/src/writeData/graphics/tail.svg',
  },
  {
    id: 'mongodb',
    name: 'mongodb',
    url: `${TELEGRAF_PLUGINS}/mongodb`,
    markdown: mongodbMarkdown,
    image: '/src/writeData/graphics/mongodb.svg',
  },
  {
    id: 'zookeeper',
    name: 'zookeeper',
    url: `${TELEGRAF_PLUGINS}/zookeeper`,
    markdown: zookeeperMarkdown,
    image: '/src/writeData/graphics/zookeeper.svg',
  },
  {
    id: 'solr',
    name: 'solr',
    url: `${TELEGRAF_PLUGINS}/solr`,
    markdown: solrMarkdown,
    image: '/src/writeData/graphics/solr.svg',
  },
  {
    id: 'systemd_units',
    name: 'systemd_units',
    url: `${TELEGRAF_PLUGINS}/systemd_units`,
    markdown: systemd_unitsMarkdown,
    image: '/src/writeData/graphics/systemd_units.svg',
  },
  {
    id: 'influxdb',
    name: 'influxdb',
    url: `${TELEGRAF_PLUGINS}/influxdb`,
    markdown: influxdbMarkdown,
    image: '/src/writeData/graphics/influxdb.svg',
  },
  {
    id: 'memcached',
    name: 'memcached',
    url: `${TELEGRAF_PLUGINS}/memcached`,
    markdown: memcachedMarkdown,
    image: '/src/writeData/graphics/memcached.svg',
  },
  {
    id: 'filestat',
    name: 'filestat',
    url: `${TELEGRAF_PLUGINS}/filestat`,
    markdown: filestatMarkdown,
    image: '/src/writeData/graphics/filestat.svg',
  },
  {
    id: 'net_response',
    name: 'net_response',
    url: `${TELEGRAF_PLUGINS}/net_response`,
    markdown: net_responseMarkdown,
    image: '/src/writeData/graphics/net_response.svg',
  },
  {
    id: 'cisco_telemetry_mdt',
    name: 'cisco_telemetry_mdt',
    url: `${TELEGRAF_PLUGINS}/cisco_telemetry_mdt`,
    markdown: cisco_telemetry_mdtMarkdown,
    image: '/src/writeData/graphics/cisco_telemetry_mdt.svg',
  },
  {
    id: 'system',
    name: 'system',
    url: `${TELEGRAF_PLUGINS}/system`,
    markdown: systemMarkdown,
    image: '/src/writeData/graphics/system.svg',
  },
  {
    id: 'zfs',
    name: 'zfs',
    url: `${TELEGRAF_PLUGINS}/zfs`,
    markdown: zfsMarkdown,
    image: '/src/writeData/graphics/zfs.svg',
  },
  {
    id: 'opensmtpd',
    name: 'opensmtpd',
    url: `${TELEGRAF_PLUGINS}/opensmtpd`,
    markdown: opensmtpdMarkdown,
    image: '/src/writeData/graphics/opensmtpd.svg',
  },
  {
    id: 'nats',
    name: 'nats',
    url: `${TELEGRAF_PLUGINS}/nats`,
    markdown: natsMarkdown,
    image: '/src/writeData/graphics/nats.svg',
  },
  {
    id: 'mcrouter',
    name: 'mcrouter',
    url: `${TELEGRAF_PLUGINS}/mcrouter`,
    markdown: mcrouterMarkdown,
    image: '/src/writeData/graphics/mcrouter.svg',
  },
  {
    id: 'lanz',
    name: 'lanz',
    url: `${TELEGRAF_PLUGINS}/lanz`,
    markdown: lanzMarkdown,
    image: '/src/writeData/graphics/lanz.svg',
  },
  {
    id: 'mesos',
    name: 'mesos',
    url: `${TELEGRAF_PLUGINS}/mesos`,
    markdown: mesosMarkdown,
    image: '/src/writeData/graphics/mesos.svg',
  },
  {
    id: 'github',
    name: 'github',
    url: `${TELEGRAF_PLUGINS}/github`,
    markdown: githubMarkdown,
    image: '/src/writeData/graphics/github.svg',
  },
  {
    id: 'win_perf_counters',
    name: 'win_perf_counters',
    url: `${TELEGRAF_PLUGINS}/win_perf_counters`,
    markdown: win_perf_countersMarkdown,
    image: '/src/writeData/graphics/win_perf_counters.svg',
  },
  {
    id: 'win_services',
    name: 'win_services',
    url: `${TELEGRAF_PLUGINS}/win_services`,
    markdown: win_servicesMarkdown,
    image: '/src/writeData/graphics/win_services.svg',
  },
  {
    id: 'zipkin',
    name: 'zipkin',
    url: `${TELEGRAF_PLUGINS}/zipkin`,
    markdown: zipkinMarkdown,
    image: '/src/writeData/graphics/zipkin.svg',
  },
  {
    id: 'consul',
    name: 'consul',
    url: `${TELEGRAF_PLUGINS}/consul`,
    markdown: consulMarkdown,
    image: '/src/writeData/graphics/consul.svg',
  },
  {
    id: 'kube_inventory',
    name: 'kube_inventory',
    url: `${TELEGRAF_PLUGINS}/kube_inventory`,
    markdown: kube_inventoryMarkdown,
    image: '/src/writeData/graphics/kube_inventory.svg',
  },
  {
    id: 'pf',
    name: 'pf',
    url: `${TELEGRAF_PLUGINS}/pf`,
    markdown: pfMarkdown,
    image: '/src/writeData/graphics/pf.svg',
  },
  {
    id: 'nginx',
    name: 'nginx',
    url: `${TELEGRAF_PLUGINS}/nginx`,
    markdown: nginxMarkdown,
    image: '/src/writeData/graphics/nginx.svg',
  },
  {
    id: 'openntpd',
    name: 'openntpd',
    url: `${TELEGRAF_PLUGINS}/openntpd`,
    markdown: openntpdMarkdown,
    image: '/src/writeData/graphics/openntpd.svg',
  },
  {
    id: 'http',
    name: 'http',
    url: `${TELEGRAF_PLUGINS}/http`,
    markdown: httpMarkdown,
    image: '/src/writeData/graphics/http.svg',
  },
  {
    id: 'aerospike',
    name: 'aerospike',
    url: `${TELEGRAF_PLUGINS}/aerospike`,
    markdown: aerospikeMarkdown,
    image: '/src/writeData/graphics/aerospike.svg',
  },
  {
    id: 'cassandra',
    name: 'cassandra',
    url: `${TELEGRAF_PLUGINS}/cassandra`,
    markdown: cassandraMarkdown,
    image: '/src/writeData/graphics/cassandra.svg',
  },
  {
    id: 'wireless',
    name: 'wireless',
    url: `${TELEGRAF_PLUGINS}/wireless`,
    markdown: wirelessMarkdown,
    image: '/src/writeData/graphics/wireless.svg',
  },
  {
    id: 'interrupts',
    name: 'interrupts',
    url: `${TELEGRAF_PLUGINS}/interrupts`,
    markdown: interruptsMarkdown,
    image: '/src/writeData/graphics/interrupts.svg',
  },
  {
    id: 'nginx_plus',
    name: 'nginx_plus',
    url: `${TELEGRAF_PLUGINS}/nginx_plus`,
    markdown: nginx_plusMarkdown,
    image: '/src/writeData/graphics/nginx_plus.svg',
  },
  {
    id: 'mysql',
    name: 'mysql',
    url: `${TELEGRAF_PLUGINS}/mysql`,
    markdown: mysqlMarkdown,
    image: '/src/writeData/graphics/mysql.svg',
  },
  {
    id: 'apcupsd',
    name: 'apcupsd',
    url: `${TELEGRAF_PLUGINS}/apcupsd`,
    markdown: apcupsdMarkdown,
    image: '/src/writeData/graphics/apcupsd.svg',
  },
  {
    id: 'sqlserver',
    name: 'sqlserver',
    url: `${TELEGRAF_PLUGINS}/sqlserver`,
    markdown: sqlserverMarkdown,
    image: '/src/writeData/graphics/sqlserver.svg',
  },
  {
    id: 'httpjson',
    name: 'httpjson',
    url: `${TELEGRAF_PLUGINS}/httpjson`,
    markdown: httpjsonMarkdown,
    image: '/src/writeData/graphics/httpjson.svg',
  },
  {
    id: 'postgresql_extensible',
    name: 'postgresql_extensible',
    url: `${TELEGRAF_PLUGINS}/postgresql_extensible`,
    markdown: postgresql_extensibleMarkdown,
    image: '/src/writeData/graphics/postgresql_extensible.svg',
  },
  {
    id: 'chrony',
    name: 'chrony',
    url: `${TELEGRAF_PLUGINS}/chrony`,
    markdown: chronyMarkdown,
    image: '/src/writeData/graphics/chrony.svg',
  },
  {
    id: 'exec',
    name: 'exec',
    url: `${TELEGRAF_PLUGINS}/exec`,
    markdown: execMarkdown,
    image: '/src/writeData/graphics/exec.svg',
  },
  {
    id: 'linux_sysctl_fs',
    name: 'linux_sysctl_fs',
    url: `${TELEGRAF_PLUGINS}/linux_sysctl_fs`,
    markdown: linux_sysctl_fsMarkdown,
    image: '/src/writeData/graphics/linux_sysctl_fs.svg',
  },
  {
    id: 'monit',
    name: 'monit',
    url: `${TELEGRAF_PLUGINS}/monit`,
    markdown: monitMarkdown,
    image: '/src/writeData/graphics/monit.svg',
  },
  {
    id: 'leofs',
    name: 'leofs',
    url: `${TELEGRAF_PLUGINS}/leofs`,
    markdown: leofsMarkdown,
    image: '/src/writeData/graphics/leofs.svg',
  },
  {
    id: 'eventhub_consumer',
    name: 'eventhub_consumer',
    url: `${TELEGRAF_PLUGINS}/eventhub_consumer`,
    markdown: eventhub_consumerMarkdown,
    image: '/src/writeData/graphics/eventhub_consumer.svg',
  },
  {
    id: 'raindrops',
    name: 'raindrops',
    url: `${TELEGRAF_PLUGINS}/raindrops`,
    markdown: raindropsMarkdown,
    image: '/src/writeData/graphics/raindrops.svg',
  },
  {
    id: 'nvidia_smi',
    name: 'nvidia_smi',
    url: `${TELEGRAF_PLUGINS}/nvidia_smi`,
    markdown: nvidia_smiMarkdown,
    image: '/src/writeData/graphics/nvidia_smi.svg',
  },
  {
    id: 'beanstalkd',
    name: 'beanstalkd',
    url: `${TELEGRAF_PLUGINS}/beanstalkd`,
    markdown: beanstalkdMarkdown,
    image: '/src/writeData/graphics/beanstalkd.svg',
  },
  {
    id: 'disk',
    name: 'disk',
    url: `${TELEGRAF_PLUGINS}/disk`,
    markdown: diskMarkdown,
    image: '/src/writeData/graphics/disk.svg',
  },
  {
    id: 'salesforce',
    name: 'salesforce',
    url: `${TELEGRAF_PLUGINS}/salesforce`,
    markdown: salesforceMarkdown,
    image: '/src/writeData/graphics/salesforce.svg',
  },
  {
    id: 'ipmi_sensor',
    name: 'ipmi_sensor',
    url: `${TELEGRAF_PLUGINS}/ipmi_sensor`,
    markdown: ipmi_sensorMarkdown,
    image: '/src/writeData/graphics/ipmi_sensor.svg',
  },
  {
    id: 'tengine',
    name: 'tengine',
    url: `${TELEGRAF_PLUGINS}/tengine`,
    markdown: tengineMarkdown,
    image: '/src/writeData/graphics/tengine.svg',
  },
  {
    id: 'prometheus',
    name: 'prometheus',
    url: `${TELEGRAF_PLUGINS}/prometheus`,
    markdown: prometheusMarkdown,
    image: '/src/writeData/graphics/prometheus.svg',
  },
  {
    id: 'ethtool',
    name: 'ethtool',
    url: `${TELEGRAF_PLUGINS}/ethtool`,
    markdown: ethtoolMarkdown,
    image: '/src/writeData/graphics/ethtool.svg',
  },
  {
    id: 'phpfpm',
    name: 'phpfpm',
    url: `${TELEGRAF_PLUGINS}/phpfpm`,
    markdown: phpfpmMarkdown,
    image: '/src/writeData/graphics/phpfpm.svg',
  },
  {
    id: 'nginx_plus_api',
    name: 'nginx_plus_api',
    url: `${TELEGRAF_PLUGINS}/nginx_plus_api`,
    markdown: nginx_plus_apiMarkdown,
    image: '/src/writeData/graphics/nginx_plus_api.svg',
  },
  {
    id: 'postfix',
    name: 'postfix',
    url: `${TELEGRAF_PLUGINS}/postfix`,
    markdown: postfixMarkdown,
    image: '/src/writeData/graphics/postfix.svg',
  },
  {
    id: 'uwsgi',
    name: 'uwsgi',
    url: `${TELEGRAF_PLUGINS}/uwsgi`,
    markdown: uwsgiMarkdown,
    image: '/src/writeData/graphics/uwsgi.svg',
  },
  {
    id: 'jenkins',
    name: 'jenkins',
    url: `${TELEGRAF_PLUGINS}/jenkins`,
    markdown: jenkinsMarkdown,
    image: '/src/writeData/graphics/jenkins.svg',
  },
  {
    id: 'kafka_consumer_legacy',
    name: 'kafka_consumer_legacy',
    url: `${TELEGRAF_PLUGINS}/kafka_consumer_legacy`,
    markdown: kafka_consumer_legacyMarkdown,
    image: '/src/writeData/graphics/kafka_consumer_legacy.svg',
  },
  {
    id: 'sysstat',
    name: 'sysstat',
    url: `${TELEGRAF_PLUGINS}/sysstat`,
    markdown: sysstatMarkdown,
    image: '/src/writeData/graphics/sysstat.svg',
  },
  {
    id: 'iptables',
    name: 'iptables',
    url: `${TELEGRAF_PLUGINS}/iptables`,
    markdown: iptablesMarkdown,
    image: '/src/writeData/graphics/iptables.svg',
  },
  {
    id: 'hddtemp',
    name: 'hddtemp',
    url: `${TELEGRAF_PLUGINS}/hddtemp`,
    markdown: hddtempMarkdown,
    image: '/src/writeData/graphics/hddtemp.svg',
  },
  {
    id: 'unbound',
    name: 'unbound',
    url: `${TELEGRAF_PLUGINS}/unbound`,
    markdown: unboundMarkdown,
    image: '/src/writeData/graphics/unbound.svg',
  },
  {
    id: 'pgbouncer',
    name: 'pgbouncer',
    url: `${TELEGRAF_PLUGINS}/pgbouncer`,
    markdown: pgbouncerMarkdown,
    image: '/src/writeData/graphics/pgbouncer.svg',
  },
  {
    id: 'kinesis_consumer',
    name: 'kinesis_consumer',
    url: `${TELEGRAF_PLUGINS}/kinesis_consumer`,
    markdown: kinesis_consumerMarkdown,
    image: '/src/writeData/graphics/kinesis_consumer.svg',
  },
  {
    id: 'kubernetes',
    name: 'kubernetes',
    url: `${TELEGRAF_PLUGINS}/kubernetes`,
    markdown: kubernetesMarkdown,
    image: '/src/writeData/graphics/kubernetes.svg',
  },
  {
    id: 'icinga2',
    name: 'icinga2',
    url: `${TELEGRAF_PLUGINS}/icinga2`,
    markdown: icinga2Markdown,
    image: '/src/writeData/graphics/icinga2.svg',
  },
  {
    id: 'openldap',
    name: 'openldap',
    url: `${TELEGRAF_PLUGINS}/openldap`,
    markdown: openldapMarkdown,
    image: '/src/writeData/graphics/openldap.svg',
  },
  {
    id: 'passenger',
    name: 'passenger',
    url: `${TELEGRAF_PLUGINS}/passenger`,
    markdown: passengerMarkdown,
    image: '/src/writeData/graphics/passenger.svg',
  },
  {
    id: 'mandrill',
    name: 'mandrill',
    url: `${TELEGRAF_PLUGINS}/mandrill`,
    markdown: mandrillMarkdown,
    image: '/src/writeData/graphics/mandrill.svg',
  },
  {
    id: 'filestack',
    name: 'filestack',
    url: `${TELEGRAF_PLUGINS}/filestack`,
    markdown: filestackMarkdown,
    image: '/src/writeData/graphics/filestack.svg',
  },
  {
    id: 'rollbar',
    name: 'rollbar',
    url: `${TELEGRAF_PLUGINS}/rollbar`,
    markdown: rollbarMarkdown,
    image: '/src/writeData/graphics/rollbar.svg',
  },
  {
    id: 'webhooks',
    name: 'webhooks',
    url: `${TELEGRAF_PLUGINS}/webhooks`,
    markdown: webhooksMarkdown,
    image: '/src/writeData/graphics/webhooks.svg',
  },
  {
    id: 'github',
    name: 'github',
    url: `${TELEGRAF_PLUGINS}/github`,
    markdown: githubMarkdown,
    image: '/src/writeData/graphics/github.svg',
  },
  {
    id: 'papertrail',
    name: 'papertrail',
    url: `${TELEGRAF_PLUGINS}/papertrail`,
    markdown: papertrailMarkdown,
    image: '/src/writeData/graphics/papertrail.svg',
  },
  {
    id: 'particle',
    name: 'particle',
    url: `${TELEGRAF_PLUGINS}/particle`,
    markdown: particleMarkdown,
    image: '/src/writeData/graphics/particle.svg',
  },
  {
    id: 'wireguard',
    name: 'wireguard',
    url: `${TELEGRAF_PLUGINS}/wireguard`,
    markdown: wireguardMarkdown,
    image: '/src/writeData/graphics/wireguard.svg',
  },
  {
    id: 'riak',
    name: 'riak',
    url: `${TELEGRAF_PLUGINS}/riak`,
    markdown: riakMarkdown,
    image: '/src/writeData/graphics/riak.svg',
  },
  {
    id: 'bind',
    name: 'bind',
    url: `${TELEGRAF_PLUGINS}/bind`,
    markdown: bindMarkdown,
    image: '/src/writeData/graphics/bind.svg',
  },
  {
    id: 'modbus',
    name: 'modbus',
    url: `${TELEGRAF_PLUGINS}/modbus`,
    markdown: modbusMarkdown,
    image: '/src/writeData/graphics/modbus.svg',
  },
  {
    id: 'minecraft',
    name: 'minecraft',
    url: `${TELEGRAF_PLUGINS}/minecraft`,
    markdown: minecraftMarkdown,
    image: '/src/writeData/graphics/minecraft.svg',
  },
  {
    id: 'tcp_listener',
    name: 'tcp_listener',
    url: `${TELEGRAF_PLUGINS}/tcp_listener`,
    markdown: tcp_listenerMarkdown,
    image: '/src/writeData/graphics/tcp_listener.svg',
  },
  {
    id: 'ipvs',
    name: 'ipvs',
    url: `${TELEGRAF_PLUGINS}/ipvs`,
    markdown: ipvsMarkdown,
    image: '/src/writeData/graphics/ipvs.svg',
  },
  {
    id: 'openweathermap',
    name: 'openweathermap',
    url: `${TELEGRAF_PLUGINS}/openweathermap`,
    markdown: openweathermapMarkdown,
    image: '/src/writeData/graphics/openweathermap.svg',
  },
  {
    id: 'powerdns_recursor',
    name: 'powerdns_recursor',
    url: `${TELEGRAF_PLUGINS}/powerdns_recursor`,
    markdown: powerdns_recursorMarkdown,
    image: '/src/writeData/graphics/powerdns_recursor.svg',
  },
  {
    id: 'puppetagent',
    name: 'puppetagent',
    url: `${TELEGRAF_PLUGINS}/puppetagent`,
    markdown: puppetagentMarkdown,
    image: '/src/writeData/graphics/puppetagent.svg',
  },
  {
    id: 'dns_query',
    name: 'dns_query',
    url: `${TELEGRAF_PLUGINS}/dns_query`,
    markdown: dns_queryMarkdown,
    image: '/src/writeData/graphics/dns_query.svg',
  },
  {
    id: 'elasticsearch',
    name: 'elasticsearch',
    url: `${TELEGRAF_PLUGINS}/elasticsearch`,
    markdown: elasticsearchMarkdown,
    image: '/src/writeData/graphics/elasticsearch.svg',
  },
  {
    id: 'apache',
    name: 'apache',
    url: `${TELEGRAF_PLUGINS}/apache`,
    markdown: apacheMarkdown,
    image: '/src/writeData/graphics/apache.svg',
  },
  {
    id: 'nginx_sts',
    name: 'nginx_sts',
    url: `${TELEGRAF_PLUGINS}/nginx_sts`,
    markdown: nginx_stsMarkdown,
    image: '/src/writeData/graphics/nginx_sts.svg',
  },
  {
    id: 'nstat',
    name: 'nstat',
    url: `${TELEGRAF_PLUGINS}/nstat`,
    markdown: nstatMarkdown,
    image: '/src/writeData/graphics/nstat.svg',
  },
  {
    id: 'http_listener_v2',
    name: 'http_listener_v2',
    url: `${TELEGRAF_PLUGINS}/http_listener_v2`,
    markdown: http_listener_v2Markdown,
    image: '/src/writeData/graphics/http_listener_v2.svg',
  },
  {
    id: 'kernel',
    name: 'kernel',
    url: `${TELEGRAF_PLUGINS}/kernel`,
    markdown: kernelMarkdown,
    image: '/src/writeData/graphics/kernel.svg',
  },
  {
    id: 'filecount',
    name: 'filecount',
    url: `${TELEGRAF_PLUGINS}/filecount`,
    markdown: filecountMarkdown,
    image: '/src/writeData/graphics/filecount.svg',
  },
  {
    id: 'nsq',
    name: 'nsq',
    url: `${TELEGRAF_PLUGINS}/nsq`,
    markdown: nsqMarkdown,
    image: '/src/writeData/graphics/nsq.svg',
  },
  {
    id: 'mem',
    name: 'mem',
    url: `${TELEGRAF_PLUGINS}/mem`,
    markdown: memMarkdown,
    image: '/src/writeData/graphics/mem.svg',
  },
  {
    id: 'nats_consumer',
    name: 'nats_consumer',
    url: `${TELEGRAF_PLUGINS}/nats_consumer`,
    markdown: nats_consumerMarkdown,
    image: '/src/writeData/graphics/nats_consumer.svg',
  },
  {
    id: 'haproxy',
    name: 'haproxy',
    url: `${TELEGRAF_PLUGINS}/haproxy`,
    markdown: haproxyMarkdown,
    image: '/src/writeData/graphics/haproxy.svg',
  },
  {
    id: 'couchbase',
    name: 'couchbase',
    url: `${TELEGRAF_PLUGINS}/couchbase`,
    markdown: couchbaseMarkdown,
    image: '/src/writeData/graphics/couchbase.svg',
  },
  {
    id: 'fireboard',
    name: 'fireboard',
    url: `${TELEGRAF_PLUGINS}/fireboard`,
    markdown: fireboardMarkdown,
    image: '/src/writeData/graphics/fireboard.svg',
  },
]

const WRITE_DATA_TELEGRAF_PLUGINS_SECTION: WriteDataSection = {
  id: TELEGRAF_PLUGINS,
  name: 'Telegraf Plugins',
  description: 'Description goes here',
  items: WRITE_DATA_TELEGRAF_PLUGINS,
}

export default WRITE_DATA_TELEGRAF_PLUGINS_SECTION
