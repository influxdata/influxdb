// Constants
import {TELEGRAF_PLUGINS} from 'src/shared/constants/routes'

// Types
import {WriteDataItem, WriteDataSection} from 'src/writeData/constants'

// Markdown
import activemqMarkdown from 'src/writeData/components/telegrafPlugins/activemq.md'
import aerospikeMarkdown from 'src/writeData/components/telegrafPlugins/aerospike.md'
import amqp_consumerMarkdown from 'src/writeData/components/telegrafPlugins/amqp_consumer.md'
import apacheMarkdown from 'src/writeData/components/telegrafPlugins/apache.md'
import apcupsdMarkdown from 'src/writeData/components/telegrafPlugins/apcupsd.md'
import auroraMarkdown from 'src/writeData/components/telegrafPlugins/aurora.md'
import azure_storage_queueMarkdown from 'src/writeData/components/telegrafPlugins/azure_storage_queue.md'
import bcacheMarkdown from 'src/writeData/components/telegrafPlugins/bcache.md'
import beanstalkdMarkdown from 'src/writeData/components/telegrafPlugins/beanstalkd.md'
import bindMarkdown from 'src/writeData/components/telegrafPlugins/bind.md'
import bondMarkdown from 'src/writeData/components/telegrafPlugins/bond.md'
import burrowMarkdown from 'src/writeData/components/telegrafPlugins/burrow.md'
import cassandraMarkdown from 'src/writeData/components/telegrafPlugins/cassandra.md'
import cephMarkdown from 'src/writeData/components/telegrafPlugins/ceph.md'
import cgroupMarkdown from 'src/writeData/components/telegrafPlugins/cgroup.md'
import chronyMarkdown from 'src/writeData/components/telegrafPlugins/chrony.md'
import cisco_telemetry_mdtMarkdown from 'src/writeData/components/telegrafPlugins/cisco_telemetry_mdt.md'
import clickhouseMarkdown from 'src/writeData/components/telegrafPlugins/clickhouse.md'
import cloud_pubsub_pushMarkdown from 'src/writeData/components/telegrafPlugins/cloud_pubsub_push.md'
import cloud_pubsubMarkdown from 'src/writeData/components/telegrafPlugins/cloud_pubsub.md'
import cloudwatchMarkdown from 'src/writeData/components/telegrafPlugins/cloudwatch.md'
import conntrackMarkdown from 'src/writeData/components/telegrafPlugins/conntrack.md'
import consulMarkdown from 'src/writeData/components/telegrafPlugins/consul.md'
import couchbaseMarkdown from 'src/writeData/components/telegrafPlugins/couchbase.md'
import couchdbMarkdown from 'src/writeData/components/telegrafPlugins/couchdb.md'
import cpuMarkdown from 'src/writeData/components/telegrafPlugins/cpu.md'
import dcosMarkdown from 'src/writeData/components/telegrafPlugins/dcos.md'
import diskioMarkdown from 'src/writeData/components/telegrafPlugins/diskio.md'
import diskMarkdown from 'src/writeData/components/telegrafPlugins/disk.md'
import disqueMarkdown from 'src/writeData/components/telegrafPlugins/disque.md'
import dmcacheMarkdown from 'src/writeData/components/telegrafPlugins/dmcache.md'
import dns_queryMarkdown from 'src/writeData/components/telegrafPlugins/dns_query.md'
import docker_logMarkdown from 'src/writeData/components/telegrafPlugins/docker_log.md'
import dockerMarkdown from 'src/writeData/components/telegrafPlugins/docker.md'
import dovecotMarkdown from 'src/writeData/components/telegrafPlugins/dovecot.md'
import ecsMarkdown from 'src/writeData/components/telegrafPlugins/ecs.md'
import elasticsearchMarkdown from 'src/writeData/components/telegrafPlugins/elasticsearch.md'
import ethtoolMarkdown from 'src/writeData/components/telegrafPlugins/ethtool.md'
import eventhub_consumerMarkdown from 'src/writeData/components/telegrafPlugins/eventhub_consumer.md'
import execdMarkdown from 'src/writeData/components/telegrafPlugins/execd.md'
import execMarkdown from 'src/writeData/components/telegrafPlugins/exec.md'
import fail2banMarkdown from 'src/writeData/components/telegrafPlugins/fail2ban.md'
import fibaroMarkdown from 'src/writeData/components/telegrafPlugins/fibaro.md'
import filecountMarkdown from 'src/writeData/components/telegrafPlugins/filecount.md'
import fileMarkdown from 'src/writeData/components/telegrafPlugins/file.md'
import filestatMarkdown from 'src/writeData/components/telegrafPlugins/filestat.md'
import fireboardMarkdown from 'src/writeData/components/telegrafPlugins/fireboard.md'
import fluentdMarkdown from 'src/writeData/components/telegrafPlugins/fluentd.md'
import githubMarkdown from 'src/writeData/components/telegrafPlugins/github.md'
import gnmiMarkdown from 'src/writeData/components/telegrafPlugins/gnmi.md'
import graylogMarkdown from 'src/writeData/components/telegrafPlugins/graylog.md'
import haproxyMarkdown from 'src/writeData/components/telegrafPlugins/haproxy.md'
import hddtempMarkdown from 'src/writeData/components/telegrafPlugins/hddtemp.md'
import http_listener_v2Markdown from 'src/writeData/components/telegrafPlugins/http_listener_v2.md'
import http_responseMarkdown from 'src/writeData/components/telegrafPlugins/http_response.md'
import httpjsonMarkdown from 'src/writeData/components/telegrafPlugins/httpjson.md'
import httpMarkdown from 'src/writeData/components/telegrafPlugins/http.md'
import icinga2Markdown from 'src/writeData/components/telegrafPlugins/icinga2.md'
import infinibandMarkdown from 'src/writeData/components/telegrafPlugins/infiniband.md'
import influxdb_listenerMarkdown from 'src/writeData/components/telegrafPlugins/influxdb_listener.md'
import influxdbMarkdown from 'src/writeData/components/telegrafPlugins/influxdb.md'
import internalMarkdown from 'src/writeData/components/telegrafPlugins/internal.md'
import interruptsMarkdown from 'src/writeData/components/telegrafPlugins/interrupts.md'
import ipmi_sensorMarkdown from 'src/writeData/components/telegrafPlugins/ipmi_sensor.md'
import ipsetMarkdown from 'src/writeData/components/telegrafPlugins/ipset.md'
import iptablesMarkdown from 'src/writeData/components/telegrafPlugins/iptables.md'
import ipvsMarkdown from 'src/writeData/components/telegrafPlugins/ipvs.md'
import jenkinsMarkdown from 'src/writeData/components/telegrafPlugins/jenkins.md'
import jolokia2Markdown from 'src/writeData/components/telegrafPlugins/jolokia2.md'
import jolokiaMarkdown from 'src/writeData/components/telegrafPlugins/jolokia.md'
import jti_openconfig_telemetryMarkdown from 'src/writeData/components/telegrafPlugins/jti_openconfig_telemetry.md'
import kafka_consumer_legacyMarkdown from 'src/writeData/components/telegrafPlugins/kafka_consumer_legacy.md'
import kafka_consumerMarkdown from 'src/writeData/components/telegrafPlugins/kafka_consumer.md'
import kapacitorMarkdown from 'src/writeData/components/telegrafPlugins/kapacitor.md'
import kernel_vmstatMarkdown from 'src/writeData/components/telegrafPlugins/kernel_vmstat.md'
import kernelMarkdown from 'src/writeData/components/telegrafPlugins/kernel.md'
import kibanaMarkdown from 'src/writeData/components/telegrafPlugins/kibana.md'
import kinesis_consumerMarkdown from 'src/writeData/components/telegrafPlugins/kinesis_consumer.md'
import kube_inventoryMarkdown from 'src/writeData/components/telegrafPlugins/kube_inventory.md'
import kubernetesMarkdown from 'src/writeData/components/telegrafPlugins/kubernetes.md'
import lanzMarkdown from 'src/writeData/components/telegrafPlugins/lanz.md'
import leofsMarkdown from 'src/writeData/components/telegrafPlugins/leofs.md'
import linux_sysctl_fsMarkdown from 'src/writeData/components/telegrafPlugins/linux_sysctl_fs.md'
import logparserMarkdown from 'src/writeData/components/telegrafPlugins/logparser.md'
import logstashMarkdown from 'src/writeData/components/telegrafPlugins/logstash.md'
import lustre2Markdown from 'src/writeData/components/telegrafPlugins/lustre2.md'
import mailchimpMarkdown from 'src/writeData/components/telegrafPlugins/mailchimp.md'
import marklogicMarkdown from 'src/writeData/components/telegrafPlugins/marklogic.md'
import mcrouterMarkdown from 'src/writeData/components/telegrafPlugins/mcrouter.md'
import memcachedMarkdown from 'src/writeData/components/telegrafPlugins/memcached.md'
import memMarkdown from 'src/writeData/components/telegrafPlugins/mem.md'
import mesosMarkdown from 'src/writeData/components/telegrafPlugins/mesos.md'
import minecraftMarkdown from 'src/writeData/components/telegrafPlugins/minecraft.md'
import modbusMarkdown from 'src/writeData/components/telegrafPlugins/modbus.md'
import mongodbMarkdown from 'src/writeData/components/telegrafPlugins/mongodb.md'
import monitMarkdown from 'src/writeData/components/telegrafPlugins/monit.md'
import mqtt_consumerMarkdown from 'src/writeData/components/telegrafPlugins/mqtt_consumer.md'
import multifileMarkdown from 'src/writeData/components/telegrafPlugins/multifile.md'
import mysqlMarkdown from 'src/writeData/components/telegrafPlugins/mysql.md'
import nats_consumerMarkdown from 'src/writeData/components/telegrafPlugins/nats_consumer.md'
import natsMarkdown from 'src/writeData/components/telegrafPlugins/nats.md'
import neptune_apexMarkdown from 'src/writeData/components/telegrafPlugins/neptune_apex.md'
import net_responseMarkdown from 'src/writeData/components/telegrafPlugins/net_response.md'
import nginx_plus_apiMarkdown from 'src/writeData/components/telegrafPlugins/nginx_plus_api.md'
import nginx_plusMarkdown from 'src/writeData/components/telegrafPlugins/nginx_plus.md'
import nginx_stsMarkdown from 'src/writeData/components/telegrafPlugins/nginx_sts.md'
import nginx_upstream_checkMarkdown from 'src/writeData/components/telegrafPlugins/nginx_upstream_check.md'
import nginx_vtsMarkdown from 'src/writeData/components/telegrafPlugins/nginx_vts.md'
import nginxMarkdown from 'src/writeData/components/telegrafPlugins/nginx.md'
import nsq_consumerMarkdown from 'src/writeData/components/telegrafPlugins/nsq_consumer.md'
import nsqMarkdown from 'src/writeData/components/telegrafPlugins/nsq.md'
import nstatMarkdown from 'src/writeData/components/telegrafPlugins/nstat.md'
import ntpqMarkdown from 'src/writeData/components/telegrafPlugins/ntpq.md'
import nvidia_smiMarkdown from 'src/writeData/components/telegrafPlugins/nvidia_smi.md'
import openldapMarkdown from 'src/writeData/components/telegrafPlugins/openldap.md'
import openntpdMarkdown from 'src/writeData/components/telegrafPlugins/openntpd.md'
import opensmtpdMarkdown from 'src/writeData/components/telegrafPlugins/opensmtpd.md'
import openweathermapMarkdown from 'src/writeData/components/telegrafPlugins/openweathermap.md'
import passengerMarkdown from 'src/writeData/components/telegrafPlugins/passenger.md'
import pfMarkdown from 'src/writeData/components/telegrafPlugins/pf.md'
import pgbouncerMarkdown from 'src/writeData/components/telegrafPlugins/pgbouncer.md'
import phpfpmMarkdown from 'src/writeData/components/telegrafPlugins/phpfpm.md'
import pingMarkdown from 'src/writeData/components/telegrafPlugins/ping.md'
import postfixMarkdown from 'src/writeData/components/telegrafPlugins/postfix.md'
import postgresql_extensibleMarkdown from 'src/writeData/components/telegrafPlugins/postgresql_extensible.md'
import postgresqlMarkdown from 'src/writeData/components/telegrafPlugins/postgresql.md'
import powerdns_recursorMarkdown from 'src/writeData/components/telegrafPlugins/powerdns_recursor.md'
import powerdnsMarkdown from 'src/writeData/components/telegrafPlugins/powerdns.md'
import processesMarkdown from 'src/writeData/components/telegrafPlugins/processes.md'
import procstatMarkdown from 'src/writeData/components/telegrafPlugins/procstat.md'
import prometheusMarkdown from 'src/writeData/components/telegrafPlugins/prometheus.md'
import proxmoxMarkdown from 'src/writeData/components/telegrafPlugins/proxmox.md'
import puppetagentMarkdown from 'src/writeData/components/telegrafPlugins/puppetagent.md'
import rabbitmqMarkdown from 'src/writeData/components/telegrafPlugins/rabbitmq.md'
import raindropsMarkdown from 'src/writeData/components/telegrafPlugins/raindrops.md'
import redfishMarkdown from 'src/writeData/components/telegrafPlugins/redfish.md'
import redisMarkdown from 'src/writeData/components/telegrafPlugins/redis.md'
import rethinkdbMarkdown from 'src/writeData/components/telegrafPlugins/rethinkdb.md'
import riakMarkdown from 'src/writeData/components/telegrafPlugins/riak.md'
import salesforceMarkdown from 'src/writeData/components/telegrafPlugins/salesforce.md'
import sensorsMarkdown from 'src/writeData/components/telegrafPlugins/sensors.md'
import sflowMarkdown from 'src/writeData/components/telegrafPlugins/sflow.md'
import smartMarkdown from 'src/writeData/components/telegrafPlugins/smart.md'
import snmp_legacyMarkdown from 'src/writeData/components/telegrafPlugins/snmp_legacy.md'
import snmp_trapMarkdown from 'src/writeData/components/telegrafPlugins/snmp_trap.md'
import snmpMarkdown from 'src/writeData/components/telegrafPlugins/snmp.md'
import socket_listenerMarkdown from 'src/writeData/components/telegrafPlugins/socket_listener.md'
import solrMarkdown from 'src/writeData/components/telegrafPlugins/solr.md'
import sqlserverMarkdown from 'src/writeData/components/telegrafPlugins/sqlserver.md'
import stackdriverMarkdown from 'src/writeData/components/telegrafPlugins/stackdriver.md'
import statsdMarkdown from 'src/writeData/components/telegrafPlugins/statsd.md'
import suricataMarkdown from 'src/writeData/components/telegrafPlugins/suricata.md'
import swapMarkdown from 'src/writeData/components/telegrafPlugins/swap.md'
import synproxyMarkdown from 'src/writeData/components/telegrafPlugins/synproxy.md'
import syslogMarkdown from 'src/writeData/components/telegrafPlugins/syslog.md'
import sysstatMarkdown from 'src/writeData/components/telegrafPlugins/sysstat.md'
import systemd_unitsMarkdown from 'src/writeData/components/telegrafPlugins/systemd_units.md'
import systemMarkdown from 'src/writeData/components/telegrafPlugins/system.md'
import tailMarkdown from 'src/writeData/components/telegrafPlugins/tail.md'
import tcp_listenerMarkdown from 'src/writeData/components/telegrafPlugins/tcp_listener.md'
import teamspeakMarkdown from 'src/writeData/components/telegrafPlugins/teamspeak.md'
import tempMarkdown from 'src/writeData/components/telegrafPlugins/temp.md'
import tengineMarkdown from 'src/writeData/components/telegrafPlugins/tengine.md'
import tomcatMarkdown from 'src/writeData/components/telegrafPlugins/tomcat.md'
import udp_listenerMarkdown from 'src/writeData/components/telegrafPlugins/udp_listener.md'
import unboundMarkdown from 'src/writeData/components/telegrafPlugins/unbound.md'
import uwsgiMarkdown from 'src/writeData/components/telegrafPlugins/uwsgi.md'
import varnishMarkdown from 'src/writeData/components/telegrafPlugins/varnish.md'
import vsphereMarkdown from 'src/writeData/components/telegrafPlugins/vsphere.md'
import webhooksMarkdown from 'src/writeData/components/telegrafPlugins/webhooks.md'
import win_perf_countersMarkdown from 'src/writeData/components/telegrafPlugins/win_perf_counters.md'
import win_servicesMarkdown from 'src/writeData/components/telegrafPlugins/win_services.md'
import wireguardMarkdown from 'src/writeData/components/telegrafPlugins/wireguard.md'
import wirelessMarkdown from 'src/writeData/components/telegrafPlugins/wireless.md'
import x509_certMarkdown from 'src/writeData/components/telegrafPlugins/x509_cert.md'
import zfsMarkdown from 'src/writeData/components/telegrafPlugins/zfs.md'
import zipkinMarkdown from 'src/writeData/components/telegrafPlugins/zipkin.md'
import zookeeperMarkdown from 'src/writeData/components/telegrafPlugins/zookeeper.md'

// Graphics
import activemqLogo from 'src/writeData/graphics/activemq.svg'
import aerospikeLogo from 'src/writeData/graphics/aerospike.svg'
import amqp_consumerLogo from 'src/writeData/graphics/amqp_consumer.svg'
import apacheLogo from 'src/writeData/graphics/apache.svg'
import apcupsdLogo from 'src/writeData/graphics/apcupsd.svg'
import auroraLogo from 'src/writeData/graphics/aurora.svg'
import azure_storage_queueLogo from 'src/writeData/graphics/azure_storage_queue.svg'
import bcacheLogo from 'src/writeData/graphics/bcache.svg'
import beanstalkdLogo from 'src/writeData/graphics/beanstalkd.svg'
import bindLogo from 'src/writeData/graphics/bind.svg'
import bondLogo from 'src/writeData/graphics/bond.svg'
import burrowLogo from 'src/writeData/graphics/burrow.svg'
import cassandraLogo from 'src/writeData/graphics/cassandra.svg'
import cephLogo from 'src/writeData/graphics/ceph.svg'
import cgroupLogo from 'src/writeData/graphics/cgroup.svg'
import chronyLogo from 'src/writeData/graphics/chrony.svg'
import cisco_telemetry_mdtLogo from 'src/writeData/graphics/cisco_telemetry_mdt.svg'
import clickhouseLogo from 'src/writeData/graphics/clickhouse.svg'
import cloud_pubsub_pushLogo from 'src/writeData/graphics/cloud_pubsub_push.svg'
import cloud_pubsubLogo from 'src/writeData/graphics/cloud_pubsub.svg'
import cloudwatchLogo from 'src/writeData/graphics/cloudwatch.svg'
import conntrackLogo from 'src/writeData/graphics/conntrack.svg'
import consulLogo from 'src/writeData/graphics/consul.svg'
import couchbaseLogo from 'src/writeData/graphics/couchbase.svg'
import couchdbLogo from 'src/writeData/graphics/couchdb.svg'
import cpuLogo from 'src/writeData/graphics/cpu.svg'
import dcosLogo from 'src/writeData/graphics/dcos.svg'
import diskioLogo from 'src/writeData/graphics/diskio.svg'
import diskLogo from 'src/writeData/graphics/disk.svg'
import disqueLogo from 'src/writeData/graphics/disque.svg'
import dmcacheLogo from 'src/writeData/graphics/dmcache.svg'
import dns_queryLogo from 'src/writeData/graphics/dns_query.svg'
import docker_logLogo from 'src/writeData/graphics/docker_log.svg'
import dockerLogo from 'src/writeData/graphics/docker.svg'
import dovecotLogo from 'src/writeData/graphics/dovecot.svg'
import ecsLogo from 'src/writeData/graphics/ecs.svg'
import elasticsearchLogo from 'src/writeData/graphics/elasticsearch.svg'
import ethtoolLogo from 'src/writeData/graphics/ethtool.svg'
import eventhub_consumerLogo from 'src/writeData/graphics/eventhub_consumer.svg'
import execdLogo from 'src/writeData/graphics/execd.svg'
import execLogo from 'src/writeData/graphics/exec.svg'
import fail2banLogo from 'src/writeData/graphics/fail2ban.svg'
import fibaroLogo from 'src/writeData/graphics/fibaro.svg'
import filecountLogo from 'src/writeData/graphics/filecount.svg'
import fileLogo from 'src/writeData/graphics/file.svg'
import filestatLogo from 'src/writeData/graphics/filestat.svg'
import fireboardLogo from 'src/writeData/graphics/fireboard.svg'
import fluentdLogo from 'src/writeData/graphics/fluentd.svg'
import githubLogo from 'src/writeData/graphics/github.svg'
import gnmiLogo from 'src/writeData/graphics/gnmi.svg'
import graylogLogo from 'src/writeData/graphics/graylog.svg'
import haproxyLogo from 'src/writeData/graphics/haproxy.svg'
import hddtempLogo from 'src/writeData/graphics/hddtemp.svg'
import http_listener_v2Logo from 'src/writeData/graphics/http_listener_v2.svg'
import http_responseLogo from 'src/writeData/graphics/http_response.svg'
import httpjsonLogo from 'src/writeData/graphics/httpjson.svg'
import httpLogo from 'src/writeData/graphics/http.svg'
import icinga2Logo from 'src/writeData/graphics/icinga2.svg'
import infinibandLogo from 'src/writeData/graphics/infiniband.svg'
import influxdb_listenerLogo from 'src/writeData/graphics/influxdb_listener.svg'
import influxdbLogo from 'src/writeData/graphics/influxdb.svg'
import internalLogo from 'src/writeData/graphics/internal.svg'
import interruptsLogo from 'src/writeData/graphics/interrupts.svg'
import ipmi_sensorLogo from 'src/writeData/graphics/ipmi_sensor.svg'
import ipsetLogo from 'src/writeData/graphics/ipset.svg'
import iptablesLogo from 'src/writeData/graphics/iptables.svg'
import ipvsLogo from 'src/writeData/graphics/ipvs.svg'
import jenkinsLogo from 'src/writeData/graphics/jenkins.svg'
import jolokia2Logo from 'src/writeData/graphics/jolokia2.svg'
import jolokiaLogo from 'src/writeData/graphics/jolokia.svg'
import jti_openconfig_telemetryLogo from 'src/writeData/graphics/jti_openconfig_telemetry.svg'
import kafka_consumer_legacyLogo from 'src/writeData/graphics/kafka_consumer_legacy.svg'
import kafka_consumerLogo from 'src/writeData/graphics/kafka_consumer.svg'
import kapacitorLogo from 'src/writeData/graphics/kapacitor.svg'
import kernel_vmstatLogo from 'src/writeData/graphics/kernel_vmstat.svg'
import kernelLogo from 'src/writeData/graphics/kernel.svg'
import kibanaLogo from 'src/writeData/graphics/kibana.svg'
import kinesis_consumerLogo from 'src/writeData/graphics/kinesis_consumer.svg'
import kube_inventoryLogo from 'src/writeData/graphics/kube_inventory.svg'
import kubernetesLogo from 'src/writeData/graphics/kubernetes.svg'
import lanzLogo from 'src/writeData/graphics/lanz.svg'
import leofsLogo from 'src/writeData/graphics/leofs.svg'
import linux_sysctl_fsLogo from 'src/writeData/graphics/linux_sysctl_fs.svg'
import logparserLogo from 'src/writeData/graphics/logparser.svg'
import logstashLogo from 'src/writeData/graphics/logstash.svg'
import lustre2Logo from 'src/writeData/graphics/lustre2.svg'
import mailchimpLogo from 'src/writeData/graphics/mailchimp.svg'
import marklogicLogo from 'src/writeData/graphics/marklogic.svg'
import mcrouterLogo from 'src/writeData/graphics/mcrouter.svg'
import memcachedLogo from 'src/writeData/graphics/memcached.svg'
import memLogo from 'src/writeData/graphics/mem.svg'
import mesosLogo from 'src/writeData/graphics/mesos.svg'
import minecraftLogo from 'src/writeData/graphics/minecraft.svg'
import modbusLogo from 'src/writeData/graphics/modbus.svg'
import mongodbLogo from 'src/writeData/graphics/mongodb.svg'
import monitLogo from 'src/writeData/graphics/monit.svg'
import mqtt_consumerLogo from 'src/writeData/graphics/mqtt_consumer.svg'
import multifileLogo from 'src/writeData/graphics/multifile.svg'
import mysqlLogo from 'src/writeData/graphics/mysql.svg'
import nats_consumerLogo from 'src/writeData/graphics/nats_consumer.svg'
import natsLogo from 'src/writeData/graphics/nats.svg'
import neptune_apexLogo from 'src/writeData/graphics/neptune_apex.svg'
import net_responseLogo from 'src/writeData/graphics/net_response.svg'
import nginx_plus_apiLogo from 'src/writeData/graphics/nginx_plus_api.svg'
import nginx_plusLogo from 'src/writeData/graphics/nginx_plus.svg'
import nginx_stsLogo from 'src/writeData/graphics/nginx_sts.svg'
import nginx_upstream_checkLogo from 'src/writeData/graphics/nginx_upstream_check.svg'
import nginx_vtsLogo from 'src/writeData/graphics/nginx_vts.svg'
import nginxLogo from 'src/writeData/graphics/nginx.svg'
import nsq_consumerLogo from 'src/writeData/graphics/nsq_consumer.svg'
import nsqLogo from 'src/writeData/graphics/nsq.svg'
import nstatLogo from 'src/writeData/graphics/nstat.svg'
import ntpqLogo from 'src/writeData/graphics/ntpq.svg'
import nvidia_smiLogo from 'src/writeData/graphics/nvidia_smi.svg'
import openldapLogo from 'src/writeData/graphics/openldap.svg'
import openntpdLogo from 'src/writeData/graphics/openntpd.svg'
import opensmtpdLogo from 'src/writeData/graphics/opensmtpd.svg'
import openweathermapLogo from 'src/writeData/graphics/openweathermap.svg'
import passengerLogo from 'src/writeData/graphics/passenger.svg'
import pfLogo from 'src/writeData/graphics/pf.svg'
import pgbouncerLogo from 'src/writeData/graphics/pgbouncer.svg'
import phpfpmLogo from 'src/writeData/graphics/phpfpm.svg'
import pingLogo from 'src/writeData/graphics/ping.svg'
import postfixLogo from 'src/writeData/graphics/postfix.svg'
import postgresql_extensibleLogo from 'src/writeData/graphics/postgresql_extensible.svg'
import postgresqlLogo from 'src/writeData/graphics/postgresql.svg'
import powerdns_recursorLogo from 'src/writeData/graphics/powerdns_recursor.svg'
import powerdnsLogo from 'src/writeData/graphics/powerdns.svg'
import processesLogo from 'src/writeData/graphics/processes.svg'
import procstatLogo from 'src/writeData/graphics/procstat.svg'
import prometheusLogo from 'src/writeData/graphics/prometheus.svg'
import proxmoxLogo from 'src/writeData/graphics/proxmox.svg'
import puppetagentLogo from 'src/writeData/graphics/puppetagent.svg'
import rabbitmqLogo from 'src/writeData/graphics/rabbitmq.svg'
import raindropsLogo from 'src/writeData/graphics/raindrops.svg'
import redfishLogo from 'src/writeData/graphics/redfish.svg'
import redisLogo from 'src/writeData/graphics/redis.svg'
import rethinkdbLogo from 'src/writeData/graphics/rethinkdb.svg'
import riakLogo from 'src/writeData/graphics/riak.svg'
import salesforceLogo from 'src/writeData/graphics/salesforce.svg'
import sensorsLogo from 'src/writeData/graphics/sensors.svg'
import sflowLogo from 'src/writeData/graphics/sflow.svg'
import smartLogo from 'src/writeData/graphics/smart.svg'
import snmp_legacyLogo from 'src/writeData/graphics/snmp_legacy.svg'
import snmp_trapLogo from 'src/writeData/graphics/snmp_trap.svg'
import snmpLogo from 'src/writeData/graphics/snmp.svg'
import socket_listenerLogo from 'src/writeData/graphics/socket_listener.svg'
import solrLogo from 'src/writeData/graphics/solr.svg'
import sqlserverLogo from 'src/writeData/graphics/sqlserver.svg'
import stackdriverLogo from 'src/writeData/graphics/stackdriver.svg'
import statsdLogo from 'src/writeData/graphics/statsd.svg'
import suricataLogo from 'src/writeData/graphics/suricata.svg'
import swapLogo from 'src/writeData/graphics/swap.svg'
import synproxyLogo from 'src/writeData/graphics/synproxy.svg'
import syslogLogo from 'src/writeData/graphics/syslog.svg'
import sysstatLogo from 'src/writeData/graphics/sysstat.svg'
import systemd_unitsLogo from 'src/writeData/graphics/systemd_units.svg'
import systemLogo from 'src/writeData/graphics/system.svg'
import tailLogo from 'src/writeData/graphics/tail.svg'
import tcp_listenerLogo from 'src/writeData/graphics/tcp_listener.svg'
import teamspeakLogo from 'src/writeData/graphics/teamspeak.svg'
import tempLogo from 'src/writeData/graphics/temp.svg'
import tengineLogo from 'src/writeData/graphics/tengine.svg'
import tomcatLogo from 'src/writeData/graphics/tomcat.svg'
import udp_listenerLogo from 'src/writeData/graphics/udp_listener.svg'
import unboundLogo from 'src/writeData/graphics/unbound.svg'
import uwsgiLogo from 'src/writeData/graphics/uwsgi.svg'
import varnishLogo from 'src/writeData/graphics/varnish.svg'
import vsphereLogo from 'src/writeData/graphics/vsphere.svg'
import webhooksLogo from 'src/writeData/graphics/webhooks.svg'
import win_perf_countersLogo from 'src/writeData/graphics/win_perf_counters.svg'
import win_servicesLogo from 'src/writeData/graphics/win_services.svg'
import wireguardLogo from 'src/writeData/graphics/wireguard.svg'
import wirelessLogo from 'src/writeData/graphics/wireless.svg'
import x509_certLogo from 'src/writeData/graphics/x509_cert.svg'
import zfsLogo from 'src/writeData/graphics/zfs.svg'
import zipkinLogo from 'src/writeData/graphics/zipkin.svg'
import zookeeperLogo from 'src/writeData/graphics/zookeeper.svg'

export const WRITE_DATA_TELEGRAF_PLUGINS: WriteDataItem[] = [
  {
    id: 'activemq',
    name: 'ActiveMQ',
    url: `${TELEGRAF_PLUGINS}/activemq`,
    markdown: activemqMarkdown,
    image: activemqLogo,
  },
  {
    id: 'aerospike',
    name: 'Aerospike',
    url: `${TELEGRAF_PLUGINS}/aerospike`,
    markdown: aerospikeMarkdown,
    image: aerospikeLogo,
  },
  {
    id: 'amqp_consumer',
    name: 'AMQP Consumer',
    url: `${TELEGRAF_PLUGINS}/amqp_consumer`,
    markdown: amqp_consumerMarkdown,
    image: amqp_consumerLogo,
  },
  {
    id: 'apache',
    name: 'Apache',
    url: `${TELEGRAF_PLUGINS}/apache`,
    markdown: apacheMarkdown,
    image: apacheLogo,
  },
  {
    id: 'apcupsd',
    name: 'APCUPSD',
    url: `${TELEGRAF_PLUGINS}/apcupsd`,
    markdown: apcupsdMarkdown,
    image: apcupsdLogo,
  },
  {
    id: 'aurora',
    name: 'Aurora',
    url: `${TELEGRAF_PLUGINS}/aurora`,
    markdown: auroraMarkdown,
    image: auroraLogo,
  },
  {
    id: 'azure_storage_queue',
    name: 'Azure Storage Queue',
    url: `${TELEGRAF_PLUGINS}/azure_storage_queue`,
    markdown: azure_storage_queueMarkdown,
    image: azure_storage_queueLogo,
  },
  {
    id: 'bcache',
    name: 'bcache',
    url: `${TELEGRAF_PLUGINS}/bcache`,
    markdown: bcacheMarkdown,
    image: bcacheLogo,
  },
  {
    id: 'beanstalkd',
    name: 'Beanstalkd',
    url: `${TELEGRAF_PLUGINS}/beanstalkd`,
    markdown: beanstalkdMarkdown,
    image: beanstalkdLogo,
  },
  {
    id: 'bind',
    name: 'BIND 9 Nameserver Statistics',
    url: `${TELEGRAF_PLUGINS}/bind`,
    markdown: bindMarkdown,
    image: bindLogo,
  },
  {
    id: 'bond',
    name: 'Bond',
    url: `${TELEGRAF_PLUGINS}/bond`,
    markdown: bondMarkdown,
    image: bondLogo,
  },
  {
    id: 'burrow',
    name: 'Burrow Kafka Consumer Lag Checking',
    url: `${TELEGRAF_PLUGINS}/burrow`,
    markdown: burrowMarkdown,
    image: burrowLogo,
  },
  {
    id: 'cassandra',
    name: 'Cassandra',
    url: `${TELEGRAF_PLUGINS}/cassandra`,
    markdown: cassandraMarkdown,
    image: cassandraLogo,
  },
  {
    id: 'ceph',
    name: 'Ceph Storage',
    url: `${TELEGRAF_PLUGINS}/ceph`,
    markdown: cephMarkdown,
    image: cephLogo,
  },
  {
    id: 'cgroup',
    name: 'CGroup',
    url: `${TELEGRAF_PLUGINS}/cgroup`,
    markdown: cgroupMarkdown,
    image: cgroupLogo,
  },
  {
    id: 'chrony',
    name: 'chrony',
    url: `${TELEGRAF_PLUGINS}/chrony`,
    markdown: chronyMarkdown,
    image: chronyLogo,
  },
  {
    id: 'cisco_telemetry_mdt',
    name: 'Cisco Model-Driven Telemetry (MDT)',
    url: `${TELEGRAF_PLUGINS}/cisco_telemetry_mdt`,
    markdown: cisco_telemetry_mdtMarkdown,
    image: cisco_telemetry_mdtLogo,
  },
  {
    id: 'clickhouse',
    name: 'ClickHouse',
    url: `${TELEGRAF_PLUGINS}/clickhouse`,
    markdown: clickhouseMarkdown,
    image: clickhouseLogo,
  },
  {
    id: 'cloud_pubsub',
    name: 'Google Cloud PubSub',
    url: `${TELEGRAF_PLUGINS}/cloud_pubsub`,
    markdown: cloud_pubsubMarkdown,
    image: cloud_pubsubLogo,
  },
  {
    id: 'cloud_pubsub_push',
    name: 'Google Cloud PubSub Push',
    url: `${TELEGRAF_PLUGINS}/cloud_pubsub_push`,
    markdown: cloud_pubsub_pushMarkdown,
    image: cloud_pubsub_pushLogo,
  },
  {
    id: 'cloudwatch',
    name: 'Amazon CloudWatch Statistics',
    url: `${TELEGRAF_PLUGINS}/cloudwatch`,
    markdown: cloudwatchMarkdown,
    image: cloudwatchLogo,
  },
  {
    id: 'conntrack',
    name: 'Conntrack',
    url: `${TELEGRAF_PLUGINS}/conntrack`,
    markdown: conntrackMarkdown,
    image: conntrackLogo,
  },
  {
    id: 'consul',
    name: 'Consul',
    url: `${TELEGRAF_PLUGINS}/consul`,
    markdown: consulMarkdown,
    image: consulLogo,
  },
  {
    id: 'couchbase',
    name: 'Couchbase',
    url: `${TELEGRAF_PLUGINS}/couchbase`,
    markdown: couchbaseMarkdown,
    image: couchbaseLogo,
  },
  {
    id: 'couchdb',
    name: 'CouchDB',
    url: `${TELEGRAF_PLUGINS}/couchdb`,
    markdown: couchdbMarkdown,
    image: couchdbLogo,
  },
  {
    id: 'cpu',
    name: 'CPU',
    url: `${TELEGRAF_PLUGINS}/cpu`,
    markdown: cpuMarkdown,
    image: cpuLogo,
  },
  {
    id: 'dcos',
    name: 'DC/OS',
    url: `${TELEGRAF_PLUGINS}/dcos`,
    markdown: dcosMarkdown,
    image: dcosLogo,
  },
  {
    id: 'disk',
    name: 'Disk',
    url: `${TELEGRAF_PLUGINS}/disk`,
    markdown: diskMarkdown,
    image: diskLogo,
  },
  {
    id: 'diskio',
    name: 'DiskIO',
    url: `${TELEGRAF_PLUGINS}/diskio`,
    markdown: diskioMarkdown,
    image: diskioLogo,
  },
  {
    id: 'disque',
    name: 'Disque',
    url: `${TELEGRAF_PLUGINS}/disque`,
    markdown: disqueMarkdown,
    image: disqueLogo,
  },
  {
    id: 'dmcache',
    name: 'DMCache',
    url: `${TELEGRAF_PLUGINS}/dmcache`,
    markdown: dmcacheMarkdown,
    image: dmcacheLogo,
  },
  {
    id: 'dns_query',
    name: 'DNS Query',
    url: `${TELEGRAF_PLUGINS}/dns_query`,
    markdown: dns_queryMarkdown,
    image: dns_queryLogo,
  },
  {
    id: 'docker',
    name: 'Docker',
    url: `${TELEGRAF_PLUGINS}/docker`,
    markdown: dockerMarkdown,
    image: dockerLogo,
  },
  {
    id: 'docker_log',
    name: 'Docker Log',
    url: `${TELEGRAF_PLUGINS}/docker_log`,
    markdown: docker_logMarkdown,
    image: docker_logLogo,
  },
  {
    id: 'dovecot',
    name: 'Dovecot',
    url: `${TELEGRAF_PLUGINS}/dovecot`,
    markdown: dovecotMarkdown,
    image: dovecotLogo,
  },
  {
    id: 'ecs',
    name: 'Amazon ECS',
    url: `${TELEGRAF_PLUGINS}/ecs`,
    markdown: ecsMarkdown,
    image: ecsLogo,
  },
  {
    id: 'elasticsearch',
    name: 'Elasticsearch',
    url: `${TELEGRAF_PLUGINS}/elasticsearch`,
    markdown: elasticsearchMarkdown,
    image: elasticsearchLogo,
  },
  {
    id: 'ethtool',
    name: 'Ethtool',
    url: `${TELEGRAF_PLUGINS}/ethtool`,
    markdown: ethtoolMarkdown,
    image: ethtoolLogo,
  },
  {
    id: 'eventhub_consumer',
    name: 'Event Hub Consumer',
    url: `${TELEGRAF_PLUGINS}/eventhub_consumer`,
    markdown: eventhub_consumerMarkdown,
    image: eventhub_consumerLogo,
  },
  {
    id: 'exec',
    name: 'Exec',
    url: `${TELEGRAF_PLUGINS}/exec`,
    markdown: execMarkdown,
    image: execLogo,
  },
  {
    id: 'execd',
    name: 'Execd',
    url: `${TELEGRAF_PLUGINS}/execd`,
    markdown: execdMarkdown,
    image: execdLogo,
  },
  {
    id: 'fail2ban',
    name: 'Fail2ban',
    url: `${TELEGRAF_PLUGINS}/fail2ban`,
    markdown: fail2banMarkdown,
    image: fail2banLogo,
  },
  {
    id: 'fibaro',
    name: 'Fibaro',
    url: `${TELEGRAF_PLUGINS}/fibaro`,
    markdown: fibaroMarkdown,
    image: fibaroLogo,
  },
  {
    id: 'file',
    name: 'File',
    url: `${TELEGRAF_PLUGINS}/file`,
    markdown: fileMarkdown,
    image: fileLogo,
  },
  {
    id: 'filecount',
    name: 'Filecount',
    url: `${TELEGRAF_PLUGINS}/filecount`,
    markdown: filecountMarkdown,
    image: filecountLogo,
  },
  {
    id: 'filestat',
    name: 'filestat',
    url: `${TELEGRAF_PLUGINS}/filestat`,
    markdown: filestatMarkdown,
    image: filestatLogo,
  },
  {
    id: 'fireboard',
    name: 'Fireboard',
    url: `${TELEGRAF_PLUGINS}/fireboard`,
    markdown: fireboardMarkdown,
    image: fireboardLogo,
  },
  {
    id: 'fluentd',
    name: 'Fluentd',
    url: `${TELEGRAF_PLUGINS}/fluentd`,
    markdown: fluentdMarkdown,
    image: fluentdLogo,
  },
  {
    id: 'github',
    name: 'GitHub',
    url: `${TELEGRAF_PLUGINS}/github`,
    markdown: githubMarkdown,
    image: githubLogo,
  },
  {
    id: 'gnmi',
    name: 'gNMI (gRPC Network Management Interface)',
    url: `${TELEGRAF_PLUGINS}/gnmi`,
    markdown: gnmiMarkdown,
    image: gnmiLogo,
  },
  {
    id: 'graylog',
    name: 'GrayLog',
    url: `${TELEGRAF_PLUGINS}/graylog`,
    markdown: graylogMarkdown,
    image: graylogLogo,
  },
  {
    id: 'haproxy',
    name: 'HAProxy',
    url: `${TELEGRAF_PLUGINS}/haproxy`,
    markdown: haproxyMarkdown,
    image: haproxyLogo,
  },
  {
    id: 'hddtemp',
    name: 'HDDtemp',
    url: `${TELEGRAF_PLUGINS}/hddtemp`,
    markdown: hddtempMarkdown,
    image: hddtempLogo,
  },
  {
    id: 'http',
    name: 'HTTP',
    url: `${TELEGRAF_PLUGINS}/http`,
    markdown: httpMarkdown,
    image: httpLogo,
  },
  {
    id: 'http_listener_v2',
    name: 'HTTP Listener v2',
    url: `${TELEGRAF_PLUGINS}/http_listener_v2`,
    markdown: http_listener_v2Markdown,
    image: http_listener_v2Logo,
  },
  {
    id: 'http_response',
    name: 'HTTP Response',
    url: `${TELEGRAF_PLUGINS}/http_response`,
    markdown: http_responseMarkdown,
    image: http_responseLogo,
  },
  {
    id: 'httpjson',
    name: 'HTTP JSON',
    url: `${TELEGRAF_PLUGINS}/httpjson`,
    markdown: httpjsonMarkdown,
    image: httpjsonLogo,
  },
  {
    id: 'icinga2',
    name: 'Icinga2',
    url: `${TELEGRAF_PLUGINS}/icinga2`,
    markdown: icinga2Markdown,
    image: icinga2Logo,
  },
  {
    id: 'infiniband',
    name: 'InfiniBand',
    url: `${TELEGRAF_PLUGINS}/infiniband`,
    markdown: infinibandMarkdown,
    image: infinibandLogo,
  },
  {
    id: 'influxdb',
    name: 'InfluxDB',
    url: `${TELEGRAF_PLUGINS}/influxdb`,
    markdown: influxdbMarkdown,
    image: influxdbLogo,
  },
  {
    id: 'influxdb_listener',
    name: 'InfluxDB Listener',
    url: `${TELEGRAF_PLUGINS}/influxdb_listener`,
    markdown: influxdb_listenerMarkdown,
    image: influxdb_listenerLogo,
  },
  {
    id: 'internal',
    name: 'Internal',
    url: `${TELEGRAF_PLUGINS}/internal`,
    markdown: internalMarkdown,
    image: internalLogo,
  },
  {
    id: 'interrupts',
    name: 'Interrupts',
    url: `${TELEGRAF_PLUGINS}/interrupts`,
    markdown: interruptsMarkdown,
    image: interruptsLogo,
  },
  {
    id: 'ipmi_sensor',
    name: 'IPMI Sensor',
    url: `${TELEGRAF_PLUGINS}/ipmi_sensor`,
    markdown: ipmi_sensorMarkdown,
    image: ipmi_sensorLogo,
  },
  {
    id: 'ipset',
    name: 'Ipset',
    url: `${TELEGRAF_PLUGINS}/ipset`,
    markdown: ipsetMarkdown,
    image: ipsetLogo,
  },
  {
    id: 'iptables',
    name: 'Iptables',
    url: `${TELEGRAF_PLUGINS}/iptables`,
    markdown: iptablesMarkdown,
    image: iptablesLogo,
  },
  {
    id: 'ipvs',
    name: 'IPVS',
    url: `${TELEGRAF_PLUGINS}/ipvs`,
    markdown: ipvsMarkdown,
    image: ipvsLogo,
  },
  {
    id: 'jenkins',
    name: 'Jenkins',
    url: `${TELEGRAF_PLUGINS}/jenkins`,
    markdown: jenkinsMarkdown,
    image: jenkinsLogo,
  },
  {
    id: 'jolokia',
    name: 'Jolokia',
    url: `${TELEGRAF_PLUGINS}/jolokia`,
    markdown: jolokiaMarkdown,
    image: jolokiaLogo,
  },
  {
    id: 'jolokia2',
    name: 'Jolokia2',
    url: `${TELEGRAF_PLUGINS}/jolokia2`,
    markdown: jolokia2Markdown,
    image: jolokia2Logo,
  },
  {
    id: 'jti_openconfig_telemetry',
    name: 'JTI OpenConfig Telemetry',
    url: `${TELEGRAF_PLUGINS}/jti_openconfig_telemetry`,
    markdown: jti_openconfig_telemetryMarkdown,
    image: jti_openconfig_telemetryLogo,
  },
  {
    id: 'kafka_consumer',
    name: 'Kafka Consumer',
    url: `${TELEGRAF_PLUGINS}/kafka_consumer`,
    markdown: kafka_consumerMarkdown,
    image: kafka_consumerLogo,
  },
  {
    id: 'kafka_consumer_legacy',
    name: 'Kafka Consumer Legacy',
    url: `${TELEGRAF_PLUGINS}/kafka_consumer_legacy`,
    markdown: kafka_consumer_legacyMarkdown,
    image: kafka_consumer_legacyLogo,
  },
  {
    id: 'kapacitor',
    name: 'Kapacitor',
    url: `${TELEGRAF_PLUGINS}/kapacitor`,
    markdown: kapacitorMarkdown,
    image: kapacitorLogo,
  },
  {
    id: 'kernel',
    name: 'Kernel',
    url: `${TELEGRAF_PLUGINS}/kernel`,
    markdown: kernelMarkdown,
    image: kernelLogo,
  },
  {
    id: 'kernel_vmstat',
    name: 'Kernel VMStat',
    url: `${TELEGRAF_PLUGINS}/kernel_vmstat`,
    markdown: kernel_vmstatMarkdown,
    image: kernel_vmstatLogo,
  },
  {
    id: 'kibana',
    name: 'Kibana',
    url: `${TELEGRAF_PLUGINS}/kibana`,
    markdown: kibanaMarkdown,
    image: kibanaLogo,
  },
  {
    id: 'kinesis_consumer',
    name: 'Kinesis Consumer',
    url: `${TELEGRAF_PLUGINS}/kinesis_consumer`,
    markdown: kinesis_consumerMarkdown,
    image: kinesis_consumerLogo,
  },
  {
    id: 'kube_inventory',
    name: 'Kubernetes Inventory',
    url: `${TELEGRAF_PLUGINS}/kube_inventory`,
    markdown: kube_inventoryMarkdown,
    image: kube_inventoryLogo,
  },
  {
    id: 'kubernetes',
    name: 'Kubernetes',
    url: `${TELEGRAF_PLUGINS}/kubernetes`,
    markdown: kubernetesMarkdown,
    image: kubernetesLogo,
  },
  {
    id: 'lanz',
    name: 'Arista LANZ Consumer',
    url: `${TELEGRAF_PLUGINS}/lanz`,
    markdown: lanzMarkdown,
    image: lanzLogo,
  },
  {
    id: 'leofs',
    name: 'LeoFS',
    url: `${TELEGRAF_PLUGINS}/leofs`,
    markdown: leofsMarkdown,
    image: leofsLogo,
  },
  {
    id: 'linux_sysctl_fs',
    name: 'Linux Sysctl FS',
    url: `${TELEGRAF_PLUGINS}/linux_sysctl_fs`,
    markdown: linux_sysctl_fsMarkdown,
    image: linux_sysctl_fsLogo,
  },
  {
    id: 'logparser',
    name: 'Logparser',
    url: `${TELEGRAF_PLUGINS}/logparser`,
    markdown: logparserMarkdown,
    image: logparserLogo,
  },
  {
    id: 'logstash',
    name: 'Logstash',
    url: `${TELEGRAF_PLUGINS}/logstash`,
    markdown: logstashMarkdown,
    image: logstashLogo,
  },
  {
    id: 'lustre2',
    name: 'Lustre',
    url: `${TELEGRAF_PLUGINS}/lustre2`,
    markdown: lustre2Markdown,
    image: lustre2Logo,
  },
  {
    id: 'mailchimp',
    name: 'Mailchimp',
    url: `${TELEGRAF_PLUGINS}/mailchimp`,
    markdown: mailchimpMarkdown,
    image: mailchimpLogo,
  },
  {
    id: 'marklogic',
    name: 'MarkLogic',
    url: `${TELEGRAF_PLUGINS}/marklogic`,
    markdown: marklogicMarkdown,
    image: marklogicLogo,
  },
  {
    id: 'mcrouter',
    name: 'Mcrouter',
    url: `${TELEGRAF_PLUGINS}/mcrouter`,
    markdown: mcrouterMarkdown,
    image: mcrouterLogo,
  },
  {
    id: 'mem',
    name: 'Memory',
    url: `${TELEGRAF_PLUGINS}/mem`,
    markdown: memMarkdown,
    image: memLogo,
  },
  {
    id: 'memcached',
    name: 'Memcached',
    url: `${TELEGRAF_PLUGINS}/memcached`,
    markdown: memcachedMarkdown,
    image: memcachedLogo,
  },
  {
    id: 'mesos',
    name: 'Mesos',
    url: `${TELEGRAF_PLUGINS}/mesos`,
    markdown: mesosMarkdown,
    image: mesosLogo,
  },
  {
    id: 'minecraft',
    name: 'Minecraft',
    url: `${TELEGRAF_PLUGINS}/minecraft`,
    markdown: minecraftMarkdown,
    image: minecraftLogo,
  },
  {
    id: 'modbus',
    name: 'Modbus',
    url: `${TELEGRAF_PLUGINS}/modbus`,
    markdown: modbusMarkdown,
    image: modbusLogo,
  },
  {
    id: 'mongodb',
    name: 'MongoDB',
    url: `${TELEGRAF_PLUGINS}/mongodb`,
    markdown: mongodbMarkdown,
    image: mongodbLogo,
  },
  {
    id: 'monit',
    name: 'Monit',
    url: `${TELEGRAF_PLUGINS}/monit`,
    markdown: monitMarkdown,
    image: monitLogo,
  },
  {
    id: 'mqtt_consumer',
    name: 'MQTT Consumer',
    url: `${TELEGRAF_PLUGINS}/mqtt_consumer`,
    markdown: mqtt_consumerMarkdown,
    image: mqtt_consumerLogo,
  },
  {
    id: 'multifile',
    name: 'Multifile',
    url: `${TELEGRAF_PLUGINS}/multifile`,
    markdown: multifileMarkdown,
    image: multifileLogo,
  },
  {
    id: 'mysql',
    name: 'MySQL',
    url: `${TELEGRAF_PLUGINS}/mysql`,
    markdown: mysqlMarkdown,
    image: mysqlLogo,
  },
  {
    id: 'nats',
    name: 'NATS',
    url: `${TELEGRAF_PLUGINS}/nats`,
    markdown: natsMarkdown,
    image: natsLogo,
  },
  {
    id: 'nats_consumer',
    name: 'NATS Consumer',
    url: `${TELEGRAF_PLUGINS}/nats_consumer`,
    markdown: nats_consumerMarkdown,
    image: nats_consumerLogo,
  },
  {
    id: 'neptune_apex',
    name: 'Neptune Apex',
    url: `${TELEGRAF_PLUGINS}/neptune_apex`,
    markdown: neptune_apexMarkdown,
    image: neptune_apexLogo,
  },
  {
    id: 'net_response',
    name: 'Network Response',
    url: `${TELEGRAF_PLUGINS}/net_response`,
    markdown: net_responseMarkdown,
    image: net_responseLogo,
  },
  {
    id: 'nginx',
    name: 'Nginx',
    url: `${TELEGRAF_PLUGINS}/nginx`,
    markdown: nginxMarkdown,
    image: nginxLogo,
  },
  {
    id: 'nginx_plus',
    name: 'Nginx Plus',
    url: `${TELEGRAF_PLUGINS}/nginx_plus`,
    markdown: nginx_plusMarkdown,
    image: nginx_plusLogo,
  },
  {
    id: 'nginx_plus_api',
    name: 'Nginx Plus API',
    url: `${TELEGRAF_PLUGINS}/nginx_plus_api`,
    markdown: nginx_plus_apiMarkdown,
    image: nginx_plus_apiLogo,
  },
  {
    id: 'nginx_sts',
    name: 'Nginx Stream STS',
    url: `${TELEGRAF_PLUGINS}/nginx_sts`,
    markdown: nginx_stsMarkdown,
    image: nginx_stsLogo,
  },
  {
    id: 'nginx_upstream_check',
    name: 'Nginx Upstream Check',
    url: `${TELEGRAF_PLUGINS}/nginx_upstream_check`,
    markdown: nginx_upstream_checkMarkdown,
    image: nginx_upstream_checkLogo,
  },
  {
    id: 'nginx_vts',
    name: 'Nginx Virtual Host Traffic (VTS)',
    url: `${TELEGRAF_PLUGINS}/nginx_vts`,
    markdown: nginx_vtsMarkdown,
    image: nginx_vtsLogo,
  },
  {
    id: 'nsq',
    name: 'NSQ',
    url: `${TELEGRAF_PLUGINS}/nsq`,
    markdown: nsqMarkdown,
    image: nsqLogo,
  },
  {
    id: 'nsq_consumer',
    name: 'NSQ Consumer',
    url: `${TELEGRAF_PLUGINS}/nsq_consumer`,
    markdown: nsq_consumerMarkdown,
    image: nsq_consumerLogo,
  },
  {
    id: 'nstat',
    name: 'Nstat',
    url: `${TELEGRAF_PLUGINS}/nstat`,
    markdown: nstatMarkdown,
    image: nstatLogo,
  },
  {
    id: 'ntpq',
    name: 'ntpq',
    url: `${TELEGRAF_PLUGINS}/ntpq`,
    markdown: ntpqMarkdown,
    image: ntpqLogo,
  },
  {
    id: 'nvidia_smi',
    name: 'Nvidia System Management Interface (SMI)',
    url: `${TELEGRAF_PLUGINS}/nvidia_smi`,
    markdown: nvidia_smiMarkdown,
    image: nvidia_smiLogo,
  },
  {
    id: 'openldap',
    name: 'OpenLDAP',
    url: `${TELEGRAF_PLUGINS}/openldap`,
    markdown: openldapMarkdown,
    image: openldapLogo,
  },
  {
    id: 'openntpd',
    name: 'OpenNTPD',
    url: `${TELEGRAF_PLUGINS}/openntpd`,
    markdown: openntpdMarkdown,
    image: openntpdLogo,
  },
  {
    id: 'opensmtpd',
    name: 'OpenSMTPD',
    url: `${TELEGRAF_PLUGINS}/opensmtpd`,
    markdown: opensmtpdMarkdown,
    image: opensmtpdLogo,
  },
  {
    id: 'openweathermap',
    name: 'OpenWeatherMap',
    url: `${TELEGRAF_PLUGINS}/openweathermap`,
    markdown: openweathermapMarkdown,
    image: openweathermapLogo,
  },
  {
    id: 'passenger',
    name: 'Passenger',
    url: `${TELEGRAF_PLUGINS}/passenger`,
    markdown: passengerMarkdown,
    image: passengerLogo,
  },
  {
    id: 'pf',
    name: 'PF',
    url: `${TELEGRAF_PLUGINS}/pf`,
    markdown: pfMarkdown,
    image: pfLogo,
  },
  {
    id: 'pgbouncer',
    name: 'PgBouncer',
    url: `${TELEGRAF_PLUGINS}/pgbouncer`,
    markdown: pgbouncerMarkdown,
    image: pgbouncerLogo,
  },
  {
    id: 'phpfpm',
    name: 'PHP-FPM',
    url: `${TELEGRAF_PLUGINS}/phpfpm`,
    markdown: phpfpmMarkdown,
    image: phpfpmLogo,
  },
  {
    id: 'ping',
    name: 'Ping',
    url: `${TELEGRAF_PLUGINS}/ping`,
    markdown: pingMarkdown,
    image: pingLogo,
  },
  {
    id: 'postfix',
    name: 'Postfix',
    url: `${TELEGRAF_PLUGINS}/postfix`,
    markdown: postfixMarkdown,
    image: postfixLogo,
  },
  {
    id: 'postgresql',
    name: 'PostgreSQL',
    url: `${TELEGRAF_PLUGINS}/postgresql`,
    markdown: postgresqlMarkdown,
    image: postgresqlLogo,
  },
  {
    id: 'postgresql_extensible',
    name: 'PostgreSQL Extensible',
    url: `${TELEGRAF_PLUGINS}/postgresql_extensible`,
    markdown: postgresql_extensibleMarkdown,
    image: postgresql_extensibleLogo,
  },
  {
    id: 'powerdns',
    name: 'PowerDNS',
    url: `${TELEGRAF_PLUGINS}/powerdns`,
    markdown: powerdnsMarkdown,
    image: powerdnsLogo,
  },
  {
    id: 'powerdns_recursor',
    name: 'PowerDNS Recursor',
    url: `${TELEGRAF_PLUGINS}/powerdns_recursor`,
    markdown: powerdns_recursorMarkdown,
    image: powerdns_recursorLogo,
  },
  {
    id: 'processes',
    name: 'Processes',
    url: `${TELEGRAF_PLUGINS}/processes`,
    markdown: processesMarkdown,
    image: processesLogo,
  },
  {
    id: 'procstat',
    name: 'Procstat',
    url: `${TELEGRAF_PLUGINS}/procstat`,
    markdown: procstatMarkdown,
    image: procstatLogo,
  },
  {
    id: 'prometheus',
    name: 'Prometheus',
    url: `${TELEGRAF_PLUGINS}/prometheus`,
    markdown: prometheusMarkdown,
    image: prometheusLogo,
  },
  {
    id: 'proxmox',
    name: 'Proxmox',
    url: `${TELEGRAF_PLUGINS}/proxmox`,
    markdown: proxmoxMarkdown,
    image: proxmoxLogo,
  },
  {
    id: 'puppetagent',
    name: 'PuppetAgent',
    url: `${TELEGRAF_PLUGINS}/puppetagent`,
    markdown: puppetagentMarkdown,
    image: puppetagentLogo,
  },
  {
    id: 'rabbitmq',
    name: 'RabbitMQ',
    url: `${TELEGRAF_PLUGINS}/rabbitmq`,
    markdown: rabbitmqMarkdown,
    image: rabbitmqLogo,
  },
  {
    id: 'raindrops',
    name: 'Raindrops',
    url: `${TELEGRAF_PLUGINS}/raindrops`,
    markdown: raindropsMarkdown,
    image: raindropsLogo,
  },
  {
    id: 'redfish',
    name: 'Redfish',
    url: `${TELEGRAF_PLUGINS}/redfish`,
    markdown: redfishMarkdown,
    image: redfishLogo,
  },
  {
    id: 'redis',
    name: 'Redis',
    url: `${TELEGRAF_PLUGINS}/redis`,
    markdown: redisMarkdown,
    image: redisLogo,
  },
  {
    id: 'rethinkdb',
    name: 'RethinkDB',
    url: `${TELEGRAF_PLUGINS}/rethinkdb`,
    markdown: rethinkdbMarkdown,
    image: rethinkdbLogo,
  },
  {
    id: 'riak',
    name: 'Riak',
    url: `${TELEGRAF_PLUGINS}/riak`,
    markdown: riakMarkdown,
    image: riakLogo,
  },
  {
    id: 'salesforce',
    name: 'Salesforce',
    url: `${TELEGRAF_PLUGINS}/salesforce`,
    markdown: salesforceMarkdown,
    image: salesforceLogo,
  },
  {
    id: 'sensors',
    name: 'LM Sensors',
    url: `${TELEGRAF_PLUGINS}/sensors`,
    markdown: sensorsMarkdown,
    image: sensorsLogo,
  },
  {
    id: 'sflow',
    name: 'SFlow',
    url: `${TELEGRAF_PLUGINS}/sflow`,
    markdown: sflowMarkdown,
    image: sflowLogo,
  },
  {
    id: 'smart',
    name: 'S.M.A.R.T.',
    url: `${TELEGRAF_PLUGINS}/smart`,
    markdown: smartMarkdown,
    image: smartLogo,
  },
  {
    id: 'snmp',
    name: 'SNMP',
    url: `${TELEGRAF_PLUGINS}/snmp`,
    markdown: snmpMarkdown,
    image: snmpLogo,
  },
  {
    id: 'snmp_legacy',
    name: 'SNMP Legacy',
    url: `${TELEGRAF_PLUGINS}/snmp_legacy`,
    markdown: snmp_legacyMarkdown,
    image: snmp_legacyLogo,
  },
  {
    id: 'snmp_trap',
    name: 'SNMP Trap',
    url: `${TELEGRAF_PLUGINS}/snmp_trap`,
    markdown: snmp_trapMarkdown,
    image: snmp_trapLogo,
  },
  {
    id: 'socket_listener',
    name: 'Socket Listener',
    url: `${TELEGRAF_PLUGINS}/socket_listener`,
    markdown: socket_listenerMarkdown,
    image: socket_listenerLogo,
  },
  {
    id: 'solr',
    name: 'Solr',
    url: `${TELEGRAF_PLUGINS}/solr`,
    markdown: solrMarkdown,
    image: solrLogo,
  },
  {
    id: 'sqlserver',
    name: 'SQL Server',
    url: `${TELEGRAF_PLUGINS}/sqlserver`,
    markdown: sqlserverMarkdown,
    image: sqlserverLogo,
  },
  {
    id: 'stackdriver',
    name: 'Stackdriver Google Cloud Monitoring',
    url: `${TELEGRAF_PLUGINS}/stackdriver`,
    markdown: stackdriverMarkdown,
    image: stackdriverLogo,
  },
  {
    id: 'statsd',
    name: 'StatsD',
    url: `${TELEGRAF_PLUGINS}/statsd`,
    markdown: statsdMarkdown,
    image: statsdLogo,
  },
  {
    id: 'suricata',
    name: 'Suricata',
    url: `${TELEGRAF_PLUGINS}/suricata`,
    markdown: suricataMarkdown,
    image: suricataLogo,
  },
  {
    id: 'swap',
    name: 'Swap',
    url: `${TELEGRAF_PLUGINS}/swap`,
    markdown: swapMarkdown,
    image: swapLogo,
  },
  {
    id: 'synproxy',
    name: 'Synproxy',
    url: `${TELEGRAF_PLUGINS}/synproxy`,
    markdown: synproxyMarkdown,
    image: synproxyLogo,
  },
  {
    id: 'syslog',
    name: 'Syslog',
    url: `${TELEGRAF_PLUGINS}/syslog`,
    markdown: syslogMarkdown,
    image: syslogLogo,
  },
  {
    id: 'sysstat',
    name: 'sysstat',
    url: `${TELEGRAF_PLUGINS}/sysstat`,
    markdown: sysstatMarkdown,
    image: sysstatLogo,
  },
  {
    id: 'system',
    name: 'System',
    url: `${TELEGRAF_PLUGINS}/system`,
    markdown: systemMarkdown,
    image: systemLogo,
  },
  {
    id: 'systemd_units',
    name: 'systemd Units',
    url: `${TELEGRAF_PLUGINS}/systemd_units`,
    markdown: systemd_unitsMarkdown,
    image: systemd_unitsLogo,
  },
  {
    id: 'tail',
    name: 'Tail',
    url: `${TELEGRAF_PLUGINS}/tail`,
    markdown: tailMarkdown,
    image: tailLogo,
  },
  {
    id: 'tcp_listener',
    name: 'TCP Listener',
    url: `${TELEGRAF_PLUGINS}/tcp_listener`,
    markdown: tcp_listenerMarkdown,
    image: tcp_listenerLogo,
  },
  {
    id: 'teamspeak',
    name: 'Teamspeak 3',
    url: `${TELEGRAF_PLUGINS}/teamspeak`,
    markdown: teamspeakMarkdown,
    image: teamspeakLogo,
  },
  {
    id: 'temp',
    name: 'Temperature',
    url: `${TELEGRAF_PLUGINS}/temp`,
    markdown: tempMarkdown,
    image: tempLogo,
  },
  {
    id: 'tengine',
    name: 'Tengine',
    url: `${TELEGRAF_PLUGINS}/tengine`,
    markdown: tengineMarkdown,
    image: tengineLogo,
  },
  {
    id: 'tomcat',
    name: 'Tomcat',
    url: `${TELEGRAF_PLUGINS}/tomcat`,
    markdown: tomcatMarkdown,
    image: tomcatLogo,
  },
  {
    id: 'udp_listener',
    name: 'UDP Listener',
    url: `${TELEGRAF_PLUGINS}/udp_listener`,
    markdown: udp_listenerMarkdown,
    image: udp_listenerLogo,
  },
  {
    id: 'unbound',
    name: 'Unbound',
    url: `${TELEGRAF_PLUGINS}/unbound`,
    markdown: unboundMarkdown,
    image: unboundLogo,
  },
  {
    id: 'uwsgi',
    name: 'uWSGI',
    url: `${TELEGRAF_PLUGINS}/uwsgi`,
    markdown: uwsgiMarkdown,
    image: uwsgiLogo,
  },
  {
    id: 'varnish',
    name: 'Varnish',
    url: `${TELEGRAF_PLUGINS}/varnish`,
    markdown: varnishMarkdown,
    image: varnishLogo,
  },
  {
    id: 'vsphere',
    name: 'VMware vSphere',
    url: `${TELEGRAF_PLUGINS}/vsphere`,
    markdown: vsphereMarkdown,
    image: vsphereLogo,
  },
  {
    id: 'webhooks',
    name: 'Webhooks',
    url: `${TELEGRAF_PLUGINS}/webhooks`,
    markdown: webhooksMarkdown,
    image: webhooksLogo,
  },
  {
    id: 'win_perf_counters',
    name: 'Windows Performance Counters',
    url: `${TELEGRAF_PLUGINS}/win_perf_counters`,
    markdown: win_perf_countersMarkdown,
    image: win_perf_countersLogo,
  },
  {
    id: 'win_services',
    name: 'Windows Services',
    url: `${TELEGRAF_PLUGINS}/win_services`,
    markdown: win_servicesMarkdown,
    image: win_servicesLogo,
  },
  {
    id: 'wireguard',
    name: 'Wireguard',
    url: `${TELEGRAF_PLUGINS}/wireguard`,
    markdown: wireguardMarkdown,
    image: wireguardLogo,
  },
  {
    id: 'wireless',
    name: 'Wireless',
    url: `${TELEGRAF_PLUGINS}/wireless`,
    markdown: wirelessMarkdown,
    image: wirelessLogo,
  },
  {
    id: 'x509_cert',
    name: 'x509 Certificate',
    url: `${TELEGRAF_PLUGINS}/x509_cert`,
    markdown: x509_certMarkdown,
    image: x509_certLogo,
  },
  {
    id: 'zfs',
    name: 'ZFS',
    url: `${TELEGRAF_PLUGINS}/zfs`,
    markdown: zfsMarkdown,
    image: zfsLogo,
  },
  {
    id: 'zipkin',
    name: 'Zipkin',
    url: `${TELEGRAF_PLUGINS}/zipkin`,
    markdown: zipkinMarkdown,
    image: zipkinLogo,
  },
  {
    id: 'zookeeper',
    name: 'Zookeeper',
    url: `${TELEGRAF_PLUGINS}/zookeeper`,
    markdown: zookeeperMarkdown,
    image: zookeeperLogo,
  },
]

const WRITE_DATA_TELEGRAF_PLUGINS_SECTION: WriteDataSection = {
  id: TELEGRAF_PLUGINS,
  name: 'Telegraf Plugins',
  description:
    'An open-source agent for collecting data and reporting metrics via a vast library of plugins',
  items: WRITE_DATA_TELEGRAF_PLUGINS,
  featureFlag: 'write-data-telegraf-plugins',
}

export default WRITE_DATA_TELEGRAF_PLUGINS_SECTION
