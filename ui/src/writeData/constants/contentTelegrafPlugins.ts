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

// Graphics
import stackdriverLogo from 'src/writeData/graphics/stackdriver.svg'
import ntpqLogo from 'src/writeData/graphics/ntpq.svg'
import activemqLogo from 'src/writeData/graphics/activemq.svg'
import infinibandLogo from 'src/writeData/graphics/infiniband.svg'
import clickhouseLogo from 'src/writeData/graphics/clickhouse.svg'
import jolokia2Logo from 'src/writeData/graphics/jolokia2.svg'
import postgresqlLogo from 'src/writeData/graphics/postgresql.svg'
import dmcacheLogo from 'src/writeData/graphics/dmcache.svg'
import sensorsLogo from 'src/writeData/graphics/sensors.svg'
import mqtt_consumerLogo from 'src/writeData/graphics/mqtt_consumer.svg'
import cloudwatchLogo from 'src/writeData/graphics/cloudwatch.svg'
import tempLogo from 'src/writeData/graphics/temp.svg'
import pingLogo from 'src/writeData/graphics/ping.svg'
import ipsetLogo from 'src/writeData/graphics/ipset.svg'
import cloud_pubsubLogo from 'src/writeData/graphics/cloud_pubsub.svg'
import docker_logLogo from 'src/writeData/graphics/docker_log.svg'
import diskioLogo from 'src/writeData/graphics/diskio.svg'
import graylogLogo from 'src/writeData/graphics/graylog.svg'
import dockerLogo from 'src/writeData/graphics/docker.svg'
import bcacheLogo from 'src/writeData/graphics/bcache.svg'
import syslogLogo from 'src/writeData/graphics/syslog.svg'
//import netLogo from 'src/writeData/graphics/net.svg'
//import netLogo from 'src/writeData/graphics/net.svg'
import logparserLogo from 'src/writeData/graphics/logparser.svg'
import fluentdLogo from 'src/writeData/graphics/fluentd.svg'
import couchdbLogo from 'src/writeData/graphics/couchdb.svg'
import udp_listenerLogo from 'src/writeData/graphics/udp_listener.svg'
import snmpLogo from 'src/writeData/graphics/snmp.svg'
import kernel_vmstatLogo from 'src/writeData/graphics/kernel_vmstat.svg'
import vsphereLogo from 'src/writeData/graphics/vsphere.svg'
import teamspeakLogo from 'src/writeData/graphics/teamspeak.svg'
import dovecotLogo from 'src/writeData/graphics/dovecot.svg'
import tomcatLogo from 'src/writeData/graphics/tomcat.svg'
import kibanaLogo from 'src/writeData/graphics/kibana.svg'
import nginx_vtsLogo from 'src/writeData/graphics/nginx_vts.svg'
import gnmiLogo from 'src/writeData/graphics/gnmi.svg'
import socket_listenerLogo from 'src/writeData/graphics/socket_listener.svg'
import fileLogo from 'src/writeData/graphics/file.svg'
import cpuLogo from 'src/writeData/graphics/cpu.svg'
import azure_storage_queueLogo from 'src/writeData/graphics/azure_storage_queue.svg'
import logstashLogo from 'src/writeData/graphics/logstash.svg'
import redisLogo from 'src/writeData/graphics/redis.svg'
import http_responseLogo from 'src/writeData/graphics/http_response.svg'
import cephLogo from 'src/writeData/graphics/ceph.svg'
import snmp_legacyLogo from 'src/writeData/graphics/snmp_legacy.svg'
import redfishLogo from 'src/writeData/graphics/redfish.svg'
import amqp_consumerLogo from 'src/writeData/graphics/amqp_consumer.svg'
import sflowLogo from 'src/writeData/graphics/sflow.svg'
import execdLogo from 'src/writeData/graphics/execd.svg'
import shimLogo from 'src/writeData/graphics/shim.svg'
import conntrackLogo from 'src/writeData/graphics/conntrack.svg'
import kapacitorLogo from 'src/writeData/graphics/kapacitor.svg'
import bondLogo from 'src/writeData/graphics/bond.svg'
import processesLogo from 'src/writeData/graphics/processes.svg'
import snmp_trapLogo from 'src/writeData/graphics/snmp_trap.svg'
import fail2banLogo from 'src/writeData/graphics/fail2ban.svg'
import multifileLogo from 'src/writeData/graphics/multifile.svg'
import smartLogo from 'src/writeData/graphics/smart.svg'
import synproxyLogo from 'src/writeData/graphics/synproxy.svg'
import swapLogo from 'src/writeData/graphics/swap.svg'
import fibaroLogo from 'src/writeData/graphics/fibaro.svg'
import suricataLogo from 'src/writeData/graphics/suricata.svg'
import burrowLogo from 'src/writeData/graphics/burrow.svg'
import powerdnsLogo from 'src/writeData/graphics/powerdns.svg'
import lustre2Logo from 'src/writeData/graphics/lustre2.svg'
import kafka_consumerLogo from 'src/writeData/graphics/kafka_consumer.svg'
import nsq_consumerLogo from 'src/writeData/graphics/nsq_consumer.svg'
import marklogicLogo from 'src/writeData/graphics/marklogic.svg'
import internalLogo from 'src/writeData/graphics/internal.svg'
import cloud_pubsub_pushLogo from 'src/writeData/graphics/cloud_pubsub_push.svg'
import jti_openconfig_telemetryLogo from 'src/writeData/graphics/jti_openconfig_telemetry.svg'
import procstatLogo from 'src/writeData/graphics/procstat.svg'
import neptune_apexLogo from 'src/writeData/graphics/neptune_apex.svg'
import nginx_upstream_checkLogo from 'src/writeData/graphics/nginx_upstream_check.svg'
import x509_certLogo from 'src/writeData/graphics/x509_cert.svg'
import mailchimpLogo from 'src/writeData/graphics/mailchimp.svg'
import auroraLogo from 'src/writeData/graphics/aurora.svg'
import rabbitmqLogo from 'src/writeData/graphics/rabbitmq.svg'
import influxdb_listenerLogo from 'src/writeData/graphics/influxdb_listener.svg'
import ecsLogo from 'src/writeData/graphics/ecs.svg'
import statsdLogo from 'src/writeData/graphics/statsd.svg'
import cgroupLogo from 'src/writeData/graphics/cgroup.svg'
import disqueLogo from 'src/writeData/graphics/disque.svg'
import rethinkdbLogo from 'src/writeData/graphics/rethinkdb.svg'
import dcosLogo from 'src/writeData/graphics/dcos.svg'
import jolokiaLogo from 'src/writeData/graphics/jolokia.svg'
import varnishLogo from 'src/writeData/graphics/varnish.svg'
import tailLogo from 'src/writeData/graphics/tail.svg'
import mongodbLogo from 'src/writeData/graphics/mongodb.svg'
import zookeeperLogo from 'src/writeData/graphics/zookeeper.svg'
import solrLogo from 'src/writeData/graphics/solr.svg'
import systemd_unitsLogo from 'src/writeData/graphics/systemd_units.svg'
import influxdbLogo from 'src/writeData/graphics/influxdb.svg'
import memcachedLogo from 'src/writeData/graphics/memcached.svg'
import filestatLogo from 'src/writeData/graphics/filestat.svg'
import net_responseLogo from 'src/writeData/graphics/net_response.svg'
import cisco_telemetry_mdtLogo from 'src/writeData/graphics/cisco_telemetry_mdt.svg'
import systemLogo from 'src/writeData/graphics/system.svg'
import zfsLogo from 'src/writeData/graphics/zfs.svg'
import opensmtpdLogo from 'src/writeData/graphics/opensmtpd.svg'
import natsLogo from 'src/writeData/graphics/nats.svg'
import mcrouterLogo from 'src/writeData/graphics/mcrouter.svg'
import lanzLogo from 'src/writeData/graphics/lanz.svg'
import mesosLogo from 'src/writeData/graphics/mesos.svg'
import githubLogo from 'src/writeData/graphics/github.svg'
import win_perf_countersLogo from 'src/writeData/graphics/win_perf_counters.svg'
import win_servicesLogo from 'src/writeData/graphics/win_services.svg'
import zipkinLogo from 'src/writeData/graphics/zipkin.svg'
import consulLogo from 'src/writeData/graphics/consul.svg'
import kube_inventoryLogo from 'src/writeData/graphics/kube_inventory.svg'
import pfLogo from 'src/writeData/graphics/pf.svg'
import nginxLogo from 'src/writeData/graphics/nginx.svg'
import openntpdLogo from 'src/writeData/graphics/openntpd.svg'
import httpLogo from 'src/writeData/graphics/http.svg'
import aerospikeLogo from 'src/writeData/graphics/aerospike.svg'
import cassandraLogo from 'src/writeData/graphics/cassandra.svg'
import wirelessLogo from 'src/writeData/graphics/wireless.svg'
import interruptsLogo from 'src/writeData/graphics/interrupts.svg'
import nginx_plusLogo from 'src/writeData/graphics/nginx_plus.svg'
import mysqlLogo from 'src/writeData/graphics/mysql.svg'
import apcupsdLogo from 'src/writeData/graphics/apcupsd.svg'
import sqlserverLogo from 'src/writeData/graphics/sqlserver.svg'
import httpjsonLogo from 'src/writeData/graphics/httpjson.svg'
import postgresql_extensibleLogo from 'src/writeData/graphics/postgresql_extensible.svg'
import chronyLogo from 'src/writeData/graphics/chrony.svg'
import execLogo from 'src/writeData/graphics/exec.svg'
import linux_sysctl_fsLogo from 'src/writeData/graphics/linux_sysctl_fs.svg'
import monitLogo from 'src/writeData/graphics/monit.svg'
import leofsLogo from 'src/writeData/graphics/leofs.svg'
import eventhub_consumerLogo from 'src/writeData/graphics/eventhub_consumer.svg'
import raindropsLogo from 'src/writeData/graphics/raindrops.svg'
import nvidia_smiLogo from 'src/writeData/graphics/nvidia_smi.svg'
import beanstalkdLogo from 'src/writeData/graphics/beanstalkd.svg'
import diskLogo from 'src/writeData/graphics/disk.svg'
import salesforceLogo from 'src/writeData/graphics/salesforce.svg'
import ipmi_sensorLogo from 'src/writeData/graphics/ipmi_sensor.svg'
import tengineLogo from 'src/writeData/graphics/tengine.svg'
import prometheusLogo from 'src/writeData/graphics/prometheus.svg'
import ethtoolLogo from 'src/writeData/graphics/ethtool.svg'
import phpfpmLogo from 'src/writeData/graphics/phpfpm.svg'
import nginx_plus_apiLogo from 'src/writeData/graphics/nginx_plus_api.svg'
import postfixLogo from 'src/writeData/graphics/postfix.svg'
import uwsgiLogo from 'src/writeData/graphics/uwsgi.svg'
import jenkinsLogo from 'src/writeData/graphics/jenkins.svg'
import kafka_consumer_legacyLogo from 'src/writeData/graphics/kafka_consumer_legacy.svg'
import sysstatLogo from 'src/writeData/graphics/sysstat.svg'
import iptablesLogo from 'src/writeData/graphics/iptables.svg'
import hddtempLogo from 'src/writeData/graphics/hddtemp.svg'
import unboundLogo from 'src/writeData/graphics/unbound.svg'
import pgbouncerLogo from 'src/writeData/graphics/pgbouncer.svg'
import kinesis_consumerLogo from 'src/writeData/graphics/kinesis_consumer.svg'
import kubernetesLogo from 'src/writeData/graphics/kubernetes.svg'
import icinga2Logo from 'src/writeData/graphics/icinga2.svg'
import openldapLogo from 'src/writeData/graphics/openldap.svg'
import passengerLogo from 'src/writeData/graphics/passenger.svg'
import mandrillLogo from 'src/writeData/graphics/mandrill.svg'
import filestackLogo from 'src/writeData/graphics/filestack.svg'
import rollbarLogo from 'src/writeData/graphics/rollbar.svg'
import webhooksLogo from 'src/writeData/graphics/webhooks.svg'
//import githubLogo from 'src/writeData/graphics/github.svg'
import papertrailLogo from 'src/writeData/graphics/papertrail.svg'
import particleLogo from 'src/writeData/graphics/particle.svg'
import wireguardLogo from 'src/writeData/graphics/wireguard.svg'
import riakLogo from 'src/writeData/graphics/riak.svg'
import bindLogo from 'src/writeData/graphics/bind.svg'
import modbusLogo from 'src/writeData/graphics/modbus.svg'
import minecraftLogo from 'src/writeData/graphics/minecraft.svg'
import tcp_listenerLogo from 'src/writeData/graphics/tcp_listener.svg'
import ipvsLogo from 'src/writeData/graphics/ipvs.svg'
import openweathermapLogo from 'src/writeData/graphics/openweathermap.svg'
import powerdns_recursorLogo from 'src/writeData/graphics/powerdns_recursor.svg'
import puppetagentLogo from 'src/writeData/graphics/puppetagent.svg'
import dns_queryLogo from 'src/writeData/graphics/dns_query.svg'
import elasticsearchLogo from 'src/writeData/graphics/elasticsearch.svg'
import apacheLogo from 'src/writeData/graphics/apache.svg'
import nginx_stsLogo from 'src/writeData/graphics/nginx_sts.svg'
import nstatLogo from 'src/writeData/graphics/nstat.svg'
import http_listener_v2Logo from 'src/writeData/graphics/http_listener_v2.svg'
import kernelLogo from 'src/writeData/graphics/kernel.svg'
import filecountLogo from 'src/writeData/graphics/filecount.svg'
import nsqLogo from 'src/writeData/graphics/nsq.svg'
import memLogo from 'src/writeData/graphics/mem.svg'
import nats_consumerLogo from 'src/writeData/graphics/nats_consumer.svg'
import haproxyLogo from 'src/writeData/graphics/haproxy.svg'
import couchbaseLogo from 'src/writeData/graphics/couchbase.svg'
import fireboardLogo from 'src/writeData/graphics/fireboard.svg'

export const WRITE_DATA_TELEGRAF_PLUGINS: WriteDataItem[] = [
  {
    id: 'stackdriver',
    name: 'stackdriver',
    url: `${TELEGRAF_PLUGINS}/stackdriver`,
    markdown: stackdriverMarkdown,
    image: stackdriverLogo,
  },
  {
    id: 'ntpq',
    name: 'ntpq',
    url: `${TELEGRAF_PLUGINS}/ntpq`,
    markdown: ntpqMarkdown,
    image: ntpqLogo,
  },
  {
    id: 'activemq',
    name: 'activemq',
    url: `${TELEGRAF_PLUGINS}/activemq`,
    markdown: activemqMarkdown,
    image: activemqLogo,
  },
  {
    id: 'infiniband',
    name: 'infiniband',
    url: `${TELEGRAF_PLUGINS}/infiniband`,
    markdown: infinibandMarkdown,
    image: infinibandLogo,
  },
  {
    id: 'clickhouse',
    name: 'clickhouse',
    url: `${TELEGRAF_PLUGINS}/clickhouse`,
    markdown: clickhouseMarkdown,
    image: clickhouseLogo,
  },
  {
    id: 'jolokia2',
    name: 'jolokia2',
    url: `${TELEGRAF_PLUGINS}/jolokia2`,
    markdown: jolokia2Markdown,
    image: jolokia2Logo,
  },
  {
    id: 'postgresql',
    name: 'postgresql',
    url: `${TELEGRAF_PLUGINS}/postgresql`,
    markdown: postgresqlMarkdown,
    image: postgresqlLogo,
  },
  {
    id: 'dmcache',
    name: 'dmcache',
    url: `${TELEGRAF_PLUGINS}/dmcache`,
    markdown: dmcacheMarkdown,
    image: dmcacheLogo,
  },
  {
    id: 'sensors',
    name: 'sensors',
    url: `${TELEGRAF_PLUGINS}/sensors`,
    markdown: sensorsMarkdown,
    image: sensorsLogo,
  },
  {
    id: 'mqtt_consumer',
    name: 'mqtt_consumer',
    url: `${TELEGRAF_PLUGINS}/mqtt_consumer`,
    markdown: mqtt_consumerMarkdown,
    image: mqtt_consumerLogo,
  },
  {
    id: 'cloudwatch',
    name: 'cloudwatch',
    url: `${TELEGRAF_PLUGINS}/cloudwatch`,
    markdown: cloudwatchMarkdown,
    image: cloudwatchLogo,
  },
  {
    id: 'temp',
    name: 'temp',
    url: `${TELEGRAF_PLUGINS}/temp`,
    markdown: tempMarkdown,
    image: tempLogo,
  },
  {
    id: 'ping',
    name: 'ping',
    url: `${TELEGRAF_PLUGINS}/ping`,
    markdown: pingMarkdown,
    image: pingLogo,
  },
  {
    id: 'ipset',
    name: 'ipset',
    url: `${TELEGRAF_PLUGINS}/ipset`,
    markdown: ipsetMarkdown,
    image: ipsetLogo,
  },
  {
    id: 'cloud_pubsub',
    name: 'cloud_pubsub',
    url: `${TELEGRAF_PLUGINS}/cloud_pubsub`,
    markdown: cloud_pubsubMarkdown,
    image: cloud_pubsubLogo,
  },
  {
    id: 'docker_log',
    name: 'docker_log',
    url: `${TELEGRAF_PLUGINS}/docker_log`,
    markdown: docker_logMarkdown,
    image: docker_logLogo,
  },
  {
    id: 'diskio',
    name: 'diskio',
    url: `${TELEGRAF_PLUGINS}/diskio`,
    markdown: diskioMarkdown,
    image: diskioLogo,
  },
  {
    id: 'graylog',
    name: 'graylog',
    url: `${TELEGRAF_PLUGINS}/graylog`,
    markdown: graylogMarkdown,
    image: graylogLogo,
  },
  {
    id: 'docker',
    name: 'docker',
    url: `${TELEGRAF_PLUGINS}/docker`,
    markdown: dockerMarkdown,
    image: dockerLogo,
  },
  {
    id: 'bcache',
    name: 'bcache',
    url: `${TELEGRAF_PLUGINS}/bcache`,
    markdown: bcacheMarkdown,
    image: bcacheLogo,
  },
  {
    id: 'syslog',
    name: 'syslog',
    url: `${TELEGRAF_PLUGINS}/syslog`,
    markdown: syslogMarkdown,
    image: syslogLogo,
  },
  //   {
  //     id: 'net',
  //     name: 'net',
  //     url: `${TELEGRAF_PLUGINS}/net`,
  //     markdown: netMarkdown,
  //     image: netLogo,
  //   },
  //   {
  //     id: 'net',
  //     name: 'net',
  //     url: `${TELEGRAF_PLUGINS}/net`,
  //     markdown: netMarkdown,
  //     image: netLogo,
  //   },
  {
    id: 'logparser',
    name: 'logparser',
    url: `${TELEGRAF_PLUGINS}/logparser`,
    markdown: logparserMarkdown,
    image: logparserLogo,
  },
  {
    id: 'fluentd',
    name: 'fluentd',
    url: `${TELEGRAF_PLUGINS}/fluentd`,
    markdown: fluentdMarkdown,
    image: fluentdLogo,
  },
  {
    id: 'couchdb',
    name: 'couchdb',
    url: `${TELEGRAF_PLUGINS}/couchdb`,
    markdown: couchdbMarkdown,
    image: couchdbLogo,
  },
  {
    id: 'udp_listener',
    name: 'udp_listener',
    url: `${TELEGRAF_PLUGINS}/udp_listener`,
    markdown: udp_listenerMarkdown,
    image: udp_listenerLogo,
  },
  {
    id: 'snmp',
    name: 'snmp',
    url: `${TELEGRAF_PLUGINS}/snmp`,
    markdown: snmpMarkdown,
    image: snmpLogo,
  },
  {
    id: 'kernel_vmstat',
    name: 'kernel_vmstat',
    url: `${TELEGRAF_PLUGINS}/kernel_vmstat`,
    markdown: kernel_vmstatMarkdown,
    image: kernel_vmstatLogo,
  },
  {
    id: 'vsphere',
    name: 'vsphere',
    url: `${TELEGRAF_PLUGINS}/vsphere`,
    markdown: vsphereMarkdown,
    image: vsphereLogo,
  },
  {
    id: 'teamspeak',
    name: 'teamspeak',
    url: `${TELEGRAF_PLUGINS}/teamspeak`,
    markdown: teamspeakMarkdown,
    image: teamspeakLogo,
  },
  {
    id: 'dovecot',
    name: 'dovecot',
    url: `${TELEGRAF_PLUGINS}/dovecot`,
    markdown: dovecotMarkdown,
    image: dovecotLogo,
  },
  {
    id: 'tomcat',
    name: 'tomcat',
    url: `${TELEGRAF_PLUGINS}/tomcat`,
    markdown: tomcatMarkdown,
    image: tomcatLogo,
  },
  {
    id: 'kibana',
    name: 'kibana',
    url: `${TELEGRAF_PLUGINS}/kibana`,
    markdown: kibanaMarkdown,
    image: kibanaLogo,
  },
  {
    id: 'nginx_vts',
    name: 'nginx_vts',
    url: `${TELEGRAF_PLUGINS}/nginx_vts`,
    markdown: nginx_vtsMarkdown,
    image: nginx_vtsLogo,
  },
  {
    id: 'gnmi',
    name: 'gnmi',
    url: `${TELEGRAF_PLUGINS}/gnmi`,
    markdown: gnmiMarkdown,
    image: gnmiLogo,
  },
  {
    id: 'socket_listener',
    name: 'socket_listener',
    url: `${TELEGRAF_PLUGINS}/socket_listener`,
    markdown: socket_listenerMarkdown,
    image: socket_listenerLogo,
  },
  {
    id: 'file',
    name: 'file',
    url: `${TELEGRAF_PLUGINS}/file`,
    markdown: fileMarkdown,
    image: fileLogo,
  },
  {
    id: 'cpu',
    name: 'cpu',
    url: `${TELEGRAF_PLUGINS}/cpu`,
    markdown: cpuMarkdown,
    image: cpuLogo,
  },
  {
    id: 'azure_storage_queue',
    name: 'azure_storage_queue',
    url: `${TELEGRAF_PLUGINS}/azure_storage_queue`,
    markdown: azure_storage_queueMarkdown,
    image: azure_storage_queueLogo,
  },
  {
    id: 'logstash',
    name: 'logstash',
    url: `${TELEGRAF_PLUGINS}/logstash`,
    markdown: logstashMarkdown,
    image: logstashLogo,
  },
  {
    id: 'redis',
    name: 'redis',
    url: `${TELEGRAF_PLUGINS}/redis`,
    markdown: redisMarkdown,
    image: redisLogo,
  },
  {
    id: 'http_response',
    name: 'http_response',
    url: `${TELEGRAF_PLUGINS}/http_response`,
    markdown: http_responseMarkdown,
    image: http_responseLogo,
  },
  {
    id: 'ceph',
    name: 'ceph',
    url: `${TELEGRAF_PLUGINS}/ceph`,
    markdown: cephMarkdown,
    image: cephLogo,
  },
  {
    id: 'snmp_legacy',
    name: 'snmp_legacy',
    url: `${TELEGRAF_PLUGINS}/snmp_legacy`,
    markdown: snmp_legacyMarkdown,
    image: snmp_legacyLogo,
  },
  {
    id: 'redfish',
    name: 'redfish',
    url: `${TELEGRAF_PLUGINS}/redfish`,
    markdown: redfishMarkdown,
    image: redfishLogo,
  },
  {
    id: 'amqp_consumer',
    name: 'amqp_consumer',
    url: `${TELEGRAF_PLUGINS}/amqp_consumer`,
    markdown: amqp_consumerMarkdown,
    image: amqp_consumerLogo,
  },
  {
    id: 'sflow',
    name: 'sflow',
    url: `${TELEGRAF_PLUGINS}/sflow`,
    markdown: sflowMarkdown,
    image: sflowLogo,
  },
  {
    id: 'execd',
    name: 'execd',
    url: `${TELEGRAF_PLUGINS}/execd`,
    markdown: execdMarkdown,
    image: execdLogo,
  },
  {
    id: 'shim',
    name: 'shim',
    url: `${TELEGRAF_PLUGINS}/shim`,
    markdown: shimMarkdown,
    image: shimLogo,
  },
  {
    id: 'conntrack',
    name: 'conntrack',
    url: `${TELEGRAF_PLUGINS}/conntrack`,
    markdown: conntrackMarkdown,
    image: conntrackLogo,
  },
  {
    id: 'kapacitor',
    name: 'kapacitor',
    url: `${TELEGRAF_PLUGINS}/kapacitor`,
    markdown: kapacitorMarkdown,
    image: kapacitorLogo,
  },
  {
    id: 'bond',
    name: 'bond',
    url: `${TELEGRAF_PLUGINS}/bond`,
    markdown: bondMarkdown,
    image: bondLogo,
  },
  {
    id: 'processes',
    name: 'processes',
    url: `${TELEGRAF_PLUGINS}/processes`,
    markdown: processesMarkdown,
    image: processesLogo,
  },
  {
    id: 'snmp_trap',
    name: 'snmp_trap',
    url: `${TELEGRAF_PLUGINS}/snmp_trap`,
    markdown: snmp_trapMarkdown,
    image: snmp_trapLogo,
  },
  {
    id: 'fail2ban',
    name: 'fail2ban',
    url: `${TELEGRAF_PLUGINS}/fail2ban`,
    markdown: fail2banMarkdown,
    image: fail2banLogo,
  },
  {
    id: 'multifile',
    name: 'multifile',
    url: `${TELEGRAF_PLUGINS}/multifile`,
    markdown: multifileMarkdown,
    image: multifileLogo,
  },
  {
    id: 'smart',
    name: 'smart',
    url: `${TELEGRAF_PLUGINS}/smart`,
    markdown: smartMarkdown,
    image: smartLogo,
  },
  {
    id: 'synproxy',
    name: 'synproxy',
    url: `${TELEGRAF_PLUGINS}/synproxy`,
    markdown: synproxyMarkdown,
    image: synproxyLogo,
  },
  {
    id: 'swap',
    name: 'swap',
    url: `${TELEGRAF_PLUGINS}/swap`,
    markdown: swapMarkdown,
    image: swapLogo,
  },
  {
    id: 'fibaro',
    name: 'fibaro',
    url: `${TELEGRAF_PLUGINS}/fibaro`,
    markdown: fibaroMarkdown,
    image: fibaroLogo,
  },
  {
    id: 'suricata',
    name: 'suricata',
    url: `${TELEGRAF_PLUGINS}/suricata`,
    markdown: suricataMarkdown,
    image: suricataLogo,
  },
  {
    id: 'burrow',
    name: 'burrow',
    url: `${TELEGRAF_PLUGINS}/burrow`,
    markdown: burrowMarkdown,
    image: burrowLogo,
  },
  {
    id: 'powerdns',
    name: 'powerdns',
    url: `${TELEGRAF_PLUGINS}/powerdns`,
    markdown: powerdnsMarkdown,
    image: powerdnsLogo,
  },
  {
    id: 'lustre2',
    name: 'lustre2',
    url: `${TELEGRAF_PLUGINS}/lustre2`,
    markdown: lustre2Markdown,
    image: lustre2Logo,
  },
  {
    id: 'kafka_consumer',
    name: 'kafka_consumer',
    url: `${TELEGRAF_PLUGINS}/kafka_consumer`,
    markdown: kafka_consumerMarkdown,
    image: kafka_consumerLogo,
  },
  {
    id: 'nsq_consumer',
    name: 'nsq_consumer',
    url: `${TELEGRAF_PLUGINS}/nsq_consumer`,
    markdown: nsq_consumerMarkdown,
    image: nsq_consumerLogo,
  },
  {
    id: 'marklogic',
    name: 'marklogic',
    url: `${TELEGRAF_PLUGINS}/marklogic`,
    markdown: marklogicMarkdown,
    image: marklogicLogo,
  },
  {
    id: 'internal',
    name: 'internal',
    url: `${TELEGRAF_PLUGINS}/internal`,
    markdown: internalMarkdown,
    image: internalLogo,
  },
  {
    id: 'cloud_pubsub_push',
    name: 'cloud_pubsub_push',
    url: `${TELEGRAF_PLUGINS}/cloud_pubsub_push`,
    markdown: cloud_pubsub_pushMarkdown,
    image: cloud_pubsub_pushLogo,
  },
  {
    id: 'jti_openconfig_telemetry',
    name: 'jti_openconfig_telemetry',
    url: `${TELEGRAF_PLUGINS}/jti_openconfig_telemetry`,
    markdown: jti_openconfig_telemetryMarkdown,
    image: jti_openconfig_telemetryLogo,
  },
  {
    id: 'procstat',
    name: 'procstat',
    url: `${TELEGRAF_PLUGINS}/procstat`,
    markdown: procstatMarkdown,
    image: procstatLogo,
  },
  {
    id: 'neptune_apex',
    name: 'neptune_apex',
    url: `${TELEGRAF_PLUGINS}/neptune_apex`,
    markdown: neptune_apexMarkdown,
    image: neptune_apexLogo,
  },
  {
    id: 'nginx_upstream_check',
    name: 'nginx_upstream_check',
    url: `${TELEGRAF_PLUGINS}/nginx_upstream_check`,
    markdown: nginx_upstream_checkMarkdown,
    image: nginx_upstream_checkLogo,
  },
  {
    id: 'x509_cert',
    name: 'x509_cert',
    url: `${TELEGRAF_PLUGINS}/x509_cert`,
    markdown: x509_certMarkdown,
    image: x509_certLogo,
  },
  {
    id: 'mailchimp',
    name: 'mailchimp',
    url: `${TELEGRAF_PLUGINS}/mailchimp`,
    markdown: mailchimpMarkdown,
    image: mailchimpLogo,
  },
  {
    id: 'aurora',
    name: 'aurora',
    url: `${TELEGRAF_PLUGINS}/aurora`,
    markdown: auroraMarkdown,
    image: auroraLogo,
  },
  {
    id: 'rabbitmq',
    name: 'rabbitmq',
    url: `${TELEGRAF_PLUGINS}/rabbitmq`,
    markdown: rabbitmqMarkdown,
    image: rabbitmqLogo,
  },
  {
    id: 'influxdb_listener',
    name: 'influxdb_listener',
    url: `${TELEGRAF_PLUGINS}/influxdb_listener`,
    markdown: influxdb_listenerMarkdown,
    image: influxdb_listenerLogo,
  },
  {
    id: 'ecs',
    name: 'ecs',
    url: `${TELEGRAF_PLUGINS}/ecs`,
    markdown: ecsMarkdown,
    image: ecsLogo,
  },
  {
    id: 'statsd',
    name: 'statsd',
    url: `${TELEGRAF_PLUGINS}/statsd`,
    markdown: statsdMarkdown,
    image: statsdLogo,
  },
  {
    id: 'cgroup',
    name: 'cgroup',
    url: `${TELEGRAF_PLUGINS}/cgroup`,
    markdown: cgroupMarkdown,
    image: cgroupLogo,
  },
  {
    id: 'disque',
    name: 'disque',
    url: `${TELEGRAF_PLUGINS}/disque`,
    markdown: disqueMarkdown,
    image: disqueLogo,
  },
  {
    id: 'rethinkdb',
    name: 'rethinkdb',
    url: `${TELEGRAF_PLUGINS}/rethinkdb`,
    markdown: rethinkdbMarkdown,
    image: rethinkdbLogo,
  },
  {
    id: 'dcos',
    name: 'dcos',
    url: `${TELEGRAF_PLUGINS}/dcos`,
    markdown: dcosMarkdown,
    image: dcosLogo,
  },
  {
    id: 'jolokia',
    name: 'jolokia',
    url: `${TELEGRAF_PLUGINS}/jolokia`,
    markdown: jolokiaMarkdown,
    image: jolokiaLogo,
  },
  {
    id: 'varnish',
    name: 'varnish',
    url: `${TELEGRAF_PLUGINS}/varnish`,
    markdown: varnishMarkdown,
    image: varnishLogo,
  },
  {
    id: 'tail',
    name: 'tail',
    url: `${TELEGRAF_PLUGINS}/tail`,
    markdown: tailMarkdown,
    image: tailLogo,
  },
  {
    id: 'mongodb',
    name: 'mongodb',
    url: `${TELEGRAF_PLUGINS}/mongodb`,
    markdown: mongodbMarkdown,
    image: mongodbLogo,
  },
  {
    id: 'zookeeper',
    name: 'zookeeper',
    url: `${TELEGRAF_PLUGINS}/zookeeper`,
    markdown: zookeeperMarkdown,
    image: zookeeperLogo,
  },
  {
    id: 'solr',
    name: 'solr',
    url: `${TELEGRAF_PLUGINS}/solr`,
    markdown: solrMarkdown,
    image: solrLogo,
  },
  {
    id: 'systemd_units',
    name: 'systemd_units',
    url: `${TELEGRAF_PLUGINS}/systemd_units`,
    markdown: systemd_unitsMarkdown,
    image: systemd_unitsLogo,
  },
  {
    id: 'influxdb',
    name: 'influxdb',
    url: `${TELEGRAF_PLUGINS}/influxdb`,
    markdown: influxdbMarkdown,
    image: influxdbLogo,
  },
  {
    id: 'memcached',
    name: 'memcached',
    url: `${TELEGRAF_PLUGINS}/memcached`,
    markdown: memcachedMarkdown,
    image: memcachedLogo,
  },
  {
    id: 'filestat',
    name: 'filestat',
    url: `${TELEGRAF_PLUGINS}/filestat`,
    markdown: filestatMarkdown,
    image: filestatLogo,
  },
  {
    id: 'net_response',
    name: 'net_response',
    url: `${TELEGRAF_PLUGINS}/net_response`,
    markdown: net_responseMarkdown,
    image: net_responseLogo,
  },
  {
    id: 'cisco_telemetry_mdt',
    name: 'cisco_telemetry_mdt',
    url: `${TELEGRAF_PLUGINS}/cisco_telemetry_mdt`,
    markdown: cisco_telemetry_mdtMarkdown,
    image: cisco_telemetry_mdtLogo,
  },
  {
    id: 'system',
    name: 'system',
    url: `${TELEGRAF_PLUGINS}/system`,
    markdown: systemMarkdown,
    image: systemLogo,
  },
  {
    id: 'zfs',
    name: 'zfs',
    url: `${TELEGRAF_PLUGINS}/zfs`,
    markdown: zfsMarkdown,
    image: zfsLogo,
  },
  {
    id: 'opensmtpd',
    name: 'opensmtpd',
    url: `${TELEGRAF_PLUGINS}/opensmtpd`,
    markdown: opensmtpdMarkdown,
    image: opensmtpdLogo,
  },
  {
    id: 'nats',
    name: 'nats',
    url: `${TELEGRAF_PLUGINS}/nats`,
    markdown: natsMarkdown,
    image: natsLogo,
  },
  {
    id: 'mcrouter',
    name: 'mcrouter',
    url: `${TELEGRAF_PLUGINS}/mcrouter`,
    markdown: mcrouterMarkdown,
    image: mcrouterLogo,
  },
  {
    id: 'lanz',
    name: 'lanz',
    url: `${TELEGRAF_PLUGINS}/lanz`,
    markdown: lanzMarkdown,
    image: lanzLogo,
  },
  {
    id: 'mesos',
    name: 'mesos',
    url: `${TELEGRAF_PLUGINS}/mesos`,
    markdown: mesosMarkdown,
    image: mesosLogo,
  },
  {
    id: 'github',
    name: 'github',
    url: `${TELEGRAF_PLUGINS}/github`,
    markdown: githubMarkdown,
    image: githubLogo,
  },
  {
    id: 'win_perf_counters',
    name: 'win_perf_counters',
    url: `${TELEGRAF_PLUGINS}/win_perf_counters`,
    markdown: win_perf_countersMarkdown,
    image: win_perf_countersLogo,
  },
  {
    id: 'win_services',
    name: 'win_services',
    url: `${TELEGRAF_PLUGINS}/win_services`,
    markdown: win_servicesMarkdown,
    image: win_servicesLogo,
  },
  {
    id: 'zipkin',
    name: 'zipkin',
    url: `${TELEGRAF_PLUGINS}/zipkin`,
    markdown: zipkinMarkdown,
    image: zipkinLogo,
  },
  {
    id: 'consul',
    name: 'consul',
    url: `${TELEGRAF_PLUGINS}/consul`,
    markdown: consulMarkdown,
    image: consulLogo,
  },
  {
    id: 'kube_inventory',
    name: 'kube_inventory',
    url: `${TELEGRAF_PLUGINS}/kube_inventory`,
    markdown: kube_inventoryMarkdown,
    image: kube_inventoryLogo,
  },
  {
    id: 'pf',
    name: 'pf',
    url: `${TELEGRAF_PLUGINS}/pf`,
    markdown: pfMarkdown,
    image: pfLogo,
  },
  {
    id: 'nginx',
    name: 'nginx',
    url: `${TELEGRAF_PLUGINS}/nginx`,
    markdown: nginxMarkdown,
    image: nginxLogo,
  },
  {
    id: 'openntpd',
    name: 'openntpd',
    url: `${TELEGRAF_PLUGINS}/openntpd`,
    markdown: openntpdMarkdown,
    image: openntpdLogo,
  },
  {
    id: 'http',
    name: 'http',
    url: `${TELEGRAF_PLUGINS}/http`,
    markdown: httpMarkdown,
    image: httpLogo,
  },
  {
    id: 'aerospike',
    name: 'aerospike',
    url: `${TELEGRAF_PLUGINS}/aerospike`,
    markdown: aerospikeMarkdown,
    image: aerospikeLogo,
  },
  {
    id: 'cassandra',
    name: 'cassandra',
    url: `${TELEGRAF_PLUGINS}/cassandra`,
    markdown: cassandraMarkdown,
    image: cassandraLogo,
  },
  {
    id: 'wireless',
    name: 'wireless',
    url: `${TELEGRAF_PLUGINS}/wireless`,
    markdown: wirelessMarkdown,
    image: wirelessLogo,
  },
  {
    id: 'interrupts',
    name: 'interrupts',
    url: `${TELEGRAF_PLUGINS}/interrupts`,
    markdown: interruptsMarkdown,
    image: interruptsLogo,
  },
  {
    id: 'nginx_plus',
    name: 'nginx_plus',
    url: `${TELEGRAF_PLUGINS}/nginx_plus`,
    markdown: nginx_plusMarkdown,
    image: nginx_plusLogo,
  },
  {
    id: 'mysql',
    name: 'mysql',
    url: `${TELEGRAF_PLUGINS}/mysql`,
    markdown: mysqlMarkdown,
    image: mysqlLogo,
  },
  {
    id: 'apcupsd',
    name: 'apcupsd',
    url: `${TELEGRAF_PLUGINS}/apcupsd`,
    markdown: apcupsdMarkdown,
    image: apcupsdLogo,
  },
  {
    id: 'sqlserver',
    name: 'sqlserver',
    url: `${TELEGRAF_PLUGINS}/sqlserver`,
    markdown: sqlserverMarkdown,
    image: sqlserverLogo,
  },
  {
    id: 'httpjson',
    name: 'httpjson',
    url: `${TELEGRAF_PLUGINS}/httpjson`,
    markdown: httpjsonMarkdown,
    image: httpjsonLogo,
  },
  {
    id: 'postgresql_extensible',
    name: 'postgresql_extensible',
    url: `${TELEGRAF_PLUGINS}/postgresql_extensible`,
    markdown: postgresql_extensibleMarkdown,
    image: postgresql_extensibleLogo,
  },
  {
    id: 'chrony',
    name: 'chrony',
    url: `${TELEGRAF_PLUGINS}/chrony`,
    markdown: chronyMarkdown,
    image: chronyLogo,
  },
  {
    id: 'exec',
    name: 'exec',
    url: `${TELEGRAF_PLUGINS}/exec`,
    markdown: execMarkdown,
    image: execLogo,
  },
  {
    id: 'linux_sysctl_fs',
    name: 'linux_sysctl_fs',
    url: `${TELEGRAF_PLUGINS}/linux_sysctl_fs`,
    markdown: linux_sysctl_fsMarkdown,
    image: linux_sysctl_fsLogo,
  },
  {
    id: 'monit',
    name: 'monit',
    url: `${TELEGRAF_PLUGINS}/monit`,
    markdown: monitMarkdown,
    image: monitLogo,
  },
  {
    id: 'leofs',
    name: 'leofs',
    url: `${TELEGRAF_PLUGINS}/leofs`,
    markdown: leofsMarkdown,
    image: leofsLogo,
  },
  {
    id: 'eventhub_consumer',
    name: 'eventhub_consumer',
    url: `${TELEGRAF_PLUGINS}/eventhub_consumer`,
    markdown: eventhub_consumerMarkdown,
    image: eventhub_consumerLogo,
  },
  {
    id: 'raindrops',
    name: 'raindrops',
    url: `${TELEGRAF_PLUGINS}/raindrops`,
    markdown: raindropsMarkdown,
    image: raindropsLogo,
  },
  {
    id: 'nvidia_smi',
    name: 'nvidia_smi',
    url: `${TELEGRAF_PLUGINS}/nvidia_smi`,
    markdown: nvidia_smiMarkdown,
    image: nvidia_smiLogo,
  },
  {
    id: 'beanstalkd',
    name: 'beanstalkd',
    url: `${TELEGRAF_PLUGINS}/beanstalkd`,
    markdown: beanstalkdMarkdown,
    image: beanstalkdLogo,
  },
  {
    id: 'disk',
    name: 'disk',
    url: `${TELEGRAF_PLUGINS}/disk`,
    markdown: diskMarkdown,
    image: diskLogo,
  },
  {
    id: 'salesforce',
    name: 'salesforce',
    url: `${TELEGRAF_PLUGINS}/salesforce`,
    markdown: salesforceMarkdown,
    image: salesforceLogo,
  },
  {
    id: 'ipmi_sensor',
    name: 'ipmi_sensor',
    url: `${TELEGRAF_PLUGINS}/ipmi_sensor`,
    markdown: ipmi_sensorMarkdown,
    image: ipmi_sensorLogo,
  },
  {
    id: 'tengine',
    name: 'tengine',
    url: `${TELEGRAF_PLUGINS}/tengine`,
    markdown: tengineMarkdown,
    image: tengineLogo,
  },
  {
    id: 'prometheus',
    name: 'prometheus',
    url: `${TELEGRAF_PLUGINS}/prometheus`,
    markdown: prometheusMarkdown,
    image: prometheusLogo,
  },
  {
    id: 'ethtool',
    name: 'ethtool',
    url: `${TELEGRAF_PLUGINS}/ethtool`,
    markdown: ethtoolMarkdown,
    image: ethtoolLogo,
  },
  {
    id: 'phpfpm',
    name: 'phpfpm',
    url: `${TELEGRAF_PLUGINS}/phpfpm`,
    markdown: phpfpmMarkdown,
    image: phpfpmLogo,
  },
  {
    id: 'nginx_plus_api',
    name: 'nginx_plus_api',
    url: `${TELEGRAF_PLUGINS}/nginx_plus_api`,
    markdown: nginx_plus_apiMarkdown,
    image: nginx_plus_apiLogo,
  },
  {
    id: 'postfix',
    name: 'postfix',
    url: `${TELEGRAF_PLUGINS}/postfix`,
    markdown: postfixMarkdown,
    image: postfixLogo,
  },
  {
    id: 'uwsgi',
    name: 'uwsgi',
    url: `${TELEGRAF_PLUGINS}/uwsgi`,
    markdown: uwsgiMarkdown,
    image: uwsgiLogo,
  },
  {
    id: 'jenkins',
    name: 'jenkins',
    url: `${TELEGRAF_PLUGINS}/jenkins`,
    markdown: jenkinsMarkdown,
    image: jenkinsLogo,
  },
  {
    id: 'kafka_consumer_legacy',
    name: 'kafka_consumer_legacy',
    url: `${TELEGRAF_PLUGINS}/kafka_consumer_legacy`,
    markdown: kafka_consumer_legacyMarkdown,
    image: kafka_consumer_legacyLogo,
  },
  {
    id: 'sysstat',
    name: 'sysstat',
    url: `${TELEGRAF_PLUGINS}/sysstat`,
    markdown: sysstatMarkdown,
    image: sysstatLogo,
  },
  {
    id: 'iptables',
    name: 'iptables',
    url: `${TELEGRAF_PLUGINS}/iptables`,
    markdown: iptablesMarkdown,
    image: iptablesLogo,
  },
  {
    id: 'hddtemp',
    name: 'hddtemp',
    url: `${TELEGRAF_PLUGINS}/hddtemp`,
    markdown: hddtempMarkdown,
    image: hddtempLogo,
  },
  {
    id: 'unbound',
    name: 'unbound',
    url: `${TELEGRAF_PLUGINS}/unbound`,
    markdown: unboundMarkdown,
    image: unboundLogo,
  },
  {
    id: 'pgbouncer',
    name: 'pgbouncer',
    url: `${TELEGRAF_PLUGINS}/pgbouncer`,
    markdown: pgbouncerMarkdown,
    image: pgbouncerLogo,
  },
  {
    id: 'kinesis_consumer',
    name: 'kinesis_consumer',
    url: `${TELEGRAF_PLUGINS}/kinesis_consumer`,
    markdown: kinesis_consumerMarkdown,
    image: kinesis_consumerLogo,
  },
  {
    id: 'kubernetes',
    name: 'kubernetes',
    url: `${TELEGRAF_PLUGINS}/kubernetes`,
    markdown: kubernetesMarkdown,
    image: kubernetesLogo,
  },
  {
    id: 'icinga2',
    name: 'icinga2',
    url: `${TELEGRAF_PLUGINS}/icinga2`,
    markdown: icinga2Markdown,
    image: icinga2Logo,
  },
  {
    id: 'openldap',
    name: 'openldap',
    url: `${TELEGRAF_PLUGINS}/openldap`,
    markdown: openldapMarkdown,
    image: openldapLogo,
  },
  {
    id: 'passenger',
    name: 'passenger',
    url: `${TELEGRAF_PLUGINS}/passenger`,
    markdown: passengerMarkdown,
    image: passengerLogo,
  },
  {
    id: 'mandrill',
    name: 'mandrill',
    url: `${TELEGRAF_PLUGINS}/mandrill`,
    markdown: mandrillMarkdown,
    image: mandrillLogo,
  },
  {
    id: 'filestack',
    name: 'filestack',
    url: `${TELEGRAF_PLUGINS}/filestack`,
    markdown: filestackMarkdown,
    image: filestackLogo,
  },
  {
    id: 'rollbar',
    name: 'rollbar',
    url: `${TELEGRAF_PLUGINS}/rollbar`,
    markdown: rollbarMarkdown,
    image: rollbarLogo,
  },
  {
    id: 'webhooks',
    name: 'webhooks',
    url: `${TELEGRAF_PLUGINS}/webhooks`,
    markdown: webhooksMarkdown,
    image: webhooksLogo,
  },
  {
    id: 'github',
    name: 'github',
    url: `${TELEGRAF_PLUGINS}/github`,
    markdown: githubMarkdown,
    image: githubLogo,
  },
  {
    id: 'papertrail',
    name: 'papertrail',
    url: `${TELEGRAF_PLUGINS}/papertrail`,
    markdown: papertrailMarkdown,
    image: papertrailLogo,
  },
  {
    id: 'particle',
    name: 'particle',
    url: `${TELEGRAF_PLUGINS}/particle`,
    markdown: particleMarkdown,
    image: particleLogo,
  },
  {
    id: 'wireguard',
    name: 'wireguard',
    url: `${TELEGRAF_PLUGINS}/wireguard`,
    markdown: wireguardMarkdown,
    image: wireguardLogo,
  },
  {
    id: 'riak',
    name: 'riak',
    url: `${TELEGRAF_PLUGINS}/riak`,
    markdown: riakMarkdown,
    image: riakLogo,
  },
  {
    id: 'bind',
    name: 'bind',
    url: `${TELEGRAF_PLUGINS}/bind`,
    markdown: bindMarkdown,
    image: bindLogo,
  },
  {
    id: 'modbus',
    name: 'modbus',
    url: `${TELEGRAF_PLUGINS}/modbus`,
    markdown: modbusMarkdown,
    image: modbusLogo,
  },
  {
    id: 'minecraft',
    name: 'minecraft',
    url: `${TELEGRAF_PLUGINS}/minecraft`,
    markdown: minecraftMarkdown,
    image: minecraftLogo,
  },
  {
    id: 'tcp_listener',
    name: 'tcp_listener',
    url: `${TELEGRAF_PLUGINS}/tcp_listener`,
    markdown: tcp_listenerMarkdown,
    image: tcp_listenerLogo,
  },
  {
    id: 'ipvs',
    name: 'ipvs',
    url: `${TELEGRAF_PLUGINS}/ipvs`,
    markdown: ipvsMarkdown,
    image: ipvsLogo,
  },
  {
    id: 'openweathermap',
    name: 'openweathermap',
    url: `${TELEGRAF_PLUGINS}/openweathermap`,
    markdown: openweathermapMarkdown,
    image: openweathermapLogo,
  },
  {
    id: 'powerdns_recursor',
    name: 'powerdns_recursor',
    url: `${TELEGRAF_PLUGINS}/powerdns_recursor`,
    markdown: powerdns_recursorMarkdown,
    image: powerdns_recursorLogo,
  },
  {
    id: 'puppetagent',
    name: 'puppetagent',
    url: `${TELEGRAF_PLUGINS}/puppetagent`,
    markdown: puppetagentMarkdown,
    image: puppetagentLogo,
  },
  {
    id: 'dns_query',
    name: 'dns_query',
    url: `${TELEGRAF_PLUGINS}/dns_query`,
    markdown: dns_queryMarkdown,
    image: dns_queryLogo,
  },
  {
    id: 'elasticsearch',
    name: 'elasticsearch',
    url: `${TELEGRAF_PLUGINS}/elasticsearch`,
    markdown: elasticsearchMarkdown,
    image: elasticsearchLogo,
  },
  {
    id: 'apache',
    name: 'apache',
    url: `${TELEGRAF_PLUGINS}/apache`,
    markdown: apacheMarkdown,
    image: apacheLogo,
  },
  {
    id: 'nginx_sts',
    name: 'nginx_sts',
    url: `${TELEGRAF_PLUGINS}/nginx_sts`,
    markdown: nginx_stsMarkdown,
    image: nginx_stsLogo,
  },
  {
    id: 'nstat',
    name: 'nstat',
    url: `${TELEGRAF_PLUGINS}/nstat`,
    markdown: nstatMarkdown,
    image: nstatLogo,
  },
  {
    id: 'http_listener_v2',
    name: 'http_listener_v2',
    url: `${TELEGRAF_PLUGINS}/http_listener_v2`,
    markdown: http_listener_v2Markdown,
    image: http_listener_v2Logo,
  },
  {
    id: 'kernel',
    name: 'kernel',
    url: `${TELEGRAF_PLUGINS}/kernel`,
    markdown: kernelMarkdown,
    image: kernelLogo,
  },
  {
    id: 'filecount',
    name: 'filecount',
    url: `${TELEGRAF_PLUGINS}/filecount`,
    markdown: filecountMarkdown,
    image: filecountLogo,
  },
  {
    id: 'nsq',
    name: 'nsq',
    url: `${TELEGRAF_PLUGINS}/nsq`,
    markdown: nsqMarkdown,
    image: nsqLogo,
  },
  {
    id: 'mem',
    name: 'mem',
    url: `${TELEGRAF_PLUGINS}/mem`,
    markdown: memMarkdown,
    image: memLogo,
  },
  {
    id: 'nats_consumer',
    name: 'nats_consumer',
    url: `${TELEGRAF_PLUGINS}/nats_consumer`,
    markdown: nats_consumerMarkdown,
    image: nats_consumerLogo,
  },
  {
    id: 'haproxy',
    name: 'haproxy',
    url: `${TELEGRAF_PLUGINS}/haproxy`,
    markdown: haproxyMarkdown,
    image: haproxyLogo,
  },
  {
    id: 'couchbase',
    name: 'couchbase',
    url: `${TELEGRAF_PLUGINS}/couchbase`,
    markdown: couchbaseMarkdown,
    image: couchbaseLogo,
  },
  {
    id: 'fireboard',
    name: 'fireboard',
    url: `${TELEGRAF_PLUGINS}/fireboard`,
    markdown: fireboardMarkdown,
    image: fireboardLogo,
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
