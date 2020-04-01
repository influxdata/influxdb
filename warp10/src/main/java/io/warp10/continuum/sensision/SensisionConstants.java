//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.continuum.sensision;

import io.warp10.sensision.Sensision;

public class SensisionConstants {
  
  static {
    if (null == System.getProperty(Sensision.SENSISION_INSTANCE)) {
      Sensision.setInstance("warp");
    }
  }
  
  //
  // Classes
  //
  
  /**
   * Number of datalog requests which were forwarded successfully
   */
  public static final String CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FORWARDED = "warp.datalog.forwarder.requests.forwarded";

  /**
   * Number of datalog requests which failed to be forwarded
   */
  public static final String CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FAILED = "warp.datalog.forwarder.requests.failed";

  /**
   * Number of datalog requests which were ignored
   */
  public static final String CLASS_WARP_DATALOG_FORWARDER_REQUESTS_IGNORED = "warp.datalog.forwarder.requests.ignored";

  /**
   * Number of datalog requests logged
   */
  public static final String CLASS_WARP_DATALOG_REQUESTS_LOGGED = "warp.datalog.requests.logged";

  /**
   * Number of datalog requests received
   */
  public static final String CLASS_WARP_DATALOG_REQUESTS_RECEIVED = "warp.datalog.requests.received";

  /**
   * Number of errors encountered when fetching data, might be an indication of problem related to hbase
   */
  public static final String CLASS_WARP_FETCH_ERRORS = "warp.fetch.errors";

  /**
   * Number of errors encountered when finding data, might be an indication of problem related to directory
   */
  public static final String CLASS_WARP_FIND_ERRORS = "warp.find.errors";

  /**
   * Number of errors encountered when deleting data, might be an indication of problem related to directory
   */
  public static final String CLASS_WARP_INGRESS_DELETE_ERRORS = "warp.ingress.delete.errors";

  /**
   * Number of TimeSource calibrations  
   */
  public static final String CLASS_WARP_TIMESOURCE_CALIBRATIONS = "warp.timesource.calibrations";

  /**
   * Number of skipped TimeSource calibrations  
   */
  public static final String SENSISION_CLASS_WARP_TIMESOURCE_CALIBRATIONS_SKIPPED = "warp.timesource.calibrations.skipped";

  /**
   * Revision of components
   */
  public static final String SENSISION_CLASS_WARP_REVISION = "warp.revision";
  
  /**
   * Number of entries in the serialized metadata cache 
   */
  public static final String CLASS_WARP_DIRECTORY_METADATA_CACHE_SIZE = "warp.directory.metadata.cache.size";
  
  /**
   * Number of hits in the serialized metadata cache
   */
  public static final String CLASS_WARP_DIRECTORY_METADATA_CACHE_HITS = "warp.directory.metadata.cache.hits";

  /**
   * Number of collisions detected for class Id
   */
  public static final String CLASS_WARP_DIRECTORY_CLASS_COLLISIONS = "warp.directory.class.collisions";

  /**
   * Number of collisions detected for labels Id
   */
  public static final String CLASS_WARP_DIRECTORY_LABELS_COLLISIONS = "warp.directory.labels.collisions";
  
  /**
   * Number of GTS metadata managed by Directory.
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS = "warp.directory.gts";

  /**
   * Number of distinct classes managed by Directory
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_CLASSES = "warp.directory.classes";

  /**
   * Number of distinct owners known by a Directory
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_OWNERS = "warp.directory.owners";
  
  /**
   * Number of distinct classes per owner known by a Directory
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP = "warp.directory.gts.perapp";

  /**
   * Number of times the Thrift directory client cache was changed
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_CLIENT_CACHE_CHANGED = "warp.directory.client.cache.changed";
  
  /**
   * Free memory for directory
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_JVM_FREEMEMORY = "warp.directory.jvm.freememory";
  
  /**
   * Number of calls to the fetch method of the store client
   */
  public static final String SENSISION_CLASS_CONTINUUM_FETCH_COUNT = "warp.fetch.count";
  
  /**
   * Number of value bytes read by the calls to fetch
   */
  public static final String SENSISION_CLASS_CONTINUUM_FETCH_BYTES_VALUES = "warp.fetch.bytes.values";

  /**
   * Number of value bytes read by the calls to fetch per owner
   */
  public static final String SENSISION_CLASS_CONTINUUM_FETCH_BYTES_VALUES_PEROWNER = "warp.fetch.bytes.values.perowner";

  /**
   * Number of key bytes read by the calls to fetch
   */
  public static final String SENSISION_CLASS_CONTINUUM_FETCH_BYTES_KEYS = "warp.fetch.bytes.keys";

  /**
   * Number of key bytes read by the calls to fetch per owner
   */
  public static final String SENSISION_CLASS_CONTINUUM_FETCH_BYTES_KEYS_PEROWNER = "warp.fetch.bytes.keys.perowner";

  /**
   * Number of datapoints read by 'fetch'
   */
  public static final String SENSISION_CLASS_CONTINUUM_FETCH_DATAPOINTS = "warp.fetch.datapoints";

  /**
   * Number of datapoints read by 'fetch' per owner
   */
  public static final String SENSISION_CLASS_CONTINUUM_FETCH_DATAPOINTS_PEROWNER = "warp.fetch.datapoints.perowner";

  /**
   * Number of 'fetch' requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_FETCH_REQUESTS = "warp.fetch.requests";

  /**
   * Number of gtsWrappers produced by sfetch
   */
  public static final String SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS = "warp.sfetch.wrappers";

  /**
   * Number of gtsWrappers produced by sfetch per application
   */
  public static final String SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_PERAPP = "warp.sfetch.wrappers.perapp";

  /**
   * Size of gtsWrapper produced by sfetch
   */
  public static final String SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_SIZE = "warp.sfetch.wrappers.size";

  /**
   * Size of gtsWrapper produced by sfetch per application
   */
  public static final String SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_SIZE_PERAPP = "warp.sfetch.wrappers.size.perapp";

  /**
   * Number of datapoints in gtsWrapper produced by sfetch
   */
  public static final String SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_DATAPOINTS = "warp.sfetch.wrappers.datapoints";

  /**
   * Number of datapoints in gtsWrapper produced by sfetch per application
   */
  public static final String SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_DATAPOINTS_PERAPP = "warp.sfetch.wrappers.datapoints.perapp";

  /**
   * Number of messages sent on the throttling topic
   */
  public static final String CLASS_WARP_INGRESS_KAFKA_THROTTLING_OUT_MESSAGES = "warp.ingress.kafka.throttling.out.messages";
  
  /**
   * Number of bytes sent on the throttling topic
   */
  public static final String CLASS_WARP_INGRESS_KAFKA_THROTTLING_OUT_BYTES = "warp.ingress.kafka.throttling.out.bytes";
  
  /**
   * Number of errors encountered while sending on the throttling topic
   */
  public static final String CLASS_WARP_INGRESS_KAFKA_THROTTLING_ERRORS = "warp.ingress.kafka.throttling.error";
  
  /**
   * Number of messages consumed from the throttling topic
   */
  public static final String CLASS_WARP_INGRESS_KAFKA_THROTTLING_IN_MESSAGES = "warp.ingress.kafka.throttling.in.messages";
  
  /**
   * Number of bytes consumed from the throttling topic
   */
  public static final String CLASS_WARP_INGRESS_KAFKA_THROTTLING_IN_BYTES = "warp.ingress.kafka.throttling.in.bytes";
    
  /**
   * Number of invalid MACs encountered while consuming the throttling topic
   */
  public static final String CLASS_WARP_INGRESS_KAFKA_THROTTLING_IN_INVALIDMACS = "warp.ingress.kafka.throttling.in.invalidmacs";
  
  /**
   * Number of successful throttling estimators fusions
   */
  public static final String CLASS_WARP_INGRESS_THROTLLING_FUSIONS = "warp.ingress.throttling.fusions";
  
  /**
   * Number of failed throttling estimators fusions
   */
  public static final String CLASS_WARP_INGRESS_THROTLLING_FUSIONS_FAILED = "warp.ingress.throttling.fusions.failed";

  /**
   * Number of Metadata cached in 'ingress'
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_METADATA_CACHED = "warp.ingress.metadata.cached";
  
  /**
   * Number of Kafka messages containing data produced by 'Ingress'
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_MESSAGES = "warp.ingress.kafka.data.messages";
  
  /**
   * Number of calls to Kafka send by 'Ingress' for messages containing data
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_SEND = "warp.ingress.kafka.data.send";
  
  /**
   * Number of Producer get from the producer pool
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_POOL_GET = "warp.ingress.kafka.data.producer.pool.get";

  /**
   * Total number of nanoseconds spent waiting for a producer to be available
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_WAIT_NANO = "warp.ingress.kafka.data.producer.wait.nanos";

  /**
   * Total number of nanoseconds spent sending a list of data by a producer
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_SEND = "warp.ingress.kafka.data.producer.send.nanos";

  /**
   * Number of Producer get from the metadata producer pool
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_POOL_GET = "warp.ingress.kafka.metadata.producer.pool.get";

  /**
   * Total number of nanoseconds spent waiting for a metadata producer to be available
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_WAIT_NANO = "warp.ingress.kafka.metadata.producer.wait.nanos";

  /**
   * Total number of nanoseconds spent sending a list of metadata by a producer
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_SEND = "warp.ingress.kafka.metadata.producer.send.nanos";

  /**
   * Number of Kafka delete messages produced by 'Ingress'
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DELETE_MESSAGES = "warp.ingress.kafka.delete.messages";
  
  /**
   * Number of calls to Kafka send by 'Ingress' for deletion messages
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DELETE_SEND = "warp.ingress.kafka.delete.send";

  /**
   * Number of Kafka messages containing data produced by 'Ingress'
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_META_MESSAGES = "warp.ingress.kafka.meta.messages";
  
  /**
   * Number of calls to Kafka send by 'Ingress' for messages containing metadata
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_META_SEND = "warp.ingress.kafka.meta.send";
  
  /**
   * Number of messages consumed from the 'metadata' Kafka topic by 'Ingress'
   */
  public static final String SENSISION_CLASS_WARP_INGRESS_KAFKA_META_IN_MESSAGES = "warp.ingress.kafka.meta.in.messages";

  /**
   * Number of bytes consumed from the 'metadata' Kafka topic by 'Ingress'
   */
  public static final String SENSISION_CLASS_WARP_INGRESS_KAFKA_META_IN_BYTES = "warp.ingress.kafka.meta.in.bytes";

  /**
   * Number of invalid MACs for messages consumed from the 'metadata' Kafka topic by 'Ingress'
   */
  public static final String SENSISION_CLASS_WARP_INGRESS_KAFKA_META_IN_INVALIDMACS = "warp.ingress.kafka.meta.in.invalidmacs";

  /**
   * Number of invalid AES wrappings for messages consumed from the 'metadata' Kafka topic by 'Ingress'
   */
  public static final String SENSISION_CLASS_WARP_INGRESS_KAFKA_META_IN_INVALIDCIPHERS = "warp.ingress.kafka.meta.in.invalidciphers";

  /**
   * Number of 'update' requests received by the 'Ingress' component of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_REQUESTS = "warp.ingress.update.requests";

  /**
   * Number of datapoints pushed into Continuum (at the 'ingress' level, not by producer)
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_GLOBAL = "warp.ingress.update.datapoints.global";

  /**
   * Number of 'delete' requests received by the 'Ingress' component of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_DELETE_REQUESTS = "warp.ingress.delete.requests";

  /**
   * Number of GTS which were concerned by the 'delete' requests.
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_DELETE_GTS = "warp.ingress.delete.gts";

  /**
   * Number of times an 'update' request was done using an invalid token
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_INVALIDTOKEN = "warp.ingress.update.invalidtoken";
  
  /**
   * Number of 'update' requests which had gzipped content
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_GZIPPED = "warp.ingress.update.gzipped";
  
  /**
   * Number of parse error encountered in Ingress 'update' requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_PARSEERRORS = "warp.ingress.update.parseerrors";
    
  /**
   * Number of raw readings pushed into Continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_RAW = "warp.ingress.update.datapoints.raw";

  /**
   * Number of readings indexed in Continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_INDEXED = "warp.ingress.update.datapoints.indexed";

  /**
   * Number of microseconds spent in 'update'
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_TIME_US = "warp.ingress.update.time.us";

  /**
   * Number of microseconds spent in 'update' at the 'ingress' level
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_TIME_US_GLOBAL = "warp.ingress.update.time.us.global";

  /**
   * Number of 'meta' requests to the Ingress component
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_META_REQUESTS = "warp.ingress.meta.requests";
  
  /**
   * Number of times a 'meta' request was done using an invalid token
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALIDTOKEN = "warp.ingress.meta.invalidtoken";
  
  /**
   * Number of 'meta' requests which had gzipped content
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_META_GZIPPED = "warp.ingress.meta.gzipped";
  
  /**
   * Number of times invalid metadata were sent to the 'meta' endpoint of Ingress
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALID = "warp.ingress.meta.invalid";
  
  /**
   * Number of metadata records sent to the 'meta' endpoint of Ingress
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_META_RECORDS = "warp.ingress.meta.records";

  /**
   * Number of cached per producer estimators for GTS uniqueness estimation
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_ESTIMATORS_CACHED = "warp.ingress.estimators.cached";

  /**
   * Number of cached per app estimators for GTS uniqueness estimation
   */
  public static final String SENSISION_CLASS_CONTINUUM_INGRESS_ESTIMATORS_CACHED_PER_APP = "warp.ingress.estimators.cached.perapp";

  /**
   * Number of 'standalone' delete requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_REQUESTS = "warp.standalone.delete.requests";

  /**
   * Number of datapoints deleted by 'standalone' delete
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_DATAPOINTS = "warp.standalone.delete.datapoints";

  /**
   * Number of datapoints deleted by 'standalone' delete per owner and application
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_DATAPOINTS_PEROWNERAPP = "warp.standalone.delete.datapoints.perownerapp";

  /**
   * Number of GTS deleted by 'standalone' delete
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_GTS = "warp.standalone.delete.gts";

  /**
   * Time spent in microseconds in standalone 'delete'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_DELETE_TIME_US = "warp.standalone.delete.time.us";
      
  /**
   * Number of microseconds spent in 'update' in the standalone version of Continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_TIME_US = "warp.standalone.update.time.us";

  /**
   * Number of microseconds spent in 'update' in the standalone streaming version of Continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_TIME_US = "warp.standalone.update.stream.time.us";

  /**
   * Number of microseconds spent in 'update' in the streaming update version of Continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_TIME_US = "warp.update.stream.time.us";

  /**
   * Number of 'update' requests received by the standlone version of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_REQUESTS = "warp.standalone.update.requests";

  /**
   * Number of 'update' requests received by the standlone streaming version of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_REQUESTS = "warp.standalone.update.stream.requests";

  /*
   * Number of snapshot requests made to the standalone storage layer 
   */
  public static final String SENSISION_CLASS_WARP_STANDALONE_LEVELDB_SNAPSHOT_REQUESTS = "warp.standalone.leveldb.snapshot.requests";

  /**
   * Time spent with compactions disabled to enable snapshots
   */
  public static final String SENSISION_CLASS_WARP_STANDALONE_LEVELDB_SNAPSHOT_TIME_NS = "warp.standalone.leveldb.snapshot.time.ns";
  
  /**
   * Number of 'update' requests received by the streaming version of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_REQUESTS = "warp.update.stream.requests";

  /**
   * Number of 'update' messages received by the standlone streaming version of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_MESSAGES = "warp.standalone.update.stream.messages";

  /**
   * Number of 'update' messages received by the streaming version of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_MESSAGES = "warp.update.stream.messages";

  /**
   * Number of raw readings pushed into Continuum (standalone version)
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_DATAPOINTS_RAW = "warp.standalone.update.datapoints.raw";

  /**
   * Number of raw readings pushed into Continuum (standalone streaming version)
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_DATAPOINTS_RAW = "warp.standalone.update.stream.datapoints.raw";

  /**
   * Number of raw readings pushed into Continuum (streaming version)
   */
  public static final String SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_DATAPOINTS_RAW = "warp.update.stream.datapoints.raw";

  /**
   * Number of datapoints pushed into Continuum via the streaming update endpoint (at the global level, not by producer)
   */
  public static final String SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_DATAPOINTS_GLOBAL = "warp.update.stream.datapoints.global";

  /**
   * Number of parse error encountered in 'update' requests in the standalone version of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_UPDATE_PARSEERRORS = "warp.standalone.update.parseerrors";

  /**
   * Number of parse error encountered in 'update' requests in the standalone streaming version of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_PARSEERRORS = "warp.standalone.update.stream.parseerrors";

  /**
   * Number of parse error encountered in 'update' requests in the streaming version of continuum
   */
  public static final String SENSISION_CLASS_CONTINUUM_STREAM_UPDATE_PARSEERRORS = "warp.update.stream.parseerrors";

  /**
   * Number of HBase Puts created in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_PUTS = "warp.store.hbase.puts";

  /**
   * Number of HBase Puts committed in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_PUTS_COMMITTED = "warp.store.hbase.puts.committed";

  /**
   * Number of GTSDecoders handled by 'Store'  
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_GTSDECODERS = "warp.store.gtsdecoders";
  
  /**
   * Number of calls to 'flushCommits' done in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_COMMITS = "warp.store.hbase.commits";

  /**
   * Time spend writing to HBase in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_TIME_NANOS = "warp.store.hbase.time.nanos";
  
  /**
   * Number of barrier synchronizations
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_BARRIER_SYNCS = "warp.store.barrier.syncs";
  
  /**
   * Number of Kafka offset commits done in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_KAFKA_COMMITS = "warp.store.kafka.commits";
  
  /**
   * Number of Kafka messages read in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_KAFKA_COUNT = "warp.store.kafka.count";
  
  /**
   * Number of Kafka messages bytes read in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_KAFKA_BYTES = "warp.store.kafka.bytes";
  
  /**
   * Number of failed MAC verification for Kafka messages read in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_KAFKA_FAILEDMACS = "warp.store.kafka.failedmacs";
  
  /**
   * Number of failed decryptions for Kafka messages read in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_KAFKA_FAILEDDECRYPTS = "warp.store.kafka.faileddecrypts";

  /**
   * Number of aborts experienced in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_ABORTS = "warp.store.aborts";
  
  /**
   * Number of times commits were overdue, probably due to a call to htable.batch having hang
   */
  public static final String CLASS_WARP_STORE_KAFKA_COMMITS_OVERDUE = "warp.store.kafka.commits.overdue";
  
  /**
   * Number of times the HBase connection was reset. This is a total number across all Store instances within a JVM.
   */
  public static final String CLASS_WARP_STORE_HBASE_CONN_RESETS = "warp.store.hbase.conn.resets";
  
  /**
   * Rate at which we throttle the Kafka consumers. This rate is used by generating a random number and only
   * process an incoming message if that number is <= CLASS_WARP_STORE_THROTTLING_RATE
   */
  public static final String CLASS_WARP_STORE_THROTTLING_RATE = "warp.store.throttling.rate";
  
  /**
   * Time spent in DELETE ops in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_DELETE_TIME_NANOS = "warp.store.hbase.delete.time.nanos";
  
  /**
   * Number of DELETE ops handled by 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_DELETE_OPS = "warp.store.hbase.delete.ops";

  /**
   * Number of regions contacted to handle the DELETE ops by 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_DELETE_REGIONS = "warp.store.hbase.delete.regions";

  /**
   * Number of datapoints deleted by 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_DELETE_DATAPOINTS = "warp.store.hbase.delete.datapoints";

  /**
   * Number of datapoints deleted by 'Store' per owner and application
   */
  public static final String SENSISION_CLASS_CONTINUUM_STORE_HBASE_DELETE_DATAPOINTS_PEROWNERAPP = "warp.store.hbase.delete.datapoints.perownerapp";

  /**
   * Number of results retrieved from scanners by HBaseStoreClient
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_RESULTS = "warp.hbase.client.results";

  /**
   * Number of cells retrieved from scanners by HBaseStoreClient
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_CELLS = "warp.hbase.client.cells";

  /**
   * Number of GTSDecoder iterators produced by HBaseStoreClient
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_ITERATORS = "warp.hbase.client.iterators";

  /**
   * Number of GTSDecoder scanners retrieved by HBaseStoreClient
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_SCANNERS = "warp.hbase.client.scanners";

  /**
   * Number of HBase scanners which used a filter (SlicedRowFilter)
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_FILTERED_SCANNERS = "warp.hbase.client.scanners.filtered";
  
  /**
   * Number of ranges filtered by the filtered scanners
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_FILTERED_SCANNERS_RANGES = "warp.hbase.client.scanners.filtered.ranges";
  
  /**
   * Number of optimized scanners created
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_OPTIMIZED_SCANNERS = "warp.hbase.client.scanners.optimized";

  /**
   * Number of groups identified for optimized scanners
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_OPTIMIZED_SCANNERS_GROUPS = "warp.hbase.client.scanners.optimized.groups";

  /**
   * Number of ranges identified for optimized scanners
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_OPTIMIZED_SCANNERS_RANGES = "warp.hbase.client.scanners.optimized.ranges";

  /**
   * Number of parallel scanners spawned
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_PARALLEL_SCANNERS = "warp.hbase.client.scanners.parallel";

  /**
   * Number of parallel scanners spawned in the standalone version
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_CLIENT_PARALLEL_SCANNERS = "warp.standalone.client.scanners.parallel";

  /**
   * Total number of nanoseconds spent waiting for a scanner to be scheduled in the standalone version
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_CLIENT_PARALLEL_SCANNERS_WAITNANOS = "warp.standalone.client.scanners.parallel.waitnanos";

  /**
   * Total number of nanoseconds spent waiting for a scanner to be scheduled
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_PARALLEL_SCANNERS_WAITNANOS = "warp.hbase.client.scanners.parallel.waitnanos";

  /**
   * Number of rejections when attempting to schedule parallel scanners in the distributed version
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_PARALLEL_SCANNERS_REJECTIONS = "warp.hbase.client.scanners.parallel.rejections";

  /**
   * Number of rejections when attempting to schedule parallel scanners in the standalone version
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_CLIENT_PARALLEL_SCANNERS_REJECTIONS = "warp.standalone.client.scanners.parallel.rejections";
  
  /**
   * Number of times a mutex was requested among parallel scanners. A Mutex is requested when consecutive GTSDecoders belong
   * to the same GTS, they must be pushed to the queue as a block. This can happen when the size of a GTSDecoder grows beyond
   * a limit or when a GTS spawns several regions.
   */
  public static final String SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_PARALLEL_SCANNERS_MUTEX = "warp.hbase.client.scanners.parallel.mutex";

  /**
   * Number of times a mutex was requested among parallel scanners in the standalone version. A Mutex is requested when consecutive GTSDecoders belong
   * to the same GTS, they must be pushed to the queue as a block. This can happen when the size of a GTSDecoder grows beyond
   * a limit or when a GTS spawns several regions.
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_CLIENT_PARALLEL_SCANNERS_MUTEX = "warp.standalone.client.scanners.parallel.mutex";

  /**
   * Number of failed MAC verification for Kafka messages read in 'Directory'
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_FAILEDMACS = "warp.directory.kafka.failedmacs";
  
  /**
   * Number of failed decryptions for Kafka messages read in 'Directory'
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_FAILEDDECRYPTS = "warp.directory.kafka.faileddecrypts";

  /**
   * Number of HBase Puts done in 'Directory'
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_HBASE_PUTS = "warp.directory.hbase.puts";

  /**
   * Number of HBase Delete done in 'Directory'
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_HBASE_DELETES = "warp.directory.hbase.deletes";

  /**
   * Number of calls to 'flushCommits' done in 'Directory'
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_HBASE_COMMITS = "warp.directory.hbase.commits";

  /**
   * Number of barrier synchronizations
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_BARRIER_SYNCS = "warp.directory.barrier.syncs";

  /**
   * Number of Kafka offset commits done in 'Store'
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_COMMITS = "warp.directory.kafka.commits";

  /**
   * Number of times the Kafka connector was shutdown due to errors in the consuming threads
   */
  public static final String SENSISION_CLASS_WARP_DIRECTORY_KAFKA_SHUTDOWNS = "warp.directory.kafka.shutdowns";
  /**
   * Total number of 'find' requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_REQUESTS = "warp.directory.find.requests";

  /**
   * Total number of 'stats' requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_REQUESTS = "warp.directory.stats.requests";

  /**
   * Total number of results returned by 'find' requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_RESULTS = "warp.directory.find.results";

  /**
   * Expired find requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_EXPIRED = "warp.directory.find.expired";

  /**
   * Number of find requests which reached the hard limit
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_LIMITED = "warp.directory.find.limited";

  /**
   * Number of find requests which reached the soft limit and whose result were compacted
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_COMPACTED = "warp.directory.find.compacted";

  /**
   * Number of expired streaming requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_EXPIRED = "warp.directory.streaming.expired";
  
  /**
   * Number of successful streaming requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_REQUESTS = "warp.directory.streaming.requests";

  /**
   * Total time spent in successful streaming requests in microseconds
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_TIME_US = "warp.directory.streaming.time.us";

  /**
   * Number of streaming results
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_RESULTS = "warp.directory.streaming.results";
  
  /**
   * Expired stats requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_EXPIRED = "warp.directory.stats.expired";

  /**
   * Invalid (bad MAC) find requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_INVALID = "warp.directory.find.invalid";

  /**
   * Invalid (bad MAC) streaming requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_INVALID = "warp.directory.streaming.invalid";

  /**
   * Invalid (bad MAC) stats requests
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_INVALID = "warp.directory.stats.invalid";

  /**
   * Number of calls to the 'store' method of Directory plugin
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_CALLS = "warp.directory.plugin.store.calls";

  /**
   * Time spent (in nanoseconds) in method 'store' of Directory plugin
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_TIME_NANOS = "warp.directory.plugin.store.time.nanos";

  /**
   * Number of calls to the 'delete' method of Directory plugin
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_DELETE_CALLS = "warp.directory.plugin.delete.calls";

  /**
   * Time spent (in nanoseconds) in method 'delete' of Directory plugin
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_DELETE_TIME_NANOS = "warp.directory.plugin.delete.time.nanos";

  /**
   * Number of calls to the 'find' method of Directory plugin
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_CALLS = "warp.directory.plugin.find.calls";

  /**
   * Time spent (in nanoseconds) in method 'find' of Directory plugin
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_TIME_NANOS = "warp.directory.plugin.find.time.nanos";

  /**
   * Number of results of the 'find' method of Directory plugin
   */
  public static final String SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_RESULTS = "warp.directory.plugin.find.results";

  /**
   * Number of times the WarpScript bootstrap code was loaded
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_BOOTSTRAP_LOADS = "warp.script.bootstrap.loads";
  
  /**
   * Number of WarpScript requests
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_REQUESTS = "warp.script.requests";
  
  /**
   * Total time (us) spent in WarpScript requests
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_TIME_US = "warp.script.time.us";
  
  /**
   * Total number of ops in WarpScript scripts
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_OPS = "warp.script.ops";
  
  /**
   * Total number of errors in WarpScript scripts
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_ERRORS = "warp.script.errors";  

  /**
   * Free memory reported by the JVM
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_JVM_FREEMEMORY = "warp.script.jvm.freememory";
  
  /**
   * Number of uses of a given WarpScript function
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_FUNCTION_COUNT = "warp.script.function.count";
  
  /**
   * Number of times the stack depth limit was reached
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_STACKDEPTH_EXCEEDED = "warp.script.stackdepth.exceeded";

  /**
   * Number of times the operation count limit was reached
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_OPSCOUNT_EXCEEDED = "warp.script.opscount.exceeded";

  /**
   * Number of times the fetch limit was reached
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_FETCHCOUNT_EXCEEDED = "warp.script.fetchcount.exceeded";

  /**
   * Total time in nanoseconds spent in WarpScript functions
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_FUNCTION_TIME_US = "warp.script.function.time.us";

  /**
   * Number of times a script has been run
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_RUN_COUNT = "warp.script.run.count";

  /**
   * Number of current active executions
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_RUN_CURRENT = "warp.script.run.current";
  
  /**
   * Total time spent running a given script
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_RUN_TIME_US = "warp.script.run.time.us";

  /**
   * Number of times a script has failed
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_RUN_FAILURES = "warp.script.run.failures";

  /**
   * Elapsed time per scheduled script
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_RUN_ELAPSED = "warp.script.run.elapsed.ns";
  
  /**
   * Number of ops per scheduled script
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_RUN_OPS = "warp.script.run.ops";
  
  /**
   * Number of datapoints fetched per scheduled script
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_RUN_FETCHED = "warp.script.run.fetched";
  
  /**
   * Number of sessions with macros currently scheduled by Mobius
   */
  public static final String CLASS_WARP_MOBIUS_ACTIVE_SESSIONS = "warp.script.mobius.sessions.scheduled";

  /**
   * Total number of macro executions
   */
  public static final String CLASS_WARP_MOBIUS_MACROS_EXECUTIONS = "warp.script.mobius.macros.executions";

  /**
   * Total number of failed macro executions
   */
  public static final String CLASS_WARP_MOBIUS_MACROS_ERRORS = "warp.script.mobius.macros.errors";

  /**
   * Total time of macro executions (in ns)
   */
  public static final String CLASS_WARP_MOBIUS_MACROS_TIME_NANOS = "warp.script.mobius.macros.time.nanos";

  /**
   * Number of shards dropped by the GC since the launch of the platform instance
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_CHUNKS = "warp.standalone.inmemory.gc.chunks";
  
  /**
   * Number of points currently stored in the memory store
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_DATAPOINTS = "warp.standalone.inmemory.datapoints";

  /**
   * Number of bytes currently stored in the memory store
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_BYTES = "warp.standalone.inmemory.bytes";

  /**
   * Number of GTS currently stored in the memory store.
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GTS = "warp.standalone.inmemory.gts";

  /**
   * Number of garbage collection cycles in the memory store
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_RUNS = "warp.standalone.inmemory.gc.runs";

  /**
   * Number of data bytes garbage collected in the memory store by dropping complete chunks
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_BYTES = "warp.standalone.inmemory.gc.bytes";

  /**
   * Number of bytes reclaimed by shrinking arrays backing buffers.
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_RECLAIMED = "warp.standalone.inmemory.gc.reclaimed";
  
  /**
   * Number of datapoints garbage collected in the memory store
   */
  public static final String SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_DATAPOINTS = "warp.standalone.inmemory.gc.datapoints";

  /**
   * Number of cached estimators for GTS uniqueness estimation
   */
  public static final String SENSISION_CLASS_CONTINUUM_ESTIMATORS_CACHED = "warp.estimators.cached";

  /**
   * Number of cached per app estimators for GTS uniqueness estimation
   */
  public static final String SENSISION_CLASS_CONTINUUM_ESTIMATORS_CACHED_PER_APP = "warp.estimators.cached.perapp";

  /**
   * Number of times an estimator was reset
   */
  public static final String SENSISION_CLASS_CONTINUUM_ESTIMATOR_RESETS = "warp.estimator.resets";

  /**
   * Number of times an estimator was reset per app
   */
  public static final String SENSISION_CLASS_CONTINUUM_ESTIMATOR_RESETS_PER_APP = "warp.estimator.resets.perapp";

  /**
   * Number of invalid hashes detected when 
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_SUBSCRIPTIONS_INVALID_HASHES = "warp.plasma.backend.subscriptions.invalid.hashes";
  
  /**
   * Number of subscriptions (GTS) by backend
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_SUBSCRIPTIONS = "warp.plasma.backend.subscriptions";

  /**
   * Number of subscriptions updates
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_SUBSCRIPTIONS_UPDATES = "warp.plasma.backend.subscriptions.updates";

  /**
   * Number of messages consumed
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_KAFKA_IN_MESSAGES = "warp.plasma.backend.kafka.in.messages";

  /**
   * Number of bytes consumed
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_KAFKA_IN_BYTES = "warp.plasma.backend.kafka.in.bytes";

  /**
   * Number of invalid MACs in consumed messages
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_KAFKA_IN_INVALIDMACS = "warp.plasma.backend.kafka.in.invalidmacs";

  /**
   * Number of invalid Ciphers in consumed messages
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_KAFKA_IN_INVALIDCIPHERS = "warp.plasma.backend.kafka.in.invalidciphers";

/**
   * Number of messages buffered outwards
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_KAFKA_OUT_MESSAGES = "warp.plasma.backend.kafka.out.messages";

  /**
   * Number of bytes buffered outwards
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_KAFKA_OUT_BYTES = "warp.plasma.backend.kafka.out.bytes";
  
  /**
   * Number of send ops by the outward kafka producer
   */
  public static final String SENSISION_CLASS_PLASMA_BACKEND_KAFKA_OUT_SENT = "warp.plasma.backend.kafka.out.sent";

  /**
   * Number of failed MAC checks
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_INVALIDMACS = "warp.plasma.frontend.kafka.invalidmacs";
  
  /**
   * Number of failed decipherments
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_INVALIDCIPHERS = "warp.plasma.frontend.kafka.invalidciphers";
  
  /**
   * Number of Kafka messages consumed by Plasma Frontend 
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_MESSAGES = "warp.plasma.frontend.kafka.messages";
  
  /**
   * Number of bytes consumed by Plasma Frontend
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_BYTES = "warp.plasma.frontend.kafka.bytes";
  
  /**
   * Number of Kafka consuming loop abortions
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_ABORTS = "warp.plasma.frontend.aborts";

  /**
   * Number of Kafka offset commits
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_COMMITS = "warp.plasma.frontend.kafka.commits";

  /**
   * Number of barrier synchronizations
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_SYNCS = "warp.plasma.frontend.syncs";
  
  /**
   * Number of GTS subscribed to by Plasma Front End
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_SUBSCRIPTIONS = "warp.plasma.frontend.subscriptions";
  
  /**
   * Number of calls to dispatch
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_DISPATCH_CALLS = "warp.plasma.frontend.dispatch.calls";

  /**
   * Number of sessions targeted by 'dispatch'
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_DISPATCH_SESSIONS = "warp.plasma.frontend.dispatch.sessions";

  /**
   * Time (in microseconds) spent in 'dispatch'
   */
  public static final String SENSISION_CLASS_PLASMA_FRONTEND_DISPATCH_TIME_US = "warp.plasma.frontend.dispatch.time.ns";
  
  /**
   * Number of distinct GTS as estimated by HLL+
   */
  public static final String SENSISION_CLASS_CONTINUUM_GTS_DISTINCT = "warp.gts.distinct";
  
  /**
   * Number of distinct GTS as estimated by HLL+ per application
   */
  public static final String SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP = "warp.gts.distinct.perapp";

  /**
   * Serialized estimator for the given producer. The timestamp is that of the estimator's creation
   */
  public static final String SENSISION_CLASS_CONTINUUM_GTS_ESTIMATOR = "warp.gts.estimator";

  /**
   * Serialized estimator for the given application. The timestamp is that of the estimator's creation
   */
  public static final String SENSISION_CLASS_CONTINUUM_GTS_ESTIMATOR_PER_APP = "warp.gts.estimator.perapp";

  /**
   * Number of times the distinct GTS throttling triggered
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_GTS = "warp.throttling.gts";

  /**
   * Number of times the distinct GTS throttling triggered globally
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_GLOBAL = "warp.throttling.gts.global";
  
  /**
   * MADS per producer
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_LIMIT = "warp.throttling.gts.limit";

  /**
   * MADS per application
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_LIMIT_PER_APP = "warp.throttling.gts.limit.perapp";

  /**
   * Number of times the distinct GTS throttling triggered per app
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_PER_APP = "warp.throttling.gts.perapp";

  /**
   * Number of times the distinct GTS throttling triggered per app globally
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_PER_APP_GLOBAL = "warp.throttling.gts.perapp.global";

  /**
   * Number of times the rate throttling triggered
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_RATE = "warp.throttling.rate";

  /**
   * Number of times the rate throttling triggered globally
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_GLOBAL = "warp.throttling.rate.global";

  /**
   * Current rate limit per producer
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_LIMIT = "warp.throttling.rate.limit";

  /**
   * Current rate limit per application
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_LIMIT_PER_APP = "warp.throttling.rate.limit.perapp";

  /**
   * Number of times the rate throttling triggered per app
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_PER_APP = "warp.throttling.rate.perapp";

  /**
   * Number of times the rate throttling triggered per app globally
   */
  public static final String SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_PER_APP_GLOBAL = "warp.throttling.rate.perapp.global";

  /**
   * Number of macros cached from WarpFleet repositories
   */
  public static final String SENSISION_CLASS_WARPFLEET_MACROS_CACHED = "warpfleet.macros.cached";
  
  /**
   * Number of macros loaded from jars and the classpath which are currently cached
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_LIBRARY_CACHED = "warpscript.library.macros";
  
  /**
   * Number of macros known in the repository
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_REPOSITORY_MACROS = "warp.script.repository.macros";
    
  /**
   * Number of jar files known in the repository
   */
  public static final String SENSISION_CLASS_WARPSCRIPT_REPOSITORY_JARS = "warp.script.repository.jars";
  
  /**
   * Number of messages consumed from Kafka
   */
  public static final String SENSISION_CLASS_WEBCALL_KAFKA_IN_COUNT = "warp.webcall.kafka.in.count";
  
  /**
   * Number of bytes consumed from Kafka
   */
  public static final String SENSISION_CLASS_WEBCALL_KAFKA_IN_BYTES = "warp.webcall.kafka.in.bytes";
  
  /**
   * Number of MAC verification failures
   */
  public static final String SENSISION_CLASS_WEBCALL_KAFKA_IN_FAILEDMACS = "warp.webcall.kafka.in.failedmacs";
  
  /**
   * Number of unwrapping failures
   */
  public static final String SENSISION_CLASS_WEBCALL_KAFKA_IN_FAILEDDECRYPTS = "warp.webcall.kafka.in.faileddecrypts";
  
  /**
   * Total latency of WebCallRequests handling
   */
  public static final String SENSISION_CLASS_WEBCALL_LATENCY_MS = "warp.webcall.latency.ms";
  
  /**
   * Number of offset commits
   */
  public static final String SENSISION_CLASS_WEBCALL_KAFKA_IN_COMMITS = "warp.webcall.kafka.in.commits";
  
  /**
   * Number of consuming loop abortions
   */
  public static final String SENSISION_CLASS_WEBCALL_IN_ABORTS = "warp.webcall.kafka.in.aborts";
  
  /**
   * Number of barrier synchronizations
   */
  public static final String SENSISION_CLASS_WEBCALL_BARRIER_SYNCS = "warp.webcall.barrier.syncs";

  /**
   * Kafka Consumer Offset
   */
  public static final String SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET = "warp.kafka.consumer.offset";
  
  /**
   * Number of messages which were more than 1 message ahead of the previously consumed (or committed) message
   */
  public static final String SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET_FORWARD_LEAPS = "warp.kafka.consumer.forward.leaps";
  
  /**
   * Number of messages which were in the past relative to the previously committed message offset
   */
  public static final String SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET_BACKWARD_LEAPS = "warp.kafka.consumer.backward.leaps";
  
  //
  // ScriptRunner
  //

  /**
   * Number of messages consumed
   */
  public static final String SENSISION_CLASS_WARP_RUNNER_KAFKA_IN_MESSAGES = "warp.runner.kafka.in.messages";
  
  /**
   * Number of bytes consumed
   */
  public static final String SENSISION_CLASS_WARP_RUNNER_KAFKA_IN_BYTES = "warp.runner.kafka.in.bytes";
  
  /**
   * Number of invalid MACs for messages consumed by 'ScriptRunner'
   */
  public static final String SENSISION_CLASS_WARP_RUNNER_KAFKA_IN_INVALIDMACS = "warp.runner.kafka.in.invalidmacs";

  /**
   * Number of invalid AES wrappings for messages consumed by 'ScriptRunner'
   */
  public static final String SENSISION_CLASS_WARP_RUNNER_KAFKA_IN_INVALIDCIPHERS = "warp.runner.kafka.in.invalidciphers";
  
  /**
   * Number of execution attempts which were rejected by the ExecutorService
   */
  public static final String SENSISION_CLASS_WARP_RUNNER_REJECTIONS = "warp.runner.rejections";

  /**
   * Number of scripts which could not be executed by the ExecutorService due to too many rejections
   */
  public static final String SENSISION_CLASS_WARP_RUNNER_FAILURES = "warp.runner.failures";

  /**
   * Number of regions known by HBaseRegionKeys for the given table
   */
  public static final String SENSISION_CLASS_WARP_HBASE_KNOWNREGIONS = "warp.hbase.knownregions";
  
  /**
   * Number of Producer get from the producer pool
   */
  public static final String SENSISION_CLASS_CONTINUUM_RUNNER_KAFKA_PRODUCER_POOL_GET = "warp.runner.kafka.producer.pool.get";

  /**
   * Total number of nanoseconds spent waiting for a producer to be available
   */
  public static final String SENSISION_CLASS_CONTINUUM_RUNNER_KAFKA_PRODUCER_WAIT_NANOS = "warp.runner.kafka.producer.wait.nanos";

  /**
   * Number of tasks currently handled by a region server.
   */
  public static final String SENSISION_CLASS_WARP_HBASE_TASKS = "warp.hbase.tasks";
  
  //
  // Labels
  //
  
  /**
   * Id of the producer for which the metric is collected
   */
  public static final String SENSISION_LABEL_PRODUCER = "producer";
  
  /**
   * Name of application for which the metric is collected
   */
  public static final String SENSISION_LABEL_APPLICATION = "app";

  /**
   * Id of the owner for which the metric is collected
   */
  public static final String SENSISION_LABEL_OWNER = "owner";

  /**
   * Name of function for which a WarpScript metric is collected
   */
  public static final String SENSISION_LABEL_FUNCTION = "function";
  
  /**
   * Name of application consuming data
   */
  public static final String SENSISION_LABEL_CONSUMERAPP = "consumerapp";

  /**
   * Id of customer consuming data
   */
  public static final String SENSISION_LABEL_CONSUMERID = "consumer";

  /**
   * Thread id
   */  
  public static final String SENSISION_LABEL_THREAD = "thread";
  
  /**
   * CDN POP used to serve the request
   */
  public static final String SENSISION_LABEL_CDN = "cdn";

  /**
   * Path of script
   */
  public static final String SENSISION_LABEL_PATH = "path";

  /**
   * Kafka Topic
   */
  public static final String SENSISION_LABEL_TOPIC = "topic";

  /*
   * Kafka Partition
   */
  public static final String SENSISION_LABEL_PARTITION = "partition";
  
  /**
   * Kafka Group ID
   */
  public static final String SENSISION_LABEL_GROUPID = "groupid";
  
  /**
   * Name of GeoDirectory
   */
  public static final String SENSISION_LABEL_GEODIR = "geodir";

  /**
   * Type of something...
   */
  public static final String SENSISION_LABEL_TYPE = "type";

  /**
   * Id of something
   */
  public static final String SENSISION_LABEL_ID = "id";
  
  /**
   * Component
   */
  public static final String SENSISION_LABEL_COMPONENT = "component";

  /**
   * Server (usually RegionServer)
   */
  public static final String SENSISION_LABEL_SERVER = "server";
  
  /**
   * Table
   */
  public static final String SENSISION_LABEL_TABLE = "table";
  
  /**
   * Forwarder
   */
  public static final String SENSISION_LABEL_FORWARDER = "forwarder";
  
  //
  // TTLs (in ms)
  //
  
  public static final long SENSISION_TTL_PERUSER = 24 * 3600L * 1000L;
}
