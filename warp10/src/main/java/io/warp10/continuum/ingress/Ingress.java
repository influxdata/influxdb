//
//   Copyright 2019  SenX S.A.S.
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

package io.warp10.continuum.ingress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.sort.SortConfig;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

import io.warp10.SSLUtils;
import io.warp10.ThrowableUtils;
import io.warp10.WarpConfig;
import io.warp10.WarpManager;
import io.warp10.WarpURLDecoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.KafkaProducerPool;
import io.warp10.continuum.KafkaSynchronizedConsumerPool;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.TextFileShuffler;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.WarpException;
import io.warp10.continuum.egress.CORSHandler;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.egress.ThriftDirectoryClient;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.continuum.store.thrift.data.KafkaDataMessageType;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.IngressPlugin;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * This is the class which ingests metrics.
 */
public class Ingress extends AbstractHandler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Ingress.class);
  
  private static final Long NO_LAST_ACTIVITY = Long.MIN_VALUE;
  
  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    Configuration.INGRESS_HOST,
    Configuration.INGRESS_PORT,
    Configuration.INGRESS_ACCEPTORS,
    Configuration.INGRESS_SELECTORS,
    Configuration.INGRESS_IDLE_TIMEOUT,
    Configuration.INGRESS_JETTY_THREADPOOL,
    Configuration.INGRESS_ZK_QUORUM,
    Configuration.INGRESS_KAFKA_META_BROKERLIST,
    Configuration.INGRESS_KAFKA_META_TOPIC,
    Configuration.INGRESS_KAFKA_DATA_BROKERLIST,
    Configuration.INGRESS_KAFKA_DATA_TOPIC,
    Configuration.INGRESS_KAFKA_DATA_POOLSIZE,
    Configuration.INGRESS_KAFKA_METADATA_POOLSIZE,
    Configuration.INGRESS_KAFKA_DATA_MAXSIZE,
    Configuration.INGRESS_KAFKA_METADATA_MAXSIZE,
    Configuration.INGRESS_VALUE_MAXSIZE,
    Configuration.INGRESS_DELETE_SHUFFLE,
    Configuration.DIRECTORY_PSK,
  };
  
  private final String metaTopic;

  private final String dataTopic;

  private final DirectoryClient directoryClient;

  final long maxValueSize;

  final long ttl;
  
  final boolean useDatapointTs;
  
  private final String cacheDumpPath;
  
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();
  
  public final IngressPlugin plugin;
  
  /**
   * List of pending Kafka messages containing metadata (one per Thread)
   */
  private final ThreadLocal<List<KeyedMessage<byte[], byte[]>>> metadataMessages = new ThreadLocal<List<KeyedMessage<byte[], byte[]>>>() {
    protected java.util.List<kafka.producer.KeyedMessage<byte[],byte[]>> initialValue() {
      return new ArrayList<KeyedMessage<byte[], byte[]>>();
    };
  };
  
  /**
   * Byte size of metadataMessages
   */
  private ThreadLocal<AtomicLong> metadataMessagesSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong();
    };
  };

  /**
   * Size threshold after which we flush metadataMessages into Kafka
   */
  private final long METADATA_MESSAGES_THRESHOLD;
    
  /**
   * List of pending Kafka messages containing data
   */
  private final ThreadLocal<List<KeyedMessage<byte[], byte[]>>> dataMessages = new ThreadLocal<List<KeyedMessage<byte[], byte[]>>>() {
    protected java.util.List<kafka.producer.KeyedMessage<byte[],byte[]>> initialValue() {
      return new ArrayList<KeyedMessage<byte[], byte[]>>();
    };
  };
  
  /**
   * Byte size of dataMessages
   */
  private ThreadLocal<AtomicLong> dataMessagesSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong();
    };
  };
  
  /**
   * Size threshold after which we flush dataMessages into Kafka
   * 
   * This and METADATA_MESSAGES_THRESHOLD has to be lower than the configured Kafka max message size (message.max.bytes)
   */
  final long DATA_MESSAGES_THRESHOLD;
    
  /**
   * Kafka producer for readings
   */
  //private final Producer<byte[], byte[]> dataProducer;
  
  /**
   * Pool of producers for the 'data' topic
   */
  private final Producer<byte[], byte[]>[] dataProducers;
  
  private int dataProducersCurrentPoolSize = 0;
  
  /**
   * Pool of producers for the 'metadata' topic
   */
  private final KafkaProducerPool metaProducerPool;
  
  /**
   * Number of classId/labelsId to remember (to avoid pushing their metadata to Kafka)
   * Memory footprint is that of a BigInteger whose byte representation is 16 bytes, so probably
   * around 40 bytes
   * FIXME(hbs): need to compute exactly
   */
  private int METADATA_CACHE_SIZE = 10000000;
  
  /**
   * Cache used to determine if we should push metadata into Kafka or if it was previously seen.
   * Key is a BigInteger constructed from a byte array of classId+labelsId (we cannot use byte[] as map key)
   */
  final Map<BigInteger, Long> metadataCache = new LinkedHashMap<BigInteger, Long>(100, 0.75F, true) {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<BigInteger, Long> eldest) {
      return this.size() > METADATA_CACHE_SIZE;
    }
  };
  
  final KeyStore keystore;
  final Properties properties;
  
  final long[] classKey;
  final long[] labelsKey;
  
  final byte[] AES_KAFKA_META;
  final long[] SIPHASH_KAFKA_META;
  
  private final byte[] aesDataKey;
  private final long[] siphashDataKey;

  private final boolean sendMetadataOnDelete;
  private final boolean sendMetadataOnStore;

  private final KafkaSynchronizedConsumerPool  pool;
  
  private final boolean doShuffle;
  
  /**
   * Flag indicating whether or not we reject delete requests
   */
  private final boolean rejectDelete;
  
  final boolean activityTracking;
  final boolean updateActivity;
  private final boolean metaActivity;
  final long activityWindow;
  public final boolean parseAttributes;
  final Long maxpastDefault;
  final Long maxfutureDefault;
  final Long maxpastOverride;
  final Long maxfutureOverride;

  final boolean ignoreOutOfRange;
  
  final boolean allowDeltaAttributes;
  
  public Ingress(KeyStore keystore, Properties props) {

    //
    // Enable the ThrottlingManager
    //
    
    ThrottlingManager.enable();
    
    this.keystore = keystore;
    this.properties = props;
    
    //
    // Make sure all required configuration is present
    //
    
    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(props.getProperty(required), "Missing configuration parameter '%s'.", required);          
    }

    //
    // Extract parameters from 'props'
    //
    
    this.allowDeltaAttributes = "true".equals(props.getProperty(Configuration.INGRESS_ATTRIBUTES_ALLOWDELTA));
    this.ttl = Long.parseLong(props.getProperty(Configuration.INGRESS_HBASE_CELLTTL, "-1"));
    this.useDatapointTs = "true".equals(props.getProperty(Configuration.INGRESS_HBASE_DPTS));
    
    this.doShuffle = "true".equals(props.getProperty(Configuration.INGRESS_DELETE_SHUFFLE));
    
    this.rejectDelete = "true".equals(props.getProperty(Configuration.INGRESS_DELETE_REJECT));
        
    this.activityWindow = Long.parseLong(properties.getProperty(Configuration.INGRESS_ACTIVITY_WINDOW, "0"));
    this.activityTracking = this.activityWindow > 0;
    this.updateActivity = "true".equals(props.getProperty(Configuration.INGRESS_ACTIVITY_UPDATE));
    this.metaActivity = "true".equals(props.getProperty(Configuration.INGRESS_ACTIVITY_META));

    if (props.containsKey(Configuration.INGRESS_CACHE_DUMP_PATH)) {
      this.cacheDumpPath = props.getProperty(Configuration.INGRESS_CACHE_DUMP_PATH);
    } else {
      this.cacheDumpPath = null;
    }
    
    if (null != props.getProperty(Configuration.INGRESS_METADATA_CACHE_SIZE)) {
      this.METADATA_CACHE_SIZE = Integer.valueOf(props.getProperty(Configuration.INGRESS_METADATA_CACHE_SIZE));
    }
    
    this.metaTopic = props.getProperty(Configuration.INGRESS_KAFKA_META_TOPIC);
    
    this.dataTopic = props.getProperty(Configuration.INGRESS_KAFKA_DATA_TOPIC);

    this.DATA_MESSAGES_THRESHOLD = Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_DATA_MAXSIZE));
    this.METADATA_MESSAGES_THRESHOLD = Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_METADATA_MAXSIZE));
    this.maxValueSize = Long.parseLong(props.getProperty(Configuration.INGRESS_VALUE_MAXSIZE));
    
    if (this.maxValueSize > (this.DATA_MESSAGES_THRESHOLD / 2) - 64) {
      throw new RuntimeException("Value of '" + Configuration.INGRESS_VALUE_MAXSIZE + "' cannot exceed half of '" + Configuration.INGRESS_KAFKA_DATA_MAXSIZE + "' minus 64.");
    }
    
    extractKeys(this.keystore, props);
    
    this.classKey = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_CLASS));
    this.labelsKey = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_LABELS));
    
    this.AES_KAFKA_META = this.keystore.getKey(KeyStore.AES_KAFKA_METADATA);
    this.SIPHASH_KAFKA_META = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_KAFKA_METADATA));
    
    this.aesDataKey = this.keystore.getKey(KeyStore.AES_KAFKA_DATA);
    this.siphashDataKey = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_KAFKA_DATA));    

    this.sendMetadataOnDelete = Boolean.parseBoolean(props.getProperty(Configuration.INGRESS_DELETE_METADATA_INCLUDE, "false"));
    this.sendMetadataOnStore = Boolean.parseBoolean(props.getProperty(Configuration.INGRESS_STORE_METADATA_INCLUDE, "false"));

    this.parseAttributes = "true".equals(props.getProperty(Configuration.INGRESS_PARSE_ATTRIBUTES));
    
    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_DEFAULT)) {
      maxpastDefault = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_DEFAULT));
      if (maxpastDefault < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXPAST_DEFAULT + "' MUST be positive.");
      }
    } else {
      maxpastDefault = null;
    }
    
    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_DEFAULT)) {
      maxfutureDefault = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_DEFAULT));
      if (maxfutureDefault < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXFUTURE_DEFAULT + "' MUST be positive.");
      }
    } else {
      maxfutureDefault = null;
    }

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_OVERRIDE)) {
      maxpastOverride = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_OVERRIDE));
      if (maxpastOverride < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXPAST_OVERRIDE + "' MUST be positive.");
      }
    } else {
      maxpastOverride = null;
    }
    
    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_OVERRIDE)) {
      maxfutureOverride = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_OVERRIDE));
      if (maxfutureOverride < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXFUTURE_OVERRIDE + "' MUST be positive.");
      }
    } else {
      maxfutureOverride = null;
    }
    
    this.ignoreOutOfRange = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_OUTOFRANGE_IGNORE));

    //
    // Prepare meta, data and delete producers
    //
    
    Properties metaProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    metaProps.setProperty("metadata.broker.list", props.getProperty(Configuration.INGRESS_KAFKA_META_BROKERLIST));
    if (null != props.getProperty(Configuration.INGRESS_KAFKA_META_PRODUCER_CLIENTID)) {
      metaProps.setProperty("client.id", props.getProperty(Configuration.INGRESS_KAFKA_META_PRODUCER_CLIENTID));
    }
    metaProps.setProperty("request.required.acks", "-1");
    // TODO(hbs): when we move to the new KafkaProducer API
    //metaProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    metaProps.setProperty("producer.type","sync");
    metaProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    metaProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    //??? metaProps.setProperty("block.on.buffer.full", "true");
    
    // FIXME(hbs): compression does not work
    //metaProps.setProperty("compression.codec", "snappy");
    //metaProps.setProperty("client.id","");

    ProducerConfig metaConfig = new ProducerConfig(metaProps);
    
    this.metaProducerPool = new KafkaProducerPool(metaConfig,
        Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_METADATA_POOLSIZE)),
        SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_POOL_GET,
        SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_WAIT_NANO);
    
    Properties dataProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    dataProps.setProperty("metadata.broker.list", props.getProperty(Configuration.INGRESS_KAFKA_DATA_BROKERLIST));
    if (null != props.getProperty(Configuration.INGRESS_KAFKA_DATA_PRODUCER_CLIENTID)) {
      dataProps.setProperty("client.id", props.getProperty(Configuration.INGRESS_KAFKA_DATA_PRODUCER_CLIENTID));
    }
    dataProps.setProperty("request.required.acks", "-1");
    dataProps.setProperty("producer.type","sync");
    dataProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    dataProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    // TODO(hbs): when we move to the new KafkaProducer API
    //dataProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    
    if (null != props.getProperty(Configuration.INGRESS_KAFKA_DATA_REQUEST_TIMEOUT_MS)) {
      dataProps.setProperty("request.timeout.ms", props.getProperty(Configuration.INGRESS_KAFKA_DATA_REQUEST_TIMEOUT_MS));
    }
    
    if (null != props.getProperty(Configuration.INGRESS_PLUGIN_CLASS)) {
      try {
        ClassLoader pluginCL = this.getClass().getClassLoader();

        Class pluginClass = Class.forName(properties.getProperty(Configuration.INGRESS_PLUGIN_CLASS), true, pluginCL);
        this.plugin = (IngressPlugin) pluginClass.newInstance();
        
        //
        // Now call the 'init' method of the plugin
        //
        
        this.plugin.init();
      } catch (Exception e) {
        throw new RuntimeException("Unable to instantiate plugin class", e);
      }
    } else {
      this.plugin = null;
    }
    
    ///???? dataProps.setProperty("block.on.buffer.full", "true");
    
    // FIXME(hbs): compression does not work
    //dataProps.setProperty("compression.codec", "snappy");
    //dataProps.setProperty("client.id","");

    ProducerConfig dataConfig = new ProducerConfig(dataProps);

    //this.dataProducer = new Producer<byte[], byte[]>(dataConfig);

    //
    // Allocate producer pool
    //
    
    this.dataProducers = new Producer[Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_DATA_POOLSIZE))];
    
    for (int i = 0; i < dataProducers.length; i++) {
      this.dataProducers[i] = new Producer<byte[], byte[]>(dataConfig);
    }
    
    this.dataProducersCurrentPoolSize = this.dataProducers.length;
    
    //
    // Producer for the Delete topic
    //

    /*
    Properties deleteProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    deleteProps.setProperty("metadata.broker.list", props.getProperty(INGRESS_KAFKA_DELETE_BROKERLIST));
    deleteProps.setProperty("request.required.acks", "-1");
    deleteProps.setProperty("producer.type","sync");
    deleteProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    deleteProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    
    ProducerConfig deleteConfig = new ProducerConfig(deleteProps);
    this.deleteProducer = new Producer<byte[], byte[]>(deleteConfig);
*/
  
    //
    // Attempt to load the cache file (we do that prior to starting the Kafka consumer)
    //
    
    loadCache();

    //
    // Create Kafka consumer to handle Metadata deletions
    //
    
    ConsumerFactory metadataConsumerFactory = new IngressMetadataConsumerFactory(this);
    
    if (props.containsKey(Configuration.INGRESS_KAFKA_META_GROUPID)) {
      pool = new KafkaSynchronizedConsumerPool(props.getProperty(Configuration.INGRESS_KAFKA_META_ZKCONNECT),
          props.getProperty(Configuration.INGRESS_KAFKA_META_TOPIC),
          props.getProperty(Configuration.INGRESS_KAFKA_META_CONSUMER_CLIENTID),
          props.getProperty(Configuration.INGRESS_KAFKA_META_GROUPID),
          props.getProperty(Configuration.INGRESS_KAFKA_META_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY),
          props.getProperty(Configuration.INGRESS_KAFKA_META_CONSUMER_AUTO_OFFSET_RESET),
          Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_META_NTHREADS)),
          Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_META_COMMITPERIOD)),
          metadataConsumerFactory);          
    } else {
      pool = null;
    }

    //
    // Initialize ThriftDirectoryService
    //
    
    try {
      this.directoryClient = new ThriftDirectoryClient(this.keystore, props);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    //
    // Register shutdown hook
    //
    
    final Ingress self = this;
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        //
        // Make sure the Kakfa consumers are stopped so we don't miss deletions
        // when restarting and using the cache we are about to store
        //
        
        if (null != self.pool) {
          self.pool.shutdown();
          
          LOG.info("Waiting for Ingress Kafka consumers to stop.");
          
          while(!self.pool.isStopped()) {
            LockSupport.parkNanos(250000000L);
          }
          
          LOG.info("Kafka consumers stopped, dumping GTS cache");          
        }
        
        
        self.dumpCache();
      }
    });
    
    //
    // Make sure ShutdownHookManager is initialized, otherwise it will try to
    // register a shutdown hook during the shutdown hook we just registered...
    //
    
    ShutdownHookManager.get();      

    //
    // Start Jetty server
    //
    
    int maxThreads = Integer.parseInt(props.getProperty(Configuration.INGRESS_JETTY_THREADPOOL));
    
    boolean enableStreamUpdate = !("true".equals(props.getProperty(Configuration.WARP_STREAMUPDATE_DISABLE)));

    BlockingArrayQueue<Runnable> queue = null;
    
    if (props.containsKey(Configuration.INGRESS_JETTY_MAXQUEUESIZE)) {
      int queuesize = Integer.parseInt(props.getProperty(Configuration.INGRESS_JETTY_MAXQUEUESIZE));
      queue = new BlockingArrayQueue<Runnable>(queuesize);
    }
    
    Server server = new Server(new QueuedThreadPool(maxThreads,8, 60000, queue));
    
    List<Connector> connectors = new ArrayList<Connector>();

    boolean useHttp = null != props.getProperty(Configuration.INGRESS_PORT);
    boolean useHttps = null != props.getProperty(Configuration.INGRESS_PREFIX + Configuration._SSL_PORT);
    
    if (useHttp) {
      int port = Integer.valueOf(props.getProperty(Configuration.INGRESS_PORT));
      String host = props.getProperty(Configuration.INGRESS_HOST);
      int tcpBacklog = Integer.valueOf(props.getProperty(Configuration.INGRESS_TCP_BACKLOG, "0"));
      int acceptors = Integer.valueOf(props.getProperty(Configuration.INGRESS_ACCEPTORS));
      int selectors = Integer.valueOf(props.getProperty(Configuration.INGRESS_SELECTORS));
      long idleTimeout = Long.parseLong(props.getProperty(Configuration.INGRESS_IDLE_TIMEOUT));
      
      ServerConnector connector = new ServerConnector(server, acceptors, selectors);
      connector.setIdleTimeout(idleTimeout);
      connector.setPort(port);
      connector.setHost(host);
      connector.setAcceptQueueSize(tcpBacklog);
      connector.setName("Continuum Ingress HTTP");
      
      connectors.add(connector);
    }
    
    if (useHttps) {
      ServerConnector connector = SSLUtils.getConnector(server, Configuration.INGRESS_PREFIX);
      connector.setName("Continuum Ingress HTTPS");
      connectors.add(connector);
    }
    
    server.setConnectors(connectors.toArray(new Connector[connectors.size()]));

    HandlerList handlers = new HandlerList();
    
    Handler cors = new CORSHandler();
    handlers.addHandler(cors);

    handlers.addHandler(this);
    
    if (enableStreamUpdate) {
      IngressStreamUpdateHandler suHandler = new IngressStreamUpdateHandler(this);
      handlers.addHandler(suHandler);
    }
    
    server.setHandler(handlers);
    
    JettyUtil.setSendServerVersion(server, false);

    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("Continuum Ingress");
    t.start();
  }

  @Override
  public void run() {
    //
    // Register in ZK and watch parent znode.
    // If the Ingress count exceeds the licensed number,
    // exit if we are the first of the list.
    //
    
    while(true) {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException ie) {        
      }
    }
  }
  
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    
    String token = null;
    
    if (target.equals(Constants.API_ENDPOINT_UPDATE)) {
      baseRequest.setHandled(true);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_REQUESTS, Sensision.EMPTY_LABELS, 1);
    } else if (target.startsWith(Constants.API_ENDPOINT_UPDATE + "/")) {
      baseRequest.setHandled(true);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_REQUESTS, Sensision.EMPTY_LABELS, 1);
      token = target.substring(Constants.API_ENDPOINT_UPDATE.length() + 1);
    } else if (target.equals(Constants.API_ENDPOINT_META)) {
      handleMeta(target, baseRequest, request, response);
      return;
    } else if (target.equals(Constants.API_ENDPOINT_DELETE)) {
      handleDelete(target, baseRequest, request, response);
      return;
    } else if (Constants.API_ENDPOINT_CHECK.equals(target)) {
      baseRequest.setHandled(true);
      response.setStatus(HttpServletResponse.SC_OK);
      return;
    } else {
      return;
    }
    
    if (null != WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)));
      return;
    }
    
    long nowms = System.currentTimeMillis();
    
    try {
      //
      // CORS header
      //
      
      response.setHeader("Access-Control-Allow-Origin", "*");

      long nano = System.nanoTime();
      
      //
      // TODO(hbs): Extract producer/owner from token
      //
      
      if (null == token) {
        token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
      }
      
      WriteToken writeToken;
      
      try {
        writeToken = Tokens.extractWriteToken(token);
        if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOUPDATE)) {
          throw new WarpScriptException("Token cannot be used for updating data.");
        }
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }
      
      String application = writeToken.getAppName();
      String producer = Tokens.getUUID(writeToken.getProducerId());
      String owner = Tokens.getUUID(writeToken.getOwnerId());
        
      Map<String,String> sensisionLabels = new HashMap<String,String>();
      sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      long count = 0;
      
      //
      // Extract KafkaDataMessage attributes
      //
      
      Map<String,String> kafkaDataMessageAttributes = null;
      
      if (-1 != ttl || useDatapointTs) {
        kafkaDataMessageAttributes = new HashMap<String,String>();
        if (-1 != ttl) {
          kafkaDataMessageAttributes.put(Constants.STORE_ATTR_TTL, Long.toString(ttl));
        }
        if (useDatapointTs) {
          kafkaDataMessageAttributes.put(Constants.STORE_ATTR_USEDATAPOINTTS, "t");
        }
      }
      
      boolean expose = false;
      
      if (writeToken.getAttributesSize() > 0) {
        
        expose = writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);
        
        if (writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_TTL)
            || writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_DPTS)) {
          if (null == kafkaDataMessageAttributes) {
            kafkaDataMessageAttributes = new HashMap<String,String>();
          }
          if (writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_TTL)) {
            kafkaDataMessageAttributes.put(Constants.STORE_ATTR_TTL, writeToken.getAttributes().get(Constants.TOKEN_ATTR_TTL));
          }
          if (writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_DPTS)) {
            kafkaDataMessageAttributes.put(Constants.STORE_ATTR_USEDATAPOINTTS, writeToken.getAttributes().get(Constants.TOKEN_ATTR_DPTS));
          }
        }
      }

      try {
        if (null == producer || null == owner) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_INVALIDTOKEN, Sensision.EMPTY_LABELS, 1);
          response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
          return;
        }
                
        //
        // Build extra labels
        //
        
        Map<String,String> extraLabels = new HashMap<String,String>();
        
        // Add labels from the WriteToken if they exist
        if (writeToken.getLabelsSize() > 0) {
          extraLabels.putAll(writeToken.getLabels());
        }
        
        // Force internal labels
        extraLabels.put(Constants.PRODUCER_LABEL, producer);
        extraLabels.put(Constants.OWNER_LABEL, owner);

        // FIXME(hbs): remove me
        if (null != application) {
          extraLabels.put(Constants.APPLICATION_LABEL, application);
          sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
        }

        long maxsize = maxValueSize;
        
        if (writeToken.getAttributesSize() > 0 && null != writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXSIZE)) {
          maxsize = Long.parseLong(writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXSIZE));
          if (maxsize > (DATA_MESSAGES_THRESHOLD / 2) - 64) {
            maxsize = (DATA_MESSAGES_THRESHOLD / 2) - 64;
          }
        }
        
        //
        // Determine if content is gzipped
        //

        boolean gzipped = false;
            
        if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {      
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_GZIPPED, sensisionLabels, 1);
          gzipped = true;
        }
        
        BufferedReader br = null;
            
        if (gzipped) {
          GZIPInputStream is = new GZIPInputStream(request.getInputStream());
          br = new BufferedReader(new InputStreamReader(is));
        } else {    
          br = request.getReader();
        }
        
        Long now = TimeSource.getTime();

        //
        // Extract time limits
        //
        
        Long maxpast = null;
        if (null != maxpastDefault) {
          try {
            maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, maxpastDefault));            
          } catch (ArithmeticException ae) {
            maxpast = null;
          }
        }
        
        Long maxfuture = null;
        if (null != maxfutureDefault) {
          try {
            maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, maxfutureDefault));
          } catch (ArithmeticException ae) {
            maxfuture = null;
          }
        }
 
        Boolean ignoor = null;
        
        if (writeToken.getAttributesSize() > 0) {
          
          if (writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_IGNOOR)) {
            String v = writeToken.getAttributes().get(Constants.TOKEN_ATTR_IGNOOR).toLowerCase();
            if ("true".equals(v) || "t".equals(v)) {
              ignoor = Boolean.TRUE;
            } else if ("false".equals(v) || "f".equals(v)) {
              ignoor = Boolean.FALSE;
            }
          }

          String deltastr = writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXPAST);

          if (null != deltastr) {
            long delta = Long.parseLong(deltastr);
            if (delta < 0) {
              throw new WarpScriptException("Invalid '" + Constants.TOKEN_ATTR_MAXPAST + "' token attribute, MUST be positive.");
            }
            try {
              maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, delta));
            } catch (ArithmeticException ae) {
              maxpast = null;
            }
          }
          
          deltastr = writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXFUTURE);
          
          if (null != deltastr) {
            long delta = Long.parseLong(deltastr);
            if (delta < 0) {
              throw new WarpScriptException("Invalid '" + Constants.TOKEN_ATTR_MAXFUTURE + "' token attribute, MUST be positive.");
            }
            try {
              maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, delta));
            } catch (ArithmeticException ae) {
              maxfuture = null;
            }
          }          
        }

        if (null != maxpastOverride) {
          try {
            maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, maxpastOverride));
          } catch (ArithmeticException ae) {
            maxpast = null;
          }
        }

        if (null != maxfutureOverride) {
          try {
            maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, maxfutureOverride));
          } catch (ArithmeticException ae) {
            maxfuture = null;
          }
        }

        //
        // Check the value of the 'now' header
        //
        // The following values are supported:
        //
        // A number, which will be interpreted as an absolute time reference,
        // i.e. a number of time units since the Epoch.
        //
        // A number prefixed by '+' or '-' which will be interpreted as a
        // delta from the present time.
        //
        // A '*' which will mean to not set 'now', and to recompute its value
        // each time it's needed.
        //
        
        String nowstr = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX));
        
        if (null != nowstr) {
          if ("*".equals(nowstr)) {
            now = null;
          } else if (nowstr.startsWith("+")) {
            try {
              long delta = Long.parseLong(nowstr.substring(1));
              now = now + delta;
            } catch (Exception e) {
              throw new IOException("Invalid base timestamp.");
            }                  
          } else if (nowstr.startsWith("-")) {
            try {
              long delta = Long.parseLong(nowstr.substring(1));
              now = now - delta;
            } catch (Exception e) {
              throw new IOException("Invalid base timestamp.");
            }                            
          } else {
            try {
              now = Long.parseLong(nowstr);
            } catch (Exception e) {
              throw new IOException("Invalid base timestamp.");
            }                  
          }
        }

        boolean deltaAttributes = "delta".equals(request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES)));

        if (deltaAttributes && !this.allowDeltaAttributes) {
          throw new IOException("Delta update of attributes is disabled.");
        }
        
        //
        // Loop on all lines
        //
        
        GTSEncoder lastencoder = null;
        GTSEncoder encoder = null;
        
        byte[] bytes = new byte[16];
        
        int idx = 0;
        
        AtomicLong dms = this.dataMessagesSize.get();
        
        // Atomic boolean to track if attributes were parsed
        AtomicBoolean hadAttributes = parseAttributes ? new AtomicBoolean(false) : null;

        boolean lastHadAttributes = false;

        AtomicLong ignoredCount = null;
        
        if ((this.ignoreOutOfRange && !Boolean.FALSE.equals(ignoor)) || Boolean.TRUE.equals(ignoor)) {
          ignoredCount = new AtomicLong(0L);
        }
        
        do {
          
          // We copy the current value of hadAttributes
          if (parseAttributes) {
            lastHadAttributes = lastHadAttributes || hadAttributes.get();
            hadAttributes.set(false);
          }
          
          String line = br.readLine();
          
          if (null == line) {
            break;
          }
        
          if ("".equals(line)) {
            continue;
          }
          
          // Skip lines which start with '#'
          if ('#' == line.charAt(0)) {
            continue;
          }
                    
          try {
            encoder = GTSHelper.parse(lastencoder, line, extraLabels, now, maxsize, hadAttributes, maxpast, maxfuture, ignoredCount, deltaAttributes);
            if (null != this.plugin) {
              GTSEncoder enc = encoder;
              if (!this.plugin.update(this, writeToken, line, encoder)) {                
                hadAttributes.set(false);
                continue;
              }
            }
            count++;
          } catch (ParseException pe) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_PARSEERRORS, sensisionLabels, 1);
            throw new IOException("Parse error at '" + line + "'", pe);
          }
                  
          if (encoder != lastencoder || dms.get() + 16 + lastencoder.size() > DATA_MESSAGES_THRESHOLD) {
            //
            // Determine if we should push the metadata or not
            //
            
            encoder.setClassId(GTSHelper.classId(this.classKey, encoder.getMetadata().getName()));
            encoder.setLabelsId(GTSHelper.labelsId(this.labelsKey, encoder.getMetadata().getLabels()));

            GTSHelper.fillGTSIds(bytes, 0, encoder.getClassId(), encoder.getLabelsId());

            BigInteger metadataCacheKey = new BigInteger(bytes);

            //
            // Check throttling
            //
            
            if (null != lastencoder && lastencoder.size() > 0) {
              ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId(), expose);
              ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount(), expose);
            }
            
            boolean pushMeta = false;
            
            Long val = this.metadataCache.getOrDefault(metadataCacheKey, NO_LAST_ACTIVITY);

            if (NO_LAST_ACTIVITY.equals(val)) {
              pushMeta = true;
            } else if (activityTracking && updateActivity) {
              Long lastActivity = val;
              
              if (null == lastActivity) {
                pushMeta = true;
              } else if (nowms - lastActivity > activityWindow) {
                pushMeta = true;
              }
            }
            
            if (pushMeta) {
              // Build metadata object to push
              Metadata metadata = new Metadata();
              // Set source to indicate we
              metadata.setSource(Configuration.INGRESS_METADATA_SOURCE);
              metadata.setName(encoder.getMetadata().getName());
              metadata.setLabels(encoder.getMetadata().getLabels());
              
              if (this.activityTracking && updateActivity) {
                metadata.setLastActivity(nowms);
              }
              
              TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
              try {
                pushMetadataMessage(bytes, serializer.serialize(metadata));
                
                // Update metadataCache with the current key
                synchronized(metadataCache) {
                  this.metadataCache.put(metadataCacheKey, (activityTracking && updateActivity) ? nowms : null);
                }
              } catch (TException te) {
                throw new IOException("Unable to push metadata.");
              }
            }
            
            if (null != lastencoder) {
              pushDataMessage(lastencoder, kafkaDataMessageAttributes);
                           
              if (parseAttributes && lastHadAttributes) {
                // We need to push lastencoder's metadata update as they were updated since the last
                // metadata update message sent
                Metadata meta = new Metadata(lastencoder.getMetadata());
                if (deltaAttributes) {
                  meta.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);                  
                } else {
                  meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                }
                pushMetadataMessage(meta);
                // Reset lastHadAttributes
                lastHadAttributes = false;
              }
            }
            
            if (encoder != lastencoder) {
              // This is the case when we just parsed either the first input line or one for a different
              // GTS than the previous one.
              lastencoder = encoder;
            } else {
              // This is the case when lastencoder and encoder are identical, but lastencoder was too big and needed
              // to be flushed

              //lastencoder = null;
              //
              // Allocate a new GTSEncoder and reuse Metadata so we can
              // correctly handle a continuation line if this is what occurs next
              //
              Metadata metadata = lastencoder.getMetadata();
              lastencoder = new GTSEncoder(0L);
              lastencoder.setMetadata(metadata);              
            }
          }

          if (0 == count % 1000) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_GLOBAL, Sensision.EMPTY_LABELS, count);
            count = 0;
          }
        } while (true); 
        
        if (null != lastencoder && lastencoder.size() > 0) {
          ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId(), expose);
          ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount(), expose);

          pushDataMessage(lastencoder, kafkaDataMessageAttributes);
          
          if (parseAttributes && lastHadAttributes) {
            // Push a metadata UPDATE message so attributes are stored
            // Build metadata object to push
            Metadata meta = new Metadata(lastencoder.getMetadata());
            if (deltaAttributes) {
              meta.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);              
            } else {
              meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
            }
            pushMetadataMessage(meta);
          }
        }
      } catch (WarpException we) {
        throw new IOException(we);      
      } finally {
        //
        // Flush message buffers into Kafka
        //
        
        pushMetadataMessage(null, null);
        pushDataMessage(null, kafkaDataMessageAttributes);

        if (null != this.plugin) {
          this.plugin.flush(this);
        }
        
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_GLOBAL, Sensision.EMPTY_LABELS, count);
        
        long micros = (System.nanoTime() - nano) / 1000L;
        
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_TIME_US, sensisionLabels, micros);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_TIME_US_GLOBAL, Sensision.EMPTY_LABELS, micros);      
      }      
    } catch (Throwable t) { // Catch everything else this handler could return 200 on a OOM exception
      if (!response.isCommitted()) {
        String prefix = "Error when updating data: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
        return;
      }
    }
    
    response.setStatus(HttpServletResponse.SC_OK);
  }
  
  /**
   * Handle Metadata updating
   */
  public void handleMeta(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        
    if (target.equals(Constants.API_ENDPOINT_META)) {
      baseRequest.setHandled(true);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_REQUESTS, Sensision.EMPTY_LABELS, 1);
    } else {
      return;
    }
    
    if (null != WarpManager.getAttribute(WarpManager.META_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.META_DISABLED)));
      return;
    }

    //
    // CORS header
    //
    
    response.setHeader("Access-Control-Allow-Origin", "*");

    //
    // Loop over the input lines.
    // Each has the following format:
    //
    // class{labels}{attributes}
    //
    
    boolean deltaAttributes = "delta".equals(request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES)));

    if (deltaAttributes && !this.allowDeltaAttributes) {
      throw new IOException("Delta update of attributes is disabled.");
    }
    
    //
    // TODO(hbs): Extract producer/owner from token
    //
    
    String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
        
    WriteToken writeToken;
    
    try {
      try {
        writeToken = Tokens.extractWriteToken(token);
        if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOMETA)) {
          throw new WarpScriptException("Token cannot be used for updating metadata.");
        }
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }
      
      String application = writeToken.getAppName();
      String producer = Tokens.getUUID(writeToken.getProducerId());
      String owner = Tokens.getUUID(writeToken.getOwnerId());
        
      if (null == producer || null == owner) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALIDTOKEN, Sensision.EMPTY_LABELS, 1);      
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }

      Map<String,String> sensisionLabels = new HashMap<String,String>();
      sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      if (null != application) {
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }
      
      long count = 0;
           
      //
      // Determine if content if gzipped
      //

      boolean gzipped = false;
          
      if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {       
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_REQUESTS, Sensision.EMPTY_LABELS, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_GZIPPED, sensisionLabels, 1);     
        gzipped = true;
      }
      
      BufferedReader br = null;
          
      if (gzipped) {
        GZIPInputStream is = new GZIPInputStream(request.getInputStream());
        br = new BufferedReader(new InputStreamReader(is));
      } else {    
        br = request.getReader();
      }
      
      long nowms = System.currentTimeMillis();
      
      //
      // Loop on all lines
      //

      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        //
        // Ignore blank lines
        //
        
        if ("".equals(line)) {
          continue;
        }
        
        // Skip lines which start with '#'
        if ('#' == line.charAt(0)) {
          continue;
        }

        Metadata metadata = MetadataUtils.parseMetadata(line);
        
        if (null == metadata) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALID, sensisionLabels, 1);
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid metadata " + line);
          return;
        }
        
        // Add labels from the WriteToken if they exist
        if (writeToken.getLabelsSize() > 0) {
          metadata.getLabels().putAll(writeToken.getLabels());
        }

        //
        // Force owner/producer
        //
        
        metadata.getLabels().put(Constants.PRODUCER_LABEL, producer);
        metadata.getLabels().put(Constants.OWNER_LABEL, owner);
      
        if (null != application) {
          metadata.getLabels().put(Constants.APPLICATION_LABEL, application);
        }

        if (!MetadataUtils.validateMetadata(metadata)) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALID, sensisionLabels, 1);
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid metadata " + line);
          return;
        }
        
        count++;

        if (deltaAttributes) {
          metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);
        } else {
          metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);          
        }
        
        try {
          // We do not take into consideration this activity timestamp in the cache
          // this way we do not allocate BigIntegers
          if (activityTracking && metaActivity) {
            metadata.setLastActivity(nowms);
          }
          if (null != this.plugin) {
            if (!this.plugin.meta(this, writeToken, line, metadata)) {
              continue;
            }
          }
          pushMetadataMessage(metadata);
        } catch (Exception e) {
          throw new IOException("Unable to push metadata");
        }
      }

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_RECORDS, sensisionLabels, count);
    } catch (Throwable t) { // Catch everything else this handler could return 200 on a OOM exception
      if (!response.isCommitted()) {
        String prefix = "Error when updating meta: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
        return;
      }
    } finally {
      //
      // Flush message buffers into Kafka
      //
    
      pushMetadataMessage(null, null);
      
      if (null != this.plugin) {
        this.plugin.flush(this);
      }
    }
  
    response.setStatus(HttpServletResponse.SC_OK);
  }

  public void handleDelete(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (target.equals(Constants.API_ENDPOINT_DELETE)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }    
    
    if (this.rejectDelete) {
      throw new IOException(Constants.API_ENDPOINT_DELETE + " endpoint is not activated.");
    }
    
    if (null != WarpManager.getAttribute(WarpManager.DELETE_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.DELETE_DISABLED)));
      return;
    }

    //
    // CORS header
    //
    
    response.setHeader("Access-Control-Allow-Origin", "*");

    long nano = System.nanoTime();
    
    //
    // Extract token infos
    //
    
    String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
            
    WriteToken writeToken;
    
    try {
      writeToken = Tokens.extractWriteToken(token);
      if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NODELETE)) {
        throw new WarpScriptException("Token cannot be used for deletions.");
      }
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }
    
    
    String application = writeToken.getAppName();
    String producer = Tokens.getUUID(writeToken.getProducerId());
    String owner = Tokens.getUUID(writeToken.getOwnerId());
      
    //
    // For delete operations, producer and owner MUST be equal
    //
    
    if (!producer.equals(owner)) {
      throw new IOException("Invalid write token for deletion.");
    }
    
    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

    long count = 0;
    long gts = 0;
    
    boolean completeDeletion = false;

    boolean dryrun = null != request.getParameter(Constants.HTTP_PARAM_DRYRUN);
    
    boolean showErrors = null != request.getParameter(Constants.HTTP_PARAM_SHOW_ERRORS);    

    PrintWriter pw = null;
    
    boolean metaonly = null != request.getParameter(Constants.HTTP_PARAM_METAONLY);

    try {      
      if (null == producer || null == owner) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }
      
      //
      // Build extra labels
      //
      
      Map<String,String> extraLabels = new HashMap<String,String>();

      // Add extra labels, remove producer,owner,app
      if (writeToken.getLabelsSize() > 0) {
        extraLabels.putAll(writeToken.getLabels());
        extraLabels.remove(Constants.PRODUCER_LABEL);
        extraLabels.remove(Constants.OWNER_LABEL);
        extraLabels.remove(Constants.APPLICATION_LABEL);
      }

      //
      // Only set owner and potentially app, producer may vary
      //      
      extraLabels.put(Constants.OWNER_LABEL, owner);
      if (null != application) {
        extraLabels.put(Constants.APPLICATION_LABEL, application);
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      boolean hasRange = false;
      
      //
      // Extract start/end
      //
      
      String startstr = request.getParameter(Constants.HTTP_PARAM_START);
      String endstr = request.getParameter(Constants.HTTP_PARAM_END);
      
      String minagestr = request.getParameter(Constants.HTTP_PARAM_MINAGE);
      
      long start = Long.MIN_VALUE;
      long end = Long.MAX_VALUE;
      
      long minage = 0L;

      if (null != minagestr) {        
        if (metaonly) {
          throw new IOException("Parameter '" + Constants.HTTP_PARAM_MINAGE + "' cannot be specified with '" + Constants.HTTP_PARAM_METAONLY + "'.");
        }
        
        minage = Long.parseLong(minagestr);
        
        if (minage < 0) {
          throw new IOException("Invalid value for '" + Constants.HTTP_PARAM_MINAGE + "', expected a number of ms >= 0");
        }
      }
      
      if (null != startstr) {
        if (null == endstr) {
          throw new IOException("Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
        }
        if (startstr.contains("T")) {
          start = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(startstr);
        } else {
          start = Long.valueOf(startstr);
        }
      }
      
      if (null != endstr) {
        if (null == startstr) {
          throw new IOException("Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
        }
        if (endstr.contains("T")) {
          end = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(endstr);
        } else {
          end = Long.valueOf(endstr);
        }
      }

      if (Long.MIN_VALUE == start && Long.MAX_VALUE == end && (null == request.getParameter(Constants.HTTP_PARAM_DELETEALL) && !metaonly)) {
        throw new IOException("Parameter " + Constants.HTTP_PARAM_DELETEALL + " or " + Constants.HTTP_PARAM_METAONLY + " should be set when no time range is specified.");
      }
      
      if (Long.MIN_VALUE != start || Long.MAX_VALUE != end) {
        hasRange = true;
      }
      
      if (metaonly && !Constants.DELETE_METAONLY_SUPPORT) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Parameter " + Constants.HTTP_PARAM_METAONLY + " cannot be used as metaonly support is not enabled.");
        return;        
      }
      
      if (metaonly && hasRange) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Parameter " + Constants.HTTP_PARAM_METAONLY + " can only be set if no range is specified.");
        return;
      }

      if (start > end) {
        throw new IOException("Invalid time range specification.");
      }
      
      //
      // Extract selector
      //
      
      String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);
      
      //
      // Extract the class and labels selectors
      // The class selector and label selectors are supposed to have
      // values which use percent encoding, i.e. explicit percent encoding which
      // might have been re-encoded using percent encoding when passed as parameter
      //
      //
      
      Matcher m = EgressFetchHandler.SELECTOR_RE.matcher(selector);
      
      if (!m.matches()) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }
      
      String classSelector = WarpURLDecoder.decode(m.group(1), StandardCharsets.UTF_8);
      String labelsSelection = m.group(2);
      
      Map<String,String> labelsSelectors;

      try {
        labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
      } catch (ParseException pe) {
        throw new IOException(pe);
      }
      
      //
      // Force 'owner'/'app' from token
      //
      
      labelsSelectors.putAll(extraLabels);

      List<Metadata> metadatas = null;
      
      List<String> clsSels = new ArrayList<String>();
      List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
      
      clsSels.add(classSelector);
      lblsSels.add(labelsSelectors);
      
      
      response.setStatus(HttpServletResponse.SC_OK);

      pw = response.getWriter();
      StringBuilder sb = new StringBuilder();

      boolean expose = writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);
      //
      // Shuffle only if not in dryrun mode
      //
      
      if (!dryrun && doShuffle) {
        //
        // Loop over the iterators, storing the read metadata to a temporary file encrypted on disk
        // Data is encrypted using a onetime pad
        //
        
        final byte[] onetimepad = new byte[(int) Math.max(65537, System.currentTimeMillis() % 100000)];
        new Random().nextBytes(onetimepad);
        
        final File cache = File.createTempFile(Long.toHexString(System.currentTimeMillis()) + "-" + Long.toHexString(System.nanoTime()), ".delete.dircache");
        cache.deleteOnExit();
        
        FileWriter writer = new FileWriter(cache);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        
        DirectoryRequest drequest = new DirectoryRequest();
        
        Long activeAfter = null == request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER) ? null : Long.parseLong(request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER));
        Long quietAfter = null == request.getParameter(Constants.HTTP_PARAM_QUIETAFTER) ? null : Long.parseLong(request.getParameter(Constants.HTTP_PARAM_QUIETAFTER));

        if (!Constants.DELETE_ACTIVITY_SUPPORT) {
          if (null != activeAfter || null != quietAfter) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Activity based selection is disabled by configuration.");
            return;
          }
        }
        
        if (null != activeAfter) {
          drequest.setActiveAfter(activeAfter);
        }
        
        if (null != quietAfter) {
          drequest.setQuietAfter(quietAfter);
        }
        
        drequest.setClassSelectors(clsSels);
        drequest.setLabelsSelectors(lblsSels);
        
        try (MetadataIterator iterator = directoryClient.iterator(drequest)) {
          while(iterator.hasNext()) {
            Metadata metadata = iterator.next();
          
            try {
              byte[] bytes = serializer.serialize(metadata);
              // Apply onetimepad
              // We pad each line separately since we will later shuffle them!
              int padidx = 0;
              
              for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (bytes[i] ^ onetimepad[padidx++]);
                if (padidx >= onetimepad.length) {
                  padidx = 0;
                }
              }
              OrderPreservingBase64.encodeToWriter(bytes, writer);
              writer.write('\n');
            } catch (TException te) {
              throw new IOException(te);
            }         
          }
        } catch (Exception e) {
          try { writer.close(); } catch(IOException ioe) {}
          cache.delete();
          throw new IOException(e);
        }        
        
        writer.close();
        
        //
        // Shuffle the content of the file
        //
        
        final File shuffled = File.createTempFile(Long.toHexString(System.currentTimeMillis()) + "-" + Long.toHexString(System.nanoTime()), ".delete.shuffled");
        shuffled.deleteOnExit();

        TextFileShuffler shuffler = new TextFileShuffler(new SortConfig().withMaxMemoryUsage(1000000L));
        
        InputStream in = new FileInputStream(cache);
        OutputStream out = new FileOutputStream(shuffled);
        
        try {
          shuffler.sort(in, out);
        } catch (Exception e) {
          try { in.close(); } catch (IOException ioe) {}
          try { out.close(); } catch (IOException ioe) {}
          shuffler.close();
          shuffled.delete();
          cache.delete();
          throw new IOException(e);
        }
        
        shuffler.close();
        out.close();
        in.close();

        // Delete the unshuffled file
        cache.delete();
        
        //
        // Create an iterator based on the shuffled cache
        //
        
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);
        
        MetadataIterator shufflediterator = new MetadataIterator() {
          
          BufferedReader reader = new BufferedReader(new FileReader(shuffled));
          
          private Metadata current = null;
          private boolean done = false;
          
          private TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
                    
          @Override
          public boolean hasNext() {
            if (done) {
              return false;
            }
            
            if (null != current) {
              return true;
            }
            
            try {
              String line = reader.readLine();
              if (null == line) {
                done = true;
                return false;
              }
              byte[] raw = OrderPreservingBase64.decode(line.getBytes(StandardCharsets.US_ASCII));
              // Apply one time pad
              int padidx = 0;

              for (int i = 0; i < raw.length; i++) {
                raw[i] = (byte) (raw[i] ^ onetimepad[padidx++]);
                if (padidx >= onetimepad.length) {
                  padidx = 0;
                }
              }
              Metadata metadata = new Metadata();
              try {
                deserializer.deserialize(metadata, raw);
                this.current = metadata;
                return true;
              } catch (TException te) {
                error.set(te);
                LOG.error("", te);
              }
            } catch (IOException ioe) {
              error.set(ioe);
              LOG.error("", ioe);
            }
            
            return false;
          }
          
          @Override
          public Metadata next() {
            if (null != this.current) {
              Metadata metadata = this.current;
              this.current = null;
              return metadata;
            } else {
              throw new NoSuchElementException();
            }
          }
          
          @Override
          public void close() throws Exception {
            this.reader.close();
            shuffled.delete();
          }
        };
  
        try {
          while(shufflediterator.hasNext()) {
            Metadata metadata = shufflediterator.next();
            
            if (!dryrun) {
              if (null != this.plugin) {
                if (!this.plugin.delete(this, writeToken, metadata)) {
                  continue;
                }
              }
              
              if (!metaonly) {
                pushDeleteMessage(start, end, minage, metadata);
              }
              
              if (Long.MAX_VALUE == end && Long.MIN_VALUE == start && 0 == minage) {
                completeDeletion = true;
                // We must also push the metadata deletion and remove the metadata from the cache
                Metadata meta = new Metadata(metadata);
                meta.setSource(Configuration.INGRESS_METADATA_DELETE_SOURCE);
                pushMetadataMessage(meta);          
                byte[] bytes = new byte[16];
                // We know class/labels Id were computed in pushMetadataMessage
                GTSHelper.fillGTSIds(bytes, 0, meta.getClassId(), meta.getLabelsId());
                BigInteger key = new BigInteger(bytes);
                synchronized(this.metadataCache) {
                  this.metadataCache.remove(key);
                }
              }
            }
            
            sb.setLength(0);
            
            GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels(), expose);
            
            if (metadata.getAttributesSize() > 0) {
              GTSHelper.labelsToString(sb, metadata.getAttributes(), true);
            } else {
              sb.append("{}");
            }

            pw.write(sb.toString());
            pw.write("\r\n");
            gts++;          
          }
          
          if (null != error.get()) {
            throw error.get();
          }
        } finally {
          try { shufflediterator.close(); } catch (Exception e) {}          
        }
      } else {
        
        DirectoryRequest drequest = new DirectoryRequest();
        drequest.setClassSelectors(clsSels);
        drequest.setLabelsSelectors(lblsSels);

        try (MetadataIterator iterator = directoryClient.iterator(drequest)) {
          while(iterator.hasNext()) {
            Metadata metadata = iterator.next();
            
            if (!dryrun) {
              if (null != this.plugin) {
                if (!this.plugin.delete(this, writeToken, metadata)) {
                  continue;
                }
              }

              if (!metaonly) {
                pushDeleteMessage(start, end, minage, metadata);
              }
              
              if (Long.MAX_VALUE == end && Long.MIN_VALUE == start && 0 == minage) {
                completeDeletion = true;
                // We must also push the metadata deletion and remove the metadata from the cache
                Metadata meta = new Metadata(metadata);
                meta.setSource(Configuration.INGRESS_METADATA_DELETE_SOURCE);
                pushMetadataMessage(meta);          
                byte[] bytes = new byte[16];
                // We know class/labels Id were computed in pushMetadataMessage
                GTSHelper.fillGTSIds(bytes, 0, meta.getClassId(), meta.getLabelsId());
                BigInteger key = new BigInteger(bytes);
                synchronized(this.metadataCache) {
                  this.metadataCache.remove(key);
                }
              }
            }
            
            sb.setLength(0);
            
            GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels(), expose);
            
            if (metadata.getAttributesSize() > 0) {
              GTSHelper.labelsToString(sb, metadata.getAttributes(), true);
            } else {
              sb.append("{}");
            }

            pw.write(sb.toString());
            pw.write("\r\n");
            gts++;
          }      
        } catch (Exception e) {  
          throw new IOException(e);
        }        
      }
    } catch (Throwable t) {
      LOG.error("",t);
      Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_DELETE_ERRORS, Sensision.EMPTY_LABELS, 1);
      if (showErrors && null != pw) {
        pw.println();
        StringWriter sw = new StringWriter();
        PrintWriter pw2 = new PrintWriter(sw);
        t.printStackTrace(pw2);
        pw2.close();
        sw.flush();
        String error = URLEncoder.encode(sw.toString(), StandardCharsets.UTF_8.name());
        pw.println(Constants.INGRESS_DELETE_ERROR_PREFIX + error);
      }
      if (!response.isCommitted()) {
        String prefix = "Error when deleting data: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
      }
      return;
    } finally {
      // Flush delete messages
      if (!dryrun) {
        if (!metaonly) {
          pushDeleteMessage(0L,0L,0L,null);
        }
        if (completeDeletion) {
          pushMetadataMessage(null, null);
        }
        if (null != this.plugin) {
          this.plugin.flush(this);
        }
      }
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_DELETE_REQUESTS, sensisionLabels, 1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_DELETE_GTS, sensisionLabels, gts);
    }

    response.setStatus(HttpServletResponse.SC_OK);
  }
  
  /**
   * Extract Ingress related keys and populate the KeyStore with them.
   * 
   * @param props Properties from which to extract the key specs
   */
  private static void extractKeys(KeyStore keystore, Properties props) {
    String keyspec = props.getProperty(Configuration.INGRESS_KAFKA_META_MAC);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.INGRESS_KAFKA_META_MAC + " MUST be 128 bits long.");
      keystore.setKey(KeyStore.SIPHASH_KAFKA_METADATA, key);
    }
    
    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_DATA_MAC);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.INGRESS_KAFKA_DATA_MAC + " MUST be 128 bits long.");
      keystore.setKey(KeyStore.SIPHASH_KAFKA_DATA, key);
    }

    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_META_AES);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.INGRESS_KAFKA_META_AES + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_KAFKA_METADATA, key);
    }

    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_DATA_AES);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.INGRESS_KAFKA_DATA_AES + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_KAFKA_DATA, key);
    }
    
    keyspec = props.getProperty(Configuration.DIRECTORY_PSK);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.DIRECTORY_PSK + " MUST be 128 bits long.");
      keystore.setKey(KeyStore.SIPHASH_DIRECTORY_PSK, key);
    }    
    
    keystore.forget();
  }
  
  void pushMetadataMessage(Metadata metadata) throws IOException {
    
    if (null == metadata) {
      pushMetadataMessage(null, null);
      return;
    }
    
    //
    // Compute class/labels Id
    //
    // 128bits
    metadata.setClassId(GTSHelper.classId(this.classKey, metadata.getName()));
    metadata.setLabelsId(GTSHelper.labelsId(this.labelsKey, metadata.getLabels()));
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    try {
      byte[] bytes = new byte[16];
      GTSHelper.fillGTSIds(bytes, 0, metadata.getClassId(), metadata.getLabelsId());
      pushMetadataMessage(bytes, serializer.serialize(metadata));
    } catch (TException te) {
      throw new IOException("Unable to push metadata.");
    }
  }
  
  /**
   * Push a metadata message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   * 
   * @param key Key of the message to queue
   * @param value Value of the message to queue
   */
  private void pushMetadataMessage(byte[] key, byte[] value) throws IOException {
    
    AtomicLong mms = this.metadataMessagesSize.get();
    List<KeyedMessage<byte[], byte[]>> msglist = this.metadataMessages.get();
    
    if (null != key && null != value) {
      
      //
      // Add key as a prefix of value
      //
      
      byte[] kv = Arrays.copyOf(key, key.length + value.length);
      System.arraycopy(value, 0, kv, key.length, value.length);
      value = kv;
      
      //
      // Encrypt value if the AES key is defined
      //
      
      if (null != this.AES_KAFKA_META) {
        value = CryptoUtils.wrap(this.AES_KAFKA_META, value);               
      }
      
      //
      // Compute MAC if the SipHash key is defined
      //
      
      if (null != this.SIPHASH_KAFKA_META) {
        value = CryptoUtils.addMAC(this.SIPHASH_KAFKA_META, value);
      }
      
      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.metaTopic, Arrays.copyOf(key, key.length), value);
      msglist.add(message);
      mms.addAndGet(key.length + value.length);
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_META_MESSAGES, Sensision.EMPTY_LABELS, 1);
    }
    
    if (msglist.size() > 0 && (null == key || null == value || mms.get() > METADATA_MESSAGES_THRESHOLD)) {
      Producer<byte[],byte[]> producer = this.metaProducerPool.getProducer();
      try {

        //
        // How long it takes to send messages to Kafka
        //

        long nano = System.nanoTime();

        producer.send(msglist);

        nano = System.nanoTime() - nano;
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_SEND, Sensision.EMPTY_LABELS, nano);

      } catch (Throwable t) {
        //
        // We need to remove the IDs of Metadata in 'msglist' from the cache so they get a chance to be
        // pushed later
        //

        for (KeyedMessage<byte[],byte[]> msg: msglist) {
          synchronized(this.metadataCache) {
            this.metadataCache.remove(new BigInteger(msg.key()));
          }          
        }

        throw t;
      } finally {
        this.metaProducerPool.recycleProducer(producer);
      }
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_META_SEND, Sensision.EMPTY_LABELS, 1);
      msglist.clear();
      mms.set(0L);
      // Update sensision metric with size of metadata cache
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_METADATA_CACHED, Sensision.EMPTY_LABELS, this.metadataCache.size());
    }      
  }

  /**
   * Push a metadata message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   * 
   * @param encoder GTSEncoder to push to Kafka. It MUST have classId/labelsId set.
   */
  void pushDataMessage(GTSEncoder encoder, Map<String,String> attributes) throws IOException {    
    if (null != encoder) {
      KafkaDataMessage msg = new KafkaDataMessage();
      msg.setType(KafkaDataMessageType.STORE);
      msg.setData(encoder.getBytes());
      msg.setClassId(encoder.getClassId());
      msg.setLabelsId(encoder.getLabelsId());

      if (this.sendMetadataOnStore) {
        msg.setMetadata(encoder.getMetadata());
      }

      if (null != attributes && !attributes.isEmpty()) {
        msg.setAttributes(new HashMap<String,String>(attributes));
      }
      sendDataMessage(msg);
    } else {
      sendDataMessage(null);
    }
  }

  private void sendDataMessage(KafkaDataMessage msg) throws IOException {    
    AtomicLong dms = this.dataMessagesSize.get();
    List<KeyedMessage<byte[], byte[]>> msglist = this.dataMessages.get();
    
    if (null != msg) {
      //
      // Build key
      //
      
      byte[] bytes = new byte[16];
      
      GTSHelper.fillGTSIds(bytes, 0, msg.getClassId(), msg.getLabelsId());

      //ByteBuffer bb = ByteBuffer.wrap(new byte[16]).order(ByteOrder.BIG_ENDIAN);
      //bb.putLong(encoder.getClassId());
      //bb.putLong(encoder.getLabelsId());

      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
           
      byte[] msgbytes = null;
      
      try {
        msgbytes = serializer.serialize(msg);
      } catch (TException te) {
        throw new IOException(te);
      }
      
      //
      // Encrypt value if the AES key is defined
      //
        
      if (null != this.aesDataKey) {
        msgbytes = CryptoUtils.wrap(this.aesDataKey, msgbytes);               
      }
        
      //
      // Compute MAC if the SipHash key is defined
      //
        
      if (null != this.siphashDataKey) {
        msgbytes = CryptoUtils.addMAC(this.siphashDataKey, msgbytes);
      }
        
      //KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.dataTopic, bb.array(), msgbytes);
      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.dataTopic, bytes, msgbytes);
      msglist.add(message);
      //this.dataMessagesSize.get().addAndGet(bb.array().length + msgbytes.length);      
      dms.addAndGet(bytes.length + msgbytes.length);      
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_MESSAGES, Sensision.EMPTY_LABELS, 1);
    }

    if (msglist.size() > 0 && (null == msg || dms.get() > DATA_MESSAGES_THRESHOLD)) {
      Producer<byte[],byte[]> producer = getDataProducer();
      //this.dataProducer.send(msglist);
      try {

        //
        // How long it takes to send messages to Kafka
        //

        long nano = System.nanoTime();

        producer.send(msglist);

        nano = System.nanoTime() - nano;
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_SEND, Sensision.EMPTY_LABELS, nano);

      } catch (Throwable t) {
        throw t;
      } finally {
        recycleDataProducer(producer);
      }
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_SEND, Sensision.EMPTY_LABELS, 1);
      msglist.clear();
      dms.set(0L);          
    }        
  }
  
  private Producer<byte[],byte[]> getDataProducer() {
    
    //
    // We will count how long we wait for a producer
    //

    long nano = System.nanoTime();
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_POOL_GET, Sensision.EMPTY_LABELS, 1);
    
    while(true) {
      synchronized (this.dataProducers) {
        if (this.dataProducersCurrentPoolSize > 0) {
          //
          // hand out the producer at index 0
          //
          
          Producer<byte[],byte[]> producer = this.dataProducers[0];
          
          //
          // Decrement current pool size
          //
          
          this.dataProducersCurrentPoolSize--;
          
          //
          // Move the last element of the array at index 0
          //
          
          this.dataProducers[0] = this.dataProducers[this.dataProducersCurrentPoolSize];
          this.dataProducers[this.dataProducersCurrentPoolSize] = null;

          //
          // Log waiting time
          //
          
          nano = System.nanoTime() - nano;          
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_WAIT_NANO, Sensision.EMPTY_LABELS, nano);

          return producer;
        }
      }
      
      LockSupport.parkNanos(500000L);
    }    
  }
  
  private void recycleDataProducer(Producer<byte[],byte[]> producer) {
    
    if (this.dataProducersCurrentPoolSize == this.dataProducers.length) {
      throw new RuntimeException("Invalid call to recycleProducer, pool already full!");
    }
    
    synchronized (this.dataProducers) {
      //
      // Add the recycled producer at the end of the pool
      //

      this.dataProducers[this.dataProducersCurrentPoolSize++] = producer;
    }
  }
  
  
  /**
   * Push a deletion message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   * 
   * Deletion messages MUST be pushed onto the data topic, otherwise the
   * ordering won't be respected and you risk deleting a GTS which has been
   * since repopulated with data.
   * 
   * @param start Start timestamp for deletion
   * @param end End timestamp for deletion
   * @param metadata Metadata of the GTS to delete
   */
  private void pushDeleteMessage(long start, long end, long minage, Metadata metadata) throws IOException {    
    if (null != metadata) {
      KafkaDataMessage msg = new KafkaDataMessage();
      msg.setType(KafkaDataMessageType.DELETE);
      msg.setDeletionStartTimestamp(start);
      msg.setDeletionEndTimestamp(end);
      msg.setDeletionMinAge(minage);
      msg.setClassId(metadata.getClassId());
      msg.setLabelsId(metadata.getLabelsId());

      if (this.sendMetadataOnDelete) {
        msg.setMetadata(metadata);
      }

      sendDataMessage(msg);
    } else {
      sendDataMessage(null);
    }
  }

  private void dumpCache() {
    if (null == this.cacheDumpPath) {
      return;
    }

    OutputStream out = null;
    
    long nano = System.nanoTime();
    long count = 0;
    
    try {
      out = new GZIPOutputStream(new FileOutputStream(this.cacheDumpPath));
      
      Set<BigInteger> bis = new HashSet<BigInteger>();

      synchronized(this.metadataCache) {
        boolean error = false;
        do {
          try {
            error = false;          
            bis.addAll(this.metadataCache.keySet());
          } catch (ConcurrentModificationException cme) {
            error = true;
          }
        } while (error);
      }
      
      Iterator<BigInteger> iter = bis.iterator();
      
      //
      // 128bits
      //
      
      byte[] allzeroes = new byte[16];
      Arrays.fill(allzeroes, (byte) 0); 
      byte[] allones = new byte[16];
      Arrays.fill(allones, (byte) 0xff);
      
      while (true) {
        try {
          if (!iter.hasNext()) {
            break;
          }
          BigInteger bi = iter.next();
          
          byte[] raw = bi.toByteArray();

          //
          // 128bits
          //
          
          if (raw.length != 16) {
            if (bi.signum() < 0) {
              out.write(allones, 0, 16 - raw.length);
            } else {
              out.write(allzeroes, 0, 16 - raw.length);              
            }
          }
          
          out.write(raw);
          
          if (this.activityTracking) {
            Long lastActivity = this.metadataCache.get(bi);
            byte[] bytes;
            if (null != lastActivity) {
              bytes = Longs.toByteArray(lastActivity);
            } else {
              bytes = new byte[8];
            }
            out.write(bytes);
          }
          count++;
        } catch (ConcurrentModificationException cme) {          
        }
      }          
    } catch (IOException ioe) {      
    } finally {
      if (null != out) {
        try { out.close(); } catch (Exception e) {}
      }
    }
    
    nano = System.nanoTime() - nano;
    
    LOG.info("Dumped " + count + " cache entries in " + (nano / 1000000.0D) + " ms.");    
  }
  
  private void loadCache() {
    if (null == this.cacheDumpPath) {
      return;
    }
    
    InputStream in = null;
    
    byte[] buf = new byte[8192];
    
    long nano = System.nanoTime();
    long count = 0;
    
    try {
      in = new GZIPInputStream(new FileInputStream(this.cacheDumpPath));
      
      int offset = 0;
      
      // 128 bits
      int reclen = this.activityTracking ? 24 : 16;
      byte[] raw = new byte[16];
      
      while(true) {
        int len = in.read(buf, offset, buf.length - offset);
                
        offset += len;

        int idx = 0;
        
        while(idx < offset && offset - idx >= reclen) {
          System.arraycopy(buf, idx, raw, 0, 16);
          BigInteger id = new BigInteger(raw);
          if (this.activityTracking) {
            long lastActivity = 0L;
              
            for (int i = 0; i < 8; i++) {
              lastActivity <<= 8;
              lastActivity |= ((long) buf[idx + 16 + i]) & 0xFFL;
            }
            synchronized(this.metadataCache) {  
              this.metadataCache.put(id, lastActivity);
            }
          } else {
            synchronized(this.metadataCache) {  
              this.metadataCache.put(id, null);
            }
          }
          count++;
          idx += reclen;
        }
        
        if (idx < offset) {
          for (int i = idx; i < offset; i++) {
            buf[i - idx] = buf[i];
          }
          offset = offset - idx;
        } else {
          offset = 0;
        }
        
        if (len < 0) {
          break;
        }
      }      
    } catch (IOException ioe) {      
    } finally {
      if (null != in) {
        try { in.close(); } catch (Exception e) {}
      }
    }
    
    nano = System.nanoTime() - nano;
    
    LOG.info("Loaded " + count + " cache entries in " + (nano / 1000000.0D) + " ms.");
  }
}
