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

package io.warp10.continuum.store;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceInstanceBuilder;
import com.netflix.curator.x.discovery.ServiceType;

import io.warp10.SmartPattern;
import io.warp10.continuum.DirectoryUtil;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.KafkaOffsetCounters;
import io.warp10.continuum.LogUtil;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.MetadataUtils.MetadataID;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.DirectoryFindRequest;
import io.warp10.continuum.store.thrift.data.DirectoryFindResponse;
import io.warp10.continuum.store.thrift.data.DirectoryGetRequest;
import io.warp10.continuum.store.thrift.data.DirectoryGetResponse;
import io.warp10.continuum.store.thrift.data.DirectoryStatsRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsResponse;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.continuum.store.thrift.service.DirectoryService;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.HyperLogLogPlus;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.PARSESELECTOR;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.DirectoryPlugin;
import io.warp10.warp.sdk.DirectoryPlugin.GTS;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * Manages Metadata for a subset of known GTS.
 * Listens to Kafka to get updates of and new Metadatas 
 */
public class Directory extends AbstractHandler implements DirectoryService.Iface, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Directory.class);

  /**
   * Maximum size of the input URI
   */
  public static final int DIRECTORY_REQUEST_HEADER_SIZE = 64 * 1024;
  
  /**
   * Comparator which sorts the IDs in their lexicographical order suitable for scanning HBase keys
   */
  private static final Comparator<Long> ID_COMPARATOR = new Comparator<Long>() {
    @Override
    public int compare(Long o1, Long o2) {
      if (Long.signum(o1) == Long.signum(o2)) {
        return o1.compareTo(o2);
      } else {
        //
        // 0 is the first key
        //
        if (0 == o1) {
          return -1;
        } else if (0 == o2) {
          return 1;
        } else {
          //
          // If the two numbers have different signs, then the positive values MUST appear before the negative ones
          //
          if (o1 > 0) {
            return -1;
          } else {
            return 1;
          }          
        }
      }
    }
  };

  public static final String PAYLOAD_MODULUS_KEY = "modulus";
  public static final String PAYLOAD_REMAINDER_KEY = "remainder";
  public static final String PAYLOAD_THRIFT_PROTOCOL_KEY = "thrift.protocol";
  public static final String PAYLOAD_THRIFT_TRANSPORT_KEY = "thrift.transport";
  public static final String PAYLOAD_THRIFT_MAXFRAMELEN_KEY = "thrift.maxframelen";
  public static final String PAYLOAD_STREAMING_PORT_KEY = "streaming.port";

  /**
   * Values of P and P' for the HyperLogLogPlus estimators
   */
  public static final int ESTIMATOR_P = 14;
  public static final int ESTIMATOR_PPRIME = 25;

  /**
   * Allow individual tracking of 100 class names
   */
  private long LIMIT_CLASS_CARDINALITY = 100;
  
  /**
   * Allow tracking of 100 label names
   */
  private long LIMIT_LABELS_CARDINALITY = 100;
  
  private final KeyStore keystore;

  /**
   * Name under which the directory service is registered in ZK
   */
  public static final String DIRECTORY_SERVICE = "com.cityzendata.continuum.directory";
  
  private static final String DIRECTORY_INIT_NTHREADS_DEFAULT = "4";
  
  private final int modulus;
  private final int remainder;
  private String host;
  private int port;
  private int tcpBacklog;
  private int streamingport;
  private int streamingTcpBacklog;
  private int streamingselectors;
  private int streamingacceptors;
    
  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    io.warp10.continuum.Configuration.DIRECTORY_ZK_QUORUM,
    io.warp10.continuum.Configuration.DIRECTORY_ZK_ZNODE,
    io.warp10.continuum.Configuration.DIRECTORY_SERVICE_NTHREADS,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_NTHREADS,
    io.warp10.continuum.Configuration.DIRECTORY_PARTITION,
    io.warp10.continuum.Configuration.DIRECTORY_HOST,
    io.warp10.continuum.Configuration.DIRECTORY_PORT,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_ZKCONNECT,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_TOPIC,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_GROUPID,
    io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_COMMITPERIOD,
    io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_MAXPENDINGPUTSSIZE,
    io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_ZKCONNECT,
    io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_TABLE,
    io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_COLFAM,
    io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_ZNODE,
    io.warp10.continuum.Configuration.DIRECTORY_PSK,
    io.warp10.continuum.Configuration.DIRECTORY_MAXAGE,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_PORT,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_SELECTORS,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_ACCEPTORS,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_IDLE_TIMEOUT,
    io.warp10.continuum.Configuration.DIRECTORY_STREAMING_THREADPOOL,
    io.warp10.continuum.Configuration.DIRECTORY_FIND_MAXRESULTS_HARD,
    io.warp10.continuum.Configuration.DIRECTORY_REGISTER,
    io.warp10.continuum.Configuration.DIRECTORY_INIT,
    io.warp10.continuum.Configuration.DIRECTORY_STORE,
    io.warp10.continuum.Configuration.DIRECTORY_DELETE,
  };

  /**
   * Name of HBase table where metadata should be written
   */
  private final TableName hbaseTable;
  
  /**
   * Name of column family where metadata should be written
   */
  private final byte[] colfam;
  
  /**
   * How often to commit Kafka offsets
   */
  private final long commitPeriod;

  /**
   * How big do we allow the Put list to grow
   */
  private final long maxPendingPutsSize;
  
  /**
   * Instance of HBase connection to create Table instances
   */
  private final Connection conn;
  
  /**
   * CyclicBarrier instance to synchronize consuming threads prior to committing offsets
   */
  private CyclicBarrier barrier;
  
  /**
   * Flag for signaling abortion of consuming process
   */
  private final AtomicBoolean abort = new AtomicBoolean(false);
  
  /**
   * Maps of class name to labelsId to metadata
   */
  private final Map<String,Map<Long,Metadata>> metadatas = new MapMaker().concurrencyLevel(64).makeMap();
  
  /**
   * Map of classId to class names
   */
  //private final Map<Long,String> classNames = new MapMaker().concurrencyLevel(64).makeMap();
  private final Map<Long,String> classNames = new ConcurrentSkipListMap<Long, String>(ID_COMPARATOR);
  
  private final Map<String,Set<String>> classesPerOwner = new MapMaker().concurrencyLevel(64).makeMap();

  private final ReentrantLock metadatasLock = new ReentrantLock();
  
  /**
   * Number of threads for servicing requests
   */
  private final int serviceNThreads;
  
  private final AtomicBoolean cachePopulated = new AtomicBoolean(false);
  private final AtomicBoolean fullyInitialized = new AtomicBoolean(false);
  
  private final Properties properties;

  private final ServiceDiscovery<Map> sd;
  
  private final long[] SIPHASH_CLASS_LONGS;
  private final long[] SIPHASH_LABELS_LONGS;
  private final long[] SIPHASH_PSK_LONGS;
  
  /**
   * Maximum age of a Find request
   */
  private final long maxage;
  
  private final int maxThriftFrameLength;
  
  private final int maxFindResults;
  
  private final int maxHardFindResults;
  
  private final int initNThreads;
  
  private final long idleTimeout;
  
  /**
   * Should we register our service in ZK
   */
  private final boolean register;
  
  /**
   * Service instance
   */
  private ServiceInstance<Map> instance = null;
  
  /**
   * Thread used as shutdown hook for deregistering instance
   */
  private Thread deregisterHook = null;
  
  /**
   * Should we initialize Directory upon startup by reading from HBase
   */
  private final boolean init;
  
  /**
   * Should we store in HBase metadata we receive via Kafka
   */
  private final boolean store;

  /**
   * Should we delete in HBase
   */
  private final boolean delete;

  /**
   * Do we track activity of GeoTimeSeries?
   */
  private final boolean trackingActivity;
  
  /**
   * Activity window for activity tracking
   */
  private final long activityWindow;
  
  /**
   * Directory plugin to use
   */
  private final DirectoryPlugin plugin;
  
  private final String sourceAttribute;
  
  private int METADATA_CACHE_SIZE = 1000000;
  
  /**
   * Cache to keep a serialized version of recently returned Metadata.
   */
  final Map<MetadataID, byte[]> serializedMetadataCache = new LinkedHashMap<MetadataID, byte[]>(100, 0.75F, true) {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<MetadataID, byte[]> eldest) {
      Sensision.set(SensisionConstants.CLASS_WARP_DIRECTORY_METADATA_CACHE_SIZE, Sensision.EMPTY_LABELS, this.size());
      return this.size() > METADATA_CACHE_SIZE;
    }
  };

  public Directory(KeyStore keystore, final Properties props) throws IOException {
    this.keystore = keystore;

    SIPHASH_CLASS_LONGS = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_CLASS));
    SIPHASH_LABELS_LONGS = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_LABELS));
    
    this.properties = (Properties) props.clone();
  
    this.sourceAttribute = props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_PLUGIN_SOURCEATTR);
    
    //
    // Check mandatory parameters
    //
    
    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(required), "Missing configuration parameter '%s'.", required);          
    }

    maxThriftFrameLength = Integer.parseInt(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_FRAME_MAXLEN, "0"));

    maxFindResults = Integer.parseInt(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_FIND_MAXRESULTS, "100000"));
  
    maxHardFindResults = Integer.parseInt(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_FIND_MAXRESULTS_HARD));

    this.register = "true".equals(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_REGISTER));
    this.init = "true".equals(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_INIT));
    this.store = "true".equals(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STORE));
    this.delete = "true".equals(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_DELETE));
    
    this.activityWindow = Long.parseLong(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_ACTIVITY_WINDOW, "0"));
    this.trackingActivity = this.activityWindow > 0;
    
    //
    // Extract parameters
    //

    if (null != props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_METADATA_CACHE_SIZE)) {
      this.METADATA_CACHE_SIZE = Integer.valueOf(props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_METADATA_CACHE_SIZE));
    }

    idleTimeout = Long.parseLong(this.properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_IDLE_TIMEOUT));

    if (properties.containsKey(io.warp10.continuum.Configuration.DIRECTORY_STATS_CLASS_MAXCARDINALITY)) {
      this.LIMIT_CLASS_CARDINALITY = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STATS_CLASS_MAXCARDINALITY));
    }

    if (properties.containsKey(io.warp10.continuum.Configuration.DIRECTORY_STATS_LABELS_MAXCARDINALITY)) {
      this.LIMIT_LABELS_CARDINALITY = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STATS_LABELS_MAXCARDINALITY));
    }

    this.initNThreads = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_INIT_NTHREADS, DIRECTORY_INIT_NTHREADS_DEFAULT));

    String partition = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_PARTITION);
    String[] tokens = partition.split(":");
    this.modulus = Integer.parseInt(tokens[0]);
    this.remainder = Integer.parseInt(tokens[1]);
    
    this.maxage = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_MAXAGE));
    
    final String topic = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_TOPIC);
    final int nthreads = Integer.valueOf(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_NTHREADS));
    
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_ZKCONNECT));
    if (!"".equals(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_ZNODE))) {
      conf.set("zookeeper.znode.parent", properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_ZNODE));
    }

    if (properties.containsKey(io.warp10.continuum.Configuration.DIRECTORY_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)) {
      conf.set("hbase.zookeeper.property.clientPort", properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
    }
    
    //
    // Handle additional HBase configurations
    //
    
    if (properties.containsKey(io.warp10.continuum.Configuration.DIRECTORY_HBASE_CONFIG)) {
      String[] keys = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_CONFIG).split(",");
      for (String key: keys) {
        if (!properties.containsKey("directory." + key.trim())) {
          throw new RuntimeException("Missing declared property 'directory." + key.trim() + "'.");
        }
        conf.set(key, properties.getProperty("directory." + key.trim()));
      }
    }

    this.conn = ConnectionFactory.createConnection(conf);

    this.hbaseTable = TableName.valueOf(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_TABLE));
    this.colfam = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_COLFAM).getBytes(StandardCharsets.UTF_8);
    
    this.serviceNThreads = Integer.valueOf(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_SERVICE_NTHREADS));
    
    //
    // Extract keys
    //
    
    extractKeys(properties);

    SIPHASH_PSK_LONGS = SipHashInline.getKey(this.keystore.getKey(KeyStore.SIPHASH_DIRECTORY_PSK));

    //
    // Load Directory plugin
    //
    
    if (this.properties.containsKey(io.warp10.continuum.Configuration.DIRECTORY_PLUGIN_CLASS)) {
      try {
        ClassLoader pluginCL = this.getClass().getClassLoader();

        Class pluginClass = Class.forName(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_PLUGIN_CLASS), true, pluginCL);
        this.plugin = (DirectoryPlugin) pluginClass.newInstance();
        
        //
        // Now call the 'init' method of the plugin
        //
        
        this.plugin.init(new Properties(properties));
      } catch (Exception e) {
        throw new RuntimeException("Unable to instantiate plugin class", e);
      }
    } else {
      this.plugin = null;
    }
    
    //
    // Create Curator framework and service discovery
    //
    
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_ZK_QUORUM))
        .build();
    curatorFramework.start();

    this.sd = ServiceDiscoveryBuilder.builder(Map.class)
        .basePath(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_ZK_ZNODE))
        .client(curatorFramework)
        .build();
    
    //
    // Launch a Thread which will populate the metadata cache
    // We don't do that in the constructor otherwise it might take too long to return
    //

    final Directory self = this;

    if (this.init) {

      Thread[] initThreads = new Thread[this.initNThreads];
      final AtomicBoolean[] stopMarkers = new AtomicBoolean[this.initNThreads];
      
      final LinkedBlockingQueue<Result> resultQ = new LinkedBlockingQueue<Result>(initThreads.length * 8192);
      
      for (int i = 0; i < initThreads.length; i++) {
        stopMarkers[i] = new AtomicBoolean(false);
        final AtomicBoolean stopMe = stopMarkers[i];
        initThreads[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            AESWrapEngine engine = null;
            if (null != self.keystore.getKey(KeyStore.AES_HBASE_METADATA)) {
              engine = new AESWrapEngine();
              CipherParameters params = new KeyParameter(self.keystore.getKey(KeyStore.AES_HBASE_METADATA));
              engine.init(false, params);
            }

            PKCS7Padding padding = new PKCS7Padding();

            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

            while (!stopMe.get()) {
              try {
                
                Result result = resultQ.poll(100, TimeUnit.MILLISECONDS);
                
                if (null == result) {
                  continue;
                }
                
                byte[] value = result.getValue(self.colfam, Constants.EMPTY_COLQ);

                if (null != engine) {
                  //
                  // Unwrap
                  //
                  
                  byte[] unwrapped = engine.unwrap(value, 0, value.length);
                  
                  //
                  // Unpad
                  //
                  
                  int padcount = padding.padCount(unwrapped);
                  value = Arrays.copyOf(unwrapped, unwrapped.length - padcount);                                   
                }
                
                //
                // Deserialize
                //

                Metadata metadata = new Metadata();
                deserializer.deserialize(metadata, value);

                //
                // Compute classId/labelsId and compare it to the values in the row key
                //
                
                long classId = GTSHelper.classId(self.SIPHASH_CLASS_LONGS, metadata.getName());
                long labelsId = GTSHelper.labelsId(self.SIPHASH_LABELS_LONGS, metadata.getLabels());
                
                //
                // Recheck labelsid so we don't retain GTS with invalid labelsid in the row key (which may have happened due
                // to bugs)
                //
                
                int rem = ((int) ((labelsId >>> 56) & 0xffL)) % self.modulus;
                
                if (self.remainder != rem) {
                  continue;
                }
                
                ByteBuffer bb = ByteBuffer.wrap(result.getRow()).order(ByteOrder.BIG_ENDIAN);
                bb.position(1);
                long hbClassId = bb.getLong();
                long hbLabelsId = bb.getLong();
                
                // If classId/labelsId are incoherent, skip metadata
                if (classId != hbClassId || labelsId != hbLabelsId) {
                  LOG.warn("Incoherent class/labels Id for " + metadata);
                  continue;
                }
          
                metadata.setClassId(classId);
                metadata.setLabelsId(labelsId);
                
                if (!metadata.isSetAttributes()) {
                  metadata.setAttributes(new HashMap<String,String>());
                }
                
                //
                // Internalize Strings
                //
                
                GTSHelper.internalizeStrings(metadata);
                
                //
                // Let the DirectoryPlugin handle the Metadata
                //
                
                if (null != plugin) {
                  
                  long nano = 0;
                  
                  try {
                    //
                    // Directory plugins have no provision for delta attribute updates
                    //
                    GTS gts = new GTS(
                        new UUID(metadata.getClassId(), metadata.getLabelsId()),
                        metadata.getName(),
                        metadata.getLabels(),
                        metadata.getAttributes());
                    
                    if (null != sourceAttribute) {
                      gts.getAttributes().put(sourceAttribute, metadata.getSource());
                    }
                    
                    nano = System.nanoTime();
                    
                    if (!plugin.store(null, gts)) {
                      throw new RuntimeException("Error storing GTS " + gts + " using external plugin.");
                    }                    
                  } finally {
                    nano = System.nanoTime() - nano;
                    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_CALLS, Sensision.EMPTY_LABELS, 1);
                    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_TIME_NANOS, Sensision.EMPTY_LABELS, nano);                                      
                  }
                  continue;
                }
                
                try {
                  metadatasLock.lockInterruptibly();
                  if (!metadatas.containsKey(metadata.getName())) {
                    metadatas.put(metadata.getName(), new ConcurrentSkipListMap<Long, Metadata>(ID_COMPARATOR));
                    classNames.put(classId, metadata.getName());
                  }
                } finally {
                  if (metadatasLock.isHeldByCurrentThread()) {
                    metadatasLock.unlock();
                  }
                }
                
                //
                // Store per owner class name. We use the name since it has been internalized,
                // therefore we only consume the HashNode and the HashSet overhead
                //
                
                String owner = metadata.getLabels().get(Constants.OWNER_LABEL);

                String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
                Map<String,String> sensisionLabels = new HashMap<String,String>();
                sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);
                
                synchronized(classesPerOwner) {
                  Set<String> classes = classesPerOwner.get(owner);
                  
                  if (null == classes) {
                    classes = new ConcurrentSkipListSet<String>();
                    classesPerOwner.put(owner, classes);
                  }
                  
                  classes.add(metadata.getName());
                }

                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, 1);
                Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_OWNERS, Sensision.EMPTY_LABELS, classesPerOwner.size());

                synchronized(metadatas.get(metadata.getName())) {
                  if (!metadatas.get(metadata.getName()).containsKey(labelsId)) {
                    metadatas.get(metadata.getName()).put(labelsId, metadata);
                    continue;
                  } else if (!metadatas.get(metadata.getName()).get(labelsId).getLabels().equals(metadata.getLabels())) {
                    LOG.warn("LabelsId collision under class '" + metadata.getName() + "' " + metadata.getLabels() + " and " + metadatas.get(metadata.getName()).get(labelsId).getLabels());
                    Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_LABELS_COLLISIONS, Sensision.EMPTY_LABELS, 1);                    
                  }
                }
                
                continue;
              } catch (InvalidCipherTextException icte) {
                throw new RuntimeException(icte);
              } catch (TException te) {
                throw new RuntimeException(te);
              } catch (InterruptedException ie) {              
              }
            }
          }
        });
        
        initThreads[i].setDaemon(true);
        initThreads[i].setName("[Directory initializer #" + i + "]");
        initThreads[i].start();
      }
      
      Thread populator = new Thread(new Runnable() {
        
        @Override
        public void run() {
          
          long nano = System.nanoTime();
          
          Table htable = null;
          
          long count = 0L;
          
          boolean done = false;
          
          byte[] lastrow = Constants.HBASE_METADATA_KEY_PREFIX;
          
          while(!done) {
            try {
              //
              // Populate the metadata cache with initial data from HBase
              //
                       
              htable = self.conn.getTable(self.hbaseTable);

              Scan scan = new Scan();
              scan.setStartRow(lastrow);
              // FIXME(hbs): we know the prefix is 'M', so we use 'N' as the stoprow
              scan.setStopRow("N".getBytes(StandardCharsets.UTF_8));
              scan.addFamily(self.colfam);
              scan.setCaching(10000);
              scan.setBatch(10000);
              scan.setMaxResultSize(1000000L);

              ResultScanner scanner = htable.getScanner(scan);
                        
              do {
                Result result = scanner.next();

                if (null == result) {
                  done = true;
                  break;
                }
                
                //
                // FIXME(hbs): this could be done in a filter on the RS side
                //
                
                int r = (((int) result.getRow()[Constants.HBASE_METADATA_KEY_PREFIX.length + 8]) & 0xff) % self.modulus;
                
                //byte r = (byte) (result.getRow()[HBASE_METADATA_KEY_PREFIX.length + 8] % self.modulus);
                
                // Skip metadata if its modulus is not the one we expect
                if (self.remainder != r) {
                  continue;
                }
                
                //
                // Store the current row so we can restart from there if an exception occurs
                //
                
                lastrow = result.getRow();
                
                boolean interrupted = true;
                
                while(interrupted) {
                  interrupted = false;
                  try {
                    resultQ.put(result);
                    count++;
                    if (0 == count % 1000 && null == plugin) {
                      // We do not update this metric when using a Directory plugin
                      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, count);
                    }
                  } catch (InterruptedException ie) {
                    interrupted = true;
                  }
                }
                
              } while (true);
              
            } catch (Exception e) {
              LOG.error("Caught exception in scanning loop, will attempt to continue where we stopped", e);
            } finally {
              if (null != htable) { try { htable.close(); } catch (Exception e) {} }
              if (null == plugin) {
                // We do not update this metric when using a Directory plugin
                Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, count);
              }
            }                    
          }
          
          //
          // Wait until resultQ is empty
          //
          
          while(!resultQ.isEmpty()) {
            LockSupport.parkNanos(100000000L);
          }
          
          //
          // Notify the init threads to stop
          //
          
          for (int i = 0; i < initNThreads; i++) {
            stopMarkers[i].set(true);
          }
          
          self.cachePopulated.set(true);
          
          nano = System.nanoTime() - nano;
          
          LOG.info("Loaded " + count + " GTS in " + (nano / 1000000.0D) + " ms");
        }            
      });
      
      populator.setName("Warp Directory Populator");
      populator.setDaemon(true);
      populator.start();      
    } else {
      LOG.info("Skipped initialization");
      this.cachePopulated.set(true);
    }
    
    this.commitPeriod = Long.valueOf(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_COMMITPERIOD));
    
    this.maxPendingPutsSize = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_MAXPENDINGPUTSSIZE));
    
    this.host = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HOST);
    this.port = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_PORT));
    this.tcpBacklog = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_TCP_BACKLOG, "0"));
    this.streamingport = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_PORT));
    this.streamingTcpBacklog = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_TCP_BACKLOG, "0"));
    this.streamingacceptors = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_ACCEPTORS));
    this.streamingselectors = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_SELECTORS));
    
    int streamingMaxThreads = Integer.parseInt(props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_THREADPOOL));
    
    final String groupid = properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_GROUPID);

    final KafkaOffsetCounters counters = new KafkaOffsetCounters(topic, groupid, this.commitPeriod * 2);

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        
        //
        // Wait until directory is fully initialized
        //
        
        while(!self.fullyInitialized.get()) {
          LockSupport.parkNanos(1000000000L);
        }

        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_CLASSES, Sensision.EMPTY_LABELS, classNames.size());
        
        //
        // Enter an endless loop which will spawn 'nthreads' threads
        // each time the Kafka consumer is shut down (which will happen if an error
        // happens while talking to HBase, to get a chance to re-read data from the
        // previous snapshot).
        //
        
        while (true) {
          try {
            Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
            
            topicCountMap.put(topic, nthreads);
                        
            Properties props = new Properties();
            props.setProperty("zookeeper.connect", properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_ZKCONNECT));
            props.setProperty("group.id", groupid);
            if (null != properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_CLIENTID)) {
              props.setProperty("client.id", properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_CLIENTID));
            }
            if (null != properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY)) {
              props.setProperty("partition.assignment.strategy", properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY));
            }
            props.setProperty("auto.commit.enable", "false");    
            
            if (null != properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_AUTO_OFFSET_RESET)) {
              props.setProperty("auto.offset.reset", properties.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_CONSUMER_AUTO_OFFSET_RESET));
            }
            
            ConsumerConfig config = new ConsumerConfig(props);
            ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);

            Map<String,List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
            
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            
            self.barrier = new CyclicBarrier(streams.size() + 1);

            ExecutorService executor = Executors.newFixedThreadPool(nthreads);
            
            //
            // now create runnables which will consume messages
            //
            
            // Reset counters
            counters.reset();
            
            for (final KafkaStream<byte[],byte[]> stream : streams) {
              executor.submit(new DirectoryConsumer(self, stream, counters));
            }      

            while(!abort.get() && !Thread.currentThread().isInterrupted()) {
              try {
                if (streams.size() == barrier.getNumberWaiting()) {
                  //
                  // Check if we should abort, which could happen when
                  // an exception was thrown when flushing the commits just before
                  // entering the barrier
                  //
                  
                  if (abort.get()) {
                    break;
                  }
                    
                  //
                  // All processing threads are waiting on the barrier, this means we can flush the offsets because
                  // they have all processed data successfully for the given activity period
                  //

                  // Commit offsets
                  try {
                    connector.commitOffsets(true);
                  } catch (Throwable t) {
                    throw t;
                  } finally {
                    // We instruct the counters that we committed the offsets, in the worst case
                    // we will experience a backward leap which is not fatal, whereas missing
                    // a commit will lead to a forward leap which will make the Directory fail
                    counters.commit();
                  }
                                   
                  counters.sensisionPublish();
                  
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_COMMITS, Sensision.EMPTY_LABELS, 1);

                  // Release the waiting threads
                  try {
                    barrier.await();
                  } catch (Exception e) {
                    break;
                  }
                }                
              } catch (Throwable t) {
                // We need to catch possible errors in commitOffsets
                LOG.error("", t);
                abort.set(true);
              }
              
              LockSupport.parkNanos(1000000L);
            }

            //
            // We exited the loop, this means one of the threads triggered an abort,
            // we will shut down the executor and shut down the connector to start over.
            //
            
            executor.shutdownNow();
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_DIRECTORY_KAFKA_SHUTDOWNS, Sensision.EMPTY_LABELS, 1);
            connector.shutdown();
          } catch (Throwable t) {
            LOG.error("Caught throwable in spawner.", t);
          } finally {
            abort.set(false);
            LockSupport.parkNanos(1000000000L);
          }
        }          
      }
    });
    
    t.setName("Warp Directory Spawner");
    t.setDaemon(true);
    t.start();
    
    t = new Thread(this);
    t.setName("Warp Directory");
    t.setDaemon(true);
    t.start();
    
    //
    // Start Jetty for the streaming service
    //
    
    //
    // Start Jetty server for the streaming service
    //
    
    BlockingArrayQueue<Runnable> queue = null;
    
    if (props.containsKey(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_MAXQUEUESIZE)) {
      int queuesize = Integer.parseInt(props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_MAXQUEUESIZE));
      queue = new BlockingArrayQueue<Runnable>(queuesize);
    }

    Server server = new Server(new QueuedThreadPool(streamingMaxThreads,8, (int) idleTimeout, queue));
    
    //
    // Iterate over the properties to find those starting with DIRECTORY_STREAMING_JETTY_ATTRIBUTE and set
    // the Jetty attributes accordingly
    //
    
    for (Entry<Object,Object> entry: props.entrySet()) {
      if (entry.getKey().toString().startsWith(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_JETTY_ATTRIBUTE_PREFIX)) {
        server.setAttribute(entry.getKey().toString().substring(io.warp10.continuum.Configuration.DIRECTORY_STREAMING_JETTY_ATTRIBUTE_PREFIX.length()), entry.getValue().toString());
      }
    }
    
    //ServerConnector connector = new ServerConnector(server, this.streamingacceptors, this.streamingselectors);
    HttpConfiguration config = new HttpConfiguration();
    config.setRequestHeaderSize(DIRECTORY_REQUEST_HEADER_SIZE);
    HttpConnectionFactory factory = new HttpConnectionFactory(config);
    ServerConnector connector = new ServerConnector(server,null,null,null,this.streamingacceptors, this.streamingselectors,factory);
    
    connector.setIdleTimeout(idleTimeout);
    connector.setPort(this.streamingport);
    connector.setHost(host);
    connector.setAcceptQueueSize(this.streamingTcpBacklog);
    connector.setName("Directory Streaming Service");
    
    server.setConnectors(new Connector[] { connector });

    server.setHandler(this);
    
    JettyUtil.setSendServerVersion(server, false);
    
    //
    // Wait for initialization to be done
    //
    
    while(!this.fullyInitialized.get()) {
      LockSupport.parkNanos(1000000000L);
    }
    
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void run() {
    
    //
    // Add a shutdown hook to unregister Directory
    //
    
    if (this.register) {
      final Directory self = this;
      this.deregisterHook = new Thread() {
        @Override
        public void run() {
          try {
            LOG.info("Unregistering from ZooKeeper.");
            self.sd.close();
            LOG.info("Directory successfully unregistered from ZooKeeper.");
          } catch (Exception e) {
            LOG.error("Error while unregistering Directory.", e);
          }
        }
      };
      Runtime.getRuntime().addShutdownHook(deregisterHook);
    }

    //
    // Wait until cache has been populated
    //
    
    while(!this.cachePopulated.get()) {
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_JVM_FREEMEMORY, Sensision.EMPTY_LABELS, Runtime.getRuntime().freeMemory());
      LockSupport.parkNanos(1000000000L);
    }

    //
    // Let's call GC once after populating so we take the trash out.
    //
    
    LOG.info("Triggering a GC to clean up after initial loading.");
    long nano = System.nanoTime();
    Runtime.getRuntime().gc();
    nano = System.nanoTime() - nano;
    LOG.info("GC performed in " + (nano / 1000000.0D) + " ms.");
        
    this.fullyInitialized.set(true);
    
    Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_JVM_FREEMEMORY, Sensision.EMPTY_LABELS, Runtime.getRuntime().freeMemory());

    //
    // Start the Thrift Service
    //
        
    DirectoryService.Processor processor = new DirectoryService.Processor(this);
    
    this.instance = null;

    try {
      InetAddress bindAddress = InetAddress.getByName(this.host);
      TServerTransport transport = new TServerSocket(new ServerSocket(this.port, this.tcpBacklog, bindAddress));
      TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
      args.processor(processor);
      //
      // FIXME(dmn): Set the min/max threads in the config file ?
      args.maxWorkerThreads(this.serviceNThreads);
      args.minWorkerThreads(this.serviceNThreads);
      if (0 != maxThriftFrameLength) {
        args.inputTransportFactory(new io.warp10.thrift.TFramedTransport.Factory(maxThriftFrameLength));
        args.outputTransportFactory(new io.warp10.thrift.TFramedTransport.Factory(maxThriftFrameLength));        
      } else {
        args.inputTransportFactory(new io.warp10.thrift.TFramedTransport.Factory());
        args.outputTransportFactory(new io.warp10.thrift.TFramedTransport.Factory());
      }
      args.inputProtocolFactory(new TCompactProtocol.Factory());
      args.outputProtocolFactory(new TCompactProtocol.Factory());
      TServer server = new TThreadPoolServer(args);
      
      //
      // TODO(hbs): Check that the number of registered services does not go over the licensed number
      //

      ServiceInstanceBuilder<Map> builder = ServiceInstance.builder();
      builder.port(((TServerSocket) transport).getServerSocket().getLocalPort());
      builder.address(((TServerSocket) transport).getServerSocket().getInetAddress().getHostAddress());
      builder.id(UUID.randomUUID().toString());
      builder.name(DIRECTORY_SERVICE);
      builder.serviceType(ServiceType.DYNAMIC);
      Map<String,String> payload = new HashMap<String,String>();
      
      payload.put(PAYLOAD_MODULUS_KEY, Integer.toString(modulus));
      payload.put(PAYLOAD_REMAINDER_KEY, Integer.toString(remainder));
      payload.put(PAYLOAD_THRIFT_PROTOCOL_KEY, "org.apache.thrift.protocol.TCompactProtocol");
      payload.put(PAYLOAD_THRIFT_TRANSPORT_KEY, "org.apache.thrift.transport.TFramedTransport");
      payload.put(PAYLOAD_STREAMING_PORT_KEY, Integer.toString(this.streamingport));
      if (0 != maxThriftFrameLength) {
        payload.put(PAYLOAD_THRIFT_MAXFRAMELEN_KEY, Integer.toString(maxThriftFrameLength));
      }
      builder.payload(payload);

      this.instance = builder.build();

      if (this.register) {
        sd.start();
        sd.registerService(instance);
      }
      
      server.serve();

    } catch (TTransportException tte) {
      LOG.error("",tte);
    } catch (Exception e) {
      LOG.error("", e);
    } finally {
      if (null != instance) {
        try {
          sd.unregisterService(instance);
        } catch (Exception e) {
        }        
      }
      if (null != deregisterHook) {
        try {
          Runtime.getRuntime().removeShutdownHook(deregisterHook);
        } catch (Exception e) {          
        }
      }
    }
  }
  
  /**
   * Extract Directory related keys and populate the KeyStore with them.
   * 
   * @param props Properties from which to extract the key specs
   */
  private void extractKeys(Properties props) {
    String keyspec = props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_MAC);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_MAC + " MUST be 128 bits long.");
      this.keystore.setKey(KeyStore.SIPHASH_KAFKA_METADATA, key);
    }

    keyspec = props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_AES);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + io.warp10.continuum.Configuration.DIRECTORY_KAFKA_METADATA_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_KAFKA_METADATA, key);
    }
    
    keyspec = props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_AES);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + io.warp10.continuum.Configuration.DIRECTORY_HBASE_METADATA_AES + " MUST be 128, 192 or 256 bits long.");
      this.keystore.setKey(KeyStore.AES_HBASE_METADATA, key);
    }
    
    keyspec = props.getProperty(io.warp10.continuum.Configuration.DIRECTORY_PSK);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + io.warp10.continuum.Configuration.DIRECTORY_PSK + " MUST be 128 bits long.");      
      this.keystore.setKey(KeyStore.SIPHASH_DIRECTORY_PSK, key);      
    }
    
    this.keystore.forget();
  }

  private static class DirectoryConsumer implements Runnable {

    private final Directory directory;
    private final KafkaStream<byte[],byte[]> stream;
        
    private final KafkaOffsetCounters counters;
    
    private final AtomicBoolean localabort = new AtomicBoolean(false);
    
    public DirectoryConsumer(Directory directory, KafkaStream<byte[], byte[]> stream, KafkaOffsetCounters counters) {
      this.directory = directory;
      this.stream = stream;
      this.counters = counters;
    }
    
    @Override
    public void run() {
      Table htable = null;

      try {
        ConsumerIterator<byte[],byte[]> iter = this.stream.iterator();

        byte[] siphashKey = directory.keystore.getKey(KeyStore.SIPHASH_KAFKA_METADATA);
        byte[] kafkaAESKey = directory.keystore.getKey(KeyStore.AES_KAFKA_METADATA);
            
        htable = directory.conn.getTable(directory.hbaseTable);

        final Table ht = htable;

        //
        // AtomicLong with the timestamp of the last Put or 0 if
        // none were added since the last flush
        //
        
        final AtomicLong lastAction = new AtomicLong(0L);
        
        final List<Mutation> actions = new ArrayList<Mutation>();
        final ReentrantLock actionsLock = new ReentrantLock();
        
        final AtomicLong actionsSize = new AtomicLong(0L);
        
        //
        // Start the synchronization Thread
        //
        
        Thread synchronizer = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              long lastsync = System.currentTimeMillis();
              long lastflush = lastsync;
              
              //
              // Check for how long we've been storing readings, if we've reached the commitperiod,
              // flush any pending commits and synchronize with the other threads so offsets can be committed
              //

              while(!localabort.get() && !Thread.currentThread().isInterrupted()) { 
                long now = System.currentTimeMillis();
                
                if (now - lastsync > directory.commitPeriod) {
                  //
                  // We synchronize on 'puts' so the main Thread does not add Puts to it
                  //
                  
                  //synchronized (puts) {
                  try {
                    actionsLock.lockInterruptibly();
                    
                    //
                    // Attempt to flush
                    //
                    
                    try {
                      Object[] results = new Object[actions.size()];
                      
                      if (directory.store||directory.delete) {
                        ht.batch(actions, results);

                        // Check results for nulls
                        for (Object o: results) {
                          if (null == o) {
                            throw new IOException("At least one action (Put/Delete) failed.");
                          }
                        }
                        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_HBASE_COMMITS, Sensision.EMPTY_LABELS, 1);
                      }
                      
                      actions.clear();
                      actionsSize.set(0L);
                      // Reset lastPut to 0
                      lastAction.set(0L);                    
                    } catch (IOException ioe) {
                      // Clear list of Puts
                      actions.clear();
                      actionsSize.set(0L);
                      // If an exception is thrown, abort
                      directory.abort.set(true);
                      return;
                    } catch (InterruptedException ie) {
                      // Clear list of Puts
                      actions.clear();
                      actionsSize.set(0L);
                      // If an exception is thrown, abort
                      directory.abort.set(true);
                      return;                    
                    }
                    //
                    // Now join the cyclic barrier which will trigger the
                    // commit of offsets
                    //
                    try {
                      directory.barrier.await();
                      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_BARRIER_SYNCS, Sensision.EMPTY_LABELS, 1);
                    } catch (Exception e) {
                      directory.abort.set(true);
                      return;
                    } finally {
                      lastsync = System.currentTimeMillis();
                    }
                  } catch (InterruptedException ie) {
                    directory.abort.set(true);
                    return;
                  } finally {
                    if (actionsLock.isHeldByCurrentThread()) {
                      actionsLock.unlock();
                    }                  
                  }
                } else if (0 != lastAction.get() && (now - lastAction.get() > 500) || actionsSize.get() > directory.maxPendingPutsSize) {
                  //
                  // If the last Put was added to 'ht' more than 500ms ago, force a flush
                  //
                  
                  try {
                    //synchronized(puts) {
                    actionsLock.lockInterruptibly();
                    try {
                      Object[] results = new Object[actions.size()];
                      
                      if (directory.store||directory.delete) {
                        ht.batch(actions, results);
                        
                        // Check results for nulls
                        for (Object o: results) {
                          if (null == o) {
                            throw new IOException("At least one Put failed.");
                          }
                        }
                        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_HBASE_COMMITS, Sensision.EMPTY_LABELS, 1);
                      }
                      
                      actions.clear();
                      actionsSize.set(0L);
                      // Reset lastPut to 0
                      lastAction.set(0L);
                    } catch (IOException ioe) {
                      // Clear list of Puts
                      actions.clear();
                      actionsSize.set(0L);
                      directory.abort.set(true);
                      return;
                    } catch(InterruptedException ie) {                  
                      // Clear list of Puts
                      actions.clear();
                      actionsSize.set(0L);
                      directory.abort.set(true);
                      return;
                    }                  
                  } catch (InterruptedException ie) {
                    directory.abort.set(true);
                    return;
                  } finally {
                    if (actionsLock.isHeldByCurrentThread()) {
                      actionsLock.unlock();
                    }                  
                  }
                }
   
                LockSupport.parkNanos(1000000L);
              }              
            } catch (Throwable t) {
              LOG.error("Caught exception in synchronizer", t);
              throw t;
            } finally {
              directory.abort.set(true);
            }
          }
        });
        
        synchronizer.setName("Warp Directory Synchronizer");
        synchronizer.setDaemon(true);
        synchronizer.start();
        
        // TODO(hbs): allow setting of writeBufferSize

        MetadataID id = null;
        
        byte[] hbaseAESKey = directory.keystore.getKey(KeyStore.AES_HBASE_METADATA);
        
        while (iter.hasNext()) {
          Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_JVM_FREEMEMORY, Sensision.EMPTY_LABELS, Runtime.getRuntime().freeMemory());

          //
          // Since the call to 'next' may block, we need to first
          // check that there is a message available, otherwise we
          // will miss the synchronization point with the other
          // threads.
          //
          
          boolean nonEmpty = iter.nonEmpty();
          
          if (nonEmpty) {
            MessageAndMetadata<byte[], byte[]> msg = iter.next();
            if (!counters.safeCount(msg.partition(), msg.offset())) {
              continue;
            }
            
            //
            // We do an early selection check based on the Kafka key.
            // Since 20151104 we now correctly push the Kafka key (cf Ingress bug in pushMetadataMessage(k,v))
            //
                        
            int r = (((int) msg.key()[8]) & 0xff) % directory.modulus;
            
            if (directory.remainder != r) {
              continue;
            }

            //
            // We do not rely on the Kafka key for selection as it might have been incorrectly set.
            // We therefore unwrap all messages and decide later.
            //
            
            byte[] data = msg.message();
            
            if (null != siphashKey) {
              data = CryptoUtils.removeMAC(siphashKey, data);
            }
            
            // Skip data whose MAC was not verified successfully
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_FAILEDMACS, Sensision.EMPTY_LABELS, 1);
              continue;
            }
            
            // Unwrap data if need be
            if (null != kafkaAESKey) {
              data = CryptoUtils.unwrap(kafkaAESKey, data);
            }
            
            // Skip data that was not unwrapped successfuly
            if (null == data) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_KAFKA_FAILEDDECRYPTS, Sensision.EMPTY_LABELS, 1);
              continue;
            }
            
            //
            // TODO(hbs): We could check that metadata class/labels Id match those of the key, but
            // since it was wrapped/authenticated, we suppose it's ok.
            //
                        
            //byte[] labelsBytes = Arrays.copyOfRange(data, 8, 16);
            //long labelsId = Longs.fromByteArray(labelsBytes);
            
            // 128bits
            byte[] metadataBytes = Arrays.copyOfRange(data, 16, data.length);
            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
            Metadata metadata = new Metadata();
            deserializer.deserialize(metadata, metadataBytes);
            
            //
            // Force Attributes
            //
            
            if (!metadata.isSetAttributes()) {
              metadata.setAttributes(new HashMap<String,String>());
            }
            
            //
            // Recompute labelsid and classid
            //
            
            // 128bits
            long classId = GTSHelper.classId(directory.SIPHASH_CLASS_LONGS, metadata.getName());
            long labelsId = GTSHelper.labelsId(directory.SIPHASH_LABELS_LONGS, metadata.getLabels());

            metadata.setLabelsId(labelsId);
            metadata.setClassId(classId);

            //
            // Recheck labelsid so we don't retain GTS with invalid labelsid in the row key (which may have happened due
            // to bugs)
            //
            
            int rem = ((int) ((labelsId >>> 56) & 0xffL)) % directory.modulus;
            
            if (directory.remainder != rem) {
              continue;
            }
            
            String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
            Map<String,String> sensisionLabels = new HashMap<String,String>();
            sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);
            
            //
            // Check the source of the metadata
            //

            //
            // If Metadata is from Delete, remove it from the cache AND from HBase
            //
            
            if (io.warp10.continuum.Configuration.INGRESS_METADATA_DELETE_SOURCE.equals(metadata.getSource())) {
              
              //
              // Call external plugin
              //
              
              if (null != directory.plugin) {
                
                long nano = 0;
                
                try {
                  GTS gts = new GTS(
                      // 128bits
                      new UUID(metadata.getClassId(), metadata.getLabelsId()),
                      metadata.getName(),
                      metadata.getLabels(),
                      metadata.getAttributes());
                  
                  nano = System.nanoTime();
                      
                  if (!directory.plugin.delete(gts)) {
                    break;
                  }                  
                } finally {
                  nano = System.nanoTime() - nano;
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_DELETE_CALLS, Sensision.EMPTY_LABELS, 1);
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_DELETE_TIME_NANOS, Sensision.EMPTY_LABELS, nano);                  
                }

              } else {
                // If the name does not exist AND we actually loaded the metadata from HBase, continue.
                // If we did not load from HBase, handle the delete
                if (!directory.metadatas.containsKey(metadata.getName()) && directory.init) {
                  continue;
                }

                // Remove cache entry
                Map<Long,Metadata> metamap = directory.metadatas.get(metadata.getName());
                if (null != metamap && null != metamap.remove(labelsId)) {
                  if (metamap.isEmpty()) {
                    try {
                      directory.metadatasLock.lockInterruptibly();
                      metamap = directory.metadatas.get(metadata.getName());
                      if (null != metamap && metamap.isEmpty()) {
                        directory.metadatas.remove(metadata.getName());
                        directory.classNames.remove(metadata.getClassId());
                        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_CLASSES, Sensision.EMPTY_LABELS, -1);
                      }
                    } finally {
                      if (directory.metadatasLock.isHeldByCurrentThread()) {
                        directory.metadatasLock.unlock();
                      }
                    }                    
                  }
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, -1);
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, -1);
                }
              }

              //
              // Clear the cache
              //
              
              id = MetadataUtils.id(metadata);
              directory.serializedMetadataCache.remove(id);
              
              if (!directory.delete) {
                continue;
              }

              // Remove HBase entry
              
              // Prefix + classId + labelsId
              byte[] rowkey = new byte[Constants.HBASE_METADATA_KEY_PREFIX.length + 8 + 8];

              // 128bits
              ByteBuffer bb = ByteBuffer.wrap(rowkey).order(ByteOrder.BIG_ENDIAN);
              bb.put(Constants.HBASE_METADATA_KEY_PREFIX);
              bb.putLong(classId);
              bb.putLong(labelsId);
              
              //System.arraycopy(HBASE_METADATA_KEY_PREFIX, 0, rowkey, 0, HBASE_METADATA_KEY_PREFIX.length);
              // Copy classId/labelsId
              //System.arraycopy(data, 0, rowkey, HBASE_METADATA_KEY_PREFIX.length, 16);
              
              Delete delete = new Delete(rowkey);

              try {
                actionsLock.lockInterruptibly();
                //htable.delete(delete);
                actions.add(delete);
                // estimate the size of the Delete
                actionsSize.addAndGet(rowkey.length + 16);
                lastAction.set(System.currentTimeMillis());
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_HBASE_DELETES, Sensision.EMPTY_LABELS, 1);                
              } finally {
                if (actionsLock.isHeldByCurrentThread()) {
                  actionsLock.unlock();
                }
              }
              
              continue;
            } // if (io.warp10.continuum.Configuration.INGRESS_METADATA_DELETE_SOURCE.equals(metadata.getSource())                   

            //
            // Call external plugin
            //
            
            if (null != directory.plugin) {
              long nano = 0;
              
              try {
                GTS gts = new GTS(
                    // 128bits
                    new UUID(metadata.getClassId(), metadata.getLabelsId()),
                    metadata.getName(),
                    metadata.getLabels(),
                    metadata.getAttributes());

                //
                // If we are doing a metadata update and the GTS is not known, skip the call to store.
                //
                
                if ((io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource()) || io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) && !directory.plugin.known(gts)) {
                  continue;
                }

                nano = System.nanoTime();
                
                if (!directory.plugin.store(metadata.getSource(), gts)) {
                  // If we could not store the GTS, stop the directory consumer
                  break;
                }                
              } finally {
                nano = System.nanoTime() - nano;
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_CALLS, Sensision.EMPTY_LABELS, 1);
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_STORE_TIME_NANOS, Sensision.EMPTY_LABELS, nano);                  
              }
              
            } else { // no directory plugin
              //
              // If Metadata comes from Ingress and it is already in the cache, do
              // nothing. Unless we are tracking activity in which case we need to check
              // if the last activity is more recent than the one we have in cache and then update
              // the attributes.
              //
              
              if (io.warp10.continuum.Configuration.INGRESS_METADATA_SOURCE.equals(metadata.getSource())
                  && directory.metadatas.containsKey(metadata.getName())
                  && directory.metadatas.get(metadata.getName()).containsKey(labelsId)) {
                
                if (!directory.trackingActivity) {
                  continue;
                }
                
                //
                // We need to keep track of the activity
                //
                
                Metadata meta = directory.metadatas.get(metadata.getName()).get(labelsId);
                
                // If none of the Metadata instances has a last activity recorded, do nothing
                if (!metadata.isSetLastActivity() && !meta.isSetLastActivity()) {
                  continue;
                }
                
                // If one of the Metadata instances does not have a last activity set, use the one which is defined
                
                if (!metadata.isSetLastActivity()) {
                  metadata.setLastActivity(meta.getLastActivity());
                } else if (!meta.isSetLastActivity()) {
                  meta.setLastActivity(metadata.getLastActivity());
                } else if (metadata.getLastActivity() -  meta.getLastActivity() < directory.activityWindow) {
                  // both Metadata have a last activity set, but the activity window has not yet passed, so do nothing
                  continue;
                }

                // Update last activity, replace serialized GTS so we have the correct activity timestamp AND
                // the correct attributes when storing. Clear the serialized metadata cache
                meta.setLastActivity(Math.max(meta.getLastActivity(), metadata.getLastActivity()));
                metadata.setLastActivity(meta.getLastActivity());
                
                // Copy attributes from the currently store Metadata instance
                metadata.setAttributes(meta.getAttributes());
                
                TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
                metadataBytes = serializer.serialize(meta);
                
                id = MetadataUtils.id(metadata);
                directory.serializedMetadataCache.remove(id);
              }              
              
              //
              // If metadata is an update, only take it into consideration if the GTS is already known
              //
              
              if ((io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())
                  || io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource()))
                  && (!directory.metadatas.containsKey(metadata.getName())
                      || !directory.metadatas.get(metadata.getName()).containsKey(labelsId))) {
                continue;
              }              
            }
            
            //
            // Clear the cache if it is an update
            //
            
            if (io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())
                || io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) {
              id = MetadataUtils.id(metadata);
              directory.serializedMetadataCache.remove(id);
              
              //
              // If this is a delta update of attributes, consolidate them
              //
              
              if (io.warp10.continuum.Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) {
                Metadata meta = directory.metadatas.get(metadata.getName()).get(labelsId);
                
                for (Entry<String,String> attr: metadata.getAttributes().entrySet()) {
                  if ("".equals(attr.getValue())) {
                    meta.getAttributes().remove(attr.getKey());
                  } else {
                    meta.putToAttributes(attr.getKey(), attr.getValue());
                  }
                }
                
                // We need to update the attributes with those from 'meta' so we
                // store the up to date version of the Metadata in HBase
                metadata.setAttributes(new HashMap<String,String>(meta.getAttributes()));
                  
                // We re-serialize metadata
                TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
                metadataBytes = serializer.serialize(metadata);
              }
              
              //
              // Update the last activity
              //
              
              if (null == directory.plugin) {                
                Metadata meta = directory.metadatas.get(metadata.getName()).get(labelsId);
                
                boolean hasChanged = false;
                
                if (!metadata.isSetLastActivity() && meta.isSetLastActivity()) {
                  metadata.setLastActivity(meta.getLastActivity());
                  hasChanged = true;
                } else if (metadata.isSetLastActivity() && meta.isSetLastActivity() && meta.getLastActivity() > metadata.getLastActivity()) {
                  // Take the most recent last activity timestamp
                  metadata.setLastActivity(meta.getLastActivity());
                  hasChanged = true;
                }
                                
                if (hasChanged) {
                  // We re-serialize metadata
                  TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
                  metadataBytes = serializer.serialize(metadata);
                }                
              }
            }
                        
            //
            // Write Metadata to HBase as it is either new or an updated version\
            // WARNING(hbs): in case of an updated version, we might erase a newer version of
            // the metadata (in case we updated it already but the Kafka offsets were not committed prior to
            // a failure of this Directory process). This will eventually be corrected when the newer version is
            // later re-read from Kafka.
            //
            
            // Prefix + classId + labelsId
            byte[] rowkey = new byte[Constants.HBASE_METADATA_KEY_PREFIX.length + 8 + 8];

            ByteBuffer bb = ByteBuffer.wrap(rowkey).order(ByteOrder.BIG_ENDIAN);
            bb.put(Constants.HBASE_METADATA_KEY_PREFIX);
            bb.putLong(classId);
            bb.putLong(labelsId);

            //System.arraycopy(HBASE_METADATA_KEY_PREFIX, 0, rowkey, 0, HBASE_METADATA_KEY_PREFIX.length);
            // Copy classId/labelsId
            //System.arraycopy(data, 0, rowkey, HBASE_METADATA_KEY_PREFIX.length, 16);

            //
            // Encrypt contents
            //
                        
            Put put = null;
            byte[] encrypted = null;
            
            if (directory.store) {
              put = new Put(rowkey);
              if (null != hbaseAESKey) {
                encrypted = CryptoUtils.wrap(hbaseAESKey, metadataBytes);
                put.addColumn(directory.colfam, new byte[0], encrypted);
              } else {
                put.addColumn(directory.colfam, new byte[0], metadataBytes);
              }
            }
              
            try {
              actionsLock.lockInterruptibly();
              //synchronized (puts) {
              if (directory.store) {
                actions.add(put);
                actionsSize.addAndGet(encrypted.length);
                lastAction.set(System.currentTimeMillis());
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_HBASE_PUTS, Sensision.EMPTY_LABELS, 1);                
              }

              if (null != directory.plugin) {
                continue;
              }

              //
              // Internalize Strings
              //
              
              GTSHelper.internalizeStrings(metadata);

              //
              // Store it in the cache (we dot that in the synchronized section)
              //

              //byte[] classBytes = Arrays.copyOf(data, 8);            
              //long classId = Longs.fromByteArray(classBytes);

              try {
                directory.metadatasLock.lockInterruptibly();
                if (!directory.metadatas.containsKey(metadata.getName())) {
                  //directory.metadatas.put(metadata.getName(), new ConcurrentHashMap<Long,Metadata>());
                  // This is done under the synchronization of actionsLock
                  directory.metadatas.put(metadata.getName(), new ConcurrentSkipListMap<Long,Metadata>(ID_COMPARATOR));
                  directory.classNames.put(classId, metadata.getName());
                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_CLASSES, Sensision.EMPTY_LABELS, 1);
                }                
              } finally {
                if (directory.metadatasLock.isHeldByCurrentThread()) {
                  directory.metadatasLock.unlock();
                }
              }
              
              //
              // Store per owner class
              //

              String owner = metadata.getLabels().get(Constants.OWNER_LABEL);
              
              synchronized(directory.classesPerOwner) {
                Set<String> classes = directory.classesPerOwner.get(owner);
                
                if (null == classes) {
                  classes = new HashSet<String>();
                  directory.classesPerOwner.put(owner, classes);
                }
                
                classes.add(metadata.getName());
              }
              
              Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_OWNERS, Sensision.EMPTY_LABELS, directory.classesPerOwner.size());
              
              //
              // Force classId/labelsId in Metadata, we will need them!
              //
              
              metadata.setClassId(classId);
              metadata.setLabelsId(labelsId);
              
              // 128bits
              if (null == directory.metadatas.get(metadata.getName()).put(labelsId, metadata)) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, 1);
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, 1);
              }
           
            } finally {
              if (actionsLock.isHeldByCurrentThread()) {
                actionsLock.unlock();
              }
            }
          } else {
            // Sleep a tiny while
            LockSupport.parkNanos(2000000L);
          }          
        }        
      } catch (Throwable t) {
        LOG.error("", t);
      } finally {
        // Set abort to true in case we exit the 'run' method
        directory.abort.set(true);
        this.localabort.set(true);
        if (null != htable) {
          try { htable.close(); } catch (IOException ioe) {}
        }
      }
    }
  }

  @Override
  public DirectoryFindResponse find(DirectoryFindRequest request) throws TException {
    
    DirectoryFindResponse response = new DirectoryFindResponse();

    //
    // Check request age
    //
    
    long now = System.currentTimeMillis();
    
    if (now - request.getTimestamp() > this.maxage) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_EXPIRED, Sensision.EMPTY_LABELS, 1);
      response.setError("Request has expired.");
      return response;
    }
    
    //
    // Compute request hash
    //
    
    long hash = DirectoryUtil.computeHash(SIPHASH_PSK_LONGS[0], SIPHASH_PSK_LONGS[1], request);
    
    // Check hash against value in the request
    
    if (hash != request.getHash()) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_INVALID, Sensision.EMPTY_LABELS, 1);
      response.setError("Invalid request.");
      return response;
    }
    
    //
    // Build patterns from expressions
    //
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_REQUESTS, Sensision.EMPTY_LABELS, 1);

    SmartPattern classSmartPattern;
    
    Collection<Metadata> metas;
    
    //
    // Allocate a set if there is more than one class selector as we may have
    // duplicate results
    //
    
    if (request.getClassSelectorSize() > 1) {
      metas = new HashSet<Metadata>();
    } else {
      metas = new ArrayList<Metadata>();
    }
    
    long count = 0;

    List<String> missingLabels = Constants.ABSENT_LABEL_SUPPORT ? new ArrayList<String>() : null;
    
    for (int i = 0; i < request.getClassSelectorSize(); i++) {
      
      //
      // Call external plugin if it is defined
      //
      
      if (null != this.plugin) {
        
        long time = 0;
        long precount = 0;
        long nano = System.nanoTime();
        
        try (DirectoryPlugin.GTSIterator iter = this.plugin.find(this.remainder, request.getClassSelector().get(i), request.getLabelsSelectors().get(i))) {
          
          while(iter.hasNext()) {
            
            GTS gts = iter.next();
            nano = System.nanoTime() - nano;
            time += nano;
            
            count++;
            
            if (count >= maxHardFindResults) {
              Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_LIMITED, Sensision.EMPTY_LABELS, 1);
              response.setError("Find request would return more than " + maxHardFindResults + " results, aborting.");
              return response;
            }

            Metadata meta = new Metadata();
            meta.setName(gts.getName());
            //meta.setLabels(ImmutableMap.copyOf(metadata.getLabels()));
            meta.setLabels(new HashMap<String, String>(gts.getLabels()));
            //meta.setAttributes(ImmutableMap.copyOf(metadata.getAttributes()));
            meta.setAttributes(new HashMap<String,String>(gts.getAttributes()));
            meta.setClassId(GTSHelper.classId(SIPHASH_CLASS_LONGS, meta.getName()));
            meta.setLabelsId(GTSHelper.labelsId(SIPHASH_LABELS_LONGS, meta.getLabels()));
            
            metas.add(meta);
            nano = System.nanoTime();
          }                  
        } catch (Exception e) {          
        } finally {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_CALLS, Sensision.EMPTY_LABELS, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_RESULTS, Sensision.EMPTY_LABELS, count - precount);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_TIME_NANOS, Sensision.EMPTY_LABELS, time);                  
        }
      } else { // No Directory plugin
        String exactClassName = null;
        
        if (request.getClassSelector().get(i).startsWith("=") || !request.getClassSelector().get(i).startsWith("~")) {
          exactClassName = request.getClassSelector().get(i).startsWith("=") ? request.getClassSelector().get(i).substring(1) : request.getClassSelector().get(i);
          classSmartPattern = new SmartPattern(exactClassName);
        } else {
          classSmartPattern = new SmartPattern(Pattern.compile(request.getClassSelector().get(i).substring(1)));
        }
        
        Map<String,SmartPattern> labelPatterns = new HashMap<String,SmartPattern>();
        
        if (null != missingLabels) {
          missingLabels.clear();          
        }
        
        if (request.getLabelsSelectors().get(i).size() > 0) {
          for (Entry<String,String> entry: request.getLabelsSelectors().get(i).entrySet()) {
            String label = entry.getKey();
            String expr = entry.getValue();
            SmartPattern pattern;
            
            if (null != missingLabels && ("=".equals(expr) || "".equals(expr))) {
              missingLabels.add(label);
              continue;
            }

            if (expr.startsWith("=") || !expr.startsWith("~")) {
              pattern = new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr);
            } else {
              pattern = new SmartPattern(Pattern.compile(expr.substring(1)));
            }
            
            labelPatterns.put(label,  pattern);
          }      
        }
              
        //
        // Loop over the class names to find matches
        //

        // Copy the class names as 'this.classNames' might be updated while in the for loop
        Collection<String> classNames = new ArrayList<String>();
        
        if (null != exactClassName) {
          // If the class name is an exact match, check if it is known, if not, skip to the next selector
          if(!this.metadatas.containsKey(exactClassName)) {
            continue;
          }
          classNames.add(exactClassName);
        } else {
          //
          // Extract per owner classes if owner selector exists
          //
          if (request.getLabelsSelectors().get(i).size() > 0) {
            String ownersel = request.getLabelsSelectors().get(i).get(Constants.OWNER_LABEL);
            
            if (null != ownersel && ownersel.startsWith("=")) {
              classNames = classesPerOwner.get(ownersel.substring(1));
            } else {
              classNames = this.classNames.values();
            }
          } else {
            classNames = this.classNames.values();
          }
        }

        List<String> labelNames = new ArrayList<String>(labelPatterns.size());
        List<SmartPattern> labelSmartPatterns = new ArrayList<SmartPattern>(labelPatterns.size());
        String[] labelValues = null;
        
        //
        // Put producer/app/owner first
        //
        
        if (labelPatterns.containsKey(Constants.PRODUCER_LABEL)) {
          labelNames.add(Constants.PRODUCER_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.PRODUCER_LABEL));
          labelPatterns.remove(Constants.PRODUCER_LABEL);
        }
        if (labelPatterns.containsKey(Constants.APPLICATION_LABEL)) {
          labelNames.add(Constants.APPLICATION_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.APPLICATION_LABEL));
          labelPatterns.remove(Constants.APPLICATION_LABEL);
        }
        if (labelPatterns.containsKey(Constants.OWNER_LABEL)) {
          labelNames.add(Constants.OWNER_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.OWNER_LABEL));
          labelPatterns.remove(Constants.OWNER_LABEL);
        }
        
        //
        // Now add the other labels
        //
        
        for(Entry<String,SmartPattern> entry: labelPatterns.entrySet()) {
          labelNames.add(entry.getKey());
          labelSmartPatterns.add(entry.getValue());
        }

        labelValues = new String[labelNames.size()];
        
        for (String className: classNames) {
          
          //
          // If class matches, check all labels for matches
          //
          
          if (classSmartPattern.matches(className)) {
            for (Metadata metadata: this.metadatas.get(className).values()) {
              boolean exclude = false;

              if (null != missingLabels) {
                for (String missing: missingLabels) {
                  // If the Metadata contain one of the missing labels, exclude the entry
                  if (null != metadata.getLabels().get(missing)) {
                    exclude = true;
                    break;
                  }
                }              
                // Check attributes
                if (!exclude && metadata.getAttributesSize() > 0) {
                  for (String missing: missingLabels) {
                    // If the Metadata contain one of the missing labels, exclude the entry
                    if (null != metadata.getAttributes().get(missing)) {
                      exclude = true;
                      break;
                    }
                  }                              
                }
                if (exclude) {
                  continue;
                }
              }
              
              int idx = 0;
        
              for (String labelName: labelNames) {
                //
                // Immediately exclude metadata which do not contain one of the
                // labels for which we have patterns either in labels or in attributes
                //

                String labelValue = metadata.getLabels().get(labelName);
                
                if (null == labelValue) {
                  labelValue = metadata.getAttributes().get(labelName);
                  if (null == labelValue) {
                    exclude = true;
                    break;
                  }
                }
                
                labelValues[idx++] = labelValue;
              }
              
              // If we did not collect enough label/attribute values, exclude the GTS
              if (idx < labelNames.size()) {
                exclude = true;
              }
              
              if (exclude) {
                continue;
              }
              
              //
              // Check if the label value matches, if not, exclude the GTS
              //
              
              for (int j = 0; j < labelNames.size(); j++) {
                if (!labelSmartPatterns.get(j).matches(labelValues[j])) {
                  exclude = true;
                  break;
                }
              }
              
              if (exclude) {
                continue;
              }

              //
              // We have a match, rebuild metadata
              //

              if (count >= maxHardFindResults) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_LIMITED, Sensision.EMPTY_LABELS, 1);
                response.setError("Find request would return more than " + maxHardFindResults + " results, aborting.");
                return response;
              }

              Metadata meta = new Metadata();
              meta.setName(className);
              //meta.setLabels(ImmutableMap.copyOf(metadata.getLabels()));
              meta.setLabels(new HashMap<String, String>(metadata.getLabels()));
              //meta.setAttributes(ImmutableMap.copyOf(metadata.getAttributes()));
              meta.setAttributes(new HashMap<String,String>(metadata.getAttributes()));
              meta.setClassId(GTSHelper.classId(SIPHASH_CLASS_LONGS, meta.getName()));
              meta.setLabelsId(GTSHelper.labelsId(SIPHASH_LABELS_LONGS, meta.getLabels()));
              
              metas.add(meta);
              
              count++;
            }
          }
        }      
      }        
    }

    if (request.getClassSelectorSize() > 1) {
      // We create a list because 'metas' is a set
      response.setMetadatas(new ArrayList<Metadata>());
      response.getMetadatas().addAll(metas);
    } else {
      response.setMetadatas((List<Metadata>) metas);
    }

    //
    // Optimize the result when the number of matching Metadata exceeds maxFindResults.
    // We extract common labels and attempt to compress the result
    //
    
    count = response.getMetadatasSize();
    
    if (count >= this.maxFindResults) {
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_COMPACTED, Sensision.EMPTY_LABELS, 1);
      
      //
      // Extract common labels
      //
      
      Map<String,String> commonLabels = null;
      Set<String> remove = new HashSet<String>();
      
      for (Metadata metadata: response.getMetadatas()) {
        if (null == commonLabels) {
          commonLabels = new HashMap<String, String>(metadata.getLabels());
          continue;
        }
        
        remove.clear();
        
        for (Entry<String,String> entry: commonLabels.entrySet()) {
          if (!metadata.getLabels().containsKey(entry.getKey()) || !entry.getValue().equals(metadata.getLabels().get(entry.getKey()))) {
            remove.add(entry.getKey());
          }
        }
        
        if (!remove.isEmpty()) {
          for (String label: remove) {
            commonLabels.remove(label);
          }
        }
      }
      
      //
      // Remove common labels from all Metadata
      //
      
      long commonLabelsSize = 0;
      
      if (!commonLabels.isEmpty()) {
        for (Metadata metadata: response.getMetadatas()) {
          for (String label: commonLabels.keySet()) {
            metadata.getLabels().remove(label);
          }
        }
        
        //
        // Estimate common labels size
        //
        
        for (Entry<String,String> entry: commonLabels.entrySet()) {
          commonLabelsSize += entry.getKey().length() * 2 + entry.getValue().length() * 2;
        }
        
        response.setCommonLabels(commonLabels);
      }
      
      
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

      byte[] serialized = serializer.serialize(response);
        
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        GZIPOutputStream gzos = new GZIPOutputStream(baos);
        gzos.write(serialized);
        gzos.close();
      } catch (IOException ioe) {
        throw new TException(ioe);
      }
      
      serialized = baos.toByteArray();
        
      if (serialized.length > this.maxThriftFrameLength - commonLabelsSize - 256) {
        response.setError("Find request result would exceed maximum result size (" + this.maxThriftFrameLength + " bytes).");
        response.getMetadatas().clear();
        response.getCommonLabels().clear();
        return response;
      }
      
      response = new DirectoryFindResponse();
      response.setCompressed(serialized);
    }
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_FIND_RESULTS, Sensision.EMPTY_LABELS, count);

    return response;
  }
  
  @Override
  public DirectoryGetResponse get(DirectoryGetRequest request) throws TException {
    DirectoryGetResponse response = new DirectoryGetResponse();
    
    String name = this.classNames.get(request.getClassId());
    
    if (null != name) {
      Metadata metadata = this.metadatas.get(name).get(request.getLabelsId()); 
      if (null != metadata) {
        response.setMetadata(metadata);
      }
    }
    
    return response;
  }
  
  @Override
  public DirectoryStatsResponse stats(DirectoryStatsRequest request) throws TException {
    
    try {
      DirectoryStatsResponse response = new DirectoryStatsResponse();

      //
      // Check request age
      //
      
      long now = System.currentTimeMillis();
      
      if (now - request.getTimestamp() > this.maxage) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_EXPIRED, Sensision.EMPTY_LABELS, 1);
        response.setError("Request has expired.");
        return response;
      }
      
      //
      // Compute request hash
      //
      
      long hash = DirectoryUtil.computeHash(SIPHASH_PSK_LONGS[0], SIPHASH_PSK_LONGS[1], request);
      
      // Check hash against value in the request
      
      if (hash != request.getHash()) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_INVALID, Sensision.EMPTY_LABELS, 1);
        response.setError("Invalid request.");
        return response;
      }
      
      //
      // Build patterns from expressions
      //
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_REQUESTS, Sensision.EMPTY_LABELS, 1);

      SmartPattern classSmartPattern;
      
      Collection<Metadata> metas;
      
      //
      // Allocate a set if there is more than one class selector as we may have
      // duplicate results
      //
      
      if (request.getClassSelectorSize() > 1) {
        metas = new HashSet<Metadata>();
      } else {
        metas = new ArrayList<Metadata>();
      }
            
      HyperLogLogPlus gtsCount = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
      Map<String,HyperLogLogPlus> perClassCardinality = new HashMap<String,HyperLogLogPlus>();
      Map<String,HyperLogLogPlus> perLabelValueCardinality = new HashMap<String,HyperLogLogPlus>();
      HyperLogLogPlus labelNamesCardinality = null;
      HyperLogLogPlus labelValuesCardinality = null;
      HyperLogLogPlus classCardinality = null;
      
      List<String> missingLabels = Constants.ABSENT_LABEL_SUPPORT ? new ArrayList<String>() : null;

      for (int i = 0; i < request.getClassSelectorSize(); i++) {
        String exactClassName = null;
        
        if (request.getClassSelector().get(i).startsWith("=") || !request.getClassSelector().get(i).startsWith("~")) {
          exactClassName = request.getClassSelector().get(i).startsWith("=") ? request.getClassSelector().get(i).substring(1) : request.getClassSelector().get(i);
          classSmartPattern = new SmartPattern(exactClassName);
        } else {
          classSmartPattern = new SmartPattern(Pattern.compile(request.getClassSelector().get(i).substring(1)));
        }
        
        Map<String,SmartPattern> labelPatterns = new HashMap<String,SmartPattern>();

        if (null != missingLabels) {
          missingLabels.clear();
        }

        if (null != request.getLabelsSelectors()) {
          for (Entry<String,String> entry: request.getLabelsSelectors().get(i).entrySet()) {
            String label = entry.getKey();
            String expr = entry.getValue();
            SmartPattern pattern;
            
            if (null != missingLabels && ("=".equals(expr) || "".equals(expr))) {
              missingLabels.add(label);
              continue;
            }

            if (expr.startsWith("=") || !expr.startsWith("~")) {
              //pattern = Pattern.compile(Pattern.quote(expr.startsWith("=") ? expr.substring(1) : expr));
              pattern = new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr);
            } else {
              pattern = new SmartPattern(Pattern.compile(expr.substring(1)));
            }
            
            //labelPatterns.put(label,  pattern.matcher(""));
            labelPatterns.put(label,  pattern);
          }      
        }
              
        //
        // Loop over the class names to find matches
        //

        Collection<String> classNames = new ArrayList<String>();
        
        if (null != exactClassName) {
          // If the class name is an exact match, check if it is known, if not, skip to the next selector
          if(!this.metadatas.containsKey(exactClassName)) {
            continue;
          }
          classNames.add(exactClassName);
        } else {
          //
          // Extract per owner classes if owner selector exists
          //
          
          if (request.getLabelsSelectors().get(i).size() > 0) {
            String ownersel = request.getLabelsSelectors().get(i).get(Constants.OWNER_LABEL);
            
            if (null != ownersel && ownersel.startsWith("=")) {
              classNames = classesPerOwner.get(ownersel.substring(1));
            } else {
              classNames = this.classNames.values();
            }
          } else {
            classNames = this.classNames.values();
          }
        }
        
        List<String> labelNames = new ArrayList<String>(labelPatterns.size());
        List<SmartPattern> labelSmartPatterns = new ArrayList<SmartPattern>(labelPatterns.size());
        List<String> labelValues = new ArrayList<String>(labelPatterns.size());
        
        //
        // Put producer/app/owner first
        //
        
        if (labelPatterns.containsKey(Constants.PRODUCER_LABEL)) {
          labelNames.add(Constants.PRODUCER_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.PRODUCER_LABEL));
          labelPatterns.remove(Constants.PRODUCER_LABEL);   
          labelValues.add(null);
        }
        if (labelPatterns.containsKey(Constants.APPLICATION_LABEL)) {
          labelNames.add(Constants.APPLICATION_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.APPLICATION_LABEL));
          labelPatterns.remove(Constants.APPLICATION_LABEL);
          labelValues.add(null);
        }
        if (labelPatterns.containsKey(Constants.OWNER_LABEL)) {
          labelNames.add(Constants.OWNER_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.OWNER_LABEL));
          labelPatterns.remove(Constants.OWNER_LABEL);
          labelValues.add(null);
        }
        
        //
        // Now add the other labels
        //
        
        for(Entry<String,SmartPattern> entry: labelPatterns.entrySet()) {
          labelNames.add(entry.getKey());
          labelSmartPatterns.add(entry.getValue());
          labelValues.add(null);
        }

        for (String className: classNames) {
          
          //
          // If class matches, check all labels for matches
          //
          
          if (classSmartPattern.matches(className)) {
            Map<Long,Metadata> classMetadatas = this.metadatas.get(className);
            if (null == classMetadatas) {
              continue;
            }
            for (Metadata metadata: classMetadatas.values()) {
              
              boolean exclude = false;

              if (null != missingLabels) {
                for (String missing: missingLabels) {
                  // If the Metadata contain one of the missing labels, exclude the entry
                  if (null != metadata.getLabels().get(missing)) {
                    exclude = true;
                    break;
                  }
                }              
                // Check attributes
                if (!exclude && metadata.getAttributesSize() > 0) {
                  for (String missing: missingLabels) {
                    // If the Metadata contain one of the missing labels, exclude the entry
                    if (null != metadata.getAttributes().get(missing)) {
                      exclude = true;
                      break;
                    }
                  }                              
                }
                if (exclude) {
                  continue;
                }
              }
              
              int idx = 0;
        
              for (String labelName: labelNames) {
                //
                // Immediately exclude metadata which do not contain one of the
                // labels for which we have patterns either in labels or in attributes
                //

                String labelValue = metadata.getLabels().get(labelName);
                
                if (null == labelValue) {
                  labelValue = metadata.getAttributes().get(labelName);
                  if (null == labelValue) {
                    exclude = true;
                    break;
                  }
                }
                
                labelValues.set(idx++, labelValue);
              }
              
              // If we did not collect enough label/attribute values, exclude the GTS
              if (idx < labelNames.size()) {
                exclude = true;
              }
              
              if (exclude) {
                continue;
              }
              
              //
              // Check if the label value matches, if not, exclude the GTS
              //
              
              for (int j = 0; j < labelNames.size(); j++) {
                if (!labelSmartPatterns.get(j).matches(labelValues.get(j))) {
                  exclude = true;
                  break;
                }
              }
              
              if (exclude) {
                continue;
              }

              //
              // We have a match, update estimators
              //

              // Compute classId/labelsId
              long classId = GTSHelper.classId(SIPHASH_CLASS_LONGS, metadata.getName());
              long labelsId = GTSHelper.labelsId(SIPHASH_LABELS_LONGS, metadata.getLabels());
              
              // Compute gtsId, we use the GTS Id String from which we extract the 16 bytes
              byte[] data = GTSHelper.gtsIdToString(classId, labelsId).getBytes(StandardCharsets.UTF_16BE);
              long gtsId = SipHashInline.hash24(SIPHASH_CLASS_LONGS[0], SIPHASH_CLASS_LONGS[1], data, 0, data.length);
              
              gtsCount.aggregate(gtsId);
              
              if (null != perClassCardinality) {              
                HyperLogLogPlus count = perClassCardinality.get(metadata.getName());
                if (null == count) {
                  count = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
                  perClassCardinality.put(metadata.getName(), count);
                }
                                
                count.aggregate(gtsId);
                
                // If we reached the limit in detailed number of classes, we fallback to a simple estimator
                if (perClassCardinality.size() >= LIMIT_CLASS_CARDINALITY) {
                  classCardinality = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
                  for (String cls: perClassCardinality.keySet()) {
                    data = cls.getBytes(StandardCharsets.UTF_8);
                    classCardinality.aggregate(SipHashInline.hash24(SIPHASH_CLASS_LONGS[0], SIPHASH_CLASS_LONGS[1], data, 0, data.length, false));
                    perClassCardinality = null;
                  }
                }
              } else {
                data = metadata.getName().getBytes(StandardCharsets.UTF_8);
                classCardinality.aggregate(SipHashInline.hash24(SIPHASH_CLASS_LONGS[0], SIPHASH_CLASS_LONGS[1], data, 0, data.length, false));
              }
              
              if (null != perLabelValueCardinality) {
                if (metadata.getLabelsSize() > 0) {
                  for (Entry<String,String> entry: metadata.getLabels().entrySet()) {
                    HyperLogLogPlus estimator = perLabelValueCardinality.get(entry.getKey());
                    if (null == estimator) {
                      estimator = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
                      perLabelValueCardinality.put(entry.getKey(), estimator);
                    }
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    long siphash = SipHashInline.hash24(SIPHASH_LABELS_LONGS[0], SIPHASH_LABELS_LONGS[1], data, 0, data.length, false);
                    estimator.aggregate(siphash);
                  }
                }

                if (metadata.getAttributesSize() > 0) {
                  for (Entry<String,String> entry: metadata.getAttributes().entrySet()) {
                    HyperLogLogPlus estimator = perLabelValueCardinality.get(entry.getKey());
                    if (null == estimator) {
                      estimator = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
                      perLabelValueCardinality.put(entry.getKey(), estimator);
                    }
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    estimator.aggregate(SipHashInline.hash24(SIPHASH_LABELS_LONGS[0], SIPHASH_LABELS_LONGS[1], data, 0, data.length, false));
                  }
                }

                if (perLabelValueCardinality.size() >= LIMIT_LABELS_CARDINALITY) {
                  labelNamesCardinality = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
                  labelValuesCardinality = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
                  for (Entry<String,HyperLogLogPlus> entry: perLabelValueCardinality.entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelNamesCardinality.aggregate(SipHashInline.hash24(SIPHASH_LABELS_LONGS[0], SIPHASH_LABELS_LONGS[1], data, 0, data.length, false));
                    labelValuesCardinality.fuse(entry.getValue());
                  }
                  perLabelValueCardinality = null;
                }
              } else {
                if (metadata.getLabelsSize() > 0) {
                  for (Entry<String,String> entry: metadata.getLabels().entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(SIPHASH_LABELS_LONGS[0], SIPHASH_LABELS_LONGS[1], data, 0, data.length, false));
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(SIPHASH_LABELS_LONGS[0], SIPHASH_LABELS_LONGS[1], data, 0, data.length, false));
                  }
                }
                if (metadata.getAttributesSize() > 0) {
                  for (Entry<String,String> entry: metadata.getAttributes().entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(SIPHASH_LABELS_LONGS[0], SIPHASH_LABELS_LONGS[1], data, 0, data.length, false));
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(SIPHASH_LABELS_LONGS[0], SIPHASH_LABELS_LONGS[1], data, 0, data.length, false));
                  }
                }
              }            
            }
          }
        }      
      }

      response.setGtsCount(gtsCount.toBytes());
      
      if (null != perClassCardinality) {
        classCardinality = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
        for (Entry<String,HyperLogLogPlus> entry: perClassCardinality.entrySet()) {
          response.putToPerClassCardinality(entry.getKey(), ByteBuffer.wrap(entry.getValue().toBytes()));
          byte[] data = entry.getKey().getBytes(StandardCharsets.UTF_8);
          classCardinality.aggregate(SipHashInline.hash24(SIPHASH_CLASS_LONGS[0], SIPHASH_CLASS_LONGS[1], data, 0, data.length, false));        
        }
      }
      
      response.setClassCardinality(classCardinality.toBytes());
      
      if (null != perLabelValueCardinality) {
        HyperLogLogPlus estimator = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
        HyperLogLogPlus nameEstimator = new HyperLogLogPlus(ESTIMATOR_P, ESTIMATOR_PPRIME);
        for (Entry<String,HyperLogLogPlus> entry: perLabelValueCardinality.entrySet()) {
          byte[] data = entry.getKey().getBytes(StandardCharsets.UTF_8);
          nameEstimator.aggregate(SipHashInline.hash24(SIPHASH_LABELS_LONGS[0], SIPHASH_LABELS_LONGS[1], data, 0, data.length, false));
          estimator.fuse(entry.getValue());
          response.putToPerLabelValueCardinality(entry.getKey(), ByteBuffer.wrap(entry.getValue().toBytes()));
        }
        response.setLabelNamesCardinality(nameEstimator.toBytes());
        response.setLabelValuesCardinality(estimator.toBytes());
      } else {
        response.setLabelNamesCardinality(labelNamesCardinality.toBytes());
        response.setLabelValuesCardinality(labelValuesCardinality.toBytes());
      }
      
      return response;   
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new TException(ioe);
    } catch (Exception e) {
      e.printStackTrace();
      throw new TException(e);
    }
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    handleStreaming(target, baseRequest, request, response);
    handleStats(target, baseRequest, request, response);
  }
  
  void handleStats(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException { 
    if (!Constants.API_ENDPOINT_DIRECTORY_STATS_INTERNAL.equals(target)) {
      return;
    }
    
    long nano = System.nanoTime();

    baseRequest.setHandled(true);

    //
    // Read DirectoryRequests from stdin
    //
    
    BufferedReader br = new BufferedReader(request.getReader());
    
    while (true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      byte[] raw = OrderPreservingBase64.decode(line.getBytes(StandardCharsets.US_ASCII));

      // Extract DirectoryStatsRequest
      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
      DirectoryStatsRequest req = new DirectoryStatsRequest();
      
      try {
        deser.deserialize(req, raw);
        DirectoryStatsResponse resp = stats(req);

        response.setContentType("text/plain");
        OutputStream out = response.getOutputStream();
              
        TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
        byte[] data = ser.serialize(resp);
        
        OrderPreservingBase64.encodeToStream(data, out);
        
        out.write('\r');
        out.write('\n');
      } catch (TException te) {
        throw new IOException(te);
      }            
    }
  }
  
  public void handleStreaming(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (!Constants.API_ENDPOINT_DIRECTORY_STREAMING_INTERNAL.equals(target)) {
      return;
    }
    
    long now = System.currentTimeMillis();
    
    long nano = System.nanoTime();
    
    baseRequest.setHandled(true);
    
    //
    // Extract 'selector'
    //
    
    String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);
    
    if (null == selector) {
      throw new IOException("Missing parameter '" + Constants.HTTP_PARAM_SELECTOR + "'.");
    }
        
    // Decode selector
    
    selector = new String(OrderPreservingBase64.decode(selector.getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8);
    
    //
    // Check request signature
    //
    
    String signature = request.getHeader(Constants.getHeader(io.warp10.continuum.Configuration.HTTP_HEADER_DIRECTORY_SIGNATURE));
    
    if (null == signature) {
      throw new IOException("Missing header '" + Constants.getHeader(io.warp10.continuum.Configuration.HTTP_HEADER_DIRECTORY_SIGNATURE) + "'.");
    }
    
    boolean signed = false;
    
    //
    // Signature has the form hex(ts):hex(hash)
    //
    
    String[] subelts = signature.split(":");
    
    if (2 != subelts.length) {
      throw new IOException("Invalid signature.");
    }

    long nowts = System.currentTimeMillis();
    long sigts = new BigInteger(subelts[0], 16).longValue();
    long sighash = new BigInteger(subelts[1], 16).longValue();
        
    if (nowts - sigts > this.maxage) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_EXPIRED, Sensision.EMPTY_LABELS, 1);
      throw new IOException("Signature has expired.");
    }
        
    // Recompute hash of ts:selector
        
    String tssel = Long.toString(sigts) + ":" + selector;
        
    byte[] bytes = tssel.getBytes(StandardCharsets.UTF_8);
    long checkedhash = SipHashInline.hash24(SIPHASH_PSK_LONGS[0], SIPHASH_PSK_LONGS[1], bytes, 0, bytes.length);
        
    if (checkedhash != sighash) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_INVALID, Sensision.EMPTY_LABELS, 1);
      throw new IOException("Corrupted signature");
    }
    
    boolean hasActiveAfter = null != request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER);
    long activeAfter = hasActiveAfter ? Long.parseLong(request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER)) : 0L;
    boolean hasQuietAfter = null != request.getParameter(Constants.HTTP_PARAM_QUIETAFTER);
    long quietAfter = hasQuietAfter ? Long.parseLong(request.getParameter(Constants.HTTP_PARAM_QUIETAFTER)) : 0L;

    //
    // Parse selector  
    //
    
    Object[] tokens = null;

    try {
      tokens = PARSESELECTOR.parse(selector);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }
    
    String classSelector = (String) tokens[0];
    Map<String,String> labelsSelector = (Map<String,String>) tokens[1];
        
    //
    // Loop over the Metadata, outputing the matching ones
    //
    
    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("text/plain");

    //
    // Delegate to the external plugin if it is defined
    //
    
    long count = 0;
    long hits = 0;
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

    MetadataID id = null;
    
    OutputStream out = response.getOutputStream();
    
    if (null != this.plugin) {
      
      long nanofind = System.nanoTime();
      long time = 0;

      Metadata metadata = new Metadata();

      try (DirectoryPlugin.GTSIterator iter = this.plugin.find(this.remainder, classSelector, labelsSelector)) {
        
        while(iter.hasNext()) {
          GTS gts = iter.next();
          nanofind = System.nanoTime() - nanofind;
          time += nanofind;
    
          metadata.clear();
          metadata.setName(gts.getName());
          metadata.setLabels(gts.getLabels());
          metadata.setAttributes(gts.getAttributes());
          
          //
          // Recompute class/labels Id
          //
          
          long classId = GTSHelper.classId(this.SIPHASH_CLASS_LONGS, metadata.getName());
          long labelsId = GTSHelper.labelsId(this.SIPHASH_LABELS_LONGS, metadata.getLabels());

          metadata.setClassId(classId);
          metadata.setLabelsId(labelsId);
          
          try {
            //
            // Extract id 
            //
            
            id = MetadataUtils.id(id, metadata);
            
            //
            // Attempt to retrieve serialized content from the cache
            //
            
            byte[] data = serializedMetadataCache.get(id);
            
            if (null == data) {
              data = serializer.serialize(metadata);
              synchronized(serializedMetadataCache) {
                // cache content
                serializedMetadataCache.put(MetadataUtils.id(metadata),data);                
              }
            } else {
              hits++;
            }

            OrderPreservingBase64.encodeToStream(data, out);
            out.write('\r');
            out.write('\n');
            count++;
          } catch (TException te) {
          }        
          nanofind = System.nanoTime();
        }
        
      } catch (Exception e) {        
      } finally {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_CALLS, Sensision.EMPTY_LABELS, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_RESULTS, Sensision.EMPTY_LABELS, count);
        Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_METADATA_CACHE_HITS, Sensision.EMPTY_LABELS, hits);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_PLUGIN_FIND_TIME_NANOS, Sensision.EMPTY_LABELS, time);        
      }
      return;
    } else { // No Plugin
      
      String exactClassName = null;
      SmartPattern classSmartPattern;
      
      List<String> missingLabels = Constants.ABSENT_LABEL_SUPPORT ? new ArrayList<String>() : null;

      if (classSelector.startsWith("=") || !classSelector.startsWith("~")) {
        exactClassName = classSelector.startsWith("=") ? classSelector.substring(1) : classSelector;
        classSmartPattern = new SmartPattern(exactClassName);
      } else {
        classSmartPattern = new SmartPattern(Pattern.compile(classSelector.substring(1)));
      }
        
      Map<String,SmartPattern> labelPatterns = new HashMap<String,SmartPattern>();
        
      for (Entry<String,String> entry: labelsSelector.entrySet()) {
        String label = entry.getKey();
        String expr = entry.getValue();
        SmartPattern pattern;
            
        if (null != missingLabels && ("=".equals(expr) || "".equals(expr))) {
          missingLabels.add(label);
          continue;
        }

        if (expr.startsWith("=") || !expr.startsWith("~")) {
          pattern = new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr);
        } else {
          pattern = new SmartPattern(Pattern.compile(expr.substring(1)));
        }
            
        labelPatterns.put(label,  pattern);
      }      

      //
      // Loop over the class names to find matches
      //

      Collection<String> classNames = new ArrayList<String>();
      
      if (null != exactClassName) {
        // If the class name is an exact match, check if it is known, if not, return
        if(!this.metadatas.containsKey(exactClassName)) {
          return;
        }
        classNames.add(exactClassName);
      } else {
        //
        // Extract per owner classes if owner selector exists
        //
        
        if (labelsSelector.size() > 0) {
          String ownersel = labelsSelector.get(Constants.OWNER_LABEL);
          
          if (null != ownersel && ownersel.startsWith("=")) {
            classNames = classesPerOwner.get(ownersel.substring(1));
            if (null == classNames) {
              classNames = new ArrayList<String>();
            }
          } else {        
            classNames = this.classNames.values();
          }
        } else {
          classNames = this.classNames.values();
        }
      }
            
      List<String> labelNames = new ArrayList<String>(labelPatterns.size());
      List<SmartPattern> labelSmartPatterns = new ArrayList<SmartPattern>(labelPatterns.size());
      List<String> labelValues = new ArrayList<String>(labelPatterns.size());
      
      //
      // Put producer/app/owner first
      //
      
      if (labelPatterns.containsKey(Constants.PRODUCER_LABEL)) {
        labelNames.add(Constants.PRODUCER_LABEL);
        labelSmartPatterns.add(labelPatterns.get(Constants.PRODUCER_LABEL));
        labelPatterns.remove(Constants.PRODUCER_LABEL);
        labelValues.add(null);        
      }
      if (labelPatterns.containsKey(Constants.APPLICATION_LABEL)) {
        labelNames.add(Constants.APPLICATION_LABEL);
        labelSmartPatterns.add(labelPatterns.get(Constants.APPLICATION_LABEL));
        labelPatterns.remove(Constants.APPLICATION_LABEL);        
        labelValues.add(null);        
      }
      if (labelPatterns.containsKey(Constants.OWNER_LABEL)) {
        labelNames.add(Constants.OWNER_LABEL);
        labelSmartPatterns.add(labelPatterns.get(Constants.OWNER_LABEL));
        labelPatterns.remove(Constants.OWNER_LABEL);        
        labelValues.add(null);        
      }
      
      //
      // Now add the other labels
      //
      
      for(Entry<String,SmartPattern> entry: labelPatterns.entrySet()) {
        labelNames.add(entry.getKey());
        labelSmartPatterns.add(entry.getValue());
        labelValues.add(null);
      }
      
      long labelsComparisons = 0;
      long classesInspected = 0;
      long classesMatched = 0;
      long metadataInspected = 0;
      
      for (String className: classNames) {
        classesInspected++;
        
        //
        // If class matches, check all labels for matches
        //
        
        if (classSmartPattern.matches(className)) {
          classesMatched++;
          Map<Long,Metadata> classMetadatas = this.metadatas.get(className);
          if (null == classMetadatas) {
            continue;
          }
          for (Metadata metadata: classMetadatas.values()) {
            metadataInspected++;
            boolean exclude = false;
            
            //
            // Check activity
            //
            
            if (hasActiveAfter && metadata.getLastActivity() < activeAfter) {
              continue;
            }
            
            if (hasQuietAfter && metadata.getLastActivity() >= quietAfter) {
              continue;
            }

            if (null != missingLabels) {
              for (String missing: missingLabels) {
                // If the Metadata contain one of the missing labels, exclude the entry
                if (null != metadata.getLabels().get(missing)) {
                  exclude = true;
                  break;
                }
              }              
              // Check attributes
              if (!exclude && metadata.getAttributesSize() > 0) {
                for (String missing: missingLabels) {
                  // If the Metadata contain one of the missing labels, exclude the entry
                  if (null != metadata.getAttributes().get(missing)) {
                    exclude = true;
                    break;
                  }
                }                              
              }
              if (exclude) {
                continue;
              }
            }

            int idx = 0;
      
            for (String labelName: labelNames) {
              //
              // Immediately exclude metadata which do not contain one of the
              // labels for which we have patterns either in labels or in attributes
              //

              String labelValue = metadata.getLabels().get(labelName);
              
              if (null == labelValue) {
                labelValue = metadata.getAttributes().get(labelName);
                if (null == labelValue) {
                  exclude = true;
                  break;
                }
              }
              
              labelValues.set(idx++, labelValue);
            }
            
            // If we did not collect enough label/attribute values, exclude the GTS
            if (idx < labelNames.size()) {
              exclude = true;
            }
            
            if (exclude) {
              continue;
            }
            
            //
            // Check if the label value matches, if not, exclude the GTS
            //
            
            for (int i = 0; i < labelNames.size(); i++) {
              labelsComparisons++;
              if (!labelSmartPatterns.get(i).matches(labelValues.get(i))) {
                exclude = true;
                break;
              }
            }

            if (exclude) {
              continue;
            }

            try {
              //
              // Extract id 
              //
              
              id = MetadataUtils.id(id, metadata);
              
              //
              // Attempt to retrieve serialized content from the cache
              //
              
              byte[] data = serializedMetadataCache.get(id);
              
              if (null == data) {                
                data = serializer.serialize(metadata);
                synchronized(serializedMetadataCache) {
                  // cache content
                  serializedMetadataCache.put(MetadataUtils.id(metadata),data);                  
                }
              } else {
                hits++;
              }
              
              OrderPreservingBase64.encodeToStream(data, out);
              out.write('\r');
              out.write('\n');
              count++;
            } catch (TException te) {
              continue;
            }
          }
        }
      }    
      
      long nownano = System.nanoTime();
      
      LoggingEvent event = LogUtil.setLoggingEventAttribute(null, LogUtil.DIRECTORY_SELECTOR, selector);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_REQUEST_TIMESTAMP, now);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_RESULTS, count);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_NANOS, nownano - nano);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_METADATA_INSPECTED, metadataInspected);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_CLASSES_INSPECTED, classesInspected);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_CLASSES_MATCHED, classesMatched);
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.DIRECTORY_LABELS_COMPARISONS, labelsComparisons);
      
      String eventstr = LogUtil.serializeLoggingEvent(this.keystore, event);
      
      LOG.info("Search returned " + count + " results in " + ((nownano - nano) / 1000000.0D) + " ms, inspected " + metadataInspected + " metadatas in " + classesInspected + " classes (" + classesMatched + " matched) and performed " + labelsComparisons + " comparisons. EVENT=" + eventstr);
    }
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_REQUESTS, Sensision.EMPTY_LABELS, 1);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_RESULTS, Sensision.EMPTY_LABELS, count);
    Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_METADATA_CACHE_HITS, Sensision.EMPTY_LABELS, hits);
    nano = System.nanoTime() - nano;
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STREAMING_TIME_US, Sensision.EMPTY_LABELS, nano / 1000);
  }  
}
