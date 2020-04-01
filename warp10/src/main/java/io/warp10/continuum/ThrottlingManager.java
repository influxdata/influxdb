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

package io.warp10.continuum;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

import io.warp10.WarpConfig;
import io.warp10.WarpURLDecoder;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.HyperLogLogPlus;
import io.warp10.sensision.Sensision;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * This class manages the throttling of data ingestion.
 * It controls both DDP (Daily Data Points) and MADS (Monthly Active Device Streams).
 * 
 */
public class ThrottlingManager {

  private static final Logger LOG = LoggerFactory.getLogger(ThrottlingManager.class);
  
  private static final char APPLICATION_PREFIX_CHAR = '+';
  
  public static final String LIMITS_PRODUCER_RATE_CURRENT = "producer.rate.limit";
  public static final String LIMITS_PRODUCER_MADS_LIMIT = "producer.mads.limit";
  public static final String LIMITS_PRODUCER_MADS_CURRENT = "producer.mads.current";
  
  public static final String LIMITS_APPLICATION_RATE_CURRENT = "application.rate.limit";
  public static final String LIMITS_APPLICATION_MADS_LIMIT = "application.mads.limit";
  public static final String LIMITS_APPLICATION_MADS_CURRENT = "application.mads.current";

  /**
   * Minimal limit (1 per hour) because 0 is not acceptable by RateLimiter.
   */
  public static final double MINIMUM_RATE_LIMIT = 1.0D/3600.0D;
  
  /**
   * Default rate of datapoints per second per producer.
   */
  private static double DEFAULT_RATE_PRODUCER = 0.0D;
  
  /**
   * Default rate of datapoints per second per applciation.
   */
  private static double DEFAULT_RATE_APPLICATION = 0.0D;

  /**
   * Default MADS per producer
   */
  private static long DEFAULT_MADS_PRODUCER = 0L;

  /**
   * Suffix of the throttle files in the throttle dir
   */
  private static final String THROTTLING_MANAGER_SUFFIX = ".throttle";
    
  /**
   * Maximum number of HyperLogLogPlus estimators we retain in memory
   */
  private static int ESTIMATOR_CACHE_SIZE = 10000;
  
  /**
   * Keys to compute the hash of classId/labelsId
   */
  private static final long[] SIP_KEYS = { 0x01L, 0x02L };

  /**
   * Maximum number of milliseconds to wait for RateLimiter permits
   */
  private static long MAXWAIT_PER_DATAPOINT;
  
  /**
   * Default value for MAXWAIT_PER_DATAPOINT
   */
  private static final long MAXWAIT_PER_DATAPOINT_DEFAULT = 10L;
  
  /**
   * Number of milliseconds in a 30 days period
   */
  private static final long _30DAYS_SPAN = 30L * 86400L * 1000L;
  
  /**
   * Default value of 'p' parameter for estimator
   */
  private static final int DEFAULT_P = 14;
  
  /**
   * Default value of 'pprime' parameter for estimator
   */
  private static final int DEFAULT_PPRIME = 25;
  
  private static final double toleranceRatio = 1.0D + (1.04D / Math.sqrt(1L << DEFAULT_P));

  /**
   * Rate limiters to control the rate of datapoints ingestion per producer
   */
  private static Map<String,RateLimiter> producerRateLimiters = new HashMap<String, RateLimiter>();

  /**
   * Rate limiters to control the rate of datapoints ingestion per application
   */
  private static Map<String,RateLimiter> applicationRateLimiters = new HashMap<String, RateLimiter>();

  /**
   * Map of estimators for producers
   */
  private static Map<String,HyperLogLogPlus> producerHLLPEstimators = new LinkedHashMap<String, HyperLogLogPlus>() {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<String, HyperLogLogPlus> eldest) {
      //
      // Update estimator cache size
      //

      boolean overflow = this.size() > ESTIMATOR_CACHE_SIZE;
      
      if (!overflow) {
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_ESTIMATORS_CACHED, Sensision.EMPTY_LABELS, this.size());
      }
      
      return overflow;
    }
  };

  /**
   * Map of estimators for applications
   */
  private static Map<String,HyperLogLogPlus> applicationHLLPEstimators = new LinkedHashMap<String, HyperLogLogPlus>() {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<String, HyperLogLogPlus> eldest) {
      //
      // Update estimator cache size
      //

      boolean overflow = this.size() > ESTIMATOR_CACHE_SIZE;
      
      if (!overflow) {
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_ESTIMATORS_CACHED_PER_APP, Sensision.EMPTY_LABELS, this.size());
      }
      
      return overflow;
    }
  };

  private static AtomicBoolean initialized = new AtomicBoolean(false);

  private static boolean loaded = false;
  
  private static boolean enabled = false;
  
  private static Producer<byte[],byte[]> throttlingProducer = null;
  private static String throttlingTopic = null;
  private static byte[] throttlingMAC = null;
  
  private static String dir;
  
  static {
    init();
  }
  
  /**
   * Map of per producer MADS (Monthly Active Data Streams) limits
   */
  private static Map<String,Long> producerMADSLimits = new HashMap<String, Long>();

  /**
   * Map of per application MADS (Monthly Active Data Streams) limits
   */
  private static Map<String,Long> applicationMADSLimits = new HashMap<String, Long>();

  /**
   * Check compatibility of a GTS with the current MADS limit
   * 
   * @param metadata
   * @param producer
   * @param owner
   * @param application
   * @param classId
   * @param labelsId
   * @throws WarpException
   */
  public static void checkMADS(Metadata metadata, String producer, String owner, String application, long classId, long labelsId, boolean expose) throws WarpException {
        
    if (!loaded) {
      return;
    }
    
    //
    // Retrieve per producer limit
    //
    
    Long oProducerLimit = producerMADSLimits.get(producer);

    //
    // Extract per application limit
    //
    
    Long oApplicationLimit = applicationMADSLimits.get(application);
    
    // If there is no per producer limit, check the default one
    
    if (null == oProducerLimit) {      
      oProducerLimit = DEFAULT_MADS_PRODUCER;
      
      // -1 means don't check for MADS
      if (-1 == oProducerLimit && null == oApplicationLimit) {
        // No per producer limit and no per application limit
        return;
      } else if (0 == oProducerLimit) {
        // 0 means we don't accept datastreams anymore for this producer
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);
        StringBuilder sb = new StringBuilder();
        sb.append("Geo Time Series ");
        // Do not expose producer and owner as the update did not contain them so
        // identifying the line responsible for the error is easier
        GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels(), false);
        sb.append(" would exceed your Monthly Active Data Streams limit (");
        sb.append(oProducerLimit);
        sb.append(").");
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS, labels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_GLOBAL, Sensision.EMPTY_LABELS, 1);
        throw new WarpException(sb.toString());
      }
      
      // Adjust limit so we account for the error of the estimator
      if (-1 != oProducerLimit) {
        oProducerLimit = (long) Math.ceil(oProducerLimit * toleranceRatio);
      }
    }

    long producerLimit = oProducerLimit;

    //
    // Retrieve estimator. If none is defined or if the current estimator has expired
    // was created in the previous 30 days period, allocate a new one
    //
    
    HyperLogLogPlus producerHLLP = null;
    
    if (-1 != producerLimit) {
      synchronized(producerHLLPEstimators) {
        producerHLLP = producerHLLPEstimators.get(producer);
        // If the HyperLogLogPlus is older than 30 days or not yet created, generate a new one
        if (null == producerHLLP || producerHLLP.hasExpired()) {
          producerHLLP = new HyperLogLogPlus(DEFAULT_P, DEFAULT_PPRIME);
          try {
            producerHLLP.toNormal();
          } catch (IOException ioe) {
            throw new WarpException(ioe);
          }
          producerHLLP.setKey(producer);
          producerHLLPEstimators.put(producer, producerHLLP);
        }
      }      
    }
    
    //
    // Compute hash
    //
    
    long hash = GTSHelper.gtsId(SIP_KEYS, classId, labelsId);
        
    //
    // Check if hash would impact per producer cardinality, if not, return immediately if there is no per app limit
    //
    
    boolean newForProducer = null == producerHLLP ? false : producerHLLP.isNew(hash);
    
    if (!newForProducer && null == oApplicationLimit) {
      return;
    }
    
    HyperLogLogPlus applicationHLLP = null;
    
    long applicationLimit = Long.MIN_VALUE;
    
    if (null != oApplicationLimit) {
      applicationLimit = oApplicationLimit;
      
      synchronized(applicationHLLPEstimators) {
        applicationHLLP = applicationHLLPEstimators.get(application);
        // If the HyperLogLogPlus is older than 30 days or not yet created, generate a new one
        if (null == applicationHLLP || applicationHLLP.hasExpired()) {
          applicationHLLP = new HyperLogLogPlus(DEFAULT_P, DEFAULT_PPRIME);
          try {
            applicationHLLP.toNormal();
          } catch (IOException ioe) {
            throw new WarpException(ioe);
          }
          applicationHLLP.setKey(APPLICATION_PREFIX_CHAR + application);
          applicationHLLPEstimators.put(application, applicationHLLP);
        }
      }
    }
    
    //
    // Check per app estimator if it exists
    //
    
    if (null != applicationHLLP) {
      // If the element is not new, return immediately
      if (!applicationHLLP.isNew(hash)) {
        return;
      }
      
      try {
        long cardinality = applicationHLLP.cardinality();
        
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
        
        if (cardinality > applicationLimit) {
          StringBuilder sb = new StringBuilder();
          sb.append("Geo Time Series ");
          GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels(), expose);
          sb.append(" would exceed the Monthly Active Data Streams limit for application '" + application + "' (");
          sb.append((long) Math.floor(applicationLimit / toleranceRatio));
          sb.append(").");
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_PER_APP, labels, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_PER_APP_GLOBAL, Sensision.EMPTY_LABELS, 1);
          throw new WarpException(sb.toString());          
        }
        
        applicationHLLP.aggregate(hash);
        
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels, applicationHLLP.cardinality());
      } catch (IOException ioe){
        // Ignore for now...
      }
    }
    
    if (-1 == producerLimit) {
      return;
    }
    
    //
    // If we are already above the monthly limit, throw an exception
    //
        
    try {
      
      long cardinality = producerHLLP.cardinality();

      Map<String,String> labels = new HashMap<String, String>();
      labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      if (cardinality > producerLimit) {
        StringBuilder sb = new StringBuilder();
        sb.append("Geo Time Series ");
        GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels(), expose);
        sb.append(" would exceed your Monthly Active Data Streams limit (");
        sb.append((long) Math.floor(producerLimit / toleranceRatio));
        sb.append(").");
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS, labels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_GLOBAL, Sensision.EMPTY_LABELS, 1);
        throw new WarpException(sb.toString());
      }

      producerHLLP.aggregate(hash);
      
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, producerHLLP.cardinality());
    } catch (IOException ioe) {
      // Ignore for now...
    }
  }
  
  /**
   * Validate the ingestion of datapoints against the DDP limit
   * 
   * We tolerate inputs only if they wouldn't incur a wait greater than 2 seconds
   * 
   * @param producer
   * @param owner
   * @param application
   * @param count
   * @param maxwait Max wait per datapoint
   */
  public static void checkDDP(Metadata metadata, String producer, String owner, String application, int count, long maxwait, boolean expose) throws WarpException {
    if (!loaded) {
      return;
    }
    
    //
    // Extract RateLimiter
    //
    
    RateLimiter producerLimiter = producerRateLimiters.get(producer);
    RateLimiter applicationLimiter = applicationRateLimiters.get(application);
    
    //
    // TODO(hbs): store per producer/per app maxwait values? Extract them from the throttling file?
    //
    
    long appMaxWait = maxwait;
    long producerMaxWait = maxwait;
      
    // -1.0 as the default rate means do not enforce DDP limit
    if (null == producerLimiter && null == applicationLimiter && -1.0D == DEFAULT_RATE_PRODUCER) {      
      return;
    } else if (null == producerLimiter && -1.0D != DEFAULT_RATE_PRODUCER) {
      // Create a rate limiter with the default rate      
      producerLimiter = RateLimiter.create(Math.max(MINIMUM_RATE_LIMIT,DEFAULT_RATE_PRODUCER));
      producerRateLimiters.put(producer, producerLimiter);
    }
     
    // Check per application limiter
    if (null != applicationLimiter) {
      if (!applicationLimiter.tryAcquire(count, appMaxWait * count, TimeUnit.MILLISECONDS)) {
        StringBuilder sb = new StringBuilder();
        sb.append("Storing data for ");
        if (null != metadata) {
          GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels(), expose);
        }
        sb.append(" would incur a wait greater than ");
        sb.append(appMaxWait);
        sb.append(" ms per datapoint due to your Daily Data Points limit being already exceeded for application '" + application + "'. Current max rate is " + applicationLimiter.getRate() + " datapoints/s.");

        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_PER_APP, labels, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_PER_APP_GLOBAL, Sensision.EMPTY_LABELS, 1);
        
        throw new WarpException(sb.toString());      
      }
    }
    
    if (null == producerLimiter) {
      return;
    }
    
    if (!producerLimiter.tryAcquire(count, producerMaxWait * count, TimeUnit.MILLISECONDS)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Storing data for ");
      if (null != metadata) {
        GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels(), expose);
      }
      sb.append(" would incur a wait greater than ");
      sb.append(producerMaxWait);
      sb.append(" ms per datapoint due to your Daily Data Points limit being already exceeded. Current maximum rate is " + producerLimiter.getRate() + " datapoints/s.");

      Map<String,String> labels = new HashMap<String, String>();
      labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE, labels, 1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_GLOBAL, Sensision.EMPTY_LABELS, 1);
      
      throw new WarpException(sb.toString());      
    }
  }

  public static void checkDDP(Metadata metadata, String producer, String owner, String application, int count, boolean expose) throws WarpException {
    checkDDP(metadata, producer, owner, application, count, MAXWAIT_PER_DATAPOINT, expose);
  }
  
  public static Map<String,Object> getLimits(String producer, String app) {
    Map<String,Object> limits = new HashMap<String, Object>();
    
    RateLimiter producerLimiter = producerRateLimiters.get(producer);
    RateLimiter applicationLimiter = applicationRateLimiters.get(app);

    Long oProducerLimit = producerMADSLimits.get(producer);
    Long oApplicationLimit = applicationMADSLimits.get(app);

    long producerLimit = Long.MAX_VALUE;
    long applicationLimit = Long.MAX_VALUE;
    
    HyperLogLogPlus prodHLLP = producerHLLPEstimators.get(producer);
    HyperLogLogPlus appHLLP = applicationHLLPEstimators.get(app);
    
    if (null != producerLimiter) {
      limits.put(LIMITS_PRODUCER_RATE_CURRENT, producerLimiter.getRate());
    }
    
    if (null != applicationLimiter) {
      limits.put(LIMITS_APPLICATION_RATE_CURRENT, applicationLimiter.getRate());
    }
    
    if (null != oProducerLimit) {
      limits.put(LIMITS_PRODUCER_MADS_LIMIT, oProducerLimit);
      producerLimit = (long) oProducerLimit;
    }
    
    if (null != oApplicationLimit) {
      limits.put(LIMITS_APPLICATION_MADS_LIMIT, oApplicationLimit);
      applicationLimit = (long) oApplicationLimit;
    }
    
    if (null != prodHLLP) {
      try {
        long cardinality = prodHLLP.cardinality();
        
        // Change cardinality so it is capped by 'producerLimit', we don't want to expose the
        // toleranceRatio
        
        if (cardinality > producerLimit) {
          cardinality = producerLimit;
        }        
        
        limits.put(LIMITS_PRODUCER_MADS_CURRENT, cardinality);
      } catch (IOException ioe) {        
      }
    }
    
    if (null != appHLLP) {
      try {
        long cardinality = appHLLP.cardinality();
        
        // Change cardinality so it is capped by 'producerLimit', we don't want to expose the
        // toleranceRatio
        
        if (cardinality > applicationLimit) {
          cardinality = applicationLimit;
        }        
        
        limits.put(LIMITS_APPLICATION_MADS_CURRENT, cardinality);
      } catch (IOException ioe) {        
      }      
    }
    
    return limits;
  }
  
  public static void init() {    
    if (initialized.get()) {
      return;
    }
    
    ESTIMATOR_CACHE_SIZE = Integer.parseInt(WarpConfig.getProperty(Configuration.THROTTLING_MANAGER_ESTIMATOR_CACHE_SIZE, Integer.toString(ESTIMATOR_CACHE_SIZE)));
    
    String rate = WarpConfig.getProperty(Configuration.THROTTLING_MANAGER_RATE_DEFAULT);
    
    if (null != rate) {
      DEFAULT_RATE_PRODUCER = Double.parseDouble(rate); 
    }

    String mads = WarpConfig.getProperty(Configuration.THROTTLING_MANAGER_MADS_DEFAULT);
    
    if (null != mads) {
      DEFAULT_MADS_PRODUCER = Long.parseLong(mads);
    }

    MAXWAIT_PER_DATAPOINT = Long.parseLong(WarpConfig.getProperty(Configuration.THROTTLING_MANAGER_MAXWAIT_DEFAULT, Long.toString(MAXWAIT_PER_DATAPOINT_DEFAULT)));

    //
    // Start the thread which will read the throttling configuration periodically
    //
    
    dir = WarpConfig.getProperty(Configuration.THROTTLING_MANAGER_DIR);

    final long now = System.currentTimeMillis();
   
    final long rampup = Long.parseLong(WarpConfig.getProperty(Configuration.THROTTLING_MANAGER_RAMPUP, "0"));
    
    //
    // Register a shutdown hook which will dump the current throttling configuration to a file
    //
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        dumpCurrentConfig();
      }
    });
    
    //
    // Configure Kafka if defined
    //

    String brokerlistProp = WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_BROKERLIST);
    if (null != brokerlistProp) {
      Properties dataProps = new Properties();
      // @see http://kafka.apache.org/documentation.html#producerconfigs
      dataProps.setProperty("metadata.broker.list", brokerlistProp);
      String producerClientId = WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_PRODUCER_CLIENTID);
      if (null != producerClientId) {
        dataProps.setProperty("client.id", producerClientId);
      }
      dataProps.setProperty("request.required.acks", "-1");
      dataProps.setProperty("producer.type","sync");
      dataProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");

      String timeoutProp = WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_REQUEST_TIMEOUT_MS);
      if (null != timeoutProp) {
        dataProps.setProperty("request.timeout.ms", timeoutProp);
      }

      ProducerConfig dataConfig = new ProducerConfig(dataProps);
      
      throttlingProducer = new Producer<byte[],byte[]>(dataConfig);
      throttlingTopic = WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_TOPIC);
    }
    
    final long delay = Long.parseLong(WarpConfig.getProperty(Configuration.THROTTLING_MANAGER_PERIOD, "60000"));

    String zkConnectProp = WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_ZKCONNECT);
    if (null != zkConnectProp) {
      ConsumerFactory estimatorConsumerFactory = new ThrottlingManagerEstimatorConsumerFactory(throttlingMAC);
      
      //
      // The commitPeriod is set to twice the delay between throttling file reloads.
      // The reason behind this is that the estimators are pushed into Kafka when the file is reloaded
      // and therefore we do not want to have committed offsets for all estimators when we stop and restart
      // ingress (and the throttling manager), because then it would not have any estimators to read from Kafka
      // and would have empty estimators which it would push to kafka when the file is next read. This would have the
      // unwanted consequence of resetting MADS for all accounts.
      //
      
      long commitOffset = 2 * delay;
      
      KafkaSynchronizedConsumerPool pool = new KafkaSynchronizedConsumerPool(zkConnectProp,
          WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_TOPIC),
          WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_CONSUMER_CLIENTID),
          WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_GROUPID),
          null,
          WarpConfig.getProperty(Configuration.INGRESS_KAFKA_THROTTLING_CONSUMER_AUTO_OFFSET_RESET, "largest"),
          1,
          commitOffset,
          estimatorConsumerFactory);      
    }
    
    Thread t = new Thread() {
      
      // Set of files already read
      
      private Set<String> read = new HashSet<String>();
                  
      @Override
      public void run() {
        
        while(true) {
          
          //
          // If manager was not enabled, sleep then continue the loop
          //
          
          if (!enabled) {
            try { Thread.sleep(1000); } catch (InterruptedException ie) {}
            continue;
          }
          
          //
          // Open the directory
          //
          
          final File root = new File(dir);
          
          String[] files = root.list(new FilenameFilter() {            
            @Override
            public boolean accept(File d, String name) {
              if (!d.equals(root)) {
                return false;
              }
              if (!name.endsWith(THROTTLING_MANAGER_SUFFIX)) {
                return false;
              }
              return true;
            }
          });
          
          // Sort files in lexicographic order
          
          if (null == files) {
            files = new String[0];
          }
          
          Arrays.sort(files);
          
          Set<String> newreads = new HashSet<String>();
                    
          for (String file: files) {
            if (read.contains(file)) {
              newreads.add(file);
              continue;
            }
            
            //
            // Read each line
            //
            
            try {
              BufferedReader br = new BufferedReader(new FileReader(new File(dir, file)));
              
              while (true) {
                String line = br.readLine();
                if (null == line) {
                  break;
                }
                line = line.trim();
                if (line.startsWith("#")) {
                  continue;
                }
                String[] tokens = line.split(":");
                
                if (5 != tokens.length) {
                  continue;
                }
                
                // Lines end with ':#'
                
                if (!"#".equals(tokens[4])) {
                  continue;
                }
                
                String entity = tokens[0];
                String mads = tokens[1];
                String rate = tokens[2];
                String estimator = tokens[3];
                
                boolean isProducer = entity.charAt(0) != APPLICATION_PREFIX_CHAR;
                
                if (isProducer) {
                  // Attempt to read UUID
                  UUID uuid = UUID.fromString(entity);
                  entity = uuid.toString().toLowerCase();
                } else {
                  // Remove leading '+' and decode application name which may be URL encoded
                  entity = WarpURLDecoder.decode(entity.substring(1), StandardCharsets.UTF_8);
                }
                
                if ("-".equals(estimator)) {
                  //
                  // Clear estimator, we also push an event with a GTS_DISTINCT set to 0 for the producer/app
                  // We also remove the metric
                  //
                  
                  if (isProducer) {
                    synchronized(producerHLLPEstimators) {
                      producerHLLPEstimators.remove(entity);
                      Map<String,String> labels = new HashMap<String, String>();
                      labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, entity);
                      Sensision.clear(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels);
                      Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, 0);
                      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_ESTIMATOR_RESETS, labels, 1);
                    }
                  } else {
                    synchronized(applicationHLLPEstimators) {
                      applicationHLLPEstimators.remove(entity);
                      Map<String,String> labels = new HashMap<String, String>();
                      labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, entity);
                      Sensision.clear(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels);
                      Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels, 0);
                      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_ESTIMATOR_RESETS_PER_APP, labels, 1);
                    }
                  }
                } else if (!"".equals(estimator)) {                  
                  byte[] ser = OrderPreservingBase64.decode(estimator.getBytes(StandardCharsets.US_ASCII));
                  HyperLogLogPlus hllp = HyperLogLogPlus.fromBytes(ser);
                  
                  // Force mode to 'NORMAL', SPARSE is too slow as it calls merge repeatdly
                  hllp.toNormal();
                  
                  //
                  // Ignore estimator if it has expired
                  //
                  
                  if (hllp.hasExpired()) {
                    hllp = new HyperLogLogPlus(hllp.getP(), hllp.getPPrime());
                    hllp.toNormal();
                    hllp.setInitTime(0);
                  }
                  
                  // Retrieve current estimator
                  
                  if (isProducer) {
                
                    isProducer = true;
                    
                    HyperLogLogPlus old = producerHLLPEstimators.get(entity);

                    // Merge estimators and replace with the result, keeping the most recent estimator as the base
                    if (null == old || hllp.getInitTime() > old.getInitTime()) {
                      if (null != old){
                        hllp.fuse(old);
                      }
                      
                      hllp.setKey(entity);
                      
                      synchronized(producerHLLPEstimators) {
                        producerHLLPEstimators.put(entity, hllp);                      
                      }
                    } else {
                      old.fuse(hllp);
                    }                    
                  } else {
                    HyperLogLogPlus old = applicationHLLPEstimators.get(entity);

                    // Merge estimators and replace with the result, keeping the most recent estimator as the base
                    if (null == old || hllp.getInitTime() > old.getInitTime()) {
                      if (null != old) {
                        hllp.fuse(old);
                      }

                      hllp.setKey(APPLICATION_PREFIX_CHAR + entity);
                      
                      synchronized(applicationHLLPEstimators) {
                        applicationHLLPEstimators.put(entity, hllp);                      
                      }
                    } else {
                      old.fuse(hllp);
                    }

                  }
                }
                
                if (!"".equals(mads)) {
                  long limit = Long.parseLong(mads);
                  // Adjust limit so we account for the error of the estimator
                  limit = (long) Math.ceil(limit * toleranceRatio);
                  
                  Map<String,String> labels = new HashMap<String, String>();
                  
                  if (isProducer) {
                    producerMADSLimits.put(entity, limit);
                    labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, entity);
                    Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_LIMIT, labels, limit);
                  } else {
                    applicationMADSLimits.put(entity, limit);                                        
                    labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, entity);
                    Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_GTS_LIMIT_PER_APP, labels, limit);
                  }
                }
                
                if (!"".equals(rate)) {
                  Map<String,String> labels = new HashMap<String, String>();

                  double rlimit = Double.parseDouble(rate);
                  
                  if (isProducer) {
                    producerRateLimiters.put(entity, RateLimiter.create(Math.max(MINIMUM_RATE_LIMIT, rlimit)));
                    labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, entity);
                    Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_LIMIT, labels, rlimit);
                  } else {
                    applicationRateLimiters.put(entity, RateLimiter.create(Math.max(MINIMUM_RATE_LIMIT, rlimit)));
                    labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, entity);
                    Sensision.event(SensisionConstants.SENSISION_CLASS_CONTINUUM_THROTTLING_RATE_LIMIT_PER_APP, labels, rlimit);
                  }
                } else {
                  if (isProducer) {
                    producerRateLimiters.remove(entity);
                  } else {
                    applicationRateLimiters.remove(entity);
                  }
                }
                
              }
              
              br.close();
              
              newreads.add(file);
            } catch (Exception e) {              
              e.printStackTrace();
            }            
          }

          loaded = true;
          
          //
          // Replace the list of read files
          //
          
          read = newreads;
          
          //
          // Store events with the current versions of all estimators.
          //
          
          TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
              
          if (System.currentTimeMillis() - now > rampup) {
            try {
              for (Map.Entry<String, HyperLogLogPlus> keyAndHllp: producerHLLPEstimators.entrySet()) {
                String key = keyAndHllp.getKey();
                HyperLogLogPlus hllp = keyAndHllp.getValue();
                
                if (null == hllp) {
                  continue;
                }
                
                if (hllp.hasExpired()) {
                  Map<String,String> labels = new HashMap<String, String>();
                  labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, key);
                  Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, 0, 24 * 3600 * 1000L);              
                  continue;
                }
                
                try {
                  byte[] bytes = hllp.toBytes();
                  String encoded = new String(OrderPreservingBase64.encode(bytes), StandardCharsets.US_ASCII);
                  Map<String,String> labels = new HashMap<String, String>();
                  labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, key);
                  Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, hllp.cardinality());              
                  Sensision.event(0L, null, null, null, SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_ESTIMATOR, labels, encoded);
                  broadcastEstimator(bytes);
                } catch (IOException ioe) {
                  // Ignore exception
                }
              }

              for (Map.Entry<String, HyperLogLogPlus> keyAndHllp: applicationHLLPEstimators.entrySet()) {
                String key = keyAndHllp.getKey();
                HyperLogLogPlus hllp = keyAndHllp.getValue();

                if (null == hllp) {
                  continue;
                }
                
                if (hllp.hasExpired()) {
                  Map<String,String> labels = new HashMap<String, String>();
                  labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, key);
                  Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels, 0L, 24 * 3600 * 1000L);
                  continue;
                }
                
                try {
                  byte[] bytes = hllp.toBytes();
                  String encoded = new String(OrderPreservingBase64.encode(bytes), StandardCharsets.US_ASCII);
                  Map<String,String> labels = new HashMap<String, String>();
                  labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, key);
                  Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels, hllp.cardinality());
                  Sensision.event(0L, null, null, null, SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_ESTIMATOR_PER_APP, labels, encoded);
                  broadcastEstimator(bytes);
                } catch (IOException ioe) {
                  // Ignore exception
                }
              }            
            } catch (ConcurrentModificationException cme) {              
            }            
          }

          try { Thread.sleep(delay); } catch (InterruptedException ie) {}
        }
      }
    };
    
    t.setName("[ThrottlingManager]");
    t.setDaemon(true);
    
    if (null != dir) {
      t.start();
    } 
    
    initialized.set(true); 
  }
  
  public static void enable() {
    enabled = true;
  }
  
  /**
   * Broadcast an estimator on Kafka if Kafka is configured.
   * @param bytes serialized estimator to forward
   */
  private static synchronized void broadcastEstimator(byte[] bytes) {
    if (null != throttlingProducer) {
      try {
        //
        // Apply the MAC if defined
        //
        
        if (null != throttlingMAC) {
          bytes = CryptoUtils.addMAC(throttlingMAC, bytes);
        }

        KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(throttlingTopic, bytes);
        
        throttlingProducer.send(message);
        Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_KAFKA_THROTTLING_OUT_MESSAGES, Sensision.EMPTY_LABELS, 1);
        Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_KAFKA_THROTTLING_OUT_BYTES, Sensision.EMPTY_LABELS, bytes.length);
      } catch (Exception e) {
        Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_KAFKA_THROTTLING_ERRORS, Sensision.EMPTY_LABELS, 1);
      }
    }
  }
  
  public static void fuse(HyperLogLogPlus hllp) throws Exception {
    //
    // Ignore estimators with no keys
    //
    
    if (null == hllp.getKey()) {
      return;
    }
    
    //
    // Ignore expired estimators
    //
    
    if (hllp.hasExpired()) {
      return;
    }
    
    boolean isApp = false;
    
    if (hllp.getKey().length() > 0 && hllp.getKey().charAt(0) == APPLICATION_PREFIX_CHAR) {
      isApp = true;
    }
    
    if (isApp) {
      HyperLogLogPlus old = applicationHLLPEstimators.get(hllp.getKey().substring(1));

      if (null != old && old.hasExpired()) {
        old = null;
      }
      
      // Merge estimators and replace with the result, keeping the most recent estimator as the base
      if (null == old || hllp.getInitTime() > old.getInitTime()) {
        if (null != old) {
          hllp.fuse(old);
        }

        synchronized(applicationHLLPEstimators) {
          applicationHLLPEstimators.put(hllp.getKey().substring(1), hllp);                      
        }
        Map<String,String> labels = new HashMap<String,String>(1);
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, hllp.getKey().substring(1));
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels, hllp.cardinality());
      } else {
        old.fuse(hllp);
        Map<String,String> labels = new HashMap<String,String>(1);
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, old.getKey().substring(1));
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT_PER_APP, labels, old.cardinality());
      }      
    } else {
      HyperLogLogPlus old = producerHLLPEstimators.get(hllp.getKey());

      if (null != old && old.hasExpired()) {
        old = null;
      }
      
      // Merge estimators and replace with the result, keeping the most recent estimator as the base
      if (null == old || hllp.getInitTime() > old.getInitTime()) {
        if (null != old){
          hllp.fuse(old);
        }
        
        synchronized(producerHLLPEstimators) {
          producerHLLPEstimators.put(hllp.getKey(), hllp);                      
        }
        
        Map<String,String> labels = new HashMap<String,String>(1);
        labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, hllp.getKey());
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, hllp.cardinality());
      } else {
        old.fuse(hllp);
        Map<String,String> labels = new HashMap<String,String>(1);
        labels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, old.getKey());
        Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_DISTINCT, labels, old.cardinality());
      }                          
    }
  }
  
  private static void dumpCurrentConfig() {
    if (null != dir && !producerHLLPEstimators.isEmpty() && !applicationHLLPEstimators.isEmpty()) {
      File config;
      if (".dump".endsWith(THROTTLING_MANAGER_SUFFIX)) {
        config = new File(dir, "current" + THROTTLING_MANAGER_SUFFIX + ".dump.");      
      } else {
        config = new File(dir, "current" + THROTTLING_MANAGER_SUFFIX + ".dump");
      }
      
      try {
        PrintWriter pw = new PrintWriter(config);
        
        pw.println("###");
        pw.println("### Automatic throttling configuration dumped on " + ISODateTimeFormat.dateTime().print(System.currentTimeMillis()));
        pw.println("###");
        
        Set<String> keys = producerHLLPEstimators.keySet();
        keys.addAll(producerRateLimiters.keySet());

        for (String key: keys) {
          pw.print(key);
          pw.print(":");
          Long limit = producerMADSLimits.get(key);
          if (null != limit) {
            pw.print(limit);
          }
          pw.print(":");
          RateLimiter limiter = producerRateLimiters.get(key);
          if (null != limiter) {
            pw.print(limiter.getRate());
          }
          pw.print(":");
          if (producerHLLPEstimators.containsKey(key)) {
            pw.print(new String(OrderPreservingBase64.encode(producerHLLPEstimators.get(key).toBytes()), StandardCharsets.US_ASCII));
          }
          pw.println(":#");
        }
        
        keys = applicationHLLPEstimators.keySet();
        keys.addAll(applicationRateLimiters.keySet());
        
        for (String key: keys) {
          pw.print(APPLICATION_PREFIX_CHAR);
          pw.print(URLEncoder.encode(key, StandardCharsets.UTF_8.name()));
          pw.print(":");
          Long limit = applicationMADSLimits.get(key);
          if (null != limit) {
            pw.print(limit);
          }
          pw.print(":");
          RateLimiter limiter = applicationRateLimiters.get(key);
          if (null != limiter) {
            pw.print(limiter.getRate());
          }
          pw.print(":");
          if (applicationHLLPEstimators.containsKey(key)) {
            pw.print(new String(OrderPreservingBase64.encode(applicationHLLPEstimators.get(key).toBytes()), StandardCharsets.US_ASCII));
          }
          pw.println(":#");
        }
        
        pw.close();
      } catch (Exception e) {
        LOG.error("Error while dumping throttling configuration.");
      }
    }
  }
}
