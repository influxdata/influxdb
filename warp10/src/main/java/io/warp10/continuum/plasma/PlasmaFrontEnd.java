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

package io.warp10.continuum.plasma;

import io.warp10.SSLUtils;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.KafkaOffsetCounters;
import io.warp10.continuum.KafkaSynchronizedConsumerPool;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.Hook;
import io.warp10.continuum.egress.ThriftDirectoryClient;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Directory;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandalonePlasmaHandler;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.zookeeper.CreateMode;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import com.google.common.base.Preconditions;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class PlasmaFrontEnd extends StandalonePlasmaHandler implements Runnable, PlasmaSubscriptionListener {

  
  /**
   * Curator Framework for Subscriptions
   */
  private final CuratorFramework subscriptionCuratorFramework;

  private final AtomicBoolean subscriptionChanged = new AtomicBoolean(false);

  private final String znoderoot;
  
  private int maxZnodeSize;
  
  private long subscribeDelay;
  
  private final String topic;
  
  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[] {
    Configuration.PLASMA_FRONTEND_KAFKA_ZKCONNECT,
    Configuration.PLASMA_FRONTEND_KAFKA_TOPIC,
    Configuration.PLASMA_FRONTEND_KAFKA_GROUPID,
    Configuration.PLASMA_FRONTEND_KAFKA_COMMITPERIOD,
    Configuration.PLASMA_FRONTEND_KAFKA_NTHREADS,
    Configuration.PLASMA_FRONTEND_HOST,
    Configuration.PLASMA_FRONTEND_PORT,
    Configuration.PLASMA_FRONTEND_MAXZNODESIZE,
    Configuration.PLASMA_FRONTEND_ZKCONNECT,
    Configuration.PLASMA_FRONTEND_ZNODE,
    Configuration.PLASMA_FRONTEND_SUBSCRIBE_DELAY,
    Configuration.PLASMA_FRONTEND_ACCEPTORS,
    Configuration.PLASMA_FRONTEND_SELECTORS,
    Configuration.PLASMA_FRONTEND_IDLE_TIMEOUT,
    Configuration.DIRECTORY_ZK_QUORUM,
    Configuration.DIRECTORY_ZK_ZNODE,
    Configuration.DIRECTORY_PSK,
  };

  public PlasmaFrontEnd(KeyStore keystore, final Properties properties) throws Exception {
    
    super(keystore, properties, null, false);
  
    // Extract Directory PSK
    String keyspec = properties.getProperty(Configuration.DIRECTORY_PSK);
    
    if (null != keyspec) {
      byte[] key = this.keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.DIRECTORY_PSK + " MUST be 128 bits long.");
      this.keystore.setKey(KeyStore.SIPHASH_DIRECTORY_PSK, key);
    }    

    //
    // Make sure all required configuration is present
    //
    
    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(required), "Missing configuration parameter '%s'.", required);          
    }

    this.znoderoot = properties.getProperty(Configuration.PLASMA_FRONTEND_ZNODE);
    
    this.maxZnodeSize = Integer.parseInt(properties.getProperty(Configuration.PLASMA_FRONTEND_MAXZNODESIZE));
    
    // Align maxZnodeSize on 16 bytes boundary
    this.maxZnodeSize = this.maxZnodeSize - (this.maxZnodeSize % 16);
  
    this.topic = properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_TOPIC);
    
    this.subscribeDelay = Long.parseLong(properties.getProperty(Configuration.PLASMA_FRONTEND_SUBSCRIBE_DELAY));
    
    //
    // Extract keys
    //
    
    if (null != properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_MAC)) {
      keystore.setKey(KeyStore.SIPHASH_KAFKA_PLASMA_FRONTEND_IN, keystore.decodeKey(properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_MAC)));
    }

    if (null != properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_AES)) {
      keystore.setKey(KeyStore.AES_KAFKA_PLASMA_FRONTEND_IN, keystore.decodeKey(properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_AES)));
    }

    //
    // Start Curator Framework for subscriptions
    //
    
    subscriptionCuratorFramework = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(5000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(properties.getProperty(Configuration.PLASMA_FRONTEND_ZKCONNECT))
        .build();
    subscriptionCuratorFramework.start();
        
    DirectoryClient directoryClient = new ThriftDirectoryClient(this.keystore, properties);
    
    setDirectoryClient(directoryClient);
    
    this.setSubscriptionListener(this);

    //
    // Create Kafka consumer pool
    //
    
    final PlasmaFrontEnd frontend = this;
    
    ConsumerFactory factory = new ConsumerFactory() {      
      @Override
      public Runnable getConsumer(final KafkaSynchronizedConsumerPool pool, final KafkaStream<byte[], byte[]> stream) {
        return new Runnable() {          
          @Override
          public void run() {
            ConsumerIterator<byte[],byte[]> iter = stream.iterator();

            byte[] sipHashKey = frontend.keystore.getKey(KeyStore.SIPHASH_KAFKA_PLASMA_FRONTEND_IN);
            byte[] aesKey = frontend.keystore.getKey(KeyStore.AES_KAFKA_PLASMA_FRONTEND_IN);

            // Iterate on the messages
            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

            KafkaOffsetCounters counters = pool.getCounters();
            
            // TODO(hbs): allow setting of writeBufferSize

            try {
            while (iter.hasNext()) {
              //
              // Since the cal to 'next' may block, we need to first
              // check that there is a message available
              //
              
              boolean nonEmpty = iter.nonEmpty();
              
              if (nonEmpty) {
                MessageAndMetadata<byte[], byte[]> msg = iter.next();
                counters.count(msg.partition(), msg.offset());
                
                byte[] data = msg.message();

                Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_MESSAGES, Sensision.EMPTY_LABELS, 1);
                Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_BYTES, Sensision.EMPTY_LABELS, data.length);
                
                if (null != sipHashKey) {
                  data = CryptoUtils.removeMAC(sipHashKey, data);
                }
                
                // Skip data whose MAC was not verified successfully
                if (null == data) {
                  Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_INVALIDMACS, Sensision.EMPTY_LABELS, 1);
                  continue;
                }
                
                // Unwrap data if need be
                if (null != aesKey) {
                  data = CryptoUtils.unwrap(aesKey, data);
                }
                
                // Skip data that was not unwrapped successfuly
                if (null == data) {
                  Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_INVALIDCIPHERS, Sensision.EMPTY_LABELS, 1);
                  continue;
                }
                
                //
                // Extract KafkaDataMessage
                //
                
                KafkaDataMessage tmsg = new KafkaDataMessage();
                deserializer.deserialize(tmsg, data);
                
                switch(tmsg.getType()) {
                  case STORE:
                    GTSEncoder encoder = new GTSEncoder(0L, null, tmsg.getData());
                    encoder.setClassId(tmsg.getClassId());
                    encoder.setLabelsId(tmsg.getLabelsId());
                    frontend.dispatch(encoder);
                    break;
                  case DELETE:
                  case ARCHIVE:
                    break;
                  default:
                    throw new RuntimeException("Invalid message type.");
                }            
              } else {
                // Sleep a tiny while
                try {
                  Thread.sleep(1L);
                } catch (InterruptedException ie) {             
                }
              }          
            }        
          } catch (Throwable t) {
            t.printStackTrace(System.err);
          } finally {
            // Set abort to true in case we exit the 'run' method
            pool.getAbort().set(true);
          }
                 
          }
        };
      }
    };
    
    KafkaSynchronizedConsumerPool pool = new KafkaSynchronizedConsumerPool(properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_ZKCONNECT),
        properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_TOPIC),
        properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_CONSUMER_CLIENTID),
        properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_GROUPID),
        properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY),
        Integer.parseInt(properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_NTHREADS)),
        Long.parseLong(properties.getProperty(Configuration.PLASMA_FRONTEND_KAFKA_COMMITPERIOD)), factory);
    
    pool.setAbortHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_ABORTS, Sensision.EMPTY_LABELS, 1);
      }
    });
    
    pool.setCommitOffsetHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_KAFKA_COMMITS, Sensision.EMPTY_LABELS, 1);
      }
    });
    pool.setSyncHook(new Hook() {      
      @Override
      public void call() {
        Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_SYNCS, Sensision.EMPTY_LABELS, 1);
      }
    });

    //
    // Start Jetty server
    //
    
    Server server = new Server();
    
    boolean useHttp = null != properties.getProperty(Configuration.PLASMA_FRONTEND_PORT);
    boolean useHttps = null != properties.getProperty(Configuration.PLASMA_FRONTEND_PREFIX + Configuration._SSL_PORT);
    int tcpBacklog = Integer.valueOf(properties.getProperty(Configuration.PLASMA_FRONTEND_TCP_BACKLOG, "0"));
    
    List<ServerConnector> connectors = new ArrayList<ServerConnector>();
    
    if (useHttp) {
      ServerConnector connector = new ServerConnector(server, Integer.parseInt(properties.getProperty(Configuration.PLASMA_FRONTEND_ACCEPTORS)), Integer.parseInt(properties.getProperty(Configuration.PLASMA_FRONTEND_SELECTORS)));
      connector.setIdleTimeout(Long.parseLong(properties.getProperty(Configuration.PLASMA_FRONTEND_IDLE_TIMEOUT)));    
      connector.setPort(Integer.parseInt(properties.getProperty(Configuration.PLASMA_FRONTEND_PORT)));
      connector.setHost(properties.getProperty(Configuration.PLASMA_FRONTEND_HOST));
      connector.setAcceptQueueSize(tcpBacklog);
      connector.setName("Continuum Plasma Front End HTTP");

      connectors.add(connector);
    }
    
    if (useHttps) {
      ServerConnector connector = SSLUtils.getConnector(server, Configuration.PLASMA_FRONTEND_PREFIX);
      connector.setName("Continuum Plasma Front End HTTPS");
      connectors.add(connector);
    }
    
    server.setConnectors(connectors.toArray(new Connector[connectors.size()]));

    server.setHandler(this);
    
    JettyUtil.setSendServerVersion(server, false);
    
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("[Continuum Plasma Front End]");
    t.start();
  }
  
  @Override
  public void onChange() {
    this.subscriptionChanged.set(true);
  }
  
  @Override
  public void run() {    
    
    // Current set of subscription znodes (used for deletion)
    
    Set<String> currentZnodes = new HashSet<String>();
    
    //
    // Endlessly check if we need to update the subscriptions in ZK
    //

    while(true) {
      if (!this.subscriptionChanged.get()) {
        try { Thread.sleep(this.subscribeDelay); } catch (InterruptedException ie) {}
        continue;
      }
      
      // Clear flag
      this.subscriptionChanged.set(false);
     
      // Extract current subscriptions
      Set<BigInteger> subscriptions = this.getSubscriptions();
      
      // Delete current znodes
      
      for (String znode: currentZnodes) {
        try {
          this.subscriptionCuratorFramework.delete().guaranteed().forPath(znode);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      
      currentZnodes.clear();
      
      // Store new ones
    
      byte[] bytes = new byte[subscriptions.size() * 16];
      
      int idx = 0;
            
      for (BigInteger bi: subscriptions) {
        byte[] newbytes = bi.toByteArray();

        if (bi.signum() < 0) {
          Arrays.fill(bytes, idx, idx + 16, (byte) 0xff);
        }
        
        System.arraycopy(newbytes, 0, bytes, idx + 16 - newbytes.length, newbytes.length);
        
        idx += 16;
      }
      
      //
      // We now create znodes, limiting the size of each one to maxZnodeSize
      //
      
      idx = 0;
      
      UUID uuid = UUID.randomUUID();

      while(idx < bytes.length) {
        int chunksize = Math.min(this.maxZnodeSize, bytes.length - idx);
        
        // Ensure chunksize is a multiple of 16
        chunksize = chunksize - (chunksize % 16);
        
        byte[] data = new byte[chunksize];
        
        System.arraycopy(bytes, idx, data, 0, data.length);
                
        long sip = SipHashInline.hash24(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), data, 0, data.length);
        
        String path = this.znoderoot + "/0." + uuid.toString() + "." + this.topic + "." + idx + "." + Long.toHexString(sip);
        
        try {
          this.subscriptionCuratorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
          currentZnodes.add(path);
        } catch (Exception e) {
          e.printStackTrace();
        }
        
        idx += data.length;
      }
      
      //
      // To notify the backend, update the subscription's zone data
      //
      
      byte[] randomData = (this.topic + "." + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8);
      
      try {
        this.subscriptionCuratorFramework.setData().forPath(this.znoderoot, randomData);
      } catch (Exception e) {
        e.printStackTrace();
      }
      
      Map<String,String> labels = new HashMap<String,String>();
      labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, this.topic);
      Sensision.set(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_SUBSCRIPTIONS, labels, subscriptions.size());
    }
  }
}
