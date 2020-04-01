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

package io.warp10.standalone;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.Options;

import com.google.common.base.Preconditions;

import io.warp10.Revision;
import io.warp10.WarpConfig;
import io.warp10.SSLUtils;
import io.warp10.WarpDist;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.egress.CORSHandler;
import io.warp10.continuum.egress.EgressExecHandler;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.egress.EgressFindHandler;
import io.warp10.continuum.egress.EgressInteractiveHandler;
import io.warp10.continuum.egress.EgressMobiusHandler;
import io.warp10.continuum.ingress.DatalogForwarder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.ParallelGTSDecoderIteratorWrapper;
import io.warp10.continuum.store.StoreClient;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OSSKeyStore;
import io.warp10.crypto.UnsecureKeyStore;
import io.warp10.quasar.filter.QuasarTokenFilter;
import io.warp10.script.ScriptRunner;
import io.warp10.script.WarpScriptLib;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

public class Warp extends WarpDist implements Runnable {

  private static final String DEFAULT_HTTP_ACCEPTORS = "2";
  private static final String DEFAULT_HTTP_SELECTORS = "4";
  
  private static final String NULL = "null";
        
  private static WarpDB db;
  
  private static boolean standaloneMode = false;
  
  private static int port;
  
  private static String host;
  
  private static Set<Path> datalogSrcDirs = Collections.unmodifiableSet(new HashSet<Path>()); 
  
  private static final String[] REQUIRED_PROPERTIES = {
    Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE,
    Configuration.PLASMA_FRONTEND_WEBSOCKET_MAXMESSAGESIZE,
    Configuration.WARP_HASH_CLASS,
    Configuration.WARP_HASH_LABELS,
    Configuration.WARP_HASH_TOKEN,
    Configuration.WARP_HASH_APP,
    Configuration.WARP_AES_TOKEN,
    Configuration.WARP_AES_SCRIPTS,
    Configuration.CONFIG_WARPSCRIPT_UPDATE_ENDPOINT,
    Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT,
    Configuration.WARP_TIME_UNITS,
  };
  
  public Warp() {
    // TODO Auto-generated constructor stub
  }
  
  public static void main(String[] args) throws Exception {
    // Indicate standalone mode is on
    standaloneMode = true;

    System.setProperty("java.awt.headless", "true");
    
    System.out.println();
    System.out.println(Constants.WARP10_BANNER);
    System.out.println("  Revision " + Revision.REVISION);
    System.out.println();

    Map<String,String> labels = new HashMap<String, String>();
    labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "standalone");
    Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);

    setProperties(args);
    
    Properties properties = getProperties();
    
    boolean analyticsEngineOnly = "true".equals(properties.getProperty(Configuration.ANALYTICS_ENGINE_ONLY));
    boolean nullbackend = "true".equals(properties.getProperty(NULL)) || analyticsEngineOnly;
    
    boolean plasmabackend = "true".equals(properties.getProperty(Configuration.PURE_PLASMA));
    
    boolean inmemory = "true".equals(properties.getProperty(Configuration.IN_MEMORY));
    boolean accelerator = "true".equals(properties.getProperty(Configuration.ACCELERATOR));
    
    if (inmemory && accelerator) {
      throw new RuntimeException("Accelerator mode cannot be enabled when " + Configuration.IN_MEMORY + " is set to true.");
    }
    
    boolean enablePlasma = !("true".equals(properties.getProperty(Configuration.WARP_PLASMA_DISABLE)));
    boolean enableMobius = !("true".equals(properties.getProperty(Configuration.WARP_MOBIUS_DISABLE)));
    boolean enableStreamUpdate = !("true".equals(properties.getProperty(Configuration.WARP_STREAMUPDATE_DISABLE)));
    boolean enableREL = !("true".equals(properties.getProperty(Configuration.WARP_INTERACTIVE_DISABLE)));
    
    for (String property: REQUIRED_PROPERTIES) {
      // Don't check LEVELDB_HOME when in-memory
      if (inmemory && Configuration.LEVELDB_HOME.equals(property)) {
        continue;
      }
      Preconditions.checkNotNull(properties.getProperty(property), "Property '" + property + "' MUST be set.");
    }

    //
    // Initialize KeyStore
    //

    KeyStore keystore;
    
    if (properties.containsKey(Configuration.OSS_MASTER_KEY)) {
      keystore = new OSSKeyStore(properties.getProperty(Configuration.OSS_MASTER_KEY));
    } else {
      keystore = new UnsecureKeyStore();
    }

    //
    // Decode generic keys
    // We do that first so those keys do not have precedence over the specific
    // keys.
    //
    
    for (Entry<Object,Object> entry: properties.entrySet()) {
      if (entry.getKey().toString().startsWith(Configuration.WARP_KEY_PREFIX)) {
        byte[] key = keystore.decodeKey(entry.getValue().toString());
        if (null == key) {
          throw new RuntimeException("Unable to decode key '" + entry.getKey() + "'.");
        }
        keystore.setKey(entry.getKey().toString().substring(Configuration.WARP_KEY_PREFIX.length()), key);
      }
    }

    extractKeys(keystore, properties);
    
    keystore.setKey(KeyStore.SIPHASH_CLASS, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_CLASS)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_CLASS).length, Configuration.WARP_HASH_CLASS + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.SIPHASH_LABELS, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_LABELS)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_LABELS).length, Configuration.WARP_HASH_LABELS + " MUST be 128 bits long.");
    
    //
    // Generate secondary keys. We use the ones' complement of the primary keys
    //
    
    keystore.setKey(KeyStore.SIPHASH_CLASS_SECONDARY, CryptoUtils.invert(keystore.getKey(KeyStore.SIPHASH_CLASS)));
    keystore.setKey(KeyStore.SIPHASH_LABELS_SECONDARY, CryptoUtils.invert(keystore.getKey(KeyStore.SIPHASH_LABELS)));        
    
    keystore.setKey(KeyStore.SIPHASH_TOKEN, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_TOKEN)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_TOKEN).length, Configuration.WARP_HASH_TOKEN + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.SIPHASH_APPID, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_APP)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_APPID).length, Configuration.WARP_HASH_APP + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.AES_TOKEN, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_TOKEN)));
    Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_TOKEN).length) || (24 == keystore.getKey(KeyStore.AES_TOKEN).length) || (32 == keystore.getKey(KeyStore.AES_TOKEN).length), Configuration.WARP_AES_TOKEN + " MUST be 128, 192 or 256 bits long.");
    keystore.setKey(KeyStore.AES_SECURESCRIPTS, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_SCRIPTS)));
    Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length) || (24 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length) || (32 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length), Configuration.WARP_AES_SCRIPTS + " MUST be 128, 192 or 256 bits long.");

    if (properties.containsKey(Configuration.WARP_AES_METASETS)) {
      keystore.setKey(KeyStore.AES_METASETS, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_METASETS)));
      Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_METASETS).length) || (24 == keystore.getKey(KeyStore.AES_METASETS).length) || (32 == keystore.getKey(KeyStore.AES_METASETS).length), Configuration.WARP_AES_METASETS + " MUST be 128, 192 or 256 bits long.");
    }

    if (null != properties.getProperty(Configuration.WARP_AES_LOGGING, Configuration.WARP_DEFAULT_AES_LOGGING)) {
      keystore.setKey(KeyStore.AES_LOGGING, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_LOGGING, Configuration.WARP_DEFAULT_AES_LOGGING)));
      Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_LOGGING).length) || (24 == keystore.getKey(KeyStore.AES_LOGGING).length) || (32 == keystore.getKey(KeyStore.AES_LOGGING).length), Configuration.WARP_AES_LOGGING + " MUST be 128, 192 or 256 bits long.");      
    }
    
    setKeyStore(keystore);
    
    //
    // Initialize levelDB
    //
    
    Options options = new Options();
    
    options.createIfMissing(false);
    
    if (properties.containsKey(Configuration.LEVELDB_MAXOPENFILES)) {
      int maxOpenFiles = Integer.parseInt(properties.getProperty(Configuration.LEVELDB_MAXOPENFILES));
      options.maxOpenFiles(maxOpenFiles);
    }
    
    if (null != properties.getProperty(Configuration.LEVELDB_CACHE_SIZE)) {
      options.cacheSize(Long.parseLong(properties.getProperty(Configuration.LEVELDB_CACHE_SIZE)));    
    }
    
    if (null != properties.getProperty(Configuration.LEVELDB_COMPRESSION_TYPE)) {
      if ("snappy".equalsIgnoreCase(properties.getProperty(Configuration.LEVELDB_COMPRESSION_TYPE))) {
        options.compressionType(CompressionType.SNAPPY);
      } else {
        options.compressionType(CompressionType.NONE);
      }
    }

    //
    // Attempt to load JNI library, fallback to pure java in case of error
    //
    
    if (!inmemory && !nullbackend && !plasmabackend) {
      boolean nativedisabled = "true".equals(properties.getProperty(Configuration.LEVELDB_NATIVE_DISABLE));
      boolean javadisabled = "true".equals(properties.getProperty(Configuration.LEVELDB_JAVA_DISABLE));
      String home = properties.getProperty(Configuration.LEVELDB_HOME);
      db = new WarpDB(nativedisabled, javadisabled, home, options);
    }

    // Register shutdown hook to close the DB.
    Runtime.getRuntime().addShutdownHook(new Thread(new Warp()));
    
    //
    // Initialize the backup manager
    //
    
    if (null != db) {
      String triggerPath = properties.getProperty(Configuration.STANDALONE_SNAPSHOT_TRIGGER);
      String signalPath = properties.getProperty(Configuration.STANDALONE_SNAPSHOT_SIGNAL);
      
      if (null != triggerPath && null != signalPath) {
        Thread backupManager = new StandaloneSnapshotManager(triggerPath, signalPath);
        backupManager.setDaemon(true);
        backupManager.setName("[Snapshot Manager]");
        backupManager.start();        
      }
    }
    
    WarpScriptLib.registerExtensions();

    //
    // Initialize ThrottlingManager
    //
    
    ThrottlingManager.init();

    //
    // Create Jetty server
    //
    
    Server server = new Server();

    boolean useHTTPS = null != properties.getProperty(Configuration.STANDALONE_PREFIX + Configuration._SSL_PORT);
    boolean useHTTP = null != properties.getProperty(Configuration.STANDALONE_PORT);
    
    ServerConnector httpConnector = null;
    ServerConnector httpsConnector = null;
    
    if (!useHTTPS && !useHTTP ) {
      throw new RuntimeException("Missing '" + Configuration.STANDALONE_PORT + "' or '" + Configuration.STANDALONE_PREFIX + Configuration._SSL_PORT + "' configuration");
    }
    
    List<Connector> connectors = new ArrayList<Connector>();
    
    if (useHTTP) {
      int acceptors = Integer.valueOf(properties.getProperty(Configuration.STANDALONE_ACCEPTORS, DEFAULT_HTTP_ACCEPTORS));
      int selectors = Integer.valueOf(properties.getProperty(Configuration.STANDALONE_SELECTORS, DEFAULT_HTTP_SELECTORS));
      port = Integer.valueOf(properties.getProperty(Configuration.STANDALONE_PORT));
      host = properties.getProperty(Configuration.STANDALONE_HOST, "0.0.0.0");
      int tcpBacklog = Integer.valueOf(properties.getProperty(Configuration.STANDALONE_TCP_BACKLOG, "0"));
      
      httpConnector = new ServerConnector(server, acceptors, selectors);
      
      httpConnector.setPort(port);
      httpConnector.setAcceptQueueSize(tcpBacklog);
      
      if (null != host) {
        httpConnector.setHost(host);
      }
      
      String idle = properties.getProperty(Configuration.STANDALONE_IDLE_TIMEOUT);
      
      if (null != idle) {
        httpConnector.setIdleTimeout(Long.parseLong(idle));
      }
      
      httpConnector.setName("Warp 10 Standalone HTTP Endpoint");
      
      connectors.add(httpConnector);
    }

    if (useHTTPS) {
      httpsConnector = SSLUtils.getConnector(server, Configuration.STANDALONE_PREFIX);
      httpsConnector.setName("Warp 10 Standalone HTTPS Endpoint");      
      connectors.add(httpsConnector);
    }
    
    server.setConnectors(connectors.toArray(new Connector[connectors.size()]));

    HandlerList handlers = new HandlerList();
    
    Handler cors = new CORSHandler();
    handlers.addHandler(cors);
    
    StandaloneDirectoryClient sdc = null;
    StoreClient scc = null;

    if (inmemory) {
      sdc = new StandaloneDirectoryClient(null, keystore);
      
      sdc.setActivityWindow(Long.parseLong(properties.getProperty(Configuration.INGRESS_ACTIVITY_WINDOW, "0")));
      
      if (null == properties.getProperty(Configuration.IN_MEMORY_CHUNKED)) {
        throw new RuntimeException("Configuration key '" + Configuration.IN_MEMORY_CHUNKED + "' MUST now explicitely be set to 'true' or 'false' (if you wish to use the now deprecated non chunked memory store).");
      }
      
      if (!"false".equals(properties.getProperty(Configuration.IN_MEMORY_CHUNKED))) {
        scc = new StandaloneChunkedMemoryStore(properties, keystore);
        ((StandaloneChunkedMemoryStore) scc).setDirectoryClient(sdc);
        ((StandaloneChunkedMemoryStore) scc).load();
      } else {
        scc = new StandaloneMemoryStore(keystore,
            Long.valueOf(properties.getProperty(Configuration.IN_MEMORY_DEPTH, Long.toString(60 * 60 * 1000 * Constants.TIME_UNITS_PER_MS))),
            Long.valueOf(properties.getProperty(Configuration.IN_MEMORY_HIGHWATERMARK, "100000")),
            Long.valueOf(properties.getProperty(Configuration.IN_MEMORY_LOWWATERMARK, "80000")));
        ((StandaloneMemoryStore) scc).setDirectoryClient(sdc);
        if ("true".equals(properties.getProperty(Configuration.IN_MEMORY_EPHEMERAL))) {
          ((StandaloneMemoryStore) scc).setEphemeral(true);
        }        
        ((StandaloneMemoryStore) scc).load();
      }
    } else if (plasmabackend) {
      sdc = new StandaloneDirectoryClient(null, keystore);
      scc = new PlasmaStoreClient();
    } else if (nullbackend) {
      sdc = new NullDirectoryClient(keystore);
      scc = new NullStoreClient();
    } else {
      sdc = new StandaloneDirectoryClient(db, keystore);    
      scc = new StandaloneStoreClient(db, keystore, properties);
      
      if (accelerator) {
        scc = new StandaloneAcceleratedStoreClient(sdc, scc);
      }
    }
        
    if (null != WarpConfig.getProperty(Configuration.DATALOG_DIR) && null != WarpConfig.getProperty(Configuration.DATALOG_SHARDS)) {
      sdc = new StandaloneShardedDirectoryClientWrapper(keystore, sdc);
      scc = new StandaloneShardedStoreClientWrapper(keystore, scc);
    }
    
    if (ParallelGTSDecoderIteratorWrapper.useParallelScanners()) {
      scc = new StandaloneParallelStoreClientWrapper(scc);
    }

    if (properties.containsKey(Configuration.RUNNER_ROOT)) {
      if (!properties.containsKey(Configuration.RUNNER_ENDPOINT)) {
        properties.setProperty(Configuration.RUNNER_ENDPOINT, "");
        StandaloneScriptRunner runner = new StandaloneScriptRunner(properties, keystore.clone(), scc, sdc, properties);
      } else {
        //
        // Allocate a normal runner
        //
        ScriptRunner runner = new ScriptRunner(keystore.clone(), properties);
      }
    }
    
    //
    // Start the Datalog Forwarders
    //
    
    if (!analyticsEngineOnly && properties.containsKey(Configuration.DATALOG_FORWARDERS)) {
      // Extract the names of the forwarders and start them all, ensuring we only start each one once
      String[] forwarders = properties.getProperty(Configuration.DATALOG_FORWARDERS).split(",");
      
      Set<String> names = new HashSet<String>();
      for (String name: forwarders) {
        names.add(name.trim());
      }
      
      Set<Path> srcDirs = new HashSet<Path>();
      
      Path datalogdir = new File(properties.getProperty(Configuration.DATALOG_DIR)).toPath().toRealPath();
      
      for (String name: names) {
        DatalogForwarder forwarder = new DatalogForwarder(name, keystore, properties);
        
        Path root = forwarder.getRootDir().toRealPath();
        
        if (datalogdir.equals(root)) {
          throw new RuntimeException("Datalog directory '" + datalogdir + "' cannot be used as a forwarder source.");
        }
      
        if (!srcDirs.add(root)) {
          throw new RuntimeException("Duplicate datalog source directory '" + root + "'.");
        }
      }
      
      datalogSrcDirs = Collections.unmodifiableSet(srcDirs);
      
    } else if (!analyticsEngineOnly && properties.containsKey(Configuration.DATALOG_FORWARDER_SRCDIR) && properties.containsKey(Configuration.DATALOG_FORWARDER_DSTDIR)) {
      Path datalogdir = new File(properties.getProperty(Configuration.DATALOG_DIR)).toPath().toRealPath();
      DatalogForwarder forwarder = new DatalogForwarder(keystore, properties);      
      Path root = forwarder.getRootDir().toRealPath();
      if (datalogdir.equals(root)) {
        throw new RuntimeException("Datalog directory '" + datalogdir + "' cannot be used as the source directory of a forwarder.");
      }
    }
    
    //
    // Enable the ThrottlingManager (not 
    //

    if (!analyticsEngineOnly) {
      ThrottlingManager.enable();
    }
    
    QuasarTokenFilter tf = new QuasarTokenFilter(properties, keystore);
    
    GzipHandler gzip = new GzipHandler();
    EgressExecHandler egressExecHandler = new EgressExecHandler(keystore, properties, sdc, scc); 
    gzip.setHandler(egressExecHandler);
    gzip.setMinGzipSize(0);
    gzip.addIncludedMethods("POST");
    handlers.addHandler(gzip);
    setEgress(true);
    
    if (!analyticsEngineOnly) {
      gzip = new GzipHandler();
      StandaloneIngressHandler sih = new StandaloneIngressHandler(keystore, sdc, scc); 
      gzip.setHandler(sih);
      gzip.setMinGzipSize(0);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      gzip = new GzipHandler();
      gzip.setHandler(new EgressFindHandler(keystore, sdc));
      gzip.setMinGzipSize(0);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);

      if ("true".equals(properties.getProperty(Configuration.STANDALONE_SPLITS_ENABLE))) {
        gzip = new GzipHandler();
        gzip.setHandler(new StandaloneSplitsHandler(keystore, sdc));
        gzip.setMinGzipSize(0);
        gzip.addIncludedMethods("POST");
        handlers.addHandler(gzip);      
      }

      gzip = new GzipHandler();
      StandaloneDeleteHandler sdh = new StandaloneDeleteHandler(keystore, sdc, scc);
      sdh.setPlugin(sih.getPlugin());
      gzip.setHandler(sdh);
      gzip.setMinGzipSize(0);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);
      
      if (enablePlasma) {
        StandalonePlasmaHandler plasmaHandler = new StandalonePlasmaHandler(keystore, properties, sdc);
        scc.addPlasmaHandler(plasmaHandler);     
        handlers.addHandler(plasmaHandler);
      }
      
      if (enableStreamUpdate) {
        StandaloneStreamUpdateHandler streamUpdateHandler = new StandaloneStreamUpdateHandler(keystore, properties, sdc, scc);
        streamUpdateHandler.setPlugin(sih.getPlugin());
        handlers.addHandler(streamUpdateHandler);
      }
      
      gzip = new GzipHandler();
      gzip.setHandler(new EgressFetchHandler(keystore, properties, sdc, scc));
      gzip.setMinGzipSize(0);
      gzip.addIncludedMethods("POST");
      handlers.addHandler(gzip);
    }

    if (enableMobius) {
      EgressMobiusHandler mobiusHandler = new EgressMobiusHandler(scc, sdc, properties);
      handlers.addHandler(mobiusHandler);
    }

    if (enableREL) {
      EgressInteractiveHandler erel = new EgressInteractiveHandler(keystore, properties, sdc, scc);
      handlers.addHandler(erel);
    }
    
    server.setHandler(handlers);
        
    JettyUtil.setSendServerVersion(server, false);
    
    // Clear master key from memory
    keystore.forget();

    //
    // Register the plugins after we've cleared the master key
    //
    
    AbstractWarp10Plugin.registerPlugins();

    try {
      if (useHTTP) {
        System.out.println("#### standalone.endpoint " + InetAddress.getByName(host) + ":" + port);
      }
      if (useHTTPS) {
        System.out.println("#### standalone.ssl.endpoint " + InetAddress.getByName(httpsConnector.getHost()) + ":" + httpsConnector.getPort());
      }
      server.start();
    } catch (Exception e) {
      server.stop();
      throw new RuntimeException(e);
    }
    
    // Retrieve actual local port
    if (null != httpConnector) {
      port = httpConnector.getLocalPort();
    }

    WarpDist.setInitialized(true);
    
    try {
      while(true) {
        try {
          Thread.sleep(60000L);
        } catch (InterruptedException ie) {        
        }
      }      
    } catch (Throwable t) {
      System.err.println(t.getMessage());
      server.stop();
    }
  }
  
  public static boolean isStandaloneMode() {
    return standaloneMode;
  }
  
  public static int getPort() {
    return port;
  }
  
  public static String getHost() {
    return host;
  }
  
  @Override
  public void run() {
    try {
      if (null != db) {
        synchronized(db) {
          db.close();
          db = null;
        }
        System.out.println("LevelDB was safely closed.");
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
  
  /**
   * Extract Ingress related keys and populate the KeyStore with them.
   * 
   * @param props Properties from which to extract the key specs
   */
  public static void extractKeys(KeyStore keystore, Properties props) {
    String keyspec = props.getProperty(Configuration.LEVELDB_METADATA_AES);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.LEVELDB_METADATA_AES + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_LEVELDB_METADATA, key);
    }
    
    keyspec = props.getProperty(Configuration.LEVELDB_DATA_AES);
    
    if (null != keyspec) {
      byte[] key = keystore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.LEVELDB_DATA_AES + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_LEVELDB_DATA, key);
    }
            
    if (null != props.getProperty(Configuration.CONFIG_FETCH_PSK)) {
      keystore.setKey(KeyStore.SIPHASH_FETCH_PSK, keystore.decodeKey(props.getProperty(Configuration.CONFIG_FETCH_PSK)));
      Preconditions.checkArgument((16 == keystore.getKey(KeyStore.SIPHASH_FETCH_PSK).length), Configuration.CONFIG_FETCH_PSK + " MUST be 128 bits long.");            
    }

    if (null != props.getProperty(Configuration.RUNNER_PSK)) {
      byte[] key = keystore.decodeKey(props.getProperty(Configuration.RUNNER_PSK));
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.RUNNER_PSK + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_RUNNER_PSK, key);
    }
  }  

  public static WarpDB getDB() {
    return db;
  }
  
  public static Set<Path> getDatalogSrcDirs() {
    return datalogSrcDirs;
  }
}
