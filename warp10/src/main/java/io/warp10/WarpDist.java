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

package io.warp10;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.KafkaWebCallBroker;
import io.warp10.continuum.KafkaWebCallService;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.egress.Egress;
import io.warp10.continuum.ingress.Ingress;
import io.warp10.continuum.plasma.PlasmaBackEnd;
import io.warp10.continuum.plasma.PlasmaFrontEnd;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.Directory;
import io.warp10.continuum.store.Store;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OSSKeyStore;
import io.warp10.crypto.UnsecureKeyStore;
import io.warp10.script.ScriptRunner;
import io.warp10.script.WarpScriptLib;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;

/**
 * Main class for launching components of the Continuum geo time series storage system
 */
public class WarpDist {
      
  //
  // We create an instance of SensisionConstants so Sensision gets its instance name set
  // before any metric or event is pushed. Otherwise registration won't take into account
  // the instance name.
  //
   
  static {
    SensisionConstants constant = new SensisionConstants();
  }

  private static KeyStore keystore = null;
  private static Properties properties = null;
  
  public static final String[] REQUIRED_PROPERTIES = {
    Configuration.WARP_COMPONENTS,
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

  private static boolean initialized = false;
  
  /**
   * Do we run an 'egress' service. Used in WarpScript MacroRepository to bail out if not
   */
  private static boolean hasEgress = false;
  
  public static void setProperties(Properties props) {
    if (null != properties) {
      throw new RuntimeException("Properties already set.");
    }
    
    properties = props;
  }
 
  public static void setProperties(String[] files) throws IOException {
    WarpConfig.setProperties(files);
    
    properties = WarpConfig.getProperties();    
  }
  
  public static void setProperties(String file) throws IOException {
    WarpConfig.setProperties(file);
    
    properties = WarpConfig.getProperties();
  }
  
  public static void setKeyStore(KeyStore ks) {
    if (null != keystore) {
      throw new RuntimeException("KeyStore already set.");
    }
    keystore = ks.clone();
  }
    
  public static void main(String[] args) throws Exception {

    System.out.println();
    System.out.println(Constants.WARP10_BANNER);
    System.out.println("  Revision " + Revision.REVISION);
    System.out.println();
    
    System.setProperty("java.awt.headless", "true");
    
    if (args.length > 0) {
      setProperties(args);
    } else if (null != System.getenv(WarpConfig.WARP10_CONFIG_ENV)) {
      setProperties(System.getenv(WarpConfig.WARP10_CONFIG_ENV));
    } else if (null != System.getProperty(WarpConfig.WARP10_CONFIG)) {
      setProperties(System.getProperty(WarpConfig.WARP10_CONFIG));
    }
        
    //
    // Extract components to spawn
    //
    
    String[] components = properties.getProperty(Configuration.WARP_COMPONENTS).split(",");
    
    Set<String> subprocesses = new HashSet<String>();
    
    for (String component: components) {
      component = component.trim();
           
      subprocesses.add(component);
    }

    if (properties.containsKey(Configuration.OSS_MASTER_KEY)) {
      keystore = new OSSKeyStore(properties.getProperty(Configuration.OSS_MASTER_KEY));
    } else {
      keystore = new UnsecureKeyStore();
    }

    //
    // Set SIPHASH keys for class/labels/index
    //
    
    for (String property: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(properties.getProperty(property), "Property '" + property + "' MUST be set.");
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
    
    if (null != properties.getProperty(Configuration.CONFIG_FETCH_PSK)) {
      keystore.setKey(KeyStore.SIPHASH_FETCH_PSK, keystore.decodeKey(properties.getProperty(Configuration.CONFIG_FETCH_PSK)));
      Preconditions.checkArgument((16 == keystore.getKey(KeyStore.SIPHASH_FETCH_PSK).length), Configuration.CONFIG_FETCH_PSK + " MUST be 128 bits long.");            
    }
    
    if (null != properties.getProperty(Configuration.RUNNER_PSK)) {
      byte[] key = keystore.decodeKey(properties.getProperty(Configuration.RUNNER_PSK));
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.RUNNER_PSK + " MUST be 128, 192 or 256 bits long.");
      keystore.setKey(KeyStore.AES_RUNNER_PSK, key);
    }

    WarpScriptLib.registerExtensions();
    
    KafkaWebCallService.initKeys(keystore, properties);
        
    //
    // Initialize ThrottlingManager
    //
    
    ThrottlingManager.init();
    
    if (subprocesses.contains("egress") && subprocesses.contains("fetcher")) {
      throw new RuntimeException("'fetcher' and 'egress' cannot be specified together as components to run.");
    }
    
    for (String subprocess: subprocesses) {
      if ("ingress".equals(subprocess)) {
        Ingress ingress = new Ingress(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "ingress");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("egress".equals(subprocess)) {
        Egress egress = new Egress(getKeyStore(), getProperties(), false);
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "egress");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
        hasEgress = true;
      } else if ("fetcher".equals(subprocess)) {
        Egress egress = new Egress(getKeyStore(), getProperties(), true);
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "fetcher");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);        
      } else if ("store".equals(subprocess)) {
        int nthreads = Integer.valueOf(properties.getProperty(Configuration.STORE_NTHREADS));
        for (int i = 0; i < nthreads; i++) {
          //Store store = new Store(getKeyStore(), getProperties(), null);
          Store store = new Store(getKeyStore(), getProperties(), null);
        }
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "store");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("directory".equals(subprocess)) {
        Directory directory = new Directory(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "directory");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      //} else if ("index".equals(subprocess)) {
      //  Index index = new Index(getKeyStore(), getProperties());
      } else if ("plasmaFE".equalsIgnoreCase(subprocess)) {
        PlasmaFrontEnd plasmaFE = new PlasmaFrontEnd(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "plasmafe");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("plasmaBE".equalsIgnoreCase(subprocess)) {
        PlasmaBackEnd plasmaBE = new PlasmaBackEnd(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "plasmabe");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("webcall".equals(subprocess)) {
        KafkaWebCallBroker webcall = new KafkaWebCallBroker(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "webcall");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else if ("runner".equals(subprocess)) {
        ScriptRunner runner = new ScriptRunner(getKeyStore(), getProperties());
        Map<String,String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_COMPONENT, "runner");
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_REVISION, labels, Revision.REVISION);
      } else {
        System.err.println("Unknown component '" + subprocess + "', skipping.");
        continue;
      }
    }
    
    // Clear master key from memory
    keystore.forget();
    
    //
    // Register the plugins after we've cleared the master key
    //
    
    AbstractWarp10Plugin.registerPlugins();
    
    setInitialized(true);
    
    //
    // We're done, let's sleep endlessly
    //
    
    try {
      while (true) {
        try {
          Thread.sleep(60000L);
        } catch (InterruptedException ie) {        
        }
      }      
    } catch (Throwable t) {
      System.err.println(t.getMessage());
    }
  }

  public static KeyStore getKeyStore() {
    if (null == keystore) {
      return null;
    }
    return keystore.clone();
  }
  
  public static Properties getProperties() {
    if (null == properties) {
      return null;
    }
    return (Properties) properties.clone();
  }
  
  public static synchronized void setInitialized(boolean initialized) {
    WarpDist.initialized = initialized;
  }
  
  public static synchronized boolean isInitialized() {
    return initialized;
  }
  
  public static boolean isEgress() {
    return hasEgress;
  }
  
  public static void setEgress(boolean egress) {
    hasEgress = egress;
  }
}
