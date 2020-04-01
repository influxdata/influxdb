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
package io.warp10.warp.sdk;

import io.warp10.WarpClassLoader;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.WarpScriptLib;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic interface for Warp 10 plugins
 */
public abstract class AbstractWarp10Plugin {
  
  private static Logger LOG = LoggerFactory.getLogger(AbstractWarp10Plugin.class);
  
  private static final AtomicBoolean registered = new AtomicBoolean(false);
  
  private static final List<String> plugins = new ArrayList<String>();
  
  /**
   * Method called to initialize the plugin
   * 
   * @param properties Warp 10 configuration properties
   */
  public abstract void init(Properties properties);  
  
  public static final void registerPlugins() { 
    
    if (registered.get()) {
      return;
    }
    
    registered.set(true);
    
    Properties props = WarpConfig.getProperties();
    
    Set<String> plugs = new HashSet<String>();
    
    if (null != props && props.containsKey(Configuration.WARP10_PLUGINS)) {
      String[] plugins = props.getProperty(Configuration.WARP10_PLUGINS).split(",");
           
      for (String plugin: plugins) {
        plugs.add(plugin.trim());
      }
    }

    for (String key: props.stringPropertyNames()) {
      if (key.startsWith(Configuration.WARP10_PLUGIN_PREFIX)) {
        plugs.add(props.getProperty(key).trim());
      }
    }
    
    if (plugs.isEmpty()) {
      return;
    }
    
    boolean failedPlugin = false;
      
    List<String> failed = new ArrayList<String>();
    
    //
    // Sort the plugins by prefix
    //
    
    List<String> sorted = new ArrayList<String>(plugs);
    sorted.sort(null);
    
    //
    // Determine the possible jar from which we were loaded
    //
      
    String wsljar = null;
    URL wslurl = AbstractWarp10Plugin.class.getResource('/' + AbstractWarp10Plugin.class.getCanonicalName().replace('.',  '/') + ".class");
    if (null != wslurl && "jar".equals(wslurl.getProtocol())) {
      wsljar = wslurl.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");
    }
      
    for (String plugin: sorted) {
      try {        
        // If the plugin name contains '#', remove everything up to the last '#', this was used as a sorting prefix
        
        if (plugin.contains("#")) {
          plugin = plugin.replaceAll("^.*#", "");
        }

        //
        // Locate the class using the current class loader
        //
        
        URL url = AbstractWarp10Plugin.class.getResource('/' + plugin.replace('.', '/') + ".class");
        
        if (null == url) {
          LOG.error("Unable to load plugin '" + plugin + "', make sure it is in the class path.");
          failedPlugin = true;
          failed.add(plugin);
          continue;
        }
        
        Class cls = null;

        //
        // If the class was located in a jar, load it using a specific class loader
        // so we can have fat jars with specific deps, unless the jar is the same as
        // the one from which AbstractWarp10Plugin was loaded, in which case we use the same
        // class loader.
        //
        
        if ("jar".equals(url.getProtocol())) {
          String jarfile = url.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");

          ClassLoader cl = AbstractWarp10Plugin.class.getClassLoader();
          
          // If the jar differs from that from which AbstractWarp10Plugin was loaded, create a dedicated class loader
          if (!jarfile.equals(wsljar)) {
            cl = new WarpClassLoader(jarfile, AbstractWarp10Plugin.class.getClassLoader());
          }
        
          cls = Class.forName(plugin, true, cl);
        } else {
          cls = Class.forName(plugin, true, AbstractWarp10Plugin.class.getClassLoader());
        }

        AbstractWarp10Plugin wse = (AbstractWarp10Plugin) cls.newInstance();          
        wse.init(WarpConfig.getProperties());
        LOG.info("LOADED plugin '" + plugin  + "'");
        plugins.add(plugin);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
      
    if (failedPlugin) {      
      throw new RuntimeException("Some WarpScript plugins could not be loaded, aborting. " + failed.toString());
    }
  }
  
  /**
   * Retrieve the exposed StoreClient. We use introspection to avoid having to
   * include EgressExecHandler in the WarpScript lib jar.
   * 
   * @return The exposed StoreClient or null
   */
  public static final StoreClient getExposedStoreClient() {
    try {
      Class egress = Class.forName("io.warp10.continuum.egress.EgressExecHandler");
      Method m = egress.getDeclaredMethod("getExposedStoreClient", new Class[0]);
      return (StoreClient) m.invoke(null, new Object[0]);
    } catch (Throwable t) {
      LOG.warn("Unable to retrieve StoreClient", t);
    }
    return null;
  }
  
  /**
   * Retrieve the exposed DirectoryClient. We use introspection to avoid having to
   * include EgressExecHandler in the WarpScript lib jar.
   * 
   * @return The exposed DirectoryClient or null
   */
  public static final DirectoryClient getExposedDirectoryClient() {
    try {
      Class egress = Class.forName("io.warp10.continuum.egress.EgressExecHandler");
      Method m = egress.getDeclaredMethod("getExposedDirectoryClient", new Class[0]);
      return (DirectoryClient) m.invoke(null, new Object[0]);
    } catch (Throwable t) {
      LOG.warn("Unable to retrieve DirectoryClient", t);
    }
    return null;
  }
  
  public static final List<String> plugins() {
    return new ArrayList<String>(plugins);
  }
}
