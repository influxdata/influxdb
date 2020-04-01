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

import io.warp10.WarpURLEncoder;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

public abstract class DirectoryPlugin {
  
  public static final int VERSION = 2;
  
  public static class GTS {

    // 128bits
    private final String id;
    private UUID uuid;
    private final String name;
    private final Map<String,String> labels;
    private final Map<String,String> attributes;
    private String representation = null;

    public GTS(String id, String name, Map<String,String> labels, Map<String,String> attributes) {
      this.id = id;
      this.name = name;
      this.labels = new HashMap<String,String>(labels);
      this.attributes = new HashMap<String,String>(attributes);      
    }
    
    @Deprecated
    public GTS(UUID uuid, String name, Map<String,String> labels, Map<String,String> attributes) {
      this.uuid = uuid;
      this.id = uuid.toString();
      this.name = name;
      this.labels = new HashMap<String,String>(labels);
      this.attributes = new HashMap<String,String>(attributes);
    }
    
    public String getId() {
      return this.id;
    }    
    
    @Deprecated
    public UUID getUuid() {
      return this.uuid;
    }    
    public String getName() {
      return name;
    }
    public Map<String, String> getLabels() {
      return Collections.unmodifiableMap(this.labels);
    }
    public Map<String, String> getAttributes() {
      return this.attributes;
    }
    public String toString() {
      if (null == this.representation) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("[");
        sb.append(this.id.toString());
        sb.append("] ");
        
        encode(sb, this.name);
        
        sb.append("{");
        
        boolean first = true;
        
        for (Entry<String,String> entry: this.labels.entrySet()) {
          if (!first) {
            sb.append(",");
          }
          encode(sb, entry.getKey());
          sb.append("=");
          encode(sb, entry.getValue());
          first = false;
        }
        
        sb.append("}");
        
        sb.append("{");
        
        first = true;
        
        for (Entry<String,String> entry: this.attributes.entrySet()) {
          if (!first) {
            sb.append(",");
          }
          encode(sb, entry.getKey());
          sb.append("=");
          encode(sb, entry.getValue());
          first = false;
        }
        
        sb.append("}");

        this.representation = sb.toString();
      }
      
      return this.representation;
    }
    
    private void encode(StringBuilder sb, String str) {
      try {
        String encoded = WarpURLEncoder.encode(str, StandardCharsets.UTF_8);
        sb.append(encoded);
      } catch (UnsupportedEncodingException uee) {        
      }
    }
  }
  
  public static abstract class GTSIterator implements Iterator<GTS>,AutoCloseable {}
  
  /**
   * Initialize the plugin. This method is called immediately after a plugin has been instantiated.
   * 
   * @param properties Properties from the Warp configuration file
   */
  public abstract void init(Properties properties);
  
  /**
   * Stores a GTS.
   * 
   * GTS to store might already exist in the storage layer. It may be pushed because the attributes have changed.
   * 
   * @param source Indicates the source of the data to be stored. Will be null when initializing Directory.
   * @param gts The GTS to store.
   * @return true if the storing succeeded, false otherwise
   */
  public abstract boolean store(String source, GTS gts);
  
  /**
   * Deletes a GTS from storage.
   * Note that the key for the GTS is the combination name + labels, the attributes are not part of the key.
   * 
   * @param gts GTS to delete
   * @return
   */
  public abstract boolean delete(GTS gts);
  
  /**
   * Identify matching GTS.
   * 
   * @param shard Shard ID for which the request is done
   * @param classSelector Regular expression for selecting the class name.
   * @param labelsSelectors Regular expressions for selecting the labels names.
   * @return An iterator on the matching GTS.
   */
  public abstract GTSIterator find(int shard, String classSelector, Map<String,String> labelsSelectors);
  
  /**
   * Check if a given GTS is known.
   * This is used to avoid storing unknown GTS in HBase simply because they were
   * part of a /meta request.
   * 
   * Note that the default implementation returns false, this means that attribute updates
   * will NOT be persisted in HBase. This is annoying but trust us, it's less annoying than
   * having a simple /meta request create millions of entries in HBase...
   * 
   * @param gts The GTS to check.
   * @return true if the GTS is known.
   */
  public boolean known (GTS gts) {
    return false;
  }
}
