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

package io.warp10.standalone;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;

public class StandaloneShardedDirectoryClientWrapper extends StandaloneDirectoryClient {
  private final long[] modulus;
  private final long[] remainder;
  
  /**
   * Number of bits to shift the shard key right.
   * If this is 24, then only the class Id will be considered
   */
  private final long shardkeyshift;

  private final ShardFilter filter;
  
  private final StandaloneDirectoryClient client;

  private final long[] classKeyLongs;
  private final long[] labelsKeyLongs;
    
  public StandaloneShardedDirectoryClientWrapper(KeyStore keystore, StandaloneDirectoryClient client) {
    super(null, keystore);
    
    byte[] classKey = keystore.getKey(KeyStore.SIPHASH_CLASS);
    this.classKeyLongs = SipHashInline.getKey(classKey);
    
    byte[] labelsKey = keystore.getKey(KeyStore.SIPHASH_LABELS);
    this.labelsKeyLongs = SipHashInline.getKey(labelsKey);

    this.client = client;
    
    // Check if we have a limit on the shards we should store
    if (null != WarpConfig.getProperty(Configuration.DATALOG_SHARDS)) {
      this.shardkeyshift = Long.parseLong(WarpConfig.getProperty(Configuration.DATALOG_SHARDKEY_SHIFT, "0"));
      
      if (this.shardkeyshift >= 48 || this.shardkeyshift < 0) {
        throw new RuntimeException("Invalid shard key shifting.");
      }
      
      String[] shards = WarpConfig.getProperty(Configuration.DATALOG_SHARDS).split(",");
      
      this.modulus = new long[shards.length];
      this.remainder = new long[shards.length];
        
      int idx = 0;
        
      for (String shard: shards) {
        String[] tokens = shard.trim().split(":");
        if (2 != tokens.length) {
          throw new RuntimeException("Invalid shard specification " + shard);
        }
        this.modulus[idx] = Long.parseLong(tokens[0]);
        this.remainder[idx] = Long.parseLong(tokens[1]);
          
        if (this.modulus[idx] < 1 || this.remainder[idx] >= this.modulus[idx] || this.remainder[idx] < 0) {
          throw new RuntimeException("Invalid shard specification " + shard);
        }
        
        idx++;
      }
      
      this.filter = new ShardFilter() {        
        @Override
        public boolean exclude(long classId, long labelsId) {
          if (null == modulus) {
            return false;
          }
          long key = ((classId & 0xFFFFFF000000L) | (labelsId & 0xFFFFFFL)) >>> shardkeyshift;
          boolean skip = true;
          for(int i = 0; i < modulus.length; i++) {
            if (key % modulus[i] == remainder[i]) {
              skip = false;
              break;
            }
          }
          return skip;
        }
      };
    } else {
      this.modulus = null;
      this.remainder = null;
      this.shardkeyshift = 0;
      this.filter = null;
    }
  }
  
  @Override
  public List<Metadata> find(DirectoryRequest request) {
    List<Metadata> meta = this.client.find(request);

    if (null == this.filter) {
      return meta;
    }
    
    //
    // Now only retain the Metadata which are in the shards we handle
    //
    
    List<Metadata> dropped = new ArrayList<Metadata>();
    
    // We consider that the classId/labelsId are set
    for (Metadata metadata: meta) {
      if (this.filter.exclude(metadata.getClassId(), metadata.getLabelsId())) {
        dropped.add(metadata);
      }
    }
    
    meta.removeAll(dropped);
        
    return meta;
  }
  
  @Override
  public MetadataIterator iterator(DirectoryRequest request) throws IOException {
    List<Metadata> metadatas = find(request);

    final Iterator<Metadata> iter = metadatas.iterator();

    return new MetadataIterator() {
      @Override
      public void close() throws Exception {}
      
      @Override
      public boolean hasNext() { return iter.hasNext(); }
      
      @Override
      public Metadata next() { return iter.next(); }
    };
  }
  
  @Override
  public Map<String, Object> stats(DirectoryRequest dr) throws IOException {
    return this.client.stats(dr, filter);
  }
    
  @Override
  public Metadata getMetadataById(BigInteger id) {
    Metadata metadata = this.client.getMetadataById(id);
    
    if (null != this.filter && this.filter.exclude(metadata.getClassId(), metadata.getLabelsId())) {
      return null;
    }

    return metadata;
  }
  
  @Override
  public void register(Metadata metadata) throws IOException {
    if (null == metadata) {
      this.client.register(metadata);
      return;
    }
    
    if (null != this.filter) {
      long classId = GTSHelper.classId(this.classKeyLongs, metadata.getName());
      long labelsId = GTSHelper.labelsId(this.labelsKeyLongs, metadata.getLabels());

      if (this.filter.exclude(classId, labelsId)) {
        return;
      }
    }

    this.client.register(metadata);
  }
  
  @Override
  public void setActivityWindow(long activityWindow) {
    this.client.setActivityWindow(activityWindow);
  }
  
  @Override
  public synchronized void unregister(Metadata metadata) {
    this.client.unregister(metadata);
  }    
}
