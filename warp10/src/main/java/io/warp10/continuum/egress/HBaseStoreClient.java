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

package io.warp10.continuum.egress;

import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.HBaseRegionKeys;
import io.warp10.continuum.store.MultiScanGTSDecoderIterator;
import io.warp10.continuum.store.OptimizedSlicedRowFilterGTSDecoderIterator;
import io.warp10.continuum.store.ParallelGTSDecoderIteratorWrapper;
import io.warp10.continuum.store.SlicedRowFilterGTSDecoderIterator;
import io.warp10.continuum.store.Store;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandalonePlasmaHandlerInterface;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class HBaseStoreClient implements StoreClient {
  
  /**
   * Connection to HBase
   */
  private final Connection conn;

  private final TableName tableName;
  
  private final KeyStore keystore;
  private final byte[] hbaseKey;
  
  private final Properties properties;
  private final byte[] colfam;
  
  private final boolean useHBaseFilter;
  private final int hbaseFilterThreshold;
  
  private static final int HBASE_FILTER_THRESHOLD_DEFAULT = 16;
  
  private final long blockcacheThreshold;
  
  public HBaseStoreClient(KeyStore keystore, Properties properties) throws IOException {
    
    this.keystore = keystore;
    this.hbaseKey = keystore.getKey(KeyStore.AES_HBASE_DATA);
    this.properties = properties;
    
    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_DATA_BLOCKCACHE_GTS_THRESHOLD)) {
      this.blockcacheThreshold = Long.parseLong(properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_DATA_BLOCKCACHE_GTS_THRESHOLD));
    } else {
      this.blockcacheThreshold = 0L;
    }
    
    this.useHBaseFilter = "true".equals(properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_FILTER));
    
    this.hbaseFilterThreshold = Integer.parseInt(properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_FILTER_THRESHOLD, Integer.toString(HBASE_FILTER_THRESHOLD_DEFAULT)));

    Configuration conf = new Configuration();
    conf.set(HConstants.ZOOKEEPER_QUORUM, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_DATA_ZKCONNECT));
    if (!"".equals(properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_DATA_ZNODE))) {
      conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_DATA_ZNODE));
    }

    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)) {
      conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
    }

    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_IPC_POOL_SIZE)) {
      conf.set(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_IPC_POOL_SIZE));
    }
    
    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD)) {      
      conf.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
    }
    
    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_RPC_TIMEOUT)) {
      conf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_RPC_TIMEOUT));
    }
    
    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_MAX_PERSERVER_TASKS)) {
      conf.set(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_MAX_PERSERVER_TASKS));
    }

    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_MAX_PERREGION_TASKS)) {
      conf.set(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_MAX_PERREGION_TASKS));
    }
    
    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_MAX_TOTAL_TASKS)) {
      conf.set(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS, properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_CLIENT_MAX_TOTAL_TASKS));
    }

    //
    // Handle additional HBase configurations
    //
    
    if (properties.containsKey(io.warp10.continuum.Configuration.EGRESS_HBASE_CONFIG)) {
      String[] keys = properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_CONFIG).split(",");
      for (String key: keys) {
        if (!properties.containsKey("egress." + key.trim())) {
          throw new RuntimeException("Missing declared property 'egress." + key.trim() + "'.");
        }
        conf.set(key, properties.getProperty("egress." + key.trim()));
      }
    }

    this.conn = ConnectionFactory.createConnection(conf);
    this.tableName = TableName.valueOf(properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_DATA_TABLE));
    
    //
    // Initialize HBaseRegionKeys
    //
    
    HBaseRegionKeys.getRegionKeys(conn, tableName);
    
    this.colfam = properties.getProperty(io.warp10.continuum.Configuration.EGRESS_HBASE_DATA_COLFAM).getBytes(StandardCharsets.UTF_8);
  }
  
  @Override
  public GTSDecoderIterator fetch(final ReadToken token, final List<Metadata> metadatas, final long now, final long then, long count, long skip, double sample, final boolean writeTimestamp, int preBoundary, int postBoundary) throws IOException {

    if (preBoundary < 0) {
      preBoundary = 0;
    }

    if (postBoundary < 0) {
      postBoundary = 0;
    }
     
    if (sample <= 0.0D || sample > 1.0D) {
      sample = 1.0D;
    }
    
    if (skip < 0) {
      skip = 0;
    }
    
    if (count < -1L) {
      count = -1L;
    }
    
    //
    // If we are fetching up to Long.MIN_VALUE, then don't fetch a pre boundary
    //
    if (Long.MIN_VALUE == then) {
      preBoundary = 0;
    }

    //
    // Determine the execution plan given the metadatas of the GTS we will be retrieving.
    // Some hints to choose the best plan:
    // # of different classes
    // # of different instances among each class
    // # among those instances, common labels
    //
    // TODO(hbs): we might have to gather statistics to better determine the exec plan
    

    //
    // Sort metadatas so as to optimize the range scans
    //
    
    Collections.sort(metadatas, MetadataIdComparator.COMPARATOR);

    // TODO(hbs): convert metadatas into a list of Get/Scan ops with potential filters, those ops will be
    // applied in order and the results returned by calls to 'next'.

    //
    // DON'T use SlicedRowFilterGTSDecoterIterator when using a value count based approach with a value of 'now'
    // which is not congruent to 0 modulo DEFAULT_MODULUS, because we may then have datapoints after 'now' and would then
    // need to do a full scan of every classId/labelsId in metadatas as the SlicedRowFilter does not interpret the read data
    // and is thus unable to read the timestamp
    // Don't use the filter when skip is > 0 or sample < 1.0D
    //
    // Only use SlicedRowFilter when not having a value count approach or when 'now' is congruent to 0 modulo DEFAULT_MODULUS
    // or equal to Long.MAX_VALUE (EPOCHEND)
    //
    
    boolean optimized = false;
    
    if (useHBaseFilter && metadatas.size() > this.hbaseFilterThreshold) {
      if (count > 0 && Long.MIN_VALUE == then) {
        // If we are fetching per count only (i.e. time range ends at Long.MIN_VALUE)
        optimized = true;
      } else if (-1 == count) {
        // When not fetching by count but by time range, use the filter
        optimized = true;
      }
    }

    // If sampling or skipping, don't use the filter
    if (0 != skip || 1.0D != sample) {
      optimized = false;
    }
    
    // When fetching boundaries, the optimized scanners cannot be used
    if (preBoundary > 0 || postBoundary > 0) {
      optimized = false;
    }
    
    if (metadatas.size() < ParallelGTSDecoderIteratorWrapper.getMinGTSPerScanner() || !ParallelGTSDecoderIteratorWrapper.useParallelScanners()) {
      if (optimized) {
        //return new SlicedRowFilterGTSDecoderIterator(now, timespan, metadatas, this.conn, this.tableName, this.colfam, this.keystore, metadatas.size() <= blockcacheThreshold);
        long timespan = count > 0 ? -count : (now - then + 1);
        return new OptimizedSlicedRowFilterGTSDecoderIterator(now, timespan, metadatas, this.conn, this.tableName, this.colfam, writeTimestamp, this.keystore, metadatas.size() <= blockcacheThreshold);
      } else {
        return new MultiScanGTSDecoderIterator(token, now, then, count, skip, sample, metadatas, this.conn, this.tableName, colfam, writeTimestamp, this.keystore, metadatas.size() < blockcacheThreshold, preBoundary, postBoundary);      
      }      
    } else {
      return new ParallelGTSDecoderIteratorWrapper(optimized, token, now, then, count, skip, sample, metadatas, keystore, this.conn, this.tableName, this.colfam, writeTimestamp, metadatas.size() < blockcacheThreshold, preBoundary, postBoundary);
    }
  }


  @Override
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface handler) {
    throw new RuntimeException("Not Implemented.");
  }
  
  @Override
  public void store(GTSEncoder encoder) throws IOException {
    throw new RuntimeException("Not Implemented.");   
  }

  @Override
  public void archive(int chunk, GTSEncoder encoder) throws IOException {
    throw new RuntimeException("Not Implemented.");   
  }
  
  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    throw new RuntimeException("Not Implemented.");   
  }
  
  /**
   * Return a RegionLocator instance suitable for inspecting the underlying table regions.
   * Be aware that the returned RegionLocator is not thread-safe and should be unmanaged using close().
   * 
   * @return
   * @throws IOException
   */
  public RegionLocator getRegionLocator() throws IOException {
    return this.conn.getRegionLocator(this.tableName);
  }
}
