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

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.primitives.Longs;

/**
 * This class periodically updates the region keys for a table.
 * Those keys are used to split the GTS in groups when fetching data.
 *
 */
public class HBaseRegionKeys extends Thread {
  
  private static final byte[] ZERO_BYTES = Longs.toByteArray(0L);
  private static final byte[] ONES_BYTES = Longs.toByteArray(0xffffffffffffffffL);

  private static final HBaseRegionKeys singleton;
  
  /**
   * Update region keys every 60s
   */
  private static long PERIOD = 60000L;
  
  /**
   * Map of table name to key boundaries
   */
  private static Map<TableName, List<byte[]>> regionKeys = new HashMap<TableName, List<byte[]>>();
  
  private static Map<TableName, Connection> connections = new ConcurrentHashMap<TableName, Connection>();
  
  static {
    String updatePeriodProp = WarpConfig.getProperty(Configuration.WARP_HBASE_REGIONKEYS_UPDATEPERIOD);
    if (null != updatePeriodProp) {
      PERIOD = Long.parseLong(updatePeriodProp);
    }
    
    singleton = new HBaseRegionKeys();
    singleton.start();
  }
  
  private HBaseRegionKeys() {
  }
  
  @Override
  public void run() {
    
    long last = 0L;
    
    int count = 0;
    
    while(true) {
      if (connections.size() == count) {
        LockSupport.parkNanos(100000000L);
      }
      
      if (System.currentTimeMillis() - last > PERIOD || count != connections.size()) {
        List<TableName> tables = new ArrayList<TableName>();
        
        tables.addAll(connections.keySet());
        
        for (TableName table: tables) {
          Connection conn = connections.get(table);
                    
          try {
            List<byte[]> keys = genRegionKeys(conn, table);
            regionKeys.put(table, keys);
            Map<String,String> labels = new HashMap<String,String>();
            labels.put(SensisionConstants.SENSISION_LABEL_TABLE, table.getNameAsString());
            Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_HBASE_KNOWNREGIONS, labels, keys.size() >>> 1);
          } catch (IOException ioe) {            
          }
        }
        
        count = tables.size();
        last = System.currentTimeMillis();
      }
    }
  }
  
  public static List<byte[]> getRegionKeys(Connection conn, TableName table) {
    // Update the connection
    connections.put(table, conn);
    
    List<byte[]> keys = regionKeys.get(table);
    
    if (null == keys) {
      keys = new ArrayList<byte[]>();
    }
    
    return keys;
  }
  
  private static List<byte[]> genRegionKeys(Connection conn, TableName table) throws IOException {    
    RegionLocator locator = conn.getRegionLocator(table);

    Pair<byte[][],byte[][]> regionBounds = locator.getStartEndKeys();
      
    //
    // Load and sort all region bounds
    //
      
    List<byte[]> regionKeys = new ArrayList<byte[]>();
    
    regionKeys.addAll(Arrays.asList(regionBounds.getFirst()));      
    regionKeys.addAll(Arrays.asList(regionBounds.getSecond()));
    regionKeys.sort(Bytes.BYTES_COMPARATOR);

    //
    // Start key of the first region and end key of the last region are 'nulls', we need
    // to replace those with something else
    //
      
    // INFO(hbs): this implies that prefix is between 0x00 and 0xFF
    regionKeys.remove(0);
    regionKeys.set(0, ZERO_BYTES);
    regionKeys.add(ONES_BYTES);
    
    return regionKeys;
  }
}
