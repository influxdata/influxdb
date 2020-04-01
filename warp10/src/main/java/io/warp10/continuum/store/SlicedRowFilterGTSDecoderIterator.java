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

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SlicedRowFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

/**
 * GTSIterator using RowSliceFilters to select rows
 * 
 */
public class SlicedRowFilterGTSDecoderIterator extends GTSDecoderIterator implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SlicedRowFilterGTSDecoderIterator.class);
  
  private Table htable;
  private ResultScanner scanner;
  private Iterator<Result> iter;
  
  private final long now;
  private final long timespan;

  private final KeyStore keystore;

  private long cellCount = 0;
  private long resultCount = 0;
  
  private final byte[] hbaseAESKey;

  /**
   * Number of values left to read
   */
  private long nvalues = Long.MAX_VALUE;
  
  /**
   * Map of classId/labelsId to Metadata, used to retrieve name/labels.
   * We can't use byte[] as keys as they use object identity for equals and hashcode 
   */
  private Map<String, Metadata> metadatas = new HashMap<String, Metadata>();
  
  /**
   * Pending Result
   */
  private Result pendingresult = null;
  
  private static final byte[] ZERO_BYTES = Longs.toByteArray(0L);
  private static final byte[] ONES_BYTES = Longs.toByteArray(0xffffffffffffffffL);
  
  // FIXME(hbs): use a different prefix for archived data
  private static byte[] prefix = Constants.HBASE_RAW_DATA_KEY_PREFIX;

  private final boolean writeTimestamp;
  
  public SlicedRowFilterGTSDecoderIterator(long now, long timespan, List<Metadata> metadatas, Connection conn, TableName tableName, byte[] colfam, boolean writeTimestamp, KeyStore keystore, boolean useBlockCache) {
      
    this.keystore = keystore;
    this.now = now;
    this.timespan = timespan;
    // FIXME(hbs): different key for archival
    this.hbaseAESKey = keystore.getKey(KeyStore.AES_HBASE_DATA);
    this.writeTimestamp = writeTimestamp;
    
    //
    // Check that if 'timespan' is < 0 then 'now' is either Long.MAX_VALUE or congruent to 0 modulo DEFAULT_MODULUS
    //
    
    if (timespan < 0) {
      if (Long.MAX_VALUE != now && 0 != (now % Constants.DEFAULT_MODULUS)) {
        throw new RuntimeException("Incompatible 'timespan' (" + timespan + ") and 'now' (" + now + ")");
      }
    }
    
    //
    // Create a SlicedRowFilter for the prefix, class id, labels id and ts
    // We include the prefix so we exit the filter early when the last
    // matching row has been reached
    //
    
    // 128BITS
    
    int[] bounds = { 0, 24 };
    
    //
    // Create singleton for each classId/labelsId combo
    //
    // TODO(hbs): we should really create multiple scanner, one per class Id for example,
    // 
    
    List<Pair<byte[], byte[]>> ranges = new ArrayList<Pair<byte[], byte[]>>();
    
    for (Metadata metadata: metadatas) {
      byte[][] keys = getKeys(metadata, now, timespan);
      byte[] lower = keys[0];
      byte[] upper = keys[1];
      
      this.metadatas.put(new String(Arrays.copyOfRange(lower, prefix.length, prefix.length + 16), StandardCharsets.ISO_8859_1), metadata);
      
      Pair<byte[],byte[]> range = new Pair<byte[],byte[]>(lower, upper);
      
      ranges.add(range);
    }
                
    SlicedRowFilter filter = new SlicedRowFilter(bounds, ranges, timespan < 0 ? -timespan : Long.MAX_VALUE);

    //
    // Create scanner. The start key is the lower bound of the first range
    //
    
    Scan scan = new Scan();
    scan.addFamily(colfam); // (HBaseStore.GTS_COLFAM, Longs.toByteArray(Long.MAX_VALUE - modulus));
    scan.setStartRow(filter.getStartKey());
    byte[] filterStopKey = filter.getStopKey();
    // Add one byte at the end (we can do that because we know the slice is the whole key)
    byte[] stopRow = Arrays.copyOf(filterStopKey, filterStopKey.length + 1);
    scan.setStopRow(stopRow);
    scan.setFilter(filter);
    
    scan.setMaxResultSize(1000000L);
    scan.setBatch(50000);
    scan.setCaching(50000);
    
    scan.setCacheBlocks(useBlockCache);

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_FILTERED_SCANNERS, Sensision.EMPTY_LABELS, 1);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_FILTERED_SCANNERS_RANGES, Sensision.EMPTY_LABELS, ranges.size());

    try {
      this.htable = conn.getTable(tableName);
      this.scanner = this.htable.getScanner(scan);
      iter = scanner.iterator();          
    } catch (IOException ioe) {
      LOG.error("",ioe);
      this.iter = null;
    }
  }
  
  public static byte[][] getKeys(Metadata metadata, long now, long timespan) {
    // 128BITS
    byte[] lower = new byte[24 + prefix.length];
    byte[] upper = new byte[lower.length];
    
    System.arraycopy(prefix, 0, lower, 0, prefix.length);
    
    System.arraycopy(Longs.toByteArray(metadata.getClassId()), 0, lower, prefix.length, 8);
    System.arraycopy(Longs.toByteArray(metadata.getLabelsId()), 0, lower, prefix.length + 8, 8);

    System.arraycopy(lower, 0, upper, 0, prefix.length + 16);

    //
    // Set lower/upper timestamps
    //
    
    long modulus = Constants.DEFAULT_MODULUS;
  
    if (Long.MAX_VALUE == now) {
      System.arraycopy(ZERO_BYTES, 0, lower, prefix.length + 16, 8);
    } else {
      System.arraycopy(Longs.toByteArray(Long.MAX_VALUE - (now - (now % modulus))), 0, lower, prefix.length + 16, 8);        
    }
    
    if (timespan < 0) {
      System.arraycopy(ONES_BYTES, 0, upper, prefix.length + 16, 8);                
    } else {
      // Last timestamp does not need to be offset by modulus as it is the case when using a scanner, because
      // SlicedRowFilter upper bound is included, not excluded.
      System.arraycopy(Longs.toByteArray(Long.MAX_VALUE - ((now - timespan) - ((now - timespan) % modulus))), 0, upper, prefix.length + 16, 8);        
    }

    byte[][] keys = new byte[2][];
    keys[0] = lower;
    keys[1] = upper;
    return keys;
  }
  
  @Override
  public boolean hasNext() {
    if (null != this.pendingresult) {
      return true;
    }
    if (null == this.iter) {
      return false;
    }
    return this.iter.hasNext();
  }
  
  @Override
  public GTSDecoder next() {
    
    GTSEncoder encoder = new GTSEncoder(0L);

    while(encoder.size() < Constants.MAX_ENCODER_SIZE && (null != this.pendingresult || this.iter.hasNext()) && nvalues > 0) {
      
      //
      // Extract next result from scan iterator, unless there is a current pending Result
      //
    
      Result result = null;
      
      if (null != this.pendingresult) {
        result = this.pendingresult;
        this.pendingresult = null;
      } else {
        result = this.iter.next();
        resultCount++;
      }

      // Encode this in ISO_8859_1 so we are sure every possible byte sequence is valid
      // FIXME(hbs): instead of doing a Arrays.copyOfRange, use new String(byte[],offset,len,charset)
      String classlabelsid = new String(Arrays.copyOfRange(result.getRow(), Constants.HBASE_RAW_DATA_KEY_PREFIX.length, Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 16), StandardCharsets.ISO_8859_1);
    
      Metadata metadata = this.metadatas.get(classlabelsid);

      //
      // The current row is for a different GTS, return the current encoder and record the current result
      //
      
      if (encoder.size() > 0 && (encoder.getClassId() != metadata.getClassId() || encoder.getLabelsId() != metadata.getLabelsId())) {
        this.pendingresult = result;
        return encoder.getDecoder();
      }
      
      //
      // Set metadata
      //
      
      if (0 == encoder.size()) {
        encoder.setMetadata(metadata);
      }
      
      CellScanner cscanner = result.cellScanner();

      try {
        while(cscanner.advance()) {
          Cell cell = cscanner.current();
          cellCount++;
          
          //
          // Extract timestamp base from column qualifier
          // This is true even for packed readings, those have a base timestamp of 0L
          //

          long basets = Long.MAX_VALUE;
          
          if (1 == Constants.DEFAULT_MODULUS) {
            // 128BITS
            byte[] data = cell.getRowArray();
            int offset = cell.getRowOffset();
            offset += Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8; // Add 'prefix' + 'classId' + 'labelsId' to row key offset
            long delta = data[offset] & 0xFF;
            delta <<= 8; delta |= (data[offset + 1] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 2] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 3] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 4] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 5] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 6] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 7] & 0xFFL);
            basets -= delta;              
          } else {
            byte[] data = cell.getQualifierArray();
            int offset = cell.getQualifierOffset();
            long delta = data[offset] & 0xFFL;
            delta <<= 8; delta |= (data[offset + 1] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 2] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 3] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 4] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 5] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 6] & 0xFFL);
            delta <<= 8; delta |= (data[offset + 7] & 0xFFL);
            basets -= delta;                            
          }
          
          byte[] value = cell.getValueArray();
          int valueOffset = cell.getValueOffset();
          int valueLength = cell.getValueLength();
          
          ByteBuffer bb = ByteBuffer.wrap(value,valueOffset,valueLength).order(ByteOrder.BIG_ENDIAN);

          GTSDecoder decoder = new GTSDecoder(basets, hbaseAESKey, bb);
                    
          while(decoder.next() && nvalues > 0) {
            long timestamp = decoder.getTimestamp();
            if (timestamp <= now && (timespan < 0 || (timestamp > (now - timespan)))) {
              try {
                if (writeTimestamp) {
                  encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), cell.getTimestamp() * Constants.TIME_UNITS_PER_MS);
                } else {
                  encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
                }
                nvalues--;
              } catch (IOException ioe) {
                LOG.error("", ioe);
                // FIXME(hbs): LOG?
              }
            }
          }
        }          
      } catch(IOException ioe) {
        LOG.error("",ioe);
        // FIXME(hbs): LOG?
      }
    }
    
    return encoder.getDecoder();
  }
  
  @Override
  public void remove() {
  }
  
  @Override
  public void close() throws Exception {
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_RESULTS, Sensision.EMPTY_LABELS, resultCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_CELLS, Sensision.EMPTY_LABELS, cellCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_ITERATORS, Sensision.EMPTY_LABELS, 1);

    if (null != this.scanner) { try { this.scanner.close(); } catch (Exception e) { LOG.error("scanner", e); } }
    if (null != this.htable) { try { this.htable.close(); } catch (Exception e) { LOG.error("htable", e); } }
  }
}
