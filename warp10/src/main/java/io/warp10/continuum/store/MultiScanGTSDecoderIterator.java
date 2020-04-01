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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import com.google.common.primitives.Longs;

import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.sensision.Sensision;

public class MultiScanGTSDecoderIterator extends GTSDecoderIterator {

  private static Random prng = new Random();
  
  int idx = 0;

  long nvalues = Long.MAX_VALUE;
  
  long toskip = 0;
  
  private final Table htable;

  private ResultScanner scanner = null;
  
  private Iterator<Result> scaniter = null;
  
  long resultCount = 0;
  
  long cellCount = 0;      

  private final List<Metadata> metadatas;
  
  private final long now;
  private final long then;
  
  private final boolean useBlockcache;
  
  private final ReadToken token;
  
  private final byte[] colfam;
  
  private final boolean writeTimestamp;
  
  private byte[] hbaseKey;
  
  private final int preBoundary;
  private final int postBoundary;
  
  private int preBoundaryCount = 0;
  private int postBoundaryCount = 0;
  
  // Flag indicating we scan the pre boundary.
  private boolean preBoundaryScan = false;
  // Flag indicating we scan the post boundary.
  private boolean postBoundaryScan = false;
      
  private long count = -1;
  private long skip = 0;
  private double sample = 1.0D;
  
  public MultiScanGTSDecoderIterator(ReadToken token, long now, long then, long count, long skip, double sample, List<Metadata> metadatas, Connection conn, TableName tableName, byte[] colfam, boolean writeTimestamp, KeyStore keystore, boolean useBlockcache, int preBoundary, int postBoundary) throws IOException {
    this.htable = conn.getTable(tableName);
    this.metadatas = metadatas;
    this.now = now;
    this.then = then;
    this.count = count;
    this.skip = skip;
    this.sample = sample;
    this.useBlockcache = useBlockcache;
    this.token = token;
    this.colfam = colfam;
    this.writeTimestamp = writeTimestamp;
    this.hbaseKey = keystore.getKey(KeyStore.AES_HBASE_DATA);

    // If we are fetching up to Long.MIN_VALUE, then don't fetch a pre boundary
    this.preBoundary = Long.MIN_VALUE == then ? 0 : preBoundary;
    // If now is Long.MAX_VALUE then there is no possibility to have a post boundary
    this.postBoundary = postBoundary >= 0 && now < Long.MAX_VALUE ? postBoundary : 0;
    
    this.postBoundaryScan = 0 != this.postBoundary;
    this.postBoundaryCount = this.postBoundary;
  }
      
  /**
   * Check whether or not there are more GeoTimeSerie instances.
   */
  @Override
  public boolean hasNext() {
    
    //
    // If scanner has not been nullified, it means there are more results, except if nvalues is 0
    // in which case it means we've read enough data
    //
    
    boolean scanHasMore = ((null == scaniter) || nvalues <= 0) ? false : scaniter.hasNext();
    
    // Adjust scanHasMore for pre/post boundaries
    if (postBoundaryScan && 0 == postBoundaryCount) {
      scanHasMore = false;
    } else if (preBoundaryScan && 0 == preBoundaryCount) {
      scanHasMore = false;
    }
    
    if (scanHasMore) {
      return true;
    }
    
    //
    // If scanner is not null let's close it as it does not have any more results
    //
    
    if (null != scaniter) {
      this.scanner.close();
      this.scanner = null;
      this.scaniter = null;
      
      if (postBoundaryScan) {
        // We were scanning the post boundary, the next scan will be for the core zone
        postBoundaryScan = false;
      } else if (preBoundaryScan) {
        // We were scanning the pre boundary, the next scan will be for the post boundary of the next GTS
        preBoundaryScan = false;
        postBoundaryScan = 0 != this.postBoundary;
        postBoundaryCount = this.postBoundary;
        idx++;
      } else {
        // We were scanning the core zone, now we will scan the pre boundary
        preBoundaryScan = 0 != this.preBoundary;
        preBoundaryCount = this.preBoundary;
        
        // If there is no pre boundary to scan, advance the metadata index
        if (!preBoundaryScan) {
          postBoundaryScan = 0 != this.postBoundary;
          postBoundaryCount = this.postBoundary;
          idx++;
        }
      }
    } else {      
      // Reset the type of scan we do
      postBoundaryScan = 0 != this.postBoundary;
      preBoundaryScan = false;
      postBoundaryCount = this.postBoundary;
    }
            
    //
    // If there are no more metadatas then there won't be any more data
    //

    if (idx >= metadatas.size()) {
      return false;
    }
    
    //
    // Scanner is either exhausted or had not yet been initialized, do so now
    // Evolution of idx is performed above when closing the scanner    
    
    Metadata metadata = metadatas.get(idx);
        
    //
    // Build start / end key
    //
    // CAUTION, the following code might seem wrong, but remember, timestamp
    // are reversed so the most recent (end) appears first (startkey)
    // 128bits
    
    byte[] startkey = new byte[Constants.HBASE_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];    
    // endkey has a trailing 0x00 so we include the actual end key
    byte[] endkey = new byte[startkey.length + 1];
    
    ByteBuffer bb = ByteBuffer.wrap(startkey).order(ByteOrder.BIG_ENDIAN);
    bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
    bb.putLong(metadata.getClassId());
    bb.putLong(metadata.getLabelsId());

    long modulus = now - (now % Constants.DEFAULT_MODULUS);
    
    bb.putLong(Long.MAX_VALUE - modulus);
    
    bb = ByteBuffer.wrap(endkey).order(ByteOrder.BIG_ENDIAN);
    bb.put(Constants.HBASE_RAW_DATA_KEY_PREFIX);
    bb.putLong(metadata.getClassId());
    bb.putLong(metadata.getLabelsId());
    
    boolean endAtMinlong = Long.MIN_VALUE == then;

    bb.putLong(Long.MAX_VALUE - then);
    
    //
    // Reset number of values retrieved since we just skipped to a new GTS.
    // If 'timespan' is negative this is the opposite of the number of values to retrieve
    // otherwise use Long.MAX_VALUE
    //
    
    nvalues = count >= 0 ? count : Long.MAX_VALUE;
    toskip = skip;
    
    Scan scan = new Scan();
    
    // Retrieve the whole column family
    scan.addFamily(colfam);
    
    if (postBoundaryScan) {
      scan.setReversed(true);
      // Set the stop row to the prefix of the current start key without timestamp
      scan.setStopRow(Arrays.copyOf(startkey, startkey.length - 8));
      byte[] k = Arrays.copyOf(startkey, startkey.length);
      System.arraycopy(Longs.toByteArray(Long.MAX_VALUE - (now + 1)), 0, k, k.length - 8, 8);
      scan.setStartRow(k);
    } else if (preBoundaryScan) {
      if (endAtMinlong) {
        // If the end timestamp is minlong, we will not have a preBoundaryScan, so set
        // dummy scan range
        scan.setStartRow(endkey);
        scan.setStopRow(endkey);
      } else {
        // Start right after 'endkey'
        scan.setStartRow(endkey);
        byte[] k = Arrays.copyOf(endkey, endkey.length);
        // Set the reversed time stamp to 0xFFFFFFFFFFFFFFFFL plus a 0x0 byte
        Arrays.fill(k, endkey.length - 8, k.length - 1, (byte) 0xFF);
        scan.setStopRow(k);        
      }
    } else {
      scan.setStartRow(startkey);
      scan.setStopRow(endkey);
    }

    //
    // Set batch/cache parameters
    //
    // FIXME(hbs): when using the HBase >= 0.96, use setMaxResultSize instead, and use setPrefetching
    
    // 1MB max result size
    scan.setMaxResultSize(1000000L);
    
    // Setting 'batch' too high when DEFAULT_MODULUS is != 1 will decrease performance when no filter is in use as extraneous cells may be fetched per row
    // Setting it too low will increase the number of roundtrips. A good heuristic is to set it to -timespan if timespan is < 0
    if (postBoundaryScan) {
      //scan.setBatch(Math.min(postBoundary, 100000));
      // Best performance seems to be attained when adding 1 to the boundary size for caching
      // Don't ask me why!
      scan.setCaching(Math.min(postBoundary + 1, 100000));
      if (postBoundary < 100) {
        scan.setSmall(true);
      }
    } else if (preBoundaryScan) {
      //scan.setBatch(Math.min(preBoundary , 100000));
      scan.setCaching(Math.min(preBoundary + 1, 100000));
      if (preBoundary < 100) {
        scan.setSmall(true);
      }
    } else {
      // Number of rows to cache can be set arbitrarily high as the end row will stop the scanner caching anyway
      // TODO(hbs): how should we account for sample?
      scan.setCaching((int) (count >= 0 ? Math.min(count + skip, 100000) : 100000));
      if (count >= 0 && count + skip < 100) {
        // setSmall and setBatch are not compatible, good, we are not using setBatch!
        scan.setSmall(true);
      }
    }
        
    if (this.useBlockcache) {
      scan.setCacheBlocks(true);
    } else {
      scan.setCacheBlocks(false);
    }
    
    try {
      this.scanner = htable.getScanner(scan);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_SCANNERS, Sensision.EMPTY_LABELS, 1);
      this.scaniter = this.scanner.iterator();
    } catch (IOException ioe) {
      //
      // If we caught an exception, we skip to the next metadata
      // FIXME(hbs): log exception somehow
      //
      this.scanner = null;
      this.scaniter = null;
      idx++;
      return hasNext();
    }

    //
    // If the current scanner has no more entries, call hasNext recursively so it takes
    // care of all the dirty details.
    //

    if (this.scaniter.hasNext()) {
      return true;
    } else {
      return hasNext();
    }
  }
  
  @Override
  public GTSDecoder next() {
    if (null == scaniter) {
      return null;
    }

    long datapoints = 0L;
    long keyBytes = 0L;
    long valueBytes = 0L;
    
    //
    // Create a new GTSEncoder for the results
    //
    
    GTSEncoder encoder = new GTSEncoder(0L);

    while(encoder.size() < Constants.MAX_ENCODER_SIZE && nvalues > 0 && scaniter.hasNext()) {
      
      //
      // Extract next result from scaniter
      //

      Result result = scaniter.next();
      resultCount++;
      
      CellScanner cscanner = result.cellScanner();
      
      try {
        while(nvalues > 0 && cscanner.advance()) {
          Cell cell = cscanner.current();
      
          cellCount++;
          
          //
          // Extract timestamp base from row key
          //

          long basets = Long.MAX_VALUE;
          
          if (1 == Constants.DEFAULT_MODULUS) {
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

          GTSDecoder decoder = new GTSDecoder(basets, hbaseKey, bb);
                    
          while(nvalues > 0 && decoder.next()) {
            long timestamp = decoder.getTimestamp();
                        
            if (preBoundaryScan || postBoundaryScan || (timestamp <= now && timestamp >= then)) {
              try {
                // Skip
                if (toskip > 0 && !preBoundaryScan && !postBoundaryScan) {
                  toskip--;
                  continue;
                }
                
                // Sample if we have to
                if (1.0D != sample && !preBoundaryScan && !postBoundaryScan && prng.nextDouble() > sample) {
                  continue;
                }
                
                if (writeTimestamp) {
                  encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), cell.getTimestamp() * Constants.TIME_UNITS_PER_MS);
                } else {
                  encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
                }
                
                //
                // Update statistics
                //
                
                valueBytes += valueLength;
                keyBytes += cell.getRowLength() + cell.getFamilyLength() + cell.getQualifierLength();
                datapoints++;

                // Don't decrement nvalues for the boundaries
                if (!postBoundaryScan && !preBoundaryScan) {
                  nvalues--;
                }
                
                if (preBoundaryScan) {
                  preBoundaryCount--;
                  if (0 == preBoundaryCount) {
                    break;
                  }
                } else if (postBoundaryScan) {
                  postBoundaryCount--;
                  if (0 == postBoundaryCount) {
                    break;
                  }
                }
              } catch (IOException ioe) {
                // FIXME(hbs): LOG?
              }
            }
          }
          
          if (preBoundaryScan && 0 == preBoundaryCount) {
            break;
          } else if (postBoundaryScan && 0 == postBoundaryCount) {
            break;
          }
        }          
      } catch(IOException ioe) {
        // FIXME(hbs): LOG?
      }

      /*
      NavigableMap<byte[], byte[]> colfams = result.getFamilyMap(colfam);
              
      for (byte[] qualifier: colfams.keySet()) {
        //
        // Extract timestamp base from column qualifier
        // This is true even for packed readings, those have a base timestamp of 0L
        //
     
        long basets = Long.MAX_VALUE - Longs.fromByteArray(qualifier);
        byte[] value = colfams.get(qualifier);
        
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);

        GTSDecoder decoder = new GTSDecoder(basets, keystore.getKey(KeyStore.AES_HBASE_DATA), bb);
                  
        while(decoder.next() && nvalues > 0) {
          long timestamp = decoder.getTimestamp();
          if (timestamp <= now && (timespan < 0 || (timestamp > (now - timespan)))) {
            try {
              encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());
              nvalues--;
            } catch (IOException ioe) {
              // FIXME(hbs): LOG?
            }
          }
        }
      } 
      */
     
      if (preBoundaryScan && 0 == preBoundaryCount) {
        break;
      } else if (postBoundaryScan && 0 == postBoundaryCount) {
        break;
      }
    }
            
    encoder.setMetadata(metadatas.get(idx));

    //
    // Update Sensision
    //

    //
    // Null token can happen when retrieving data from GTSSplit instances
    //
    
    if (null != token) {
      Map<String,String> labels = new HashMap<String,String>();
      
      Map<String,String> metadataLabels = metadatas.get(idx).getLabels();
      
      String billedCustomerId = Tokens.getUUID(token.getBilledId());

      if (null != billedCustomerId) {
        labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERID, billedCustomerId);
      }
      
      if (metadataLabels.containsKey(Constants.APPLICATION_LABEL)) {
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, metadataLabels.get(Constants.APPLICATION_LABEL));
      }
      
      if (metadataLabels.containsKey(Constants.OWNER_LABEL)) {
        labels.put(SensisionConstants.SENSISION_LABEL_OWNER, metadataLabels.get(Constants.OWNER_LABEL));
      }
      
      if (null != token.getAppName()) {
        labels.put(SensisionConstants.SENSISION_LABEL_CONSUMERAPP, token.getAppName());
      }
      
      //
      // Update per owner statistics, use a TTL for those
      //
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_VALUES_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, valueBytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_KEYS_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, keyBytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_DATAPOINTS_PEROWNER, labels, SensisionConstants.SENSISION_TTL_PERUSER, datapoints);          
             
      //
      // Update summary statistics
      //

      // Remove 'owner' label
      labels.remove(SensisionConstants.SENSISION_LABEL_OWNER);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_VALUES, labels, valueBytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_BYTES_KEYS, labels, keyBytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_DATAPOINTS, labels, datapoints);          
    }

    return encoder.getDecoder();
  }
  
  @Override
  public void remove() {
  }
  
  @Override
  public void close() throws Exception {
    //
    // Update Sensision metrics
    //
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_RESULTS, Sensision.EMPTY_LABELS, resultCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_CELLS, Sensision.EMPTY_LABELS, cellCount);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_ITERATORS, Sensision.EMPTY_LABELS, 1);
    if (null != this.scanner) {
      this.scanner.close();
    }
    if (null != this.htable) {
      this.htable.close();
    }
  }

}
