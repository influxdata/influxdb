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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.params.KeyParameter;

import com.geoxp.oss.jarjar.org.bouncycastle.util.Arrays;

import io.warp10.WarpURLDecoder;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OSSKeyStore;
/**
 * This class implements a migration tool to move data or metadata
 * from one state (HBase cluster, table, colfam, encryption) to another.
 * 
 * Its inception was a need to 'decrypt' data which was stored encrypted
 * in HBase by mistake.
 * 
 * Options
 *
 * --oss MASTER_KEY
 * --nokeepts
 * --fromhbase ZKPATH
 * --tohbase ZKPATH
 * --fromtable TABLE
 * --totable TABLE
 * --fromcf CF
 * --tocf CF
 * --fromkey KEY
 * --tokey KEY
 * 
 */
public class Migrate {
  
  
  public static void main(String[] args) throws Exception {
    //
    // Scan parameters
    //
    
    int i = 0;
  
    boolean keepts = true;
    String fromhbase = null;
    String fromtable = null;
    byte[] fromcf = null;
    String fromkey = null;
    String tohbase = null;
    String totable = null;
    byte[] tocf = null;
    String tokey = null;
    String oss = null;
    boolean debug = false;
    boolean skipinvalid = false;
    boolean skipstore = false;
    boolean encdataonly = false;
    boolean dodata = false;
    boolean dometadata = false;
    
    String start = null;
    String end = null;
    
    long progress = 0;
    long maxbatchsize = 2000000L;
    
    while(i < args.length) {
      if ("--nokeepts".equals(args[i])) {
        keepts = false;
      } else if ("--skipinvalid".equals(args[i])) {
        skipinvalid = true;
      } else if ("--skipstore".equals(args[i])) {
        skipstore = true;
      } else if ("--debug".equals(args[i])) {
        debug = true;
      } else if ("--progress".equals(args[i])) {
        progress = Long.parseLong(args[++i]);
      } else if ("--batchsize".equals(args[i])) {
        maxbatchsize = Long.parseLong(args[++i]);
      } else if ("--oss".equals(args[i])) {
        oss = args[++i];
      } else if ("--fromhbase".equals(args[i])) {
        fromhbase = args[++i];
      } else if ("--fromtable".equals(args[i])) {
        fromtable = args[++i];
      } else if ("--fromcf".equals(args[i])) {
        fromcf = args[++i].getBytes(StandardCharsets.UTF_8);
      } else if ("--fromkey".equals(args[i])) {
        fromkey = args[++i];
      } else if ("--tohbase".equals(args[i])) {
        tohbase = args[++i];
      } else if ("--totable".equals(args[i])) {
        totable = args[++i];
      } else if ("--tocf".equals(args[i])) {
        tocf = args[++i].getBytes(StandardCharsets.UTF_8);
      } else if ("--tokey".equals(args[i])) {
        tokey = args[++i];
      } else if ("--start".equals(args[i])) {
        start = WarpURLDecoder.decode(args[++i], StandardCharsets.ISO_8859_1);
      } else if ("--end".equals(args[i])) {
        end = WarpURLDecoder.decode(args[++i], StandardCharsets.ISO_8859_1);
      } else if ("--encryptedonly".equals(args[i])) {
        encdataonly = true;
      } else if ("--data".equals(args[i])) {
        dodata = true;
        dometadata = false;
      } else if ("--metadata".equals(args[i])) {
        dodata = false;
        dometadata = true;
      }
      
      i++;
    }
    
    //
    // Check params
    //
    
    if (!dodata && !dometadata) {
      throw new RuntimeException("--data or --metadata MUST be specified");
    }
    
    if (encdataonly && !dodata) {
      throw new RuntimeException("--encryptedonly can only be used with --data");
    }
    
    if (null == fromhbase || null == tohbase) {
      throw new RuntimeException("--fromhbase and --tohbase MUST be specified");
    }

    if (null == fromtable || null == totable) {
      throw new RuntimeException("--fromtable and --totable MUST be specified");
    }

    if (null == fromcf || null == tocf) {
      throw new RuntimeException("--fromcf and --tocf MUST be specified");
    }
    
    if (null == start || null == end) {
      throw new RuntimeException("--start and --end MUST be specified (endkey is excluded)");
    }
    
    //
    // Build keystore
    //
    
    KeyStore ks = new OSSKeyStore(oss);

    if (null != fromkey) {
      byte[] key = ks.decodeKey(fromkey);
      if (null == key) {
        throw new RuntimeException("Unable to decode 'fromkey'");
      }
      ks.setKey("fromkey", key); 
    }
    
    if (null != tokey) {
      byte[] key = ks.decodeKey(tokey);
      if (null == key) {
        throw new RuntimeException("Unable to decode 'tokey'");
      }      
      ks.setKey("tokey", key);
    }       
    
    //
    // Open HBase connections
    //
    
    Configuration fromconf = new Configuration();
    fromconf.set("hbase.zookeeper.quorum", fromhbase.replaceAll("/.*", ""));
    fromconf.set("zookeeper.znode.parent", fromhbase.replaceAll("^[^/]*", ""));

    Configuration toconf = new Configuration();
    toconf.set("hbase.zookeeper.quorum", tohbase.replaceAll("/.*", ""));
    toconf.set("zookeeper.znode.parent", tohbase.replaceAll("^[^/]*", ""));

    long nano = System.nanoTime();
    
    System.out.println("Connecting to HBase " + fromhbase);
    
    Connection fromconn = ConnectionFactory.createConnection(fromconf);
    TableName fromTableName = TableName.valueOf(fromtable);
    byte[] fromcolfam = fromcf;

    System.out.println("Connecting to HBase " + tohbase);

    Connection toconn = ConnectionFactory.createConnection(toconf);
    TableName toTableName = TableName.valueOf(totable);
    byte[] tocolfam = tocf;

    System.out.println("Connecting to table " + totable);

    Table tohtable = toconn.getTable(toTableName);    
    Table fromhtable = fromconn.getTable(fromTableName);

    System.out.println("Opening scanner");
    
    Scan scan = new Scan();
    // Retrieve the whole column family
    scan.addFamily(fromcolfam);
    scan.setStartRow(start.getBytes(StandardCharsets.UTF_8));
    scan.setStopRow(end.getBytes(StandardCharsets.UTF_8));

    scan.setBatch(1000000);
    scan.setCaching(1000000);
    scan.setMaxResultSize(10000000);
    scan.setCacheBlocks(false);
    scan.setMaxVersions(1);
    
    if (encdataonly) {
      //
      // We know the encrypted GTSEncoders start by 0x00 so we retrieve only those values LESS than 0x01
      //
      System.out.println("Setting ValueFilter");
      byte[] arg = new byte[1];
      arg[0] = 0x01;
      scan.setFilter(new ValueFilter(CompareOp.LESS, new BinaryPrefixComparator(arg)));
    }
    
    System.out.println("Retrieving ResultScanner");
    
    ResultScanner scanner = fromhtable.getScanner(scan);
    
    long count = 0;
    
    byte[] fromaeskey = ks.getKey("fromkey");
    byte[] toaeskey = ks.getKey("tokey");
        
    List<Put> puts = new ArrayList<Put>();
    long batchsize = 0;
    long totalsize = 0;
    
    long invalid = 0;
    
    AESWrapEngine fromaes = null;
    AESWrapEngine toaes = null;
    
    if (null != fromaeskey) {
      fromaes = new AESWrapEngine();
      CipherParameters params = new KeyParameter(fromaeskey);
      fromaes.init(false, params);
    }

    if (null != toaeskey) {
      toaes = new AESWrapEngine();
      CipherParameters params = new KeyParameter(toaeskey);
      toaes.init(false, params);
    }

    boolean samekeys = false;

    if (null != fromaeskey && null != toaeskey && 0 == Bytes.compareTo(fromaeskey, toaeskey)) {
      samekeys = true;
    }
    
    while(true) {
      Result result = scanner.next();
      
      if (null == result) {
        break;
      }
      
      CellScanner cscanner = result.cellScanner();
      
      while(cscanner.advance()) {
        Cell cell = cscanner.current();

        byte[] value = cell.getValueArray();
        int valueOffset = cell.getValueOffset();
        int valueLength = cell.getValueLength();

        if (dodata) {
          //
          // Extract timestamp base from column qualifier
          // This is true even for packed readings, those have a base timestamp of 0L
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
                    
          ByteBuffer bb = ByteBuffer.wrap(value,valueOffset,valueLength).order(ByteOrder.BIG_ENDIAN);

          GTSDecoder decoder = new GTSDecoder(basets, fromaeskey, bb);

          GTSEncoder encoder = new GTSEncoder(basets, toaeskey);
          
          while(decoder.next()) {
            long timestamp = decoder.getTimestamp();
            encoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
          }
          
          count++;
          
          if (0 == encoder.size()) {
            invalid++;
            if (skipinvalid) {
              continue;
            } else {
              throw new Exception("Invalid row encountered at " + cell);
            }
          }
          
          Put put = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          
          byte[] databytes = encoder.getBytes();
          
          if (keepts) {
            put.addColumn(tocolfam, CellUtil.cloneQualifier(cell), cell.getTimestamp(), databytes);
          } else {
            put.addColumn(tocolfam, CellUtil.cloneQualifier(cell), databytes);
          }

          batchsize += cell.getRowLength() + databytes.length;
          puts.add(put);

          if (progress > 0 && 0 == count % progress) {
            System.out.println(count + " (" + invalid + " invalid)");
          }          
        } else {
          byte[] data = null;
          
          if (!samekeys) {
            if (null != fromaeskey) {
              try {
                data = fromaes.unwrap(value, valueOffset, valueLength);
              } catch (InvalidCipherTextException icte) {
                if (skipinvalid) {
                  invalid++;
                  continue;
                }
                throw icte;
              }
            }

            if (null != toaeskey) {
              if (null != data) {
                data = toaes.wrap(data, 0, data.length);
              } else {
                data = toaes.wrap(value, valueOffset, valueLength);
              }
            }
          }
          
          Put put = new Put(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          
          if (null == data) {
            data = Arrays.copyOfRange(value, valueOffset, valueLength);
          }
          
          if (keepts) {
            put.addColumn(tocolfam, CellUtil.cloneQualifier(cell), cell.getTimestamp(), data);
          } else {
            put.addColumn(tocolfam, CellUtil.cloneQualifier(cell), data);
          }
          
          puts.add(put);
          
          batchsize += cell.getRowLength() + data.length;
        }        
        
        if (batchsize > maxbatchsize) {
          if (!skipstore) {
            tohtable.put(puts);
          }
          totalsize += batchsize;
          System.out.println(count + " (" + invalid + " invalid)  " + totalsize + " bytes");
          puts.clear();
          batchsize = 0;
        }
      }
    }

    if (!puts.isEmpty()) {
      if (!skipstore) {
        tohtable.put(puts);
      }
      totalsize += batchsize;
      puts.clear();
      batchsize = 0;
    }

    scanner.close();
    fromhtable.close();
    fromconn.close();

    tohtable.close();
    toconn.close();
    
    nano = System.nanoTime() - nano;
    
    System.out.println(count + " rows (" + totalsize + " bytes, " + invalid + " invalid) in " + (nano / 1000000.0D) + " ms");
  }
}
