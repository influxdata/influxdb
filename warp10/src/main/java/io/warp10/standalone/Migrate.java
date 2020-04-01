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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;

import java.io.File;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.bouncycastle.util.encoders.Hex;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.util.Arrays;

/**
 * This class is used to migrate from pre-palindromic-hash24 class/labels Id.
 * 
 */
public class Migrate {
  public static void main(String[] args) throws Exception {
    //
    // Initialize keys
    //
    
    byte[] metaKey = Hex.decode("1111111111111111111111111111111111111111111111111111111111111111");
    byte[] classIdKey = Hex.decode("88888888888888888888888888888888");
    byte[] labelsIdKey = Hex.decode("99999999999999999999999999999999");
    
    String INDB = "/var/tmp/continuum-orig";
    String OUTDB = "/var/tmp/continuum-converted";
    
    //
    // Open source/target DBs
    //
    
    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(100000000L);
    options.compressionType(CompressionType.SNAPPY);
    
    DB indb;
    
    try {
      indb = JniDBFactory.factory.open(new File(INDB), options);
    } catch (UnsatisfiedLinkError ule) {
      System.out.println("WARNING: falling back to pure java implementation of LevelDB.");
      indb = Iq80DBFactory.factory.open(new File(INDB), options);
    }
    
    DB outdb;
    
    try {
      outdb = JniDBFactory.factory.open(new File(OUTDB), options);
    } catch (UnsatisfiedLinkError ule) {
      System.out.println("WARNING: falling back to pure java implementation of LevelDB.");
      outdb = Iq80DBFactory.factory.open(new File(OUTDB), options);
    }
    
    //
    // Read/Update metadata
    //
    
    DBIterator iter = indb.iterator();
    
    // Seek start of metadata
    iter.seek("M".getBytes(StandardCharsets.UTF_8));
    
    Map<BigInteger, byte[]> metadatas = new HashMap<BigInteger, byte[]>();
    
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    long nmeta = 0;
    
    long nano = System.nanoTime();
    
    while(iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.next();
      
      // Exit when done with metadata
      if (entry.getKey()[0] != 'M') {
        break;
      }
      
      byte[] clslblids = Arrays.copyOfRange(entry.getKey(), 1, 17);
      
      BigInteger bi = new BigInteger(clslblids);
      
      Metadata metadata = new Metadata();
      
      deserializer.deserialize(metadata, CryptoUtils.unwrap(metaKey, entry.getValue()));
      
      //
      // Compute new class/labels id
      //
      
      metadata.setClassId(GTSHelper.classId(classIdKey, metadata.getName()));
      metadata.setLabelsId(GTSHelper.labelsId(labelsIdKey, metadata.getLabels()));
      
      byte[] value = CryptoUtils.wrap(metaKey, serializer.serialize(metadata));
      byte[] key = new byte[17];
      
      ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
      bb.put((byte) 'M');
      bb.putLong(metadata.getClassId());
      bb.putLong(metadata.getLabelsId());
      
      outdb.put(key, value);
      
      metadatas.put(bi, Arrays.copyOfRange(key, 1, 17));
      nmeta++;
    }
    
    System.out.println("Updated " + nmeta + " metadatas in " + ((System.nanoTime() - nano) / 1000000.0D) + " ms");
    
    //
    // Read/Store readings
    //
    
    iter.seek("R".getBytes(StandardCharsets.UTF_8));
    
    long ndata = 0;
    
    nano = System.nanoTime();
    
    while(iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.next();
      
      if (entry.getKey()[0] != 'R') {
        break;
      }
      
      byte[] clslblids = Arrays.copyOfRange(entry.getKey(), 1, 17);
      
      BigInteger bi = new BigInteger(clslblids);

      if (!metadatas.containsKey(bi)) {
        System.out.println("No metadata found for " + new String(Hex.encode(entry.getKey())));
        continue;
      }
      
      byte[] newkey = Arrays.copyOf(entry.getKey(), entry.getKey().length);
      System.arraycopy(metadatas.get(bi), 0, newkey, 1, 16);
      
      outdb.put(newkey, entry.getValue());
      
      ndata++;
    }
    
    System.out.println("Updated " + ndata + " readings in " + ((System.nanoTime() - nano) / 1000000.0D) + " ms");
    
    indb.close();
    outdb.close();
  }
}
