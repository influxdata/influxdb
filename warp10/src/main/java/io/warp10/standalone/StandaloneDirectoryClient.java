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

import io.warp10.SmartPattern;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.DirectoryUtil;
import io.warp10.continuum.egress.ThriftDirectoryClient;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.Directory;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsResponse;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.HyperLogLogPlus;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;

public class StandaloneDirectoryClient implements DirectoryClient {
  
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneDirectoryClient.class);
  
  private static final String DIRECTORY_INIT_NTHREADS_DEFAULT = "4";
  
  private static final byte[] METADATA_PREFIX = "M".getBytes(StandardCharsets.US_ASCII);
  
  private static final int MAX_BATCH_SIZE = 500000;
  
  private final DB db;
  private final KeyStore keystore;
  
  private final byte[] classKey;
  private final byte[] labelsKey;
  
  private final long[] classLongs;
  private final long[] labelsLongs;
  
  private final byte[] aesKey;
  
  private final int initNThreads;

  private final boolean syncwrites;
  private final double syncrate;
  
  private long LIMIT_CLASS_CARDINALITY = 100;
  private long LIMIT_LABELS_CARDINALITY = 100;
  
  /**
   * Maps of class name to labelsId to metadata
   */
  // 128BITS
  private static final Map<String,Map<Long,Metadata>> metadatas = new MapMaker().concurrencyLevel(64).makeMap();
  private static final Map<BigInteger,Metadata> metadatasById = new MapMaker().concurrencyLevel(64).makeMap();

  private long activityWindow = 0L;
  
  public static interface ShardFilter {
    public boolean exclude(long classId, long labelsId);
  }
  
  public StandaloneDirectoryClient(DB db, final KeyStore keystore) {

    String classMaxCardinalityProp = WarpConfig.getProperty(Configuration.DIRECTORY_STATS_CLASS_MAXCARDINALITY);
    if (null != classMaxCardinalityProp) {
      this.LIMIT_CLASS_CARDINALITY = Long.parseLong(classMaxCardinalityProp);
    }

    String labelsMaxCardinalityProp = WarpConfig.getProperty(Configuration.DIRECTORY_STATS_LABELS_MAXCARDINALITY);
    if (null != labelsMaxCardinalityProp) {
      this.LIMIT_LABELS_CARDINALITY = Long.parseLong(labelsMaxCardinalityProp);
    }

    this.activityWindow = Long.parseLong(WarpConfig.getProperty(io.warp10.continuum.Configuration.INGRESS_ACTIVITY_WINDOW, "0"));

    this.initNThreads = Integer.parseInt(WarpConfig.getProperty(Configuration.DIRECTORY_INIT_NTHREADS, DIRECTORY_INIT_NTHREADS_DEFAULT));

    this.db = db;
    this.keystore = keystore;
  
    this.aesKey = this.keystore.getKey(KeyStore.AES_LEVELDB_METADATA);
    this.classKey = this.keystore.getKey(KeyStore.SIPHASH_CLASS);
    this.classLongs = SipHashInline.getKey(this.classKey);
    
    this.labelsKey = this.keystore.getKey(KeyStore.SIPHASH_LABELS);
    this.labelsLongs = SipHashInline.getKey(this.labelsKey);
    
    syncrate = Math.min(1.0D, Math.max(0.0D, Double.parseDouble(WarpConfig.getProperty(Configuration.LEVELDB_DIRECTORY_SYNCRATE, "1.0"))));
    syncwrites = 0.0 < syncrate && syncrate < 1.0;

    //
    // Read metadata from DB
    //
    
    if (null == db) {
      return;      
    }
    
    DBIterator iter = db.iterator();
    
    iter.seek(METADATA_PREFIX);

    byte[] stop = "N".getBytes(StandardCharsets.US_ASCII);
    
    long count = 0;
    
    
    Thread[] initThreads = new Thread[this.initNThreads];
    final AtomicBoolean[] stopMarkers = new AtomicBoolean[this.initNThreads];
    final LinkedBlockingQueue<Entry<byte[],byte[]>> resultQ = new LinkedBlockingQueue<Entry<byte[],byte[]>>(initThreads.length * 8192);

    for (int i = 0; i < initThreads.length; i++) {
      stopMarkers[i] = new AtomicBoolean(false);
      final AtomicBoolean stopMe = stopMarkers[i];
      initThreads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          
          byte[] bytes = new byte[16];

          AESWrapEngine engine = null;
          PKCS7Padding padding = null;
          
          if (null != keystore.getKey(KeyStore.AES_LEVELDB_METADATA)) {
            engine = new AESWrapEngine();
            CipherParameters params = new KeyParameter(keystore.getKey(KeyStore.AES_LEVELDB_METADATA));
            engine.init(false, params);

            padding = new PKCS7Padding();
          }
          
          TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

          while (!stopMe.get()) {
            try {
              
              Entry<byte[],byte[]> result = resultQ.poll(100, TimeUnit.MILLISECONDS);
              
              if (null == result) {
                continue;
              }
              
              byte[] key = result.getKey();
              byte[] value = result.getValue();
              
              //
              // Unwrap
              //
              
              byte[] unwrapped = null != engine ? engine.unwrap(value, 0, value.length) : value;
              
              //
              // Unpad
              //
              
              int padcount = null != padding ? padding.padCount(unwrapped) : 0;
              byte[] unpadded = null != padding ? Arrays.copyOf(unwrapped, unwrapped.length - padcount) : unwrapped;
              
              //
              // Deserialize
              //

              Metadata metadata = new Metadata();
              deserializer.deserialize(metadata, unpadded);

              String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
              Map<String,String> sensisionLabels = new HashMap<String,String>();
              sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);
              
              //
              // Compute classId/labelsId and compare it to the values in the row key
              //
              
              // 128BITS
              long classId = GTSHelper.classId(classLongs, metadata.getName());
              long labelsId = GTSHelper.labelsId(labelsLongs, metadata.getLabels());
              
              ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
              bb.position(1);
              long hbClassId = bb.getLong();
              long hbLabelsId = bb.getLong();
              
              // If classId/labelsId are incoherent, skip metadata
              if (classId != hbClassId || labelsId != hbLabelsId) {
                // FIXME(hbs): LOG
                System.err.println("Incoherent class/labels Id for " + metadata);
                continue;
              }

              // 128BITS
              metadata.setClassId(classId);
              metadata.setLabelsId(labelsId);
              
              if (!metadata.isSetAttributes()) {
                metadata.setAttributes(new HashMap<String,String>());
              }
              
              //
              // Internalize Strings
              //
              
              GTSHelper.internalizeStrings(metadata);

              synchronized(metadatas) {
                if (!metadatas.containsKey(metadata.getName())) {
                  metadatas.put(metadata.getName(), (Map) new MapMaker().concurrencyLevel(64).makeMap());
                }                
              }
              
              synchronized(metadatas.get(metadata.getName())) {
                if (!metadatas.get(metadata.getName()).containsKey(labelsId)) {
                  metadatas.get(metadata.getName()).put(labelsId, metadata);
                  
                  //
                  // Store Metadata under 'id'
                  //
                  // 128BITS
                  GTSHelper.fillGTSIds(bytes, 0, classId, labelsId);
                  BigInteger id = new BigInteger(bytes);
                  metadatasById.put(id, metadata);

                  Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, 1);

                  continue;
                }
              }
              
              // FIXME(hbs): LOG
              System.err.println("Duplicate labelsId for classId " + classId + ": " + metadata);
              continue;
              
            } catch (InvalidCipherTextException icte) {
              throw new RuntimeException(icte);
            } catch (TException te) {
              throw new RuntimeException(te);
            } catch (InterruptedException ie) {
              
            }
            
          }
        }
      });
      
      initThreads[i].setDaemon(true);
      initThreads[i].setName("[Directory initializer #" + i + "]");
      initThreads[i].start();
    }
    
    try {
      
      long nano = System.nanoTime();
      
      while(iter.hasNext()) {
        Entry<byte[],byte[]> kv = iter.next();
        byte[] key = kv.getKey();
        if (Bytes.compareTo(key, stop) >= 0) {
          break;
        }
                                
        boolean interrupted = true;
        
        while(interrupted) {
          interrupted = false;
          try {
            resultQ.put(kv);
            count++;
            if (0 == count % 1000) {
              Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, count);
            }
          } catch (InterruptedException ie) {
            interrupted = true;
          }
        }
      }      
      
      //
      // Wait until resultQ is empty
      //
      
      while(!resultQ.isEmpty()) {
        try { Thread.sleep(100L); } catch (InterruptedException ie) {}
      }
      
      //
      // Notify the init threads to stop
      //
      
      for (int i = 0; i < initNThreads; i++) {
        stopMarkers[i].set(true);
      }
      
      nano = System.nanoTime() - nano;
      
      System.out.println("Loaded " + count + " GTS in " + (nano / 1000000.0D) + " ms");
    } finally {
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, count);
      try {
        iter.close();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
  
  @Override
  public List<Metadata> find(DirectoryRequest request) {
    
    List<String> classExpr = request.getClassSelectors();
    List<Map<String,String>> labelsExpr = request.getLabelsSelectors();
    
    boolean hasActiveAfter = request.isSetActiveAfter();
    long activeAfter = request.getActiveAfter();
    
    boolean hasQuietAfter = request.isSetQuietAfter();
    long quietAfter = request.getQuietAfter();
    
    //
    // Build patterns from expressions
    //
    
    SmartPattern classSmartPattern;
    
    Collection<Metadata> metadatas;
    
    if (classExpr.size() > 1) {
      metadatas = new HashSet<Metadata>();
    } else {
      metadatas = new ArrayList<Metadata>();
    }

    Set<String> classNames = null;
    List<String> missingLabels = Constants.ABSENT_LABEL_SUPPORT ? new ArrayList<String>() : null;
    
    for (int i = 0; i < classExpr.size(); i++) {
      
      String exactClassName = null;
      
      if (classExpr.get(i).startsWith("=") || !classExpr.get(i).startsWith("~")) {
        exactClassName = classExpr.get(i).startsWith("=") ? classExpr.get(i).substring(1) : classExpr.get(i);
        classSmartPattern = new SmartPattern(exactClassName);
      } else {
        classSmartPattern = new SmartPattern(Pattern.compile(classExpr.get(i).substring(1)));
      }
      
      Map<String,SmartPattern> labelPatterns = new HashMap<String,SmartPattern>();
      
      if (null != missingLabels) {
        missingLabels.clear();
      }

      if (null != labelsExpr.get(i)) {
        for (Entry<String,String> entry: labelsExpr.get(i).entrySet()) {
          String label = entry.getKey();
          String expr = entry.getValue();
          Pattern pattern;
          
          if (null != missingLabels && ("=".equals(expr) || "".equals(expr))) {
            missingLabels.add(label);
            continue;
          }
          
          if (expr.startsWith("=") || !expr.startsWith("~")) {
            labelPatterns.put(label, new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr));
          } else {
            pattern = Pattern.compile(expr.substring(1));
            labelPatterns.put(label,  new SmartPattern(pattern));
          }          
        }      
      }
             
      if (null != exactClassName) {
        if (!this.metadatas.containsKey(exactClassName)) {
          continue;
        }
        classNames = new HashSet<String>();
        classNames.add(exactClassName);
      } else {
        classNames = this.metadatas.keySet();
      }

      //
      // Create arrays to check the labels, this is to speed up discard
      //
      
      List<String> labelNames = new ArrayList<String>(labelPatterns.size());
      List<SmartPattern> labelSmartPatterns = new ArrayList<SmartPattern>(labelPatterns.size());
      String[] labelValues = null;
      
      //
      // Put producer/app/owner first
      //
      
      if (labelPatterns.containsKey(Constants.PRODUCER_LABEL)) {
        labelNames.add(Constants.PRODUCER_LABEL);
        labelSmartPatterns.add(labelPatterns.get(Constants.PRODUCER_LABEL));
        labelPatterns.remove(Constants.PRODUCER_LABEL);
      }
      if (labelPatterns.containsKey(Constants.APPLICATION_LABEL)) {
        labelNames.add(Constants.APPLICATION_LABEL);
        labelSmartPatterns.add(labelPatterns.get(Constants.APPLICATION_LABEL));
        labelPatterns.remove(Constants.APPLICATION_LABEL);
      }
      if (labelPatterns.containsKey(Constants.OWNER_LABEL)) {
        labelNames.add(Constants.OWNER_LABEL);
        labelSmartPatterns.add(labelPatterns.get(Constants.OWNER_LABEL));
        labelPatterns.remove(Constants.OWNER_LABEL);
      }
      
      //
      // Now add the other labels
      //
      
      for(Entry<String,SmartPattern> entry: labelPatterns.entrySet()) {
        labelNames.add(entry.getKey());
        labelSmartPatterns.add(entry.getValue());
      }

      labelValues = new String[labelNames.size()];

      //
      // Loop over the class names to find matches
      //
      
      for (String className: classNames) {
                
        //
        // If class matches, check all labels for matches
        //
        
        if (classSmartPattern.matches(className)) {
          for (Metadata metadata: this.metadatas.get(className).values()) {
            
            //
            // Check activity
            //
            
            if (hasActiveAfter && metadata.getLastActivity() < activeAfter) {
              continue;
            }
            
            if (hasQuietAfter && metadata.getLastActivity() >= quietAfter) {
              continue;
            }
            
            
            boolean exclude = false;
                        
            if (null != missingLabels) {
              for (String missing: missingLabels) {
                // If the Metadata contain one of the missing labels, exclude the entry
                if (null != metadata.getLabels().get(missing)) {
                  exclude = true;
                  break;
                }
              }              
              // Check attributes
              if (!exclude && metadata.getAttributesSize() > 0) {
                for (String missing: missingLabels) {
                  // If the Metadata contain one of the missing labels, exclude the entry
                  if (null != metadata.getAttributes().get(missing)) {
                    exclude = true;
                    break;
                  }
                }                              
              }
              if (exclude) {
                continue;
              }
            }
            
            int idx = 0;
      
            for (String labelName: labelNames) {
              //
              // Immediately exclude metadata which do not contain one of the
              // labels for which we have patterns either in labels or in attributes
              //

              String labelValue = metadata.getLabels().get(labelName);
              
              if (null == labelValue) {
                labelValue = metadata.getAttributes().get(labelName);
                if (null == labelValue) {
                  exclude = true;
                  break;
                }
              }
              
              labelValues[idx++] = labelValue;
            }
            
            // If we did not collect enough label/attribute values, exclude the GTS
            if (idx < labelNames.size()) {
              exclude = true;
            }
            
            if (exclude) {
              continue;
            }
            
            //
            // Check if the label value matches, if not, exclude the GTS
            //
            
            for (int j = 0; j < labelNames.size(); j++) {
              if (!labelSmartPatterns.get(j).matches(labelValues[j])) {
                exclude = true;
                break;
              }
            }
            
            if (exclude) {
              continue;
            }
            
            //
            // We have a match, rebuild metadata
            //
            // FIXME(hbs): include a 'safe' mode to expose the internal Metadata instances?
            //
            
            Metadata meta = new Metadata();
            meta.setName(className);
            meta.setLabels(ImmutableMap.copyOf(metadata.getLabels()));
            meta.setAttributes(ImmutableMap.copyOf(metadata.getAttributes()));
            // 128BITS
            if (metadata.isSetClassId()) {
              meta.setClassId(metadata.getClassId());
            } else {
              meta.setClassId(GTSHelper.classId(classKey, meta.getName()));
            }
            if (metadata.isSetLabelsId()) {
              meta.setLabelsId(metadata.getLabelsId());
            } else {
              meta.setLabelsId(GTSHelper.labelsId(labelsKey, meta.getLabels()));
            }
            
            meta.setLastActivity(metadata.getLastActivity());
            metadatas.add(meta);
          }
        }
      }      
    }
    
    if (classExpr.size() > 1) {
      List<Metadata> metas = new ArrayList<Metadata>();
      metas.addAll(metadatas);
      return metas;
    } else {
      return (List<Metadata>) metadatas;
    }    
  };
  
  public void register(Metadata metadata) throws IOException {
    
    //
    // Special case of null means flush leveldb
    //
    
    if (null == metadata) {
      store(null, null);
      return;
    }
    
    //
    // If the metadata are not known, register them
    //
        
    if (Configuration.INGRESS_METADATA_SOURCE.equals(metadata.getSource()) && !metadatas.containsKey(metadata.getName())) {
      store(metadata);
    } else if (Configuration.INGRESS_METADATA_SOURCE.equals(metadata.getSource())) {
      // Compute labelsId
      // 128BITS
      long labelsId = GTSHelper.labelsId(this.labelsLongs, metadata.getLabels());
      
      if (!metadatas.get(metadata.getName()).containsKey(labelsId)) {
        // Metadata is unknown so we know the Metadata should be stored
        store(metadata);
      } else {
        // Check that we do not have a collision
        if (!metadatas.get(metadata.getName()).get(labelsId).getLabels().equals(metadata.getLabels())) {
          LOG.warn("LabelsId collision under class '" + metadata.getName() + "' " + metadata.getLabels() + " and " + metadatas.get(metadata.getName()).get(labelsId).getLabels());
          Sensision.update(SensisionConstants.CLASS_WARP_DIRECTORY_LABELS_COLLISIONS, Sensision.EMPTY_LABELS, 1);
        }

        //
        // Check activity of the GTS, storing it if the activity window has passed
        if (activityWindow > 0) {
          //
          // If the currently stored lastactivity is more than 'activityWindow' before the one in 'metadata',
          // store the metadata
          //
          long currentLastActivity = metadatas.get(metadata.getName()).get(labelsId).getLastActivity();
          if (metadata.getLastActivity() - currentLastActivity >= activityWindow) {
            store(metadata);
          }
        }
      }
    } else if (!Configuration.INGRESS_METADATA_SOURCE.equals(metadata.getSource())) {
      //
      // Metadata registration is not from Ingress, this means we can update the value as it comes from the directory service or a metadata update
      //
      
      // When it is a metadata update request, only store the metadata if the GTS is already known
      if (Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())
          || Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())) {
        if (metadatas.containsKey(metadata.getName())) {
          // 128BITS
          long labelsId = GTSHelper.labelsId(this.labelsLongs, metadata.getLabels());
          if (metadatas.get(metadata.getName()).containsKey(labelsId)) {
            // Check the activity so we only increase it
            // 128 bits
            Metadata meta = metadatas.get(metadata.getName()).get(labelsId);
            long currentLastActivity = meta.getLastActivity();
            if (metadata.getLastActivity() < currentLastActivity) {
              metadata.setLastActivity(currentLastActivity);
            }
            
            store(metadata);
          }
        }
      } else {
        store(metadata);
      }
    }
  }
  
  public synchronized void unregister(Metadata metadata) {
    if (!metadatas.containsKey(metadata.getName())) {
      return;
    }
    // 128BITS
    long labelsId = GTSHelper.labelsId(this.labelsLongs, metadata.getLabels());
    if (!metadatas.get(metadata.getName()).containsKey(labelsId)) {
      return;
    }
    metadatas.get(metadata.getName()).remove(labelsId);
    if (metadatas.get(metadata.getName()).isEmpty()) {
      metadatas.remove(metadata.getName());
    }

    String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);

    // 128BITS
    long classId = GTSHelper.classId(this.classLongs, metadata.getName());

    // Remove Metadata indexed by id
    byte[] idbytes = new byte[16];
    GTSHelper.fillGTSIds(idbytes, 0, classId, labelsId);
    this.metadatasById.remove(new BigInteger(idbytes));
    
    //
    // Remove entry from DB if need be
    //
    
    if (null == this.db) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, -1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, -1);
      return;
    }


    byte[] bytes = new byte[1 + 8 + 8];
    System.arraycopy(METADATA_PREFIX, 0, bytes, 0, METADATA_PREFIX.length);
    
    int idx = METADATA_PREFIX.length;
    
    bytes[idx++] = (byte) ((classId >> 56) & 0xff);
    bytes[idx++] = (byte) ((classId >> 48) & 0xff);
    bytes[idx++] = (byte) ((classId >> 40) & 0xff);
    bytes[idx++] = (byte) ((classId >> 32) & 0xff);
    bytes[idx++] = (byte) ((classId >> 24) & 0xff);
    bytes[idx++] = (byte) ((classId >> 16) & 0xff);
    bytes[idx++] = (byte) ((classId >> 8) & 0xff);
    bytes[idx++] = (byte) (classId & 0xff);

    bytes[idx++] = (byte) ((labelsId >> 56) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 48) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 40) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 32) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 24) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 16) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 8) & 0xff);
    bytes[idx++] = (byte) (labelsId & 0xff);

    this.db.delete(bytes);
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, -1);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, -1);
  }
  
  private ThreadLocal<WriteBatch> perThreadWriteBatch = new ThreadLocal<WriteBatch>() {
    protected WriteBatch initialValue() {      
      return db.createWriteBatch();
    };    
  };
  
  private ThreadLocal<AtomicLong> perThreadWriteBatchSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong(0L);
    };
  };

  private void store(byte[] key, byte[] value) throws IOException {
    
    if (null == this.db) {
      return;
    }
    
    WriteBatch batch = perThreadWriteBatch.get();

    AtomicLong size = perThreadWriteBatchSize.get();
    
    boolean written = false;
    
    WriteOptions options = new WriteOptions().sync(null == key || null == value || 1.0 == syncrate);
    
    try {
      if (null != key && null != value) {
        batch.put(key, value);
        size.addAndGet(key.length + value.length);
      }
      
      if (null == key || null == value || size.get() > MAX_BATCH_SIZE) {
        
        if (syncwrites && !options.sync()) {
          options = new WriteOptions().sync(Math.random() < syncrate);
        }
        
        this.db.write(batch, options);
        size.set(0L);
        perThreadWriteBatch.remove();
        written = true;
      }
    } finally {
      if (written) {
        batch.close();
      }
    }    
  }
  
  private void store(Metadata metadata) throws IOException {
    // Compute labelsId and classId
    // 128BITS
    long classId = GTSHelper.classId(this.classLongs, metadata.getName());
    long labelsId = GTSHelper.labelsId(this.labelsLongs, metadata.getLabels());

    String app = metadata.getLabels().get(Constants.APPLICATION_LABEL);
    Map<String,String> sensisionLabels = new HashMap<String,String>();
    sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, app);
    
    //ByteBuffer bb = ByteBuffer.wrap(new byte[1 + 8 + 8]).order(ByteOrder.BIG_ENDIAN);    
    //bb.put(METADATA_PREFIX);
    //bb.putLong(classId);
    //bb.putLong(labelsId);

    byte[] bytes = new byte[1 + 8 + 8];
    System.arraycopy(METADATA_PREFIX, 0, bytes, 0, METADATA_PREFIX.length);
    
    int idx = METADATA_PREFIX.length;
    
    bytes[idx++] = (byte) ((classId >> 56) & 0xff);
    bytes[idx++] = (byte) ((classId >> 48) & 0xff);
    bytes[idx++] = (byte) ((classId >> 40) & 0xff);
    bytes[idx++] = (byte) ((classId >> 32) & 0xff);
    bytes[idx++] = (byte) ((classId >> 24) & 0xff);
    bytes[idx++] = (byte) ((classId >> 16) & 0xff);
    bytes[idx++] = (byte) ((classId >> 8) & 0xff);
    bytes[idx++] = (byte) (classId & 0xff);

    bytes[idx++] = (byte) ((labelsId >> 56) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 48) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 40) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 32) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 24) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 16) & 0xff);
    bytes[idx++] = (byte) ((labelsId >> 8) & 0xff);
    bytes[idx++] = (byte) (labelsId & 0xff);

    metadata.setClassId(classId);
    metadata.setLabelsId(labelsId);
    
    if (Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT.equals(metadata.getSource())){
      // Update the attributes
      Metadata oldmeta = null;
      synchronized(metadatas) {
        if (metadatas.containsKey(metadata.getName())) {
          oldmeta = metadatas.get(metadata.getName()).get(labelsId);
          
          if (null != oldmeta && metadata.getAttributesSize() > 0) {
            for (Entry<String,String> attr: metadata.getAttributes().entrySet()) {
              if ("".equals(attr.getValue())) {
                oldmeta.getAttributes().remove(attr.getKey());
              } else {
                oldmeta.putToAttributes(attr.getKey(), attr.getValue());
              }
            }
            metadata.setAttributes(new HashMap<String,String>(oldmeta.getAttributes()));
          } else if (metadata.getAttributesSize() > 0) {
            // Remove the attributes with an empty value
            Set<String> names = new HashSet<String>(metadata.getAttributes().keySet());
            for (String name: names) {
              if ("".equals(metadata.getAttributes().get(name))) {
                metadata.getAttributes().remove(name);
              }
            }
          }
        }
      }      
    } else if (null == metadata.getAttributes() || !Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())) {
      metadata.setAttributes(new HashMap<String,String>());
      
      // If we are not updating the attributes, copy the attributes from the directory as we are probably
      // registering the GTS due to its recent activity.
      if (!Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())) {
        Metadata oldmeta = null;
        // Copy the attributes if the Metadata is already known, which can happen when
        // tracking the activity
        synchronized(metadatas) {
          if (metadatas.containsKey(metadata.getName())) {
            oldmeta = metadatas.get(metadata.getName()).get(labelsId);        
          }        
        }
        if (null != oldmeta && oldmeta.getAttributesSize() > 0) {
          metadata.getAttributes().putAll(oldmeta.getAttributes());
        }        
      }
    }
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    try {
      if (null != this.db) {
        byte[] serialized = serializer.serialize(metadata);
        if (null != this.aesKey) {
          serialized = CryptoUtils.wrap(this.aesKey, serialized);
        }
        
        //this.db.put(bb.array(), serialized);
        //this.db.put(bytes, serialized);
        store(bytes, serialized);
      }
      
      synchronized (metadatas) {
        if (!metadatas.containsKey(metadata.getName())) {
          metadatas.put(metadata.getName(), (Map) new MapMaker().concurrencyLevel(64).makeMap());
        }
        if (null == metadatas.get(metadata.getName()).put(labelsId, metadata)) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS, Sensision.EMPTY_LABELS, 1);
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_GTS_PERAPP, sensisionLabels, 1);
        }
      }
      //
      // Store Metadata under 'id'
      //
      
      byte[] idbytes = new byte[16];
      GTSHelper.fillGTSIds(idbytes, 0, classId, labelsId);
      BigInteger id = new BigInteger(idbytes);
      this.metadatasById.put(id, metadata);

    } catch (TException te) {
      throw new RuntimeException(te);
    }
  }
  
  public Metadata getMetadataById(BigInteger id) {
    return this.metadatasById.get(id);
  }

  @Override
  public Map<String,Object> stats(DirectoryRequest dr) throws IOException {
    return stats(dr, null);
  }
  
  public Map<String,Object> stats(DirectoryRequest dr, ShardFilter filter) throws IOException {
    final DirectoryStatsRequest request = new DirectoryStatsRequest();
    request.setTimestamp(System.currentTimeMillis());
    request.setClassSelector(dr.getClassSelectors());
    request.setLabelsSelectors(dr.getLabelsSelectors());
    
    try {
      final DirectoryStatsResponse response = stats(request);

      List<Future<DirectoryStatsResponse>> responses = new ArrayList<Future<DirectoryStatsResponse>>();
      Future<DirectoryStatsResponse> f = new Future<DirectoryStatsResponse>() {
        @Override
        public DirectoryStatsResponse get() throws InterruptedException, ExecutionException {
          return response;
        }
        @Override
        public DirectoryStatsResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
          return response;
        }
        @Override
        public boolean isDone() { return true; }
        @Override
        public boolean isCancelled() { return false; }
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) { return false; }
      };
      
      responses.add(f);

      return ThriftDirectoryClient.mergeStatsResponses(responses);
    } catch (TException te) {
      throw new IOException(te);
    }
  }
  
  private DirectoryStatsResponse stats(DirectoryStatsRequest request) throws TException {
    return stats(request, null);
  }
  
  private DirectoryStatsResponse stats(DirectoryStatsRequest request, ShardFilter filter) throws TException {
    try {
      DirectoryStatsResponse response = new DirectoryStatsResponse();
      
      //
      // Build patterns from expressions
      //
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_REQUESTS, Sensision.EMPTY_LABELS, 1);

      SmartPattern classSmartPattern;
      
      Collection<Metadata> metas;
      
      //
      // Allocate a set if there is more than one class selector as we may have
      // duplicate results
      //
      
      if (request.getClassSelectorSize() > 1) {
        metas = new HashSet<Metadata>();
      } else {
        metas = new ArrayList<Metadata>();
      }
            
      HyperLogLogPlus gtsCount = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
      Map<String,HyperLogLogPlus> perClassCardinality = new HashMap<String,HyperLogLogPlus>();
      Map<String,HyperLogLogPlus> perLabelValueCardinality = new HashMap<String,HyperLogLogPlus>();
      HyperLogLogPlus labelNamesCardinality = null;
      HyperLogLogPlus labelValuesCardinality = null;
      HyperLogLogPlus classCardinality = null;
      
      List<String> missingLabels = Constants.ABSENT_LABEL_SUPPORT ? new ArrayList<String>() : null;

      for (int i = 0; i < request.getClassSelectorSize(); i++) {
        String exactClassName = null;
        
        if (request.getClassSelector().get(i).startsWith("=") || !request.getClassSelector().get(i).startsWith("~")) {
          exactClassName = request.getClassSelector().get(i).startsWith("=") ? request.getClassSelector().get(i).substring(1) : request.getClassSelector().get(i);
          classSmartPattern = new SmartPattern(exactClassName);
        } else {
          classSmartPattern = new SmartPattern(Pattern.compile(request.getClassSelector().get(i).substring(1)));
        }
        
        Map<String,SmartPattern> labelPatterns = new HashMap<String,SmartPattern>();
        
        if (null != missingLabels) {
          missingLabels.clear();
        }

        if (null != request.getLabelsSelectors()) {
          for (Entry<String,String> entry: request.getLabelsSelectors().get(i).entrySet()) {
            String label = entry.getKey();
            String expr = entry.getValue();
            SmartPattern pattern;
            
            if (null != missingLabels && ("=".equals(expr) || "".equals(expr))) {
              missingLabels.add(label);
              continue;
            }

            if (expr.startsWith("=") || !expr.startsWith("~")) {
              //pattern = Pattern.compile(Pattern.quote(expr.startsWith("=") ? expr.substring(1) : expr));
              pattern = new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr);
            } else {
              pattern = new SmartPattern(Pattern.compile(expr.substring(1)));
            }
            
            //labelPatterns.put(label,  pattern.matcher(""));
            labelPatterns.put(label,  pattern);
          }      
        }
              
        //
        // Loop over the class names to find matches
        //

        Collection<String> classNames = new ArrayList<String>();
        
        if (null != exactClassName) {
          // If the class name is an exact match, check if it is known, if not, skip to the next selector
          if(!this.metadatas.containsKey(exactClassName)) {
            continue;
          }
          classNames.add(exactClassName);
        } else {
//          //
//          // Extract per owner classes if owner selector exists
//          //
//          
//          if (request.getLabelsSelectors().get(i).size() > 0) {
//            String ownersel = request.getLabelsSelectors().get(i).get(Constants.OWNER_LABEL);
//            
//            if (null != ownersel && ownersel.startsWith("=")) {
//              classNames = classesPerOwner.get(ownersel.substring(1));
//            } else {
//              classNames = this.classNames.values();
//            }
//          } else {
//            classNames = this.classNames.values();
//          }
          classNames.addAll(metadatas.keySet());
        }
        
        List<String> labelNames = new ArrayList<String>(labelPatterns.size());
        List<SmartPattern> labelSmartPatterns = new ArrayList<SmartPattern>(labelPatterns.size());
        List<String> labelValues = new ArrayList<String>(labelPatterns.size());
        
        //
        // Put producer/app/owner first
        //
        
        if (labelPatterns.containsKey(Constants.PRODUCER_LABEL)) {
          labelNames.add(Constants.PRODUCER_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.PRODUCER_LABEL));
          labelPatterns.remove(Constants.PRODUCER_LABEL);   
          labelValues.add(null);
        }
        if (labelPatterns.containsKey(Constants.APPLICATION_LABEL)) {
          labelNames.add(Constants.APPLICATION_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.APPLICATION_LABEL));
          labelPatterns.remove(Constants.APPLICATION_LABEL);
          labelValues.add(null);
        }
        if (labelPatterns.containsKey(Constants.OWNER_LABEL)) {
          labelNames.add(Constants.OWNER_LABEL);
          labelSmartPatterns.add(labelPatterns.get(Constants.OWNER_LABEL));
          labelPatterns.remove(Constants.OWNER_LABEL);
          labelValues.add(null);
        }
        
        //
        // Now add the other labels
        //
        
        for(Entry<String,SmartPattern> entry: labelPatterns.entrySet()) {
          labelNames.add(entry.getKey());
          labelSmartPatterns.add(entry.getValue());
          labelValues.add(null);
        }

        for (String className: classNames) {
          
          //
          // If class matches, check all labels for matches
          //
          
          if (classSmartPattern.matches(className)) {
            Map<Long,Metadata> classMetadatas = this.metadatas.get(className);
            if (null == classMetadatas) {
              continue;
            }
            for (Metadata metadata: classMetadatas.values()) {
              
              boolean exclude = false;
              
              if (null != missingLabels) {
                for (String missing: missingLabels) {
                  // If the Metadata contain one of the missing labels, exclude the entry
                  if (null != metadata.getLabels().get(missing)) {
                    exclude = true;
                    break;
                  }
                }              
                // Check attributes
                if (!exclude && metadata.getAttributesSize() > 0) {
                  for (String missing: missingLabels) {
                    // If the Metadata contain one of the missing labels, exclude the entry
                    if (null != metadata.getAttributes().get(missing)) {
                      exclude = true;
                      break;
                    }
                  }                              
                }
                if (exclude) {
                  continue;
                }
              }
              
              int idx = 0;

              for (String labelName: labelNames) {
                //
                // Immediately exclude metadata which do not contain one of the
                // labels for which we have patterns either in labels or in attributes
                //

                String labelValue = metadata.getLabels().get(labelName);
                
                if (null == labelValue) {
                  labelValue = metadata.getAttributes().get(labelName);
                  if (null == labelValue) {
                    exclude = true;
                    break;
                  }
                }
                
                labelValues.set(idx++, labelValue);
              }
              
              // If we did not collect enough label/attribute values, exclude the GTS
              if (idx < labelNames.size()) {
                exclude = true;
              }
              
              if (exclude) {
                continue;
              }
              
              //
              // Check if the label value matches, if not, exclude the GTS
              //
              
              for (int j = 0; j < labelNames.size(); j++) {
                if (!labelSmartPatterns.get(j).matches(labelValues.get(j))) {
                  exclude = true;
                  break;
                }
              }
              
              if (exclude) {
                continue;
              }

              //
              // We have a match, update estimators
              //

              // Compute classId/labelsId
              long classId = GTSHelper.classId(classLongs, metadata.getName());
              long labelsId = GTSHelper.labelsId(labelsLongs, metadata.getLabels());
              
              //
              // Apply the shard filter to exclude Metadata which do not belong to the
              // shards we handle
              //
              
              if (null != filter && filter.exclude(classId, labelsId)) {
                continue;
              }
              
              // Compute gtsId, we use the GTS Id String from which we extract the 16 bytes
              byte[] data = GTSHelper.gtsIdToString(classId, labelsId).getBytes(StandardCharsets.UTF_16BE);
              long gtsId = SipHashInline.hash24(classLongs[0], classLongs[1], data, 0, data.length);
              
              gtsCount.aggregate(gtsId);
              
              if (null != perClassCardinality) {              
                HyperLogLogPlus count = perClassCardinality.get(metadata.getName());
                if (null == count) {
                  count = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                  perClassCardinality.put(metadata.getName(), count);
                }
                                
                count.aggregate(gtsId);
                
                // If we reached the limit in detailed number of classes, we fallback to a simple estimator
                if (perClassCardinality.size() >= LIMIT_CLASS_CARDINALITY) {
                  classCardinality = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                  for (String cls: perClassCardinality.keySet()) {
                    data = cls.getBytes(StandardCharsets.UTF_8);
                    classCardinality.aggregate(SipHashInline.hash24(classLongs[0], classLongs[1], data, 0, data.length, false));
                    perClassCardinality = null;
                  }
                }
              } else {
                data = metadata.getName().getBytes(StandardCharsets.UTF_8);
                classCardinality.aggregate(SipHashInline.hash24(classLongs[0], classLongs[1], data, 0, data.length, false));
              }
              
              if (null != perLabelValueCardinality) {
                if (metadata.getLabelsSize() > 0) {
                  for (Entry<String,String> entry: metadata.getLabels().entrySet()) {
                    HyperLogLogPlus estimator = perLabelValueCardinality.get(entry.getKey());
                    if (null == estimator) {
                      estimator = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                      perLabelValueCardinality.put(entry.getKey(), estimator);
                    }
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    long siphash = SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false);
                    estimator.aggregate(siphash);
                  }
                }

                if (metadata.getAttributesSize() > 0) {
                  for (Entry<String,String> entry: metadata.getAttributes().entrySet()) {
                    HyperLogLogPlus estimator = perLabelValueCardinality.get(entry.getKey());
                    if (null == estimator) {
                      estimator = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                      perLabelValueCardinality.put(entry.getKey(), estimator);
                    }
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    estimator.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                  }
                }

                if (perLabelValueCardinality.size() >= LIMIT_LABELS_CARDINALITY) {
                  labelNamesCardinality = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                  labelValuesCardinality = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                  for (Entry<String,HyperLogLogPlus> entry: perLabelValueCardinality.entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelNamesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                    labelValuesCardinality.fuse(entry.getValue());
                  }
                  perLabelValueCardinality = null;
                }
              } else {
                if (metadata.getLabelsSize() > 0) {
                  for (Entry<String,String> entry: metadata.getLabels().entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                  }
                }
                if (metadata.getAttributesSize() > 0) {
                  for (Entry<String,String> entry: metadata.getAttributes().entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                  }
                }
              }            
            }
          }
        }      
      }

      response.setGtsCount(gtsCount.toBytes());
      
      if (null != perClassCardinality) {
        classCardinality = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
        for (Entry<String,HyperLogLogPlus> entry: perClassCardinality.entrySet()) {
          response.putToPerClassCardinality(entry.getKey(), ByteBuffer.wrap(entry.getValue().toBytes()));
          byte[] data = entry.getKey().getBytes(StandardCharsets.UTF_8);
          classCardinality.aggregate(SipHashInline.hash24(classLongs[0], classLongs[1], data, 0, data.length, false));        
        }
      }
      
      response.setClassCardinality(classCardinality.toBytes());
      
      if (null != perLabelValueCardinality) {
        HyperLogLogPlus estimator = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
        HyperLogLogPlus nameEstimator = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
        for (Entry<String,HyperLogLogPlus> entry: perLabelValueCardinality.entrySet()) {
          byte[] data = entry.getKey().getBytes(StandardCharsets.UTF_8);
          nameEstimator.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
          estimator.fuse(entry.getValue());
          response.putToPerLabelValueCardinality(entry.getKey(), ByteBuffer.wrap(entry.getValue().toBytes()));
        }
        response.setLabelNamesCardinality(nameEstimator.toBytes());
        response.setLabelValuesCardinality(estimator.toBytes());
      } else {
        response.setLabelNamesCardinality(labelNamesCardinality.toBytes());
        response.setLabelValuesCardinality(labelValuesCardinality.toBytes());
      }
      
      return response;   
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new TException(ioe);
    } catch (Exception e) {
      e.printStackTrace();
      throw new TException(e);
    }
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
  
  public void setActivityWindow(long activityWindow) {
    this.activityWindow = activityWindow;
  }
  
  /**
   * Force Directory to store all its Metadata to LevelDB.
   */
  public void dbdump() throws IOException {
    if (null == db) {
      return;
    }
    
    synchronized(metadatas) {
      List<Metadata> metas = new ArrayList<Metadata>(metadatasById.values());
      for (Metadata meta: metas) {
        store(meta);
      }
      store(null);
    }
  }
}
