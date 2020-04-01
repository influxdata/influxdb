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

import io.warp10.WarpConfig;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.collect.MapMaker;

public class StandaloneMemoryStore extends Thread implements StoreClient {
  
  //private final ConcurrentHashMap<BigInteger,GTSEncoder> series;
  private final Map<BigInteger,GTSEncoder> series;
  
  //private final Map<BigInteger,Metadata> metadatas;
  
  private final long timespan;
  
  /**
   * High limit for encoder size, if an encoder goes beyond this size, it will be trimmed
   * until it goes back to below 'lowwatermark'
   */
  private final long highwatermark;
  
  private final long lowwatermark;
  
  private final KeyStore keystore;
  
  private final byte[] aesKey;
    
  private List<StandalonePlasmaHandlerInterface> plasmaHandlers = new ArrayList<StandalonePlasmaHandlerInterface>();

  private StandaloneDirectoryClient directoryClient = null;
  
  /**
   * Flag indicating whether or not we are in ephemeral mode.
   * When in ephemeral mode, any call to 'store' will overwrite the
   * current GTSEncoder thus only retaining the last one.
   */
  private boolean ephemeral = false;
  
  public StandaloneMemoryStore(KeyStore keystore, long timespan, long highwatermark, long lowwatermark) {
    this.keystore = keystore;
    this.aesKey = this.keystore.getKey(KeyStore.AES_LEVELDB_DATA);
    //this.series = new ConcurrentHashMap<BigInteger,GTSEncoder>();
    this.series = new MapMaker().concurrencyLevel(64).makeMap();
    this.timespan = timespan;
    this.highwatermark = highwatermark;
    this.lowwatermark = lowwatermark;
    
    //
    // Add a shutdown hook to dump the memory store on exit
    //

    String storeDumpProp = WarpConfig.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_STORE_DUMP);
    if (null != storeDumpProp) {
      
      final StandaloneMemoryStore self = this;
      final String path = storeDumpProp;
      Thread dumphook = new Thread() {
        @Override
        public void run() {
          try {
            self.dump(path);
          } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException(ioe);
          }
        }
      };
      
      Runtime.getRuntime().addShutdownHook(dumphook);
      
      //
      // Make sure ShutdownHookManager is initialized, otherwise it will try to
      // register a shutdown hook during the shutdown hook we just registered...
      //
      
      ShutdownHookManager.get();      
    }
    
    this.setDaemon(true);
    this.setName("[StandaloneMemoryStore Janitor]");
    this.setPriority(Thread.MIN_PRIORITY);
    this.start();
  }
  
  @Override
  //public GTSDecoderIterator fetch(final ReadToken token, final List<Metadata> metadatas, final long now, final long timespan, boolean fromArchive, boolean writeTimestamp, final int preBoundary, final int postBoundary) {
  public GTSDecoderIterator fetch(final ReadToken token, final List<Metadata> metadatas, final long now, final long then, final long count, final long skip, final double sample, boolean writeTimestamp, final int preBoundary, final int postBoundary) {

    if (0 != preBoundary || 0 != postBoundary) {
      throw new RuntimeException("Boundary retrieval is not supported by the current data store.");
    }
  
    if (0 != skip) {
      throw new RuntimeException("Unsupported skip operation.");
    }
    
    if (1.0D != sample) {
      throw new RuntimeException("Unsupported sample operation.");
    }
    
    long tspan = 0;
    
    if (count > 0) {
      tspan = -count;
    } else {
      tspan = now - then + 1;
    }
    
    final long timespan = tspan;
    
    GTSDecoderIterator iterator = new GTSDecoderIterator() {

      private int idx = 0;
      
      private GTSDecoder decoder = null;
      
      private long nvalues = Long.MAX_VALUE;
      
      @Override
      public void close() throws Exception {}
      
      @Override
      public void remove() {}
      
      @Override
      public GTSDecoder next() {
        return this.decoder;
      }
      
      @Override
      public boolean hasNext() {  
        
        byte[] bytes = new byte[16];
        
        while(true) {
          if (idx >= metadatas.size()) {
            return false;
          }
          
          while(idx < metadatas.size()) {
            //ByteBuffer bb = ByteBuffer.wrap(new byte[16]).order(ByteOrder.BIG_ENDIAN);
            //bb.putLong(metadatas.get(idx).getClassId());
            //bb.putLong(metadatas.get(idx).getLabelsId());
            //BigInteger clslbls = new BigInteger(bb.array());

            long id = metadatas.get(idx).getClassId();
            
            int bidx = 0;
            
            bytes[bidx++] = (byte) ((id >> 56) & 0xff);
            bytes[bidx++] = (byte) ((id >> 48) & 0xff);
            bytes[bidx++] = (byte) ((id >> 40) & 0xff);
            bytes[bidx++] = (byte) ((id >> 32) & 0xff);
            bytes[bidx++] = (byte) ((id >> 24) & 0xff);
            bytes[bidx++] = (byte) ((id >> 16) & 0xff);
            bytes[bidx++] = (byte) ((id >> 8) & 0xff);
            bytes[bidx++] = (byte) (id & 0xff);
            
            id = metadatas.get(idx).getLabelsId();

            bytes[bidx++] = (byte) ((id >> 56) & 0xff);
            bytes[bidx++] = (byte) ((id >> 48) & 0xff);
            bytes[bidx++] = (byte) ((id >> 40) & 0xff);
            bytes[bidx++] = (byte) ((id >> 32) & 0xff);
            bytes[bidx++] = (byte) ((id >> 24) & 0xff);
            bytes[bidx++] = (byte) ((id >> 16) & 0xff);
            bytes[bidx++] = (byte) ((id >> 8) & 0xff);
            bytes[bidx++] = (byte) (id & 0xff);

            BigInteger clslbls = new BigInteger(bytes);
            
            if (idx < metadatas.size() && series.containsKey(clslbls)) {
              GTSEncoder encoder = series.get(clslbls);
              
              GTSDecoder decoder = null;
                  
              synchronized (encoder) {
                decoder = encoder.getDecoder();              
              }
              
              //
              // Use nvalues to count the number of values within the right timerange
              //
              
              nvalues = 0L;
                            
              encoder = decoder.getCompatibleEncoder(0L);

              while(decoder.next()) {
                //
                // Ignore ticks after 'now'
                //
                
                if (decoder.getTimestamp() > now) {
                  continue;
                }
                
                // When retrieving ticks within a range, ignore those before the start timestamp
                if (timespan >= 0 && decoder.getTimestamp() < (now - timespan + 1)) {
                  continue;
                }

                try {
                  encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
                  nvalues++;
                } catch (IOException ioe) {                  
                }
              }

              if (0 == encoder.size()) {
                break;
              }
              
              //
              // If retrieving a fixed number of ticks (negative timespan), check how many
              // ticks we collected, if that number is <= to the limit, use the encoder as is,
              // otherwise we need to do something more elaborate...
              //
              
              if (timespan < 0 && nvalues > -timespan) {
                // Allocate an array for the timestamps
                long[] ticks = new long[(int) nvalues];
                
                // Extract the timestamps of the ticks we kept
                GTSDecoder dec = encoder.getDecoder(true);
                
                int idx = 0;

                while (dec.next()) {
                  ticks[idx++] = dec.getTimestamp();
                }
                
                // Sort the ticks
                Arrays.sort(ticks);
                
                // Determine the most ancient tick to consider
                long lowerbound = ticks[(int) (nvalues + timespan)];
                
                // Now iterate one more time on the encoder, only keeping the ticks in the valid range
                dec = encoder.getDecoder(true);
                
                encoder = decoder.getCompatibleEncoder(0L);

                while(dec.next()) {
                  if (dec.getTimestamp() >= lowerbound) {
                    try {
                      encoder.addValue(dec.getTimestamp(), dec.getLocation(), dec.getElevation(), dec.getBinaryValue());
                    } catch (IOException ioe) {                      
                    }
                  }
                }
              }
              
              this.decoder = encoder.getDecoder(true);              
              // We force metadatas as they might not be set in the encoder (case when we consume data off Kafka)
              this.decoder.setMetadata(metadatas.get(idx));

              idx++;
              return true;
            }
            
            idx++;
          }
          
          idx++;
        }
      }
    };
    
    return iterator;
  }
  
  public void store(GTSEncoder encoder) throws IOException {
    
    if (null == encoder) {
      return;
    }

    byte[] bytes = new byte[16];
    
    Metadata meta = encoder.getMetadata();

    // 128BITS
    long id = null != meta ? meta.getClassId() : encoder.getClassId();
    
    int bidx = 0;
    
    bytes[bidx++] = (byte) ((id >> 56) & 0xff);
    bytes[bidx++] = (byte) ((id >> 48) & 0xff);
    bytes[bidx++] = (byte) ((id >> 40) & 0xff);
    bytes[bidx++] = (byte) ((id >> 32) & 0xff);
    bytes[bidx++] = (byte) ((id >> 24) & 0xff);
    bytes[bidx++] = (byte) ((id >> 16) & 0xff);
    bytes[bidx++] = (byte) ((id >> 8) & 0xff);
    bytes[bidx++] = (byte) (id & 0xff);
    
    id = null != meta ? meta.getLabelsId() : encoder.getLabelsId();

    bytes[bidx++] = (byte) ((id >> 56) & 0xff);
    bytes[bidx++] = (byte) ((id >> 48) & 0xff);
    bytes[bidx++] = (byte) ((id >> 40) & 0xff);
    bytes[bidx++] = (byte) ((id >> 32) & 0xff);
    bytes[bidx++] = (byte) ((id >> 24) & 0xff);
    bytes[bidx++] = (byte) ((id >> 16) & 0xff);
    bytes[bidx++] = (byte) ((id >> 8) & 0xff);
    bytes[bidx++] = (byte) (id & 0xff);

    BigInteger clslbls = new BigInteger(bytes);

    GTSEncoder memencoder = null;

    //
    // WARNING(hbs): the following 2 synchronized blocks MUST stay sequential (cf run())
    //
    
    synchronized (this.series) {      
      memencoder = this.series.get(clslbls);
            
      // If in ephemeral mode, always allocate a new GTSEncoder.
      // We could probably directly store 'encoder', but this is
      // discouraged since 'encoder' could be later modified outside of 'store'
      
      if (null == memencoder || this.ephemeral) {
        memencoder = new GTSEncoder(0L, this.aesKey);
        // We're among trusted friends, use safeSetMetadata...
        if (null != meta) {
          memencoder.safeSetMetadata(meta);
        }
        this.series.put(clslbls, memencoder);
//        if (null != meta) {
//          this.metadatas.put(clslbls, meta);
//        }
      }            
    }

    boolean published = false;
        
    synchronized(memencoder) {
      //
      // If the encoder's size is 0 and it's not in 'series', call store recursively since
      // it's highly probable the encoder has been cleaned by the GC since we entered 'store'
      // Otherwise simply merge 'encoder' into 'memencoder'
      //
      if (0 == memencoder.size() && this.series.get(clslbls) != memencoder) {
        store(encoder);
        published = true;
      } else {
        memencoder.merge(encoder);
      }
    }            
    
    if (!published) {
      for (StandalonePlasmaHandlerInterface plasmaHandler: this.plasmaHandlers) {
        if (plasmaHandler.hasSubscriptions()) {
          plasmaHandler.publish(encoder);
        }
      }      
    }
  }
  
  @Override
  public void archive(int chunk, GTSEncoder encoder) throws IOException {
    throw new IOException("in-memory platform does not support archiving.");
  }
  
  @Override
  public void run() {
    //
    // Loop endlessly over the series, cleaning them as they grow
    //
    
    // TODO(hbs): when an encoder no longer contains data, we should remove it completely from memory.
    // This is tricky as we synchronize on the encoder for now, meaning we might have a waiting 'store' call.
    // Need to think about it a little more
    
    List<BigInteger> metadatas = null;

    long datapoints = 0L;
    long bytes = 0L;
    
    long gcperiod = (long) (0.25 * (timespan / Constants.TIME_UNITS_PER_MS));

    String gcPeriodProp = WarpConfig.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_GC_PERIOD);
    if (null != gcPeriodProp) {
      gcperiod = Long.valueOf(gcPeriodProp);
    }
    
    while(true) {
      // Sleep for 25% of the timespan
      try { Thread.sleep(gcperiod); } catch (InterruptedException ie) {}
            
      metadatas = new ArrayList<BigInteger>();
      metadatas.addAll(this.series.keySet());

      if (0 == metadatas.size()) { continue; }

      datapoints = 0L;
      bytes = 0L;
      
      for (int idx = 0 ; idx < metadatas.size(); idx++) {

        //
        // Extract GTSEncoder
        //
        
        GTSEncoder encoder;

        synchronized (this.series) {
          encoder = this.series.get(metadatas.get(idx));
        }
        
        long now = TimeSource.getTime();
        
        //
        // Check each encoder for the following conditions:
        //
        // The last recorded data was more than 'timespan' ago
        // The encoder size has exceeded 'highwatermark'
        //
        // If one of those conditions is met, encoder size will be reduced.
        //
        // Reducing an encoder size means synchronizing on the given encoder, thus
        // blocking any possible update
        //
              
        if (now - encoder.getLastTimestamp() > this.timespan) {
          
          synchronized (encoder) {
            GTSDecoder decoder = encoder.getDecoder(true);
            
            long skipped = 0;
            
            boolean keeplastskipped = false;
            
            while (decoder.next()) {
              skipped++;
              // Stop when reaching the first timestamp which is still within timespan
              if (decoder.getTimestamp() > now - this.timespan) {
                keeplastskipped = true;
                break;
              }
            }
            
            try {
              //
              // Only modify the encoder if we skipped values
              //
              
              if (skipped > 0) {
                if (!keeplastskipped) {
                  decoder.next();
                } else {
                  skipped--;
                }                
                encoder.reset(decoder.getEncoder(true));
                datapoints += skipped;
              }
            } catch (IOException ioe) {            
            }
          }
        } else if (encoder.size() > this.highwatermark) {
          
          synchronized (encoder) {
            GTSDecoder decoder = encoder.getDecoder(true);
            
            int skipped = 0;
            
            boolean keeplastskipped = false;
            
            while (decoder.next()) {
              skipped++;
              if (decoder.getTimestamp() > now - this.timespan) {
                keeplastskipped = true;
                break;
              }
              if (decoder.getRemainingSize() <= this.lowwatermark) {
                break;
              }
            }
            
            try {
              //
              // Only modify the encoder if we skipped values
              //
              
              if (skipped > 0) {
                if (!keeplastskipped) {
                  decoder.next();
                } else {
                  skipped--;
                }
                encoder.reset(decoder.getEncoder(true));
                datapoints += skipped;
              }
            } catch (IOException ioe) {            
            }
          }
        }
        
        bytes += encoder.size();
        
        //
        // ATTENTION.... We have a double synchronized clause, we need to make sure
        // there is no other double synchronized with the reverse order, otherwise we
        // would deadlock. The 'synchronized' in 'store' are sequential, not enclosed, so
        // we're safe!
        //
        
        synchronized(encoder) {
          if (0 == encoder.size()) {
            synchronized(this.series) {
              this.series.remove(this.series.get(metadatas.get(idx)));
              // TODO(hbs): Still need to unregister properly the Metadata from the Directory. This is tricky since
              // the call to store is re-entrant but won't go through the register phase....
            }
          }
        }
        
        //
        // TODO(hbs): remove the encoder if it's empty.
        // This is tricky to do since we might be storing data concurrently in an encoder
        // that is for now empty.
        // We also need to remove the metadata from the Directory.
        // For now we'll tolerate to have dangling Metadata (i.e. with no associated non empty encoder)
        //
        
        // Count empty encoders and report them as a Sensision metric.
      }
      
      //
      // Update the number of GC runs just before updating the number of bytes, so we reduce the probability that the two don't
      // change at the same time when polling the metrics (note that the probability still exists though)
      //
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_RUNS, Sensision.EMPTY_LABELS, 1);
      
      // We set the number of bytes but update the number of points (since we can't reliably determine the number of
      // datapoints in an encoder returned by decoder.getEncoder().
      
      Long oldbytes = (Long) Sensision.getValue(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_BYTES, Sensision.EMPTY_LABELS);     
      
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_BYTES, Sensision.EMPTY_LABELS, bytes);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_DATAPOINTS, Sensision.EMPTY_LABELS, datapoints);
      if (null != oldbytes && oldbytes > bytes) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_BYTES, Sensision.EMPTY_LABELS, oldbytes - bytes);
      }
    }
  }
  
  @Override
  public long delete(WriteToken token, Metadata metadata, long start, long end) throws IOException {
    if (Long.MIN_VALUE != start || Long.MAX_VALUE != end) {
      throw new IOException("MemoryStore only supports deleting complete Geo Time Series.");
    }
    
    //
    // Regen classId/labelsId
    //
    
    // 128BITS
    metadata.setLabelsId(GTSHelper.labelsId(this.keystore.getKey(KeyStore.SIPHASH_LABELS), metadata.getLabels()));
    metadata.setClassId(GTSHelper.classId(this.keystore.getKey(KeyStore.SIPHASH_CLASS), metadata.getName()));

    byte[] bytes = new byte[16];
    
    long id = metadata.getClassId();
    
    int bidx = 0;
    
    bytes[bidx++] = (byte) ((id >> 56) & 0xff);
    bytes[bidx++] = (byte) ((id >> 48) & 0xff);
    bytes[bidx++] = (byte) ((id >> 40) & 0xff);
    bytes[bidx++] = (byte) ((id >> 32) & 0xff);
    bytes[bidx++] = (byte) ((id >> 24) & 0xff);
    bytes[bidx++] = (byte) ((id >> 16) & 0xff);
    bytes[bidx++] = (byte) ((id >> 8) & 0xff);
    bytes[bidx++] = (byte) (id & 0xff);
    
    id = metadata.getLabelsId();

    bytes[bidx++] = (byte) ((id >> 56) & 0xff);
    bytes[bidx++] = (byte) ((id >> 48) & 0xff);
    bytes[bidx++] = (byte) ((id >> 40) & 0xff);
    bytes[bidx++] = (byte) ((id >> 32) & 0xff);
    bytes[bidx++] = (byte) ((id >> 24) & 0xff);
    bytes[bidx++] = (byte) ((id >> 16) & 0xff);
    bytes[bidx++] = (byte) ((id >> 8) & 0xff);
    bytes[bidx++] = (byte) (id & 0xff);

    BigInteger clslbls = new BigInteger(bytes);

    synchronized(this.series) {
      this.series.remove(clslbls);
    }
    
    return 0L;
  }
  
  public void addPlasmaHandler(StandalonePlasmaHandlerInterface plasmaHandler) {
    this.plasmaHandlers.add(plasmaHandler);
  } 
  
  public void dump(String path) throws IOException {
    
    long nano = System.nanoTime();
    int gts = 0;
    long bytes = 0L;
    
    Configuration conf = new Configuration();
        
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    
    CompressionCodec Codec = new DefaultCodec();
    SequenceFile.Writer writer = null;
    SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(new Path(path));
    SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(key.getClass());
    SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(value.getClass());
    SequenceFile.Writer.Option optCom = SequenceFile.Writer.compression(CompressionType.RECORD,  Codec);
    
    writer = SequenceFile.createWriter(conf, optPath, optKey, optVal, optCom);

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    try {
      for (Entry<BigInteger,GTSEncoder> entry: this.series.entrySet()) {
        gts++;
        Metadata metadata = this.directoryClient.getMetadataById(entry.getKey());

        GTSWrapper wrapper = new GTSWrapper();
        wrapper.setMetadata(metadata);        
        
        GTSEncoder encoder = entry.getValue();

        wrapper.setBase(encoder.getBaseTimestamp());
        wrapper.setCount(encoder.getCount());
        
        byte[] data = serializer.serialize(wrapper);
        key.set(data, 0, data.length);
        
        data = encoder.getBytes();
        value.set(data, 0, data.length);

        bytes += key.getLength() + value.getLength();
        
        writer.append(key, value);
      }
/*      
      for (Entry<BigInteger,Metadata> entry: this.metadatas.entrySet()) {
        gts++;
        byte[] data = serializer.serialize(entry.getValue());
        key.set(data, 0, data.length);
        
        GTSEncoder encoder = this.series.get(entry.getKey());
        data = encoder.getBytes();
        value.set(data, 0, data.length);

        bytes += key.getLength() + value.getLength();
        
        writer.append(key, value);
      }
*/      
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw ioe;
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    
    writer.close();

    nano = System.nanoTime() - nano;
    
    System.out.println("Dumped " + gts + " GTS (" + bytes + " bytes) in " + (nano / 1000000.0D) + " ms.");
  }
  
  public void load() {
    //
    // Load data from the specified file
    //

    String storeLoacProp = WarpConfig.getProperty(io.warp10.continuum.Configuration.STANDALONE_MEMORY_STORE_LOAD);
    if (null != storeLoacProp) {
      try {
        load(storeLoacProp);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }    
  }
  
  private void load(String path) throws IOException {
    
    long nano = System.nanoTime();
    int gts = 0;
    long bytes = 0L;
    
    Configuration conf = new Configuration();
        
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();
    
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    SequenceFile.Reader.Option optPath = SequenceFile.Reader.file(new Path(path));    
    
    SequenceFile.Reader reader = null;
    
    try {
      reader = new SequenceFile.Reader(conf, optPath);

      System.out.println("Loading '" + path + "' back in memory.");

      while(reader.next(key, value)) {
        gts++;
        GTSWrapper wrapper = new GTSWrapper();
        deserializer.deserialize(wrapper, key.copyBytes());
        GTSEncoder encoder = new GTSEncoder(0L, null, value.copyBytes());
        encoder.setCount(wrapper.getCount());
        
        bytes += value.getLength() + key.getLength();
        if (wrapper.isSetMetadata()) {
          encoder.safeSetMetadata(wrapper.getMetadata());
        } else {
          encoder.safeSetMetadata(new Metadata());
        }
        store(encoder);
        if (null != this.directoryClient) {
          this.directoryClient.register(encoder.getMetadata());
        }
      }
    } catch (FileNotFoundException fnfe) {
      System.err.println("File '" + path + "' was not found, skipping.");
      return;
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    reader.close();    
    
    nano = System.nanoTime() - nano;
    
    System.out.println("Loaded " + gts + " GTS (" + bytes + " bytes) in " + (nano / 1000000.0D) + " ms.");
  }
  
  public void setDirectoryClient(StandaloneDirectoryClient directoryClient) {
    this.directoryClient = directoryClient;
  }
  
  public void setEphemeral(boolean ephemeral) {
    this.ephemeral = ephemeral;
  }
}
