//
//   Copyright 2018-2020  SenX S.A.S.
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
package io.warp10.continuum;

import io.warp10.WarpConfig;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * Packs Geo Time Series into chunks spanning at most a given
 * timespan.
 */
public class Pack {
  
  private static final long MAX_ENCODER_SIZE = 16 * 1024 * 1024L;
  
  // Set of Geo Time Series already encountered
  private static Set<Metadata> parsed = new HashSet<Metadata>();

  private static void pack(long chunksize, int maxsize, String suffix, boolean expose) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    
    long now = TimeSource.getTime();
        
    GTSEncoder lastencoder = null;
    
    while(true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      GTSEncoder encoder = GTSHelper.parse(lastencoder, line, null, now);

      if (null != lastencoder && (encoder != lastencoder || lastencoder.size() > MAX_ENCODER_SIZE)) {
        // We changed the GTS, pack 'lastencoder'
        outputEncoder(lastencoder, chunksize, maxsize, suffix, expose);
        
        if (encoder == lastencoder) {
          // We allocate a branch new Encoder but set its metadata to those of 'encoder'
          // so continuation lines are supported
          lastencoder = new GTSEncoder(0L);
          lastencoder.setMetadata(encoder.getMetadata());
          continue;
        } else {
          lastencoder = null;
        }
      }
      
      lastencoder = encoder;
    }

    if (null != lastencoder) {
      outputEncoder(lastencoder, chunksize, maxsize, suffix, expose);      
    }
  }
  
  private static void outputEncoder(GTSEncoder encoder, long chunksize, long maxsize, String suffix, boolean expose) throws Exception {
    
    Metadata metadata = new Metadata(encoder.getMetadata());
    
    GTSDecoder decoder = encoder.getDecoder();
    GTSEncoder chunkEncoder = null;
    
    Long lastChunk = null;
    long maxTS = Long.MIN_VALUE;
    Long lastTs = null;
    Boolean order = null;
    
    while(decoder.next()) {
      long ts = decoder.getTimestamp();
      long chunk = ts >= 0 ? ts / chunksize : ((ts + 1) / chunksize) - 1;
      
      if (null == lastChunk) {
        lastChunk = chunk;
      }
      
      if (chunk != lastChunk) {
        outputChunk(chunkEncoder, maxTS, maxsize, expose);
        chunkEncoder = null;
        lastChunk = chunk;
        maxTS = Long.MIN_VALUE;        
      }

      if (null == chunkEncoder) {
        chunkEncoder = new GTSEncoder(0L);
        chunkEncoder.setMetadata(metadata);
        if (null != suffix) {
          chunkEncoder.setName(chunkEncoder.getName() + suffix);
        }        
      }

      chunkEncoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
      
      if (ts > maxTS) {
        maxTS = ts;
      }
      
      if (null == lastTs) {
        lastTs = ts;
      } else {
        if (null == order) {
          order = ts >= lastTs;
        } else if (order && ts < lastTs) {
          throw new RuntimeException("Timestamps moved unexpectedly backwards.");
        } else if (!order && ts > lastTs) {
          throw new RuntimeException("Timestamps moved unexpectedly forward.");
        }
      }
    }
    
    if (null != chunkEncoder && chunkEncoder.size() > 0) {
      outputChunk(chunkEncoder, maxTS, maxsize, expose);
    }
    
    parsed.add(metadata);
  }
  
  private static void outputChunk(GTSEncoder encoder, long maxTS, long maxsize, boolean expose) throws Exception {
    List<GTSEncoder> encoders = new ArrayList<GTSEncoder>();
    List<Long> maxtimestamps = new ArrayList<Long>();
    
    Metadata metadata = encoder.getMetadata();
    
    encoders.add(encoder);
    maxtimestamps.add(maxTS);
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

    long threshold = (long) Math.floor(0.75D * maxsize);
    
    while(!encoders.isEmpty()) {
      //
      // Attempt to wrap the current head encoder
      //
      
      GTSEncoder headEncoder = encoders.remove(0);
      long maxts = maxtimestamps.remove(0);
      
      if (0 == headEncoder.getCount()) {
        continue;
      }
      
      // Clear labels
      headEncoder.setMetadata(new Metadata());
      headEncoder.setName("");
      headEncoder.setLabels(new HashMap<String,String>());
      headEncoder.getMetadata().setAttributes(new HashMap<String,String>());

      GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(headEncoder, true);      
      
      //
      // Check that the size is below 75% of the maxsize (so we can base64 encode the result)
      //
      
      byte[] serialized = serializer.serialize(wrapper);
      
      if (serialized.length < threshold) {
        StringBuilder sb = new StringBuilder();
        sb.append(maxts);
        sb.append("//");
        sb.append(headEncoder.getCount());
        sb.append(" ");
                
        GTSHelper.encodeName(sb, metadata.getName());
        sb.append("{");
        boolean first = true;
        
        for (Entry<String, String> entry: metadata.getLabels().entrySet()) {
          //
          // Skip owner/producer labels and any other 'private' labels
          //
          if (!Constants.EXPOSE_OWNER_PRODUCER && !expose) {
            if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
              continue;
            }
            if (Constants.OWNER_LABEL.equals(entry.getKey())) {
              continue;
            }                      
          }
          
          if (!first) {
            sb.append(",");
          }
          GTSHelper.encodeName(sb, entry.getKey());
          sb.append("=");
          GTSHelper.encodeName(sb, entry.getValue());
          first = false;
        }
        sb.append("}");
        sb.append(" ");
        sb.append("'");
        
        System.out.print(sb.toString());
        OrderPreservingBase64.encodeToStream(serialized, System.out);
        System.out.print("'");
        System.out.print("\r\n");        
        continue;
      }
      
      if (headEncoder.getCount() >= 2) {
        GTSEncoder split = new GTSEncoder(0L);
        split.setMetadata(headEncoder.getMetadata());
        
        List<GTSEncoder> splits = new ArrayList<GTSEncoder>();
        List<Long> splitmaxts = new ArrayList<Long>();
        
        splits.add(split);
        
        int encThreshold = headEncoder.size() / 2;
        
        GTSDecoder deco = headEncoder.getDecoder(true);
        
        long splitmax = Long.MIN_VALUE;
        
        while(deco.next()) {
          split.addValue(deco.getTimestamp(), deco.getLocation(), deco.getElevation(), deco.getBinaryValue());
          
          if (deco.getTimestamp() > splitmax) {
            splitmax = deco.getTimestamp();
          }
          
          if (split.size() > encThreshold) {
            splitmaxts.add(splitmax);
            splitmax = Long.MIN_VALUE;
            split = new GTSEncoder(0L);
            split.setMetadata(headEncoder.getMetadata());
            splits.add(split);            
          }
        }
        
        splitmaxts.add(splitmax);
        
        //
        // Now insert the splits at the beginning of 'encoders'
        //
        
        for (int i = splits.size() - 1; i >= 0; i--) {
          encoders.add(0, splits.get(i));
          maxtimestamps.add(0, splitmaxts.get(i));
        }
        
        continue;
      } else {
        throw new IOException("Encountered a value whose length is above the configured threshold.");
      }      
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    
    System.setProperty(Configuration.WARP10_QUIET, "true");
    options.addOption("s", "suffix", true, "sets the suffix to add to packed Geo Time Series class name");
    options.addOption("m", "maxsize", true, "sets the maximum value size in bytes");
    options.addOption("c", "chunksize", true, "sets the length of time chunks in time units");
    options.addOption("u", "unit", true, "sets the time unit");
    options.addOption("e", "expose", false, "expose producer/owner");
    
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    int maxsize = Constants.DEFAULT_PACKED_MAXSIZE;
    String timeunits = "us";
    String suffix = ":packed";
    long chunksize = Long.MAX_VALUE;
    boolean expose = false;
    
    if (cmd.hasOption("m")) {
      maxsize = Integer.parseInt(cmd.getOptionValue("m"));
      
      if (maxsize < 64) {
        throw new RuntimeException("maxsize cannot be less than 64 bytes.");
      }
    }
    
    if (cmd.hasOption("c")) {
      chunksize = Long.parseLong(cmd.getOptionValue("c"));
    }
    
    if (cmd.hasOption("u")) {
      timeunits = cmd.getOptionValue("u");
    }
    
    if (cmd.hasOption("s")) {
      suffix = cmd.getOptionValue("s");
    }
    
    if (cmd.hasOption("e")) {
      expose = true;
    }
    
    //
    // Set system properties
    //
    
    System.setProperty(Configuration.WARP_TIME_UNITS, timeunits);
    WarpConfig.setProperties((String) null);
    
    pack(chunksize, maxsize, suffix, expose);
  }
}
