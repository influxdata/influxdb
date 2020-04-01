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

package io.warp10.continuum.gts;

import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.WarpScriptException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class dedicated to splitting a (supposedly large) GTS into smaller ones.
 */
public class GTSSplitter {
  /**
   * Split a GTSDecoder into multiple GTSWrapper instances, spilling on disk if necessary.
   * 
   * The input GTSDecoder will be clipped to ['clipfrom','clipto'].
   * The output GTSWrapper will be bucketized with each a single bucket of 'chunkwidth' span.
   * All buckets within ['clipfrom','clipto'] will have a resulting GTSWrapper.
   * Chunk ends (lastbucket of each resulting GTSWrapper) will be congruent to 'tsboundary' modulo 'chunkwidth'
   * 
   * @param decoder GTSDecoder to split
   * @param clipfrom Earliest timestamp of clipping region (included)
   * @param clipto Latest timestamp of clipping region (included)
   * @param tsboundary Timestamp boundary of the end of each chunk
   * @param chunkwidth Width of each chunk
   * @param chunklabel Name of the chunk label
   * @param fileprefix Prefix of the target files, the suffix '-' + chunkend will be appended
   * @param maxsize Maximum size in bytes the encoders can occupy in memory.
   * @param maxgts Maximum number of encoders at any given time
   * @param lwmratio When flushing encoders to disk, we will attempt to lower size or number of gts below lwmratio * {maxsize,maxgts}
   * 
   * @return a list of GTSEncoders or null if spilling on disk happened
   */
  public static List<GTSWrapper> chunk(GTSDecoder decoder, long clipfrom, long clipto, long tsboundary, long chunkwidth, String chunklabel, String fileprefix, long maxsize, long maxgts, float lwmratio) throws WarpScriptException, IOException {

    /*
    ## TODO(hbs): move code to GTSWrapperHelper as it returns GTSWrappers
    ### TODO(hbs): add clipping parameter (start/end), generate empty GTS in this range if need be.
    ### All generated GTS will be bucketized with lastbucket == chunkid, bucketcoun == 1 and bucketspan == chunkwidth
    ### TODO(hbs): modify CHUNK so it behaves in a similar way
    */
    
    //
    // Map of 'live' encoders
    //
    
    Map<Long, GTSEncoder> encoders = new LinkedHashMap<Long, GTSEncoder>();
    
    //
    // The input GTSDecoder cannot have 'chunklabel' as a labe
    //
    
    if (decoder.getLabels().containsKey(chunklabel)) {
      throw new WarpScriptException(chunklabel + " is already a label of the GTS to chunk.");
    }
    
    long totalsize = 0L;
    
    boolean spilled = false;
    
    long boundary = tsboundary % chunkwidth;
    
    while(decoder.next()) {
      //
      // Skip the point if timestamp is outside the clipping region
      //
      
      if (decoder.getTimestamp() < clipfrom || decoder.getTimestamp() > clipto) {
        continue;
      }
      
      //
      // Determine the chunk id.
      // The chunk id is the timestamp of the end of the chunk containing the current timestamp.
      // This end timestamp is congruent to 'boundary' modulo 'chunkwidth'
      //
      
      long chunk = chunkwidth * (decoder.getTimestamp() / chunkwidth) + boundary + (boundary >= decoder.getTimestamp() % chunkwidth ? 0 : chunkwidth);
      
      //
      // Determine if there is an existing encoder for this chunk
      //
      
      GTSEncoder encoder = encoders.get(chunk);
      
      if (null == encoder) {
        encoder = new GTSEncoder(0L);
        encoder.setMetadata(decoder.getMetadata());
        encoder.getMetadata().putToLabels(chunklabel, Long.toString(chunk));
        encoders.put(chunk, encoder);
      }

      //
      // Add point to the encoder
      //
      
      int encsize = encoder.size();
      
      encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
      
      totalsize += encoder.size() - encsize;
      
      //
      // Check if we need to perform some flushing of encoders to disk to stay within bounds
      //
      
      if (totalsize > maxsize || encoders.size() > maxgts) {
        
        int ngts = encoders.size();
        
        int emptyenc = 0;
        
        while(ngts > lwmratio*maxgts || totalsize > lwmratio*maxsize) {
          // Flush the first encoder
          // FIXME(hbs): should we use another heuristic, like the first one larger than the mean
          for (Map.Entry<Long, GTSEncoder> chunkidAndEncoder: encoders.entrySet()) {
            Long chunkid = chunkidAndEncoder.getKey();
            GTSEncoder enc = chunkidAndEncoder.getValue();
            // Skip empty encoders
            if(0 == enc.size()) {
              emptyenc++;
              continue;
            }            
            FileOutputStream out = new FileOutputStream(fileprefix + "-" + Long.toString(chunkid), true);
            out.write(enc.getBytes());
            spilled = true;
            out.close();
            totalsize -= enc.size();
            enc.flush(); 
            emptyenc++;
            
            if (totalsize <= lwmratio*maxsize && (ngts < maxgts || (ngts - emptyenc) <= lwmratio*maxgts)) {
              break;
            }
          }
          
          //
          // Remove empty GTS if needed
          //
          
          if (ngts > maxgts) {
            List<Long> toremove = new ArrayList<Long>();

            for (Map.Entry<Long, GTSEncoder> chunkidAndEncoder: encoders.entrySet()) {
              Long chunkid = chunkidAndEncoder.getKey();
              GTSEncoder enc = chunkidAndEncoder.getValue();
              if (0 == enc.size()) {
                toremove.add(chunkid);
                ngts--;
              }
              if (ngts <= lwmratio*maxgts) {
                break;
              }
            }         
            
            for (long chunkid: toremove) {
              encoders.remove(chunkid);
            }
          }          
        }
      }
    }
    
    //
    // Compute chunk ids of the clipping bounds
    //
    //
    // 0 1 2 3 4 5 6 7 8 9 10 11 12 13
    // [1,11] CW 3 TS 4
    // 0 1 / 2 3 4 / 5 6 7 / 8 9 10 / 11 12 13
    
    long lastchunk = chunkwidth * (clipto / chunkwidth) + boundary + (boundary >= clipto % chunkwidth ? 0 : chunkwidth);
    long firstchunk = chunkwidth * (clipfrom / chunkwidth) + boundary + (boundary >= clipfrom % chunkwidth ? 0 : chunkwidth);

    //
    // Perform the final flush
    //
    
    if (spilled) {
      for (long chunkid = firstchunk; chunkid <= lastchunk; chunkid += chunkwidth) {
        GTSEncoder encoder = encoders.get(chunkid);
        
        // If the encoder is not found, it means it's either already flushed or not encountered
        if (null == encoder) {
          File chunkfile = new File(fileprefix + "-" + Long.toString(chunkid));
          // Create file if it does not yet exist
          chunkfile.createNewFile();
          continue;
        }
        
        // If encoder is empty no need to flush it
        if (0 == encoder.size()) {
          continue;
        }
        // Flush any remaining data
        FileOutputStream out = new FileOutputStream(fileprefix + "-" + Long.toString(chunkid), true);
        out.write(encoder.getBytes());
        out.close();
      }      
    }
 
    if (spilled) {
      return null;
    } else {
      List<GTSWrapper> result = new ArrayList<GTSWrapper>();
      
      for (long chunkid = firstchunk; chunkid <= lastchunk; chunkid += chunkwidth) {
        GTSWrapper wrapper = new GTSWrapper();
        GTSEncoder encoder = encoders.get(chunkid);
        wrapper.setMetadata(new Metadata(decoder.getMetadata()));
        wrapper.getMetadata().putToLabels(chunklabel, Long.toString(chunkid));
        wrapper.setBase(0L);
        wrapper.setBucketcount(1L);
        wrapper.setBucketspan(chunkwidth);
        wrapper.setLastbucket(chunkid);
        if (null != encoder) {
          wrapper.setCount(encoder.getCount());
          wrapper.setEncoded(encoder.getBytes());
        }
        result.add(wrapper);
      }
      
      return result;
    }
  }
}
