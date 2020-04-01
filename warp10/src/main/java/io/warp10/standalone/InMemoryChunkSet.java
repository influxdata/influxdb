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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import io.warp10.CapacityExtractorOutputStream;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.sensision.Sensision;

public class InMemoryChunkSet {
  /**
   * Maximum number of wasted bytes per encoder returned by fetch. Any waste above this limit
   * will trigger a resize of the encoder.
   */
  private static final int ENCODER_MAX_WASTED = 1024;
  
  /**
   * Chunks, organized in a ring. A given timestamp will lead to a specific chunk in the ring
   */
  private final GTSEncoder[] chunks;
  
  /**
   * End timestamp of each chunk
   */
  private final long[] chunkends;
  
  /**
   * Flags indicating if timestamps are increasingly monotonic
   */
  private final BitSet chronological;
  
  /**
   * Last timestamp encountered in a chunk
   */
  private final long[] lasttimestamp;
  
  /**
   * Length of chunks in time units
   */
  private final long chunklen;
  
  /**
   * Number of chunks
   */
  private final int chunkcount;
  
  /**
   * Is this an ephemeral chunk set storing only the last stored value?
   */
  private final boolean ephemeral;
  
  private static final Random prng = new Random();
  
  public InMemoryChunkSet(int chunkcount, long chunklen, boolean ephemeral) {
    this.chunks = new GTSEncoder[chunkcount];
    this.chunkends = new long[chunkcount];
    this.chronological = new BitSet(chunkcount);
    this.lasttimestamp = new long[chunkcount];
    this.ephemeral = ephemeral;
    if (ephemeral) {
      this.chunklen = Long.MAX_VALUE;
      this.chunkcount = 1;
    } else {
      this.chunklen = chunklen;
      this.chunkcount = chunkcount;      
    }
  }
  
  /**
   * Store the content of a GTSEncoder in the various chunks we manage
   * 
   * @param encoder The GTSEncoder instance to store
   */
  public boolean store(GTSEncoder encoder) throws IOException {
    if (null == encoder) {
      return false;
    }
    
    if (this.ephemeral) {
      // Extract the first element from the encoder
      GTSDecoder decoder = encoder.getUnsafeDecoder(false);
      
      if (!decoder.next()) {
        return false;
      }
      
      // Reset the encoder      
      chunks[0] = new GTSEncoder(decoder.getBaseTimestamp());
      chunks[0].addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
      return true;
    }
    
    // Get the current time
    long now = TimeSource.getTime();
    long lastChunkEnd = chunkEnd(now);
    long firstChunkStart = lastChunkEnd - (chunkcount * chunklen) + 1;

    // Get a decoder without copying the encoder array
    GTSDecoder decoder = encoder.getUnsafeDecoder(false);
    
    int lastchunk = -1;

    GTSEncoder chunkEncoder = null;

    boolean stored = false;
        
    while(decoder.next()) {
      long timestamp = decoder.getTimestamp();
      
      // Ignore timestamp if it is not in the valid range
      if (timestamp < firstChunkStart || timestamp > lastChunkEnd) {
        continue;
      }
      
      // Compute the chunkid
      int chunkid = chunk(timestamp);
    
      if (chunkid != lastchunk) {
        chunkEncoder = null;
      
        synchronized(this.chunks) {
          // Is the chunk non existent or has expired?
          if (null == this.chunks[chunkid] || this.chunkends[chunkid] < firstChunkStart) {
            long end = chunkEnd(timestamp);
            this.chunks[chunkid] = new GTSEncoder(0L);
            this.lasttimestamp[chunkid] = end - this.chunklen;
            this.chronological.set(chunkid);
            this.chunkends[chunkid] = end;          
          }
          
          chunkEncoder = this.chunks[chunkid];          
        }
        
        lastchunk = chunkid;
      }

      synchronized(this.chunks[chunkid]) {
        if (timestamp < this.lasttimestamp[chunkid]) {
          this.chronological.set(chunkid, false);
        }
        this.lasttimestamp[chunkid] = timestamp;

        chunkEncoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
        stored = true;
      }      
    }
    
    return stored;
  }
  
  /**
   * Compute the chunk id given a timestamp.
   * @param timestamp
   * @return
   */
  private int chunk(long timestamp) {
    int chunkid;
    
    if (timestamp >= 0) {
      chunkid = (int) ((timestamp / chunklen) % chunkcount);
    } else {
      chunkid = chunkcount + (int) ((((timestamp + 1) / chunklen) % chunkcount) - 1);
      //chunkid = chunkcount - (int) ((- (timestamp + 1) / chunklen) % chunkcount);
    }
    
    return chunkid;
  }
  
  /**
   * Compute the end timestamp of the chunk this timestamp
   * belongs to.
   * 
   * @param timestamp
   * @return
   */
  private long chunkEnd(long timestamp) {    
    return chunkEnd(timestamp, chunklen);
  }
  
  public static long chunkEnd(long timestamp, long chunklen) {    
    long end;
    
    if (timestamp >= 0) {
      end = ((timestamp / chunklen) * chunklen) + chunklen - 1;
    } else {
      end = ((((timestamp + 1) / chunklen) - 1) * chunklen) + chunklen - 1;
    }
    
    return end;
  }
  
  /**
   * Fetches some data from this chunk set
   * 
   * @param now The end timestamp to consider (inclusive).
   * @param timespan The timespan or value count to consider.
   * @return
   */
  public GTSDecoder fetch(long now, long then, long count, long skip, double sample, CapacityExtractorOutputStream extractor, int preBoundary, int postBoundary) throws IOException {
    GTSEncoder encoder = fetchEncoder(now, then, count, skip, sample, preBoundary, postBoundary);

    //
    // Resize the encoder so we don't waste too much memory
    //
    
    if (null != extractor) {      
      encoder.writeTo(extractor);
      int capacity = extractor.getCapacity();

      if (capacity - encoder.size() > ENCODER_MAX_WASTED) {
        encoder.resize(encoder.size());        
      }
    }

    return encoder.getUnsafeDecoder(false);
  }

  public GTSDecoder fetch(long now, long then, long count, long skip, double sample) throws IOException {
    return fetch(now, then, count, skip, sample, null, 0, 0);
  }
  
  public List<GTSDecoder> getDecoders() {
    List<GTSDecoder> decoders = new ArrayList<GTSDecoder>();
    
    synchronized (this.chunks) {
      for (int i = 0; i < this.chunks.length; i++) {
        if (null == this.chunks[i]) {
          continue;
        }
        decoders.add(this.chunks[i].getUnsafeDecoder(false));
      }
    }
    
    return decoders;
  }
  
  public GTSEncoder fetchEncoder(long now, long then, long count, long skip, double sample, int preBoundary, int postBoundary) throws IOException {

    if (this.ephemeral) {
      return fetchCountEncoder(Long.MAX_VALUE, 1L, postBoundary);
    }
    
    // Call fetchCountEncoder if fetching by count with a 'then' of MIN_LONG, no skipping
    // and no sampling or preBoundary
    
    if (count > 0 && Long.MIN_VALUE == then && 0 == skip && 1.0D == sample && 0 == preBoundary) {
      return fetchCountEncoder(now, -count, postBoundary);
    }
    
    //
    // Determine the chunk id of 'now'
    // We offset it by chunkcount so we can safely decrement and
    // still have a positive remainder when doing a modulus
    //
    int nowchunk = chunk(now) + this.chunkcount;
    
    // Compute the first timestamp (included)
    long firstTimestamp = then;
    
    GTSEncoder encoder = new GTSEncoder(0L);
    
    //
    // Fetch the postBoundary
    //
    
    if (postBoundary > 0) {
      // We store sequence / ts / lat / lon / elev / value
      // 'sequence' is the order in which the datapoints were inserted, used
      // to order them when their timestamp is equal
      PriorityQueue<Object[]> boundary = new PriorityQueue<Object[]>(new Comparator<Object[]>() {
        @Override
        public int compare(Object[] o1, Object[] o2) {
          int comp = Long.compare((long) o1[1], (long) o2[1]);
          // Timestamps are equal, compare the sequence #
          if (0 == comp) {
            comp = Long.compare((long) o1[0], (long) o2[0]);
          }
          return comp;
        }
      });

      // Now populate the queue from the current chunk, walking back in the ring until we have
      // postBoundary elements or we have exhausted the available data
      
      long seq = 0;
      
      for (int i = 0; i < this.chunkcount; i++) {
        int chunk = (nowchunk + i) % this.chunkcount;
        
        GTSDecoder chunkDecoder = null;
        
        synchronized(this.chunks) {
          // Ignore a given chunk if it is before 'now'
          if (this.chunkends[chunk] <= now) {
            continue;
          }
          
          // Extract a decoder to scan the chunk
          if (null != this.chunks[chunk]) {
            chunkDecoder = this.chunks[chunk].getUnsafeDecoder(false);
          }
        }
        
        if (null == chunkDecoder) {
          continue;
        }

        // Add datapoints from the decoder

        while (chunkDecoder.next()) {
          if (chunkDecoder.getTimestamp() > now) {
            boundary.add(new Object[] { seq++, chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue() });
            if (boundary.size() > postBoundary) {
              boundary.remove();
            }
          }
        }
        
        // If we have enough datapoints, stop
        
        if (postBoundary == boundary.size()) {
          break;
        }
      }            
      
      // Add the elements to 'encoder'
      
      for (Object[] elt: boundary) {
        encoder.addValue((long) elt[1], (long) elt[2], (long) elt[3], elt[4]);
      }
    }

    PriorityQueue<Object[]> boundary = null;
    
    if (preBoundary > 0) {
      boundary = new PriorityQueue<Object[]>(new Comparator<Object[]>() {
        @Override
        public int compare(Object[] o1, Object[] o2) {
          // We want the largest timestamp to be in the head
          int comp = Long.compare((long) o2[1], (long) o1[1]);
          // Timestamps are equal, compare the sequence #
          // the most recent being retained first
          if (0 == comp) {
            comp = Long.compare((long) o2[0], (long) o1[0]);
          }
          return comp;
        }
      });
    }
    
    long seq = 0;
    
    for (int i = 0; i < this.chunkcount; i++) {
      int chunk = (nowchunk - i) % this.chunkcount;
      
      GTSDecoder chunkDecoder = null;
      
      boolean boundaryOnly = false;
      
      synchronized(this.chunks) {
        // Ignore a given chunk if it does not intersect our current range
        if (this.chunkends[chunk] < firstTimestamp || (this.chunkends[chunk] - this.chunklen) >= now) {
          boundaryOnly = true;
        }
        
        // Extract a decoder to scan the chunk
        if (null != this.chunks[chunk] && !(boundaryOnly && null == boundary)) {
          chunkDecoder = this.chunks[chunk].getUnsafeDecoder(false);
        }
      }
      
      if (null == chunkDecoder) {
        continue;
      }
      
      // Chunk does not intersect the main range, so if we are not fetching a preboundary, ignore it
      if (boundaryOnly && null == boundary) {
        continue;
      }
      
      long nvalues = count >= 0 ? count : Long.MAX_VALUE;

      // Merge the data from chunkDecoder which is in the requested range in 'encoder'
      while(chunkDecoder.next()) {
        long ts = chunkDecoder.getTimestamp();
        
        if (ts > now || ts < firstTimestamp) {
          if (null != boundary) {
            boundary.add(new Object[] { seq++, chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue() });
            if (boundary.size() > preBoundary) {
              boundary.remove();
            }
          }
          continue;
        }
        
        if (!boundaryOnly) {
          // Do we have more datapoints to retrieve?
          if (nvalues > 0) {
            // Skip
            if (skip > 0) {
              skip--;
              continue;
            }
            
            // Sample
            if (1.0D != sample && prng.nextDouble() > sample) {
              continue;
            }          

            encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
            nvalues--;
          }
        }
        
        // If we are done fetching and have no preBoundary, exit
        if (0 == nvalues && 0 == preBoundary) {
          break;
        }
      }
      // If the pre boundary has enough datapoints, add them to the encoder and nullify boundary
      if (null != boundary && preBoundary == boundary.size()) {
        for (Object[] elt: boundary) {
          encoder.addValue((long) elt[1], (long) elt[2], (long) elt[3], elt[4]);
        }

        boundary = null;
      }
    }

    if (null != boundary) {
      for (Object[] elt: boundary) {
        encoder.addValue((long) elt[1], (long) elt[2], (long) elt[3], elt[4]);
      }
    }
    
    return encoder;
  }
  
//  private GTSDecoder fetchCount(long now, long count) throws IOException {
//    return fetchCountEncoder(now, count).getUnsafeDecoder(false);
//  }

  private GTSEncoder fetchCountEncoder(long now, long count, int postBoundary) throws IOException {
    //
    // Determine the chunk id of 'now'
    // We offset it by chunkcount so we can safely decrement and
    // still have a positive remainder when doing a modulus
    //
    int nowchunk = chunk(now) + this.chunkcount;
    
    //
    // Create a target encoder with a hint based on the average size of datapoints
    //
    
    long oursize = this.getSize();
    long ourcount = this.getCount();
    int avgsize = (int) Math.ceil((double) oursize / (double) ourcount);
    int hint = (int) Math.min((int) (count * avgsize), this.getSize());
    GTSEncoder encoder = new GTSEncoder(0L, null, hint);
    
    //
    // Fetch the postBoundary
    //
    
    if (postBoundary > 0) {
      // We store sequence / ts / lat / lon / elev / value
      // 'sequence' is the order in which the datapoints were inserted, used
      // to order them when their timestamp is equal
      PriorityQueue<Object[]> boundary = new PriorityQueue<Object[]>(new Comparator<Object[]>() {
        @Override
        public int compare(Object[] o1, Object[] o2) {
          // We want the datapoints with the lowest timestamp to be at the head
          int comp = Long.compare((long) o1[1], (long) o2[1]);
          // Timestamps are equal, compare the sequence #
          if (0 == comp) {
            comp = Long.compare((long) o1[0], (long) o2[0]);
          }
          return comp;
        }
      });

      // Now populate the queue from the current chunk, walking back in the ring until we have
      // postBoundary elements or we have exhausted the available data
      
      long seq = 0;
      
      for (int i = 0; i < this.chunkcount; i++) {
        int chunk = (nowchunk + i) % this.chunkcount;
        
        GTSDecoder chunkDecoder = null;
        
        synchronized(this.chunks) {
          // Ignore a given chunk if it is before 'now'
          if (this.chunkends[chunk] <= now) {
            continue;
          }
          
          // Extract a decoder to scan the chunk
          if (null != this.chunks[chunk]) {
            chunkDecoder = this.chunks[chunk].getUnsafeDecoder(false);
          }
        }
        
        if (null == chunkDecoder) {
          continue;
        }

        // Add datapoints from the decoder

        while (chunkDecoder.next()) {
          if (chunkDecoder.getTimestamp() > now) {
            boundary.add(new Object[] { seq++, chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue() });
            if (boundary.size() > postBoundary) {
              boundary.remove();
            }
          }
        }
        
        // If we have enough datapoints, stop
        
        if (postBoundary == boundary.size()) {
          break;
        }
      }
      
      // Add the elements to 'encoder'
      
      for (Object[] elt: boundary) {
        encoder.addValue((long) elt[1], (long) elt[2], (long) elt[3], elt[4]);
      }
    }
    
    // Initialize the number of datapoints to fetch
    long nvalues = count;
    
    // Loop over the chunks
    for (int i = 0; i < this.chunkcount; i++) {
      
      // Are we done fetching datapoints?
      if (nvalues <= 0) {
        break;
      }
            
      int chunk = (nowchunk - i) % this.chunkcount;
      
      GTSDecoder chunkDecoder = null;
      boolean inorder = true;
      long chunkEnd = -1;
      
      synchronized(this.chunks) {
        // Ignore a given chunk if it is after 'now'
        if (this.chunkends[chunk] - this.chunklen >= now) {
          continue;
        }
        
        // Extract a decoder to scan the chunk
        if (null != this.chunks[chunk]) {
          chunkDecoder = this.chunks[chunk].getUnsafeDecoder(false);
          inorder = this.chronological.get(chunk);
          chunkEnd = this.chunkends[chunk];
        }
      }
      
      if (null == chunkDecoder) {
        continue;
      }
      
      // We now have a chunk, we will treat it differently depending if
      // it is in chronological order or not
      
      if (inorder) {
        
        if (chunkEnd <= now && chunkDecoder.getCount() <= nvalues) {
          //
          // If the end timestamp of the chunk is before 'now' and the
          // chunk contains less than the remaining values we need to fetch
          // we can add everything.
          //
          while(chunkDecoder.next()) {
            encoder.addValue(chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
            nvalues--;
          }
        } else if (chunkDecoder.getCount() <= nvalues) {
          //
          // We have a chunk with chunkEnd > 'now' but which contains less than nvalues,
          // so we add all the values whose timestamp is <= 'now'
          //
          while(chunkDecoder.next()) {
            long ts = chunkDecoder.getTimestamp();
            if (ts > now) {
              // we can break because we know the encoder is in chronological order.
              break;
            }
            encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
            nvalues--;
          }          
        } else {
          //
          // The chunk has more values than what we need.
          // If the end of the chunk is <= now then we know we must skip count - nvalues and
          // add the rest to the result.
          // Otherwise it's a little trickier
          //
          
          if (chunkEnd <= now) {
            long skip = chunkDecoder.getCount() - nvalues;
            while(skip > 0 && chunkDecoder.next()) {
              skip--;
            }
            while(chunkDecoder.next()) {
              encoder.addValue(chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
              nvalues--;
            }          
          } else {            
            // Duplicate the decoder so we can scan it again later
            GTSDecoder dupdecoder = chunkDecoder.duplicate();
            // We will count the number of datapoints whose timestamp is <= now
            long valid = 0;
            while(chunkDecoder.next()) {
              long ts = chunkDecoder.getTimestamp();
              if (ts > now) {
                // we can break because we know the encoder is in chronological order.
                break;
              }
              valid++;
            }
            
            chunkDecoder = dupdecoder;
            long skip = valid - nvalues;
            while(skip > 0 && chunkDecoder.next()) {
              skip--;
              valid--;
            }
            while(valid > 0 && chunkDecoder.next()) {
              encoder.addValue(chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
              nvalues--;
              valid--;
            }                      
          }
        }
      } else {
        // The chunk decoder is not in chronological order...
        
        // Create a duplicate of the buffer in case we need it later
        GTSDecoder dupdecoder = chunkDecoder.duplicate();
                
        if (chunkEnd <= now && chunkDecoder.getCount() <= nvalues) {
          //
          // If the chunk decoder end is <= 'now' and the decoder contains less values than
          // what is still needed, add everything.
          //
          while(chunkDecoder.next()) {
            encoder.addValue(chunkDecoder.getTimestamp(), chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
            nvalues--;
          }          
        } else if(chunkDecoder.getCount() <= nvalues) {
          //
          // We have a chunk with chunkEnd > 'now' but which contains less than nvalues,
          // so we add all the values whose timestamp is <= 'now'
          //
          while(chunkDecoder.next()) {
            long ts = chunkDecoder.getTimestamp();
            if (ts > now) {
              // we skip the value as the encoder is not in chronological order
              continue;
            }
            encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
            nvalues--;
          }          
        } else {
          // We have a chunk which has more values than what we need and/or whose end
          // is after 'now'
          // We will transfer the datapoints whose timestamp is <= now in an array so we can sort them
          
          long[] ticks = new long[(int) chunkDecoder.getCount()];

          int idx = 0;
          
          while(chunkDecoder.next()) {
            long ts = chunkDecoder.getTimestamp();
            if (ts > now) {
              continue;
            }
            
            ticks[idx++] = ts;
          }
          
          if (idx > 1) {
            Arrays.sort(ticks, 0, idx);
          }
        
          chunkDecoder = dupdecoder;
          
          // We must skip values whose timestamp is <= ticks[idx - nvalues]
          
          if (idx > nvalues) {
            long lowest = ticks[idx - (int) nvalues];
            
            while(chunkDecoder.next() && nvalues > 0) {
              long ts = chunkDecoder.getTimestamp();
              if (ts < lowest) {
                continue;
              }
              encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
              nvalues--;
            }                                  
          } else {
            // The intermediary decoder has less than nvalues whose ts is <= now, transfer everything
            chunkDecoder = dupdecoder;
            
            int valid = idx;
            
            while(valid > 0 && chunkDecoder.next()) {
              long ts = chunkDecoder.getTimestamp();
              if (ts > now) {
                continue;
              }
              encoder.addValue(ts, chunkDecoder.getLocation(), chunkDecoder.getElevation(), chunkDecoder.getBinaryValue());
              nvalues--;
              valid--;
            }                                              
          }
        }
      }      
    }
        
    return encoder;
  }
  
  /**
   * Compute the total number of datapoints stored in this chunk set.
   * 
   * @return
   */
  public long getCount() {
    long count = 0L;
    
    for (GTSEncoder encoder: chunks) {
      if (null != encoder) {
        count += encoder.getCount();
      }
    }
    
    return count;
  }
  
  /**
   * Compute the total size occupied by the encoders in this chunk set
   * 
   * @return
   */
  public long getSize() {
    long size = 0L;
    
    for (GTSEncoder encoder: chunks) {
      if (null != encoder) {
        size += encoder.size();
      }
    }
    
    return size;
  }
  
  /**
   * Clean expired chunks according to 'now'
   * 
   * @param now
   */
  public long clean(long now) {
    
    if (this.ephemeral) {
      return 0L;
    }
    
    long cutoff = chunkEnd(now) - this.chunkcount * this.chunklen;
    int dropped = 0;
    long droppedDatapoints = 0L;
    synchronized(this.chunks) {
      for (int i = 0; i < this.chunks.length; i++) {
        if (null == this.chunks[i]) {
          continue;
        }
        if (this.chunkends[i] <= cutoff) {
          droppedDatapoints += this.chunks[i].getCount();
          this.chunks[i] = null;
          dropped++;
        }
      }
    }
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_INMEMORY_GC_CHUNKS, Sensision.EMPTY_LABELS, dropped);
    
    return droppedDatapoints;
  }
  
  /**
   * Optimize all non current chunks by shrinking their buffers.
   * 
   * @param now
   */
  long optimize(CapacityExtractorOutputStream out, long now, AtomicLong allocation) {
    
    if (this.ephemeral) {
      return 0L;
    }
    
    int currentChunk = chunk(now);
    
    long reclaimed = 0L;

    synchronized(this.chunks) {      
      for (int i = 0; i < this.chunks.length; i++) {
        if (null == this.chunks[i] || i == currentChunk) {
          continue;
        }
        int size = this.chunks[i].size();
        
        try {
          this.chunks[i].writeTo(out);
          int capacity = out.getCapacity();
          
          if (capacity > size) {
            this.chunks[i].resize(size);
            allocation.addAndGet(size);
            reclaimed += (capacity - size);
          }          
        } catch (IOException ioe) {          
        }
      }
    }
    
    return reclaimed;
  }
  
  /**
   * Delete datapoints whose timestamp if >= start and <= end
   * 
   * @param start Lower timestamp to delete (inclusive)
   * @param end Upper timestamp to delete (inclusive)
   * @return
   */
  public long delete(long start, long end) {
    long count = 0L;
    
    for (int i = 0; i < chunks.length; i++) {
      if (!this.ephemeral && (chunkends[i] < start || chunkends[i] >= end + chunklen)) {
        continue;
      }
      synchronized(chunks[i]) {
        GTSEncoder encoder = new GTSEncoder();
        GTSDecoder decoder = chunks[i].getUnsafeDecoder(false);
        boolean deleted = false;
        while (decoder.next()) {
          if (decoder.getTimestamp() >= start && decoder.getTimestamp() <= end) {
            deleted = true;
            count++;
            continue;
          }
          try {
            encoder.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
          } catch (IOException ioe) {
            throw new RuntimeException("Error while deleting data.", ioe);
          }
        }
        // Replace the encoder if datapoints were deleted
        if (deleted) {
          if (this.ephemeral) {
            chunks[i] = null;
          } else {
            chunks[i] = encoder;
          }
        }
      }
    }
    
    return count;
  }
}
