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

package io.warp10.script.filter;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This class implements a 'latency' filter.
 * 
 * This filter assumes that the input GTS have values which are hashes of packet payloads. It
 * is used in the FILTER framework even though it is not a filter per se since it will produce new GTS
 * instead of simply selecting some.
 * 
 * The generated GTS will have the same ticks as the input ones except the value will be the packet latency
 * in microseconds instead of the payload hash
 */

public class LatencyFilter extends NamedWarpScriptFunction implements WarpScriptFilterFunction {
  
  /**
   * Maximum latency to consider when looking for matches
   */
  private final long maxLatency;
  
  /**
   * Minimum latency to consider when looking for matches
   */
  private final long minLatency;
  
  /**
   * Should we compute the uplink min latency
   */
  private final boolean uplinkLatencyMin;


  /**
   * Should we compute the uplink max latency
   */
  private final boolean uplinkLatencyMax;

/**
   * Should we compute per downlink min latency
   */
  private final boolean downlinkLatencyMin;

  /**
   * Should we compute per downlink max latency
   */
  private final boolean downlinkLatencyMax;

  /**
   * Should we compute per downlink match count
   */
  private final boolean downlinkMatches;
  
  /**
   * Should we compute total number of matches per downlink
   */
  private final boolean downlinksTotalMatches;

  /**
   * Should we compute the number of downlinks with matches
   */
  private final boolean downlinksWithMatches;

  /**
   * Should we compute downlinks bitset
   */
  private final boolean downlinksBitset;

  private final List<String> options;
  
  public static final class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
   
    //
    // Add min/max latency bounds + options to compute various GTS
    //
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      
      Object o = stack.pop();
      
      if (!(o instanceof List)) {
        throw new WarpScriptException(getName() + " expects a list of options on top of the stack.");
      }

      List<Object> options = (List<Object>) o;
      
      o = stack.pop();
      
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a maximum latency under the list of options.");
      }

      long maxLatency = (long) o;
      
      o = stack.pop();
      
      if (!(o instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a minimum latency under the list of options.");
      }
      
      long minLatency = ((Long) o).longValue();
      
      stack.push(new LatencyFilter(getName(), minLatency, maxLatency, options));
      return stack;
    }
  }
  public LatencyFilter(String name, long minLatency, long maxLatency, List<Object> options) {
    super(name);
    this.options = new ArrayList<String>();
    
    for (Object o: options) {
      this.options.add(o.toString());
    }
    
    this.minLatency = minLatency;
    this.maxLatency = maxLatency;
    
    if (options.contains("uplink.latency.min")) {
      this.uplinkLatencyMin = true;
    } else {
      this.uplinkLatencyMin = false;
    }

    if (options.contains("uplink.latency.max")) {
      this.uplinkLatencyMax = true;
    } else {
      this.uplinkLatencyMax = false;
    }

    if (options.contains("downlink.latency.min")) {
      this.downlinkLatencyMin = true;      
    } else {
      this.downlinkLatencyMin = false;      
    }
    
    if (options.contains("downlink.latency.max")) {
      this.downlinkLatencyMax = true;      
    } else {
      this.downlinkLatencyMax = false;      
    }

    if (options.contains("downlink.matches")) {
      this.downlinkMatches = true;      
    } else {
      this.downlinkMatches = false;      
    }

    if (options.contains("downlinks.bitset")) {
      this.downlinksBitset = true;      
    } else {
      this.downlinksBitset = false;      
    }

    if (options.contains("downlinks.totalmatches")) {
      this.downlinksTotalMatches = true;      
    } else {
      this.downlinksTotalMatches = false;      
    }
    
    if (options.contains("downlinks.withmatches")) {
      this.downlinksWithMatches = true;      
    } else {
      this.downlinksWithMatches = false;      
    }
  }
  
  @Override
  public java.util.List<GeoTimeSerie> filter(java.util.Map<String,String> labels, java.util.List<GeoTimeSerie>[] series) throws WarpScriptException {
    //
    // Sort GTS by (LONG) value
    //
    
    List<GeoTimeSerie> allseries = new ArrayList<GeoTimeSerie>();
    
    if (0 == series[0].size() || series[0].size() > 1) {
      return null;
    }
    
    for (List<GeoTimeSerie> lserie: series) {
      allseries.addAll(lserie);
    }
    
    for (GeoTimeSerie gts: allseries) {
      GTSHelper.valueSort(gts, false);
    }
    
    //
    // Allocate an array for keeping track of indices
    //
        
    int[] indices = new int[allseries.size()];
    
    //
    // Create output GTS
    //

    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
    
    //
    // Uplink packets are in the first GTS
    // All other GTS are downlink packets
    //
    
    GeoTimeSerie inGTS = allseries.get(0);
    
    GeoTimeSerie uplinkLatencyMinGTS = inGTS.cloneEmpty();
    uplinkLatencyMinGTS.getMetadata().setName(inGTS.getName() + ":uplink.latency.min");

    if (this.uplinkLatencyMin) {
      result.add(uplinkLatencyMinGTS);
    }
    
    GeoTimeSerie uplinkLatencyMax = inGTS.cloneEmpty();
    uplinkLatencyMax.getMetadata().setName(inGTS.getName() + ":uplink.latency.max");

    if (this.uplinkLatencyMax) {
      result.add(uplinkLatencyMax);
    }

    List<GeoTimeSerie> downlinkLatencyMinGTS = new ArrayList<GeoTimeSerie>();    
    List<GeoTimeSerie> downlinkLatencyMaxGTS = new ArrayList<GeoTimeSerie>();
    List<GeoTimeSerie> downlinkMatchesGTS = new ArrayList<GeoTimeSerie>();
    
    int n = allseries.size();

    for (int i = 1; i < n; i++) {
      if (this.downlinkLatencyMin) {
        GeoTimeSerie gts = allseries.get(i).cloneEmpty();
        gts.setName(gts.getName() + ":downlink.latency.min");
        downlinkLatencyMinGTS.add(gts);
      }
      if (this.downlinkLatencyMax) {
        GeoTimeSerie gts = allseries.get(i).cloneEmpty();
        gts.setName(gts.getName() + ":downlink.latency.max");
        downlinkLatencyMaxGTS.add(gts);
      }
      if (this.downlinkMatches) {
        GeoTimeSerie gts = allseries.get(i).cloneEmpty();
        gts.setName(gts.getName() + ":downlink.matches");
        downlinkMatchesGTS.add(gts);
      }
    }
    
    if (this.downlinkLatencyMin) {
      result.addAll(downlinkLatencyMinGTS);
    }
    
    if (this.downlinkLatencyMax) {
      result.addAll(downlinkLatencyMaxGTS);
    }
    
    if (this.downlinkMatches) {
      result.addAll(downlinkMatchesGTS);
    }

    GeoTimeSerie downlinksBitsetGTS = inGTS.cloneEmpty();
    downlinksBitsetGTS.setName(inGTS.getName() + ":downlinks.bitset");

    if (this.downlinksBitset) {
      result.add(downlinksBitsetGTS);
    }
    
    GeoTimeSerie downlinksTotalMatchesGTS = inGTS.cloneEmpty();
    downlinksTotalMatchesGTS.setName(inGTS.getName() + ":downlinks.totalmatches");

    if (this.downlinksTotalMatches) {
      result.add(downlinksTotalMatchesGTS);
    }
    
    GeoTimeSerie downlinksWithMatchesGTS = inGTS.cloneEmpty();
    downlinksWithMatchesGTS.setName(inGTS.getName() + ":downlinks.withmatches");

    if (this.downlinksWithMatches) {
      result.add(downlinksWithMatchesGTS);
    }

    //
    // Any packet on a downlink whose ts is more than 'maxlatency' from the same packet on the 'uplink' will not be considered
    // as a received packet but as a duplicate.
    //
    
    //
    // Loop on input packets
    //
    
    long maxinidx = inGTS.size();
    
    while(indices[0] < maxinidx) {
      long uplinkHash = ((Number) GTSHelper.valueAtIndex(inGTS, indices[0])).longValue();
      long uplinkTimestamp = GTSHelper.tickAtIndex(inGTS, indices[0]);
      
      long minLatency = Long.MAX_VALUE;
      long maxLatency = Long.MIN_VALUE;
      long totalLatency = 0L;
      int matchingDownlinkHashes = 0;
      int downlinksWithMatches = 0;
    
      //
      // Mask of downlinks having received the packet
      //
      
      long downlinksMask = 0L;
      
      //
      // Loop on downlinks
      //
          
      for (int i = 1; i < allseries.size(); i++) {
        GeoTimeSerie gts = allseries.get(i);
        
        int startidx = indices[i];
        
        long downlink_minLatency = Long.MAX_VALUE;
        long downlink_maxLatency = Long.MIN_VALUE;
        int downlinkMatches = 0;
        
        do {
          if (indices[i] >= gts.size()) {
            break;
          }
          
          long downlinkHash = ((Number) GTSHelper.valueAtIndex(gts, indices[i])).longValue();

          //
          // Skip downlinkHash which are too low
          //
          
          if (downlinkHash < uplinkHash) {
            indices[i]++;
            startidx = indices[i];
            continue;
          }
          
          //
          // Exit the loop if the current hash on the dowlink is > than the hash on the uplink,
          //
          
          if (downlinkHash > uplinkHash) {
            break;
          }
      
          long downlinkTimestamp = GTSHelper.tickAtIndex(gts, indices[i]);
          
          //
          // Skip timestamps which less than 'minLatency' after the uplink one
          //
          
          if (downlinkTimestamp < uplinkTimestamp + this.minLatency) {
            indices[i]++;
            startidx = indices[i];
            continue;
          }
          
          //
          // Compare the timestamp of the uplinkHash and that of the downlinkHash
          // FIXME(hbs): since packets may arrive out of order, we might encounter the case when identical packets were sent close to each other,
          //             in which case we cannot know for sure if the downlinkHash is to be matched with the current uplinkHash or another one
          //
          
          long latency = downlinkTimestamp - uplinkTimestamp;
      
          //
          // Stop if the hash matches but the latency is over the threshold we've set
          //
          
          if (latency > this.maxLatency) {
            break;
          }
          
          if (latency > maxLatency) {
            maxLatency = latency;
          }
          
          if (latency < minLatency) {
            minLatency = latency;
          }
        
          if (latency > downlink_maxLatency) {
            downlink_maxLatency = latency;
          }
          
          if (latency < downlink_minLatency) {
            downlink_minLatency = latency;
          }
          
          downlinkMatches++;
          
          totalLatency += latency;
          
          matchingDownlinkHashes++;
        
          if (startidx == indices[i]) {
            downlinksWithMatches++;
            downlinksMask |= (1L << (i-1));
          }
          
          // TODO(hbs): How do we keep track of the PLR for each link? We would need to issue a GTS for each valid P2P link
          //
          // TODO(hbs): emit GTS value for the downlink (latency of packets received at downlinkTimestamp).
          //
          
          indices[i]++;
                    
        } while (true);
        
        if (this.downlinkLatencyMax) {
          if (Long.MIN_VALUE == downlink_maxLatency) {
            GTSHelper.setValue(downlinkLatencyMaxGTS.get(i - 1), uplinkTimestamp, -1L);
          } else {
            GTSHelper.setValue(downlinkLatencyMaxGTS.get(i - 1), uplinkTimestamp, downlink_maxLatency);
          }
        }
        if (this.downlinkLatencyMin) {
          if (Long.MAX_VALUE == downlink_minLatency) {
            GTSHelper.setValue(downlinkLatencyMinGTS.get(i - 1), uplinkTimestamp, -1L);
          } else {
            GTSHelper.setValue(downlinkLatencyMinGTS.get(i - 1), uplinkTimestamp, downlink_minLatency);
          }          
        }
        if (this.downlinkMatches) {
          GTSHelper.setValue(downlinkMatchesGTS.get(i - 1), uplinkTimestamp, downlinkMatches);
        }
      }

      if (this.downlinksBitset) {
        GTSHelper.setValue(downlinksBitsetGTS, uplinkTimestamp, downlinksMask);
      }

      if (this.downlinksTotalMatches) {
        GTSHelper.setValue(downlinksTotalMatchesGTS, uplinkTimestamp, matchingDownlinkHashes);
      }
      
      if (this.downlinksWithMatches) {
        GTSHelper.setValue(downlinksWithMatchesGTS, uplinkTimestamp, downlinksWithMatches);        
      }
      
      if (this.uplinkLatencyMin) {
        if (Long.MAX_VALUE == minLatency) {
          GTSHelper.setValue(uplinkLatencyMinGTS, uplinkTimestamp, -1L);
        } else {
          GTSHelper.setValue(uplinkLatencyMinGTS, uplinkTimestamp, minLatency);
        }        
      }

      if (this.uplinkLatencyMax) {
        if (Long.MIN_VALUE == maxLatency) {
          GTSHelper.setValue(uplinkLatencyMax, uplinkTimestamp, -1L);
        } else {
          GTSHelper.setValue(uplinkLatencyMax, uplinkTimestamp, maxLatency);
        }
      }
      indices[0]++;
    }
    
    return result;
  }

  public List<GeoTimeSerie> filterOLD(Map<String, String> labels, List<GeoTimeSerie>... series) throws WarpScriptException {
    //
    // Sort GTS in ascending chronological order
    //
    
    List<GeoTimeSerie> allseries = new ArrayList<GeoTimeSerie>();
    
    for (List<GeoTimeSerie> lserie: series) {
      allseries.addAll(lserie);
    }
    
    for (GeoTimeSerie gts: allseries) {
      GTSHelper.sort(gts, false);
    }
    
    //
    // Assume the input packets are in the first GTS
    //
    
    int[] indices = new int[allseries.size()];
    
    //
    // We assume the packets exit the network in the same order they entered it
    //
    
    int n = allseries.get(0).size();
    
    GeoTimeSerie inGTS = allseries.get(0);
    
    //
    // Create output GTS
    //
    
    List<GeoTimeSerie> outGTS = new ArrayList<GeoTimeSerie>();
    
    for (int i = 0; i < n; i++) {
      outGTS.add(allseries.get(i).cloneEmpty());
    }
    
    //
    // Duplicate the hashes so we can sort them
    //
    
    long[][] sortedHashes = new long[allseries.size()][];

    for (int i = 1; i < allseries.size(); i++) {
      sortedHashes[i] = GTSHelper.longValues(allseries.get(i));
      Arrays.sort(sortedHashes[i]);
    }
    
    // Maximum latency, this is to enforce a stop condition when looking for a
    // matching out packet
    
    long maxlatency = 1000L;
    
    while(indices[0] < n) {
      long ints = GTSHelper.tickAtIndex(inGTS, indices[0]);
      long maxoutts = ints + maxLatency;
      long payloadHash = GTSHelper.tickAtIndex(inGTS, indices[0]);
      
      //
      // determine which outGTS has the packet
      //
      
      long minlatency = Long.MAX_VALUE;
      
      for (int i = 1; i < allseries.size(); i++) {
        GeoTimeSerie gts = allseries.get(i);
        if (payloadHash == ((Number) GTSHelper.valueAtIndex(gts, indices[i])).longValue() || -1 != Arrays.binarySearch(sortedHashes[i], payloadHash)) {
          int k = indices[i];
          int nn = GTSHelper.nvalues(gts);
          
          while(k < nn && payloadHash != ((Number) GTSHelper.valueAtIndex(gts, k)).longValue() && GTSHelper.tickAtIndex(gts, k) <= maxoutts) {
            k++;
          }
          
          if (k >= nn) {
            continue;
          }
          
          long outts = GTSHelper.tickAtIndex(gts, k);
          
          if (outts > maxoutts) {
            continue;
          }
          
          //
          // Ok, we've found a match
          //
          
          //
          // If the match was at 'indices[i]', advance 'indices[i]'
          //
          
          if (k == indices[i]) {
            indices[i]++;
          }
          
          long latency = outts - ints;
          
          GTSHelper.setValue(outGTS.get(i), outts, latency);
          
          if (latency < minlatency) {
            minlatency = latency;
          }
        }
      }
      
      //
      // Record the latency for the input GTS
      //
      
      if (Long.MAX_VALUE == minlatency) {
        GTSHelper.setValue(outGTS.get(0), ints, -1L);
      } else {
        GTSHelper.setValue(outGTS.get(0), ints, minlatency);
      }
      
      indices[0]++;
    }    
    
    return outGTS;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.minLatency);
    sb.append(" ");
    sb.append(this.maxLatency);
    sb.append(" ");
    sb.append(WarpScriptLib.LIST_START);
    sb.append(" ");
    for (String option: this.options) {
      sb.append(StackUtils.toString(option));
      sb.append(" ");
    }
    sb.append(WarpScriptLib.LIST_END);
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
