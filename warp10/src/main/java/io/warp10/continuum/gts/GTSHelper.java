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

package io.warp10.continuum.gts;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.warp10.json.JsonUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.google.common.collect.ImmutableMap;

import io.warp10.CapacityExtractorOutputStream;
import io.warp10.DoubleUtils;
import io.warp10.WarpURLDecoder;
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.SAXUtils;
import io.warp10.script.WarpScriptAggregatorFunction;
import io.warp10.script.WarpScriptBinaryOp;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptFillerFunction;
import io.warp10.script.WarpScriptFilterFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptMapperFunction;
import io.warp10.script.WarpScriptNAryFunction;
import io.warp10.script.WarpScriptReducerFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.functions.MACROMAPPER;
import io.warp10.script.functions.TOQUATERNION;
import sun.nio.cs.ArrayEncoder;


/**
 * Helper class to manipulate Geo Time Series.
 * 
 */
public class GTSHelper {
    
  /**
   * Sort the values (and associated locations/elevations) by order of their ticks
   *  
   *  @param gts The GeoTimeSerie instance to sort
   *  @param reversed If true, the ticks will be sorted from most recent to least
   *  @return gts, only sorted
   */
  public static final GeoTimeSerie sort(GeoTimeSerie gts, boolean reversed) {
    
    //
    // If GTS is sorted in another order than the one requested,
    // simply reverse the arrays
    //
    
    if (gts.sorted && gts.reversed != reversed) {
      if (null != gts.ticks) {
        int i = 0;
        int j = gts.values - 1;
        
        while(i < j) {
          long tmp = gts.ticks[i];
          gts.ticks[i] = gts.ticks[j];
          gts.ticks[j] = tmp;
          i++;
          j--;
        }
      }
      if (null != gts.locations) {
        int i = 0;
        int j = gts.values - 1;
        
        while(i < j) {
          long tmp = gts.locations[i];
          gts.locations[i] = gts.locations[j];
          gts.locations[j] = tmp;
          i++;
          j--;
        }        
      }
      if (null != gts.elevations) {
        int i = 0;
        int j = gts.values - 1;
        
        while(i < j) {
          long tmp = gts.elevations[i];
          gts.elevations[i] = gts.elevations[j];
          gts.elevations[j] = tmp;
          i++;
          j--;
        }
      }
      //
      // Now reverse the values
      //
      
      switch (gts.type) {
        case LONG:
          if (null != gts.longValues) {
            int i = 0;
            int j = gts.values - 1;
            
            while(i < j) {
              long tmp = gts.longValues[i];
              gts.longValues[i] = gts.longValues[j];
              gts.longValues[j] = tmp;
              i++;
              j--;
            }
          }
          break;
        case DOUBLE:
          if (null != gts.doubleValues) {
            int i = 0;
            int j = gts.values - 1;
            
            while(i < j) {
              double tmp = gts.doubleValues[i];
              gts.doubleValues[i] = gts.doubleValues[j];
              gts.doubleValues[j] = tmp;
              i++;
              j--;
            }
          }
          break;
        case STRING:
          if (null != gts.stringValues) {
            int i = 0;
            int j = gts.values - 1;
            
            while(i < j) {
              String tmp = gts.stringValues[i];
              gts.stringValues[i] = gts.stringValues[j];
              gts.stringValues[j] = tmp;
              i++;
              j--;
            }
          }
          break;
        case BOOLEAN:
          if (null != gts.booleanValues) {
            int i = 0;
            int j = gts.values - 1;
            
            while(i < j) {
              boolean tmp = gts.booleanValues.get(i);
              gts.booleanValues.set(i, gts.booleanValues.get(j));
              gts.booleanValues.set(j, tmp);
              i++;
              j--;
            }
          }
          break;
      }
      gts.reversed = reversed;
      return gts;
    }    

    //
    // Do not sort the GTS if it is already sorted as QS worst case is
    // encountered when the array is already sorted and is O(n^2)
    // 
    
    if (gts.sorted) {
      return gts;
    }
    
    quicksort(gts, 0, gts.values - 1, reversed);
    
    gts.sorted = true;
    gts.reversed = reversed;
        
    return gts;
  }
  
  /**
   * Sort the values (and associated locations/elevations) by order of ascending ticks.
   * 
   * @param gts GeoTimeSerie instance to sort
   * @return gts, only sorted
   */
  public static final GeoTimeSerie sort(GeoTimeSerie gts) {
    return sort(gts, false);
  }


  /**
   * Sort the GTS by respectively by tick, value, location and elevation.
   * @param gts The GTS instance to be sorted.
   * @return a fully sorted GTS.
   */
  public static final GeoTimeSerie fullsort(GeoTimeSerie gts) {
    return fullsort(gts, false);
  }

  /**
   * Sort the GTS by respectively by tick, value, location and elevation.
   * @param gts The GTS instance to be sorted.
   * @param reversed Whether to reverse the order of the returned list.
   * @return a fully sorted GTS.
   */
  public static final GeoTimeSerie fullsort(GeoTimeSerie gts, boolean reversed) {
    if (gts.sorted) {
      // If the GTS is already sorted, we can only fullsort where ticks are equals.

      if (gts.reversed != reversed) {
        // This will effectively flip the GTS
        sort(gts, reversed);
      }

      List<int[]> ranges = new ArrayList<int[]>();

      // Start index of the current possible range.
      int startIndex = 0;

      for (int i = 1; i < gts.values; i++) {
        if (gts.ticks[i] != gts.ticks[startIndex]) {
          // End of range, check that it contains several indices.
          if (i - 1 - startIndex > 0) {
            // Valid range, add it.
            ranges.add(new int[]{startIndex, i - 1});
          }
          startIndex = i;
        }
      }

      // Check if the loop ended before adding the last range. This range ends with the last index.
      if (startIndex < gts.values - 1) {
        ranges.add(new int[]{startIndex, gts.values - 1});
      }

      // Sort using the computed ranges.
      fullquicksort(gts, ranges, reversed);
    } else {
      fullquicksort(gts, 0, gts.values - 1, reversed);
    }

    gts.sorted = true;
    gts.reversed = reversed;

    return gts;
  }

  public static GTSEncoder fullsort(GTSEncoder encoder, boolean reversed) throws IOException {
    return fullsort(encoder, reversed, encoder.getBaseTimestamp());
  }
  
  /**
   * Sort an encoder
   *
   * @param encoder
   * @param reversed
   * @param baseTimestamp
   * @return
   */
  public static GTSEncoder fullsort(GTSEncoder encoder, boolean reversed, long baseTimestamp) throws IOException {
    
    GTSEncoder enc = null;
    
    //
    // Split the encoder in 5 GTS, one per type, in this order:
    //
    // LONG, DOUBLE, BOOLEAN, STRING, BINARY
    //
    
    GeoTimeSerie[] gts = new GeoTimeSerie[5];

    for (int i = 0; i < gts.length; i++) {
      gts[i] = new GeoTimeSerie();
    }
    
    GTSDecoder decoder = encoder.getDecoder();
    
    // Populate the 5 GTS
    while (decoder.next()) {
      long ts = decoder.getTimestamp();
      long location = decoder.getLocation();
      long elevation = decoder.getElevation();
      Object value = decoder.getBinaryValue();
      
      if (value instanceof Long) {
        GTSHelper.setValue(gts[0], ts, location, elevation, value, false);
      } else if (value instanceof Double || value instanceof BigDecimal) {
        GTSHelper.setValue(gts[1], ts, location, elevation, value, false);          
      } else if (value instanceof Boolean) {
        GTSHelper.setValue(gts[2], ts, location, elevation, value, false);
      } else if (value instanceof String) {
        GTSHelper.setValue(gts[3], ts, location, elevation, value, false);
      } else if (value instanceof byte[]) {
        GTSHelper.setValue(gts[4], ts, location, elevation, value, false);
      }
    }
    
    // Sort the 5 GTS using fullsort so we get a deterministic order
    // in the presence of duplicate ticks
    
    for (int i = 0; i < gts.length; i++) {
      GTSHelper.fullsort(gts[i], reversed);
    }
    
    // Now merge the GTS in time order with the type precedence of the 'gts' array
    
    enc = new GTSEncoder(baseTimestamp);
    enc.setMetadata(encoder.getMetadata());
    
    int[] idx = new int[gts.length];
    
    while (true) {
      // Determine the next GTS to add from its timestamp, lowest first
      int gtsidx = -1;
      
      long ts = Long.MAX_VALUE;
      for (int i = 0; i < gts.length; i++) {
        if (idx[i] >= GTSHelper.nvalues(gts[i])) {
          continue;
        }
        long tick = GTSHelper.tickAtIndex(gts[i], idx[i]);
        if (-1 == gtsidx || tick < ts) {
          gtsidx = i;
          ts = tick;
        }
      }
      
      if (-1 == gtsidx) {
        break;
      }
      
      do {
        long location = GTSHelper.locationAtIndex(gts[gtsidx], idx[gtsidx]);
        long elevation = GTSHelper.elevationAtIndex(gts[gtsidx], idx[gtsidx]);
        Object value = GTSHelper.valueAtIndex(gts[gtsidx], idx[gtsidx]);
        
        if (4 == gtsidx) { // BINARY
          value = value.toString().getBytes(StandardCharsets.ISO_8859_1);
        } else if (2 == gtsidx) { // DOUBLE
          // Attempt to optimize the value
          value = GTSEncoder.optimizeValue(value);
        }
        
        enc.addValue(ts, location, elevation, value);
        
        idx[gtsidx]++;
      } while (idx[gtsidx] < GTSHelper.nvalues(gts[gtsidx]) && GTSHelper.tickAtIndex(gts[gtsidx], idx[gtsidx]) == ts);            
    }
    
    return enc;
  }
  
  /**
   * Option for the binarySearchTick function.
   * In case of duplicate ticks in a GTS, specify which index to return.
   *
   * ARBITRARY: binarySearchTick will return an arbitrary index corresponding to a matching tick.
   * FIRST:     binarySearchTick will return the lowest index corresponding to a matching tick.
   * LAST:      binarySearchTick will return the highest index corresponding to a matching tick.
   */
  public enum BinarySearchTickChoice {
    ARBITRARY, FIRST, LAST
  }

  public static final int binarySearchTick(GeoTimeSerie gts, long timestamp, BinarySearchTickChoice tickChoice) {
    return binarySearchTick(gts, 0, gts.values, timestamp, tickChoice);
  }

  /**
   * Gives the index of the first/last/arbitrary instance of the given timestamp in a sorted GTS.
   * Similar in principle to Arrays.binarySearch
   *
   * @param gts       The GTS in which the timestamp is searched
   * @param timestamp The searched timestamp
   * @return the index of the last instance of the given timestamp in a sorted GTS or (-(insertion point) - 1).
   */
  public static final int binarySearchTick(GeoTimeSerie gts, int fromIndex, int toIndex, long timestamp, BinarySearchTickChoice tickChoice) {

    // If no ticks
    if (null == gts.ticks) {
      return -1;
    }

    // Make sure the GTS is sorted
    sort(gts, gts.reversed);

    int low = fromIndex;
    int high = toIndex - 1;
    int resIndex = -1;
    int compFactor = gts.reversed ? -1 : 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = gts.ticks[mid];

      int comp = Long.compare(midVal, timestamp) * compFactor;

      if (0 > comp) {
        low = mid + 1;
      } else if (0 < comp) {
        high = mid - 1;
      } else {
        if (BinarySearchTickChoice.ARBITRARY == tickChoice) {
          return mid;
        }
        resIndex = mid;
        if (BinarySearchTickChoice.FIRST == tickChoice) {
          high = mid - 1;
        } else { // BinarySearchTickChoice.LAST == tickChoice
          low = mid + 1;
        }
      }
    }

    if (0 <= resIndex) { // Key found
      return resIndex;
    } else { // Key not found, return insertion point: -(insertion_point + 1)
      if (gts.reversed) {
        return -(high + 1);
      } else {
        return -(low + 1);
      }
    }
  }
  
  public static final GeoTimeSerie valueSort(GeoTimeSerie gts, boolean reversed) {
    gts.sorted = false;

    quicksortByValue(gts, 0, gts.values - 1, reversed);

    return gts;
  }

  public static final GeoTimeSerie valueSort(GeoTimeSerie gts) {
    return valueSort(gts, false);
  }

  /**
   * Return an iterator on the GeoTimeSerie ticks.
   * 
   * If the GTS is bucketized, one tick per bucket is returned,
   * otherwise only the ticks for which there are values are returned.
   * 
   * If the GTS is not bucketized, the ticks are first sorted in natural ordering.
   * There is no tick deduplication.
   * 
   * 
   * @param gts GeoTimeSerie instance for which to return an iterator
   * @param reversed If true, ticks will be returned from most recent to oldest
   * @return an iterator on the GeoTimeSerie ticks.
   */
  public static final Iterator<Long> tickIterator(GeoTimeSerie gts, final boolean reversed) {
    final GeoTimeSerie itergts = gts;
    
    if (!isBucketized(gts)) {
      
      //
      // GTS is not bucketized, return the ticks
      //
      
      sort(gts, false);
      
      return new Iterator<Long>() {

        int idx = reversed ? itergts.values - 1: 0;
        
        @Override
        public boolean hasNext() {
          return reversed ? (idx > 0) : (idx < itergts.values);
        }
        
        @Override
        public Long next() {
          return itergts.ticks[reversed ? idx-- : idx++];
        };
        @Override
        public void remove() {
          // NOOP          
        }
      };
    } else {
      
      //
      // GTS is bucketized
      //
      
      return new Iterator<Long>() {
        long bucket = reversed ? 0 : itergts.bucketcount - 1;
        
        @Override
        public boolean hasNext() {
          return reversed ? bucket < itergts.bucketcount : bucket >= 0;
        }
        @Override
        public Long next() {
          long ts = itergts.lastbucket - bucket * itergts.bucketspan;
          
          if (reversed) {
            bucket++;
          } else {
            bucket--;
          }
          
          return ts;
        }
        @Override
        public void remove() {
          // NOOP          
        }
      };
    }
  }
  
  /**
   * Sort a range of values/locations/elevations of a GeoTimeSerie instance
   * according to the ascending order of its ticks
   * 
   * @param gts GeoTimeSerie instance to sort
   * @param low Lowest index of range to sort
   * @param high Highest index of range to sort
   */
  private static final void quicksort(GeoTimeSerie gts, int low, int high, boolean reversed) { 
    
    if (0 == gts.values) {
      return;
    }
    
    List<int[]> ranges = new ArrayList<int[]>();
    
    ranges.add(new int[] { low, high });
    
    while(!ranges.isEmpty()) {
      int[] range = ranges.remove(0);
      low = range[0];
      high = range[1];
      
      int i = low, j = high;
      // Get the pivot element from the middle of the list
      long pivot = gts.ticks[low + (high-low)/2];

      // Divide into two lists
      while (i <= j) {
        // If the current value from the left list is smaller
        // (or greater if reversed is true) than the pivot
        // element then get the next element from the left list
        while ((!reversed && gts.ticks[i] < pivot) || (reversed && gts.ticks[i] > pivot)) {
          i++;
        }
        // If the current value from the right list is larger (or lower if reversed is true)
        // than the pivot element then get the next element from the right list
        while ((!reversed && gts.ticks[j] > pivot) || (reversed && gts.ticks[j] < pivot)) {
          j--;
        }

        // If we have found a values in the left list which is larger then
        // the pivot element and if we have found a value in the right list
        // which is smaller then the pivot element then we exchange the
        // values.
        // As we are done we can increase i and j
        if (i <= j) {
          if (i != j) {
            long tmplong = gts.ticks[i];
            gts.ticks[i] = gts.ticks[j];
            gts.ticks[j] = tmplong;
            
            if (null != gts.locations) {
              tmplong = gts.locations[i];
              gts.locations[i] = gts.locations[j];
              gts.locations[j] = tmplong;          
            }
            
            if (null != gts.elevations) {
              tmplong = gts.elevations[i];
              gts.elevations[i] = gts.elevations[j];
              gts.elevations[j] = tmplong;          
            }
            
            if (TYPE.LONG == gts.type) {
              tmplong = gts.longValues[i];
              gts.longValues[i] = gts.longValues[j];
              gts.longValues[j] = tmplong;          
            } else if (TYPE.DOUBLE == gts.type) {
              double tmpdouble = gts.doubleValues[i];
              gts.doubleValues[i] = gts.doubleValues[j];
              gts.doubleValues[j] = tmpdouble;
            } else if (TYPE.STRING == gts.type) {
              String tmpstring = gts.stringValues[i];
              gts.stringValues[i] = gts.stringValues[j];
              gts.stringValues[j] = tmpstring;
            } else if (TYPE.BOOLEAN == gts.type) {
              boolean tmpboolean = gts.booleanValues.get(i);
              gts.booleanValues.set(i, gts.booleanValues.get(j));
              gts.booleanValues.set(j, tmpboolean);
            }            
          }

          i++;
          j--;
        }
      }
      
      // Recursion
      if (low < j) {
        //quicksort(gts, low, j, reversed);
        ranges.add(new int[] { low, j });
      }
      if (i < high) {   
        //quicksort(gts, i, high, reversed);
        ranges.add(new int[] { i, high });
      }      
    }    
  }

  private static final void quicksortByValue(GeoTimeSerie gts, int low, int high, boolean reversed) { 
    
    if (0 == gts.values) {
      return;
    }
    
    List<int[]> ranges = new ArrayList<int[]>();
    
    ranges.add(new int[] { low, high });
    
    while(!ranges.isEmpty()) {
      int[] range = ranges.remove(0);
      low = range[0];
      high = range[1];
      
      int i = low, j = high;
      // Get the pivot element from the middle of the list
      long lpivot = 0L;
      double dpivot = 0.0D;
      String spivot = null;
      
      TYPE type = gts.getType();
      
      if (TYPE.LONG == type) {
        lpivot = gts.longValues[low + (high-low)/2];
      } else if (TYPE.DOUBLE == type) {
        dpivot = gts.doubleValues[low + (high-low)/2];
      } else if (TYPE.STRING == type) {
        spivot = gts.stringValues[low + (high-low)/2];       
      } else if (TYPE.BOOLEAN == type) {
        // Do nothing for booleans
        return;
      }
      
      long pivotTick = gts.ticks[low + (high-low) / 2];
      
      // Divide into two lists
      while (i <= j) {
        
        if (TYPE.LONG == type) {
          
          
          if (!reversed) {
            // If the current value from the left list is smaller
            // (or greater if reversed is true) than the pivot
            // element then get the next element from the left list        
            while(gts.longValues[i] < lpivot || (gts.longValues[i] == lpivot && gts.ticks[i] < pivotTick)) {
              i++;
            }
            // If the current value from the right list is larger (or lower if reversed is true)
            // than the pivot element then get the next element from the right list
            while(gts.longValues[j] > lpivot || (gts.longValues[j] == lpivot && gts.ticks[j] > pivotTick)) {
              j--;
            }
          } else {
            while(gts.longValues[i] > lpivot || (gts.longValues[i] == lpivot && gts.ticks[i] > pivotTick)) {
              i++;
            }
            while(gts.longValues[j] < lpivot || (gts.longValues[j] == lpivot && gts.ticks[j] < pivotTick)) {
              j--;
            }
          }
        } else if (TYPE.DOUBLE == type) {        
          if (!reversed) {
            // If the current value from the left list is smaller
            // (or greater if reversed is true) than the pivot
            // element then get the next element from the left list        
            while(gts.doubleValues[i] < dpivot || (gts.doubleValues[i] == dpivot && gts.ticks[i] < pivotTick)) {
              i++;
            }
            // If the current value from the right list is larger (or lower if reversed is true)
            // than the pivot element then get the next element from the right list
            while(gts.doubleValues[j] > dpivot || (gts.doubleValues[j] == dpivot && gts.ticks[j] > pivotTick)) {
              j--;
            }
          } else {
            while(gts.doubleValues[i] > dpivot || (gts.doubleValues[i] == dpivot && gts.ticks[i] > pivotTick)) {
              i++;
            }
            while(gts.doubleValues[j] < dpivot || (gts.doubleValues[j] == dpivot && gts.ticks[j] < pivotTick)) {
              j--;
            }
          }
        } else if (TYPE.STRING == type) {
          if (!reversed) {
            // If the current value from the left list is smaller
            // (or greater if reversed is true) than the pivot
            // element then get the next element from the left list        
            while(gts.stringValues[i].compareTo(spivot) < 0 || (0 == gts.stringValues[i].compareTo(spivot) && gts.ticks[i] < pivotTick)) {
              i++;
            }
            // If the current value from the right list is larger (or lower if reversed is true)
            // than the pivot element then get the next element from the right list
            while(gts.stringValues[j].compareTo(spivot) > 0 || (0 == gts.stringValues[j].compareTo(spivot) && gts.ticks[j] > pivotTick)) {
              j--;
            }
          } else {
            while(gts.stringValues[i].compareTo(spivot) > 0 || (0 == gts.stringValues[i].compareTo(spivot) && gts.ticks[i] > pivotTick)) {
              i++;
            }
            while(gts.stringValues[j].compareTo(spivot) < 0 || (0 == gts.stringValues[j].compareTo(spivot) && gts.ticks[j] < pivotTick)) {
              j--;
            }
          }
        }

        // If we have found a values in the left list which is larger then
        // the pivot element and if we have found a value in the right list
        // which is smaller then the pivot element then we exchange the
        // values.
        // As we are done we can increase i and j
        if (i <= j) {
          if (TYPE.LONG == gts.type) {
            long tmplong = gts.longValues[i];
            gts.longValues[i] = gts.longValues[j];
            gts.longValues[j] = tmplong;          
          } else if (TYPE.DOUBLE == gts.type) {
            double tmpdouble = gts.doubleValues[i];
            gts.doubleValues[i] = gts.doubleValues[j];
            gts.doubleValues[j] = tmpdouble;            
          } else if (TYPE.STRING == gts.type) { 
            String tmpstring = gts.stringValues[i];
            gts.stringValues[i] = gts.stringValues[j];
            gts.stringValues[j] = tmpstring;            
          } else if (TYPE.BOOLEAN == gts.type) {
            boolean tmpboolean = gts.booleanValues.get(i);
            gts.booleanValues.set(i, gts.booleanValues.get(j));
            gts.booleanValues.set(j, tmpboolean);
          }

          long tmplong = gts.ticks[i];
          gts.ticks[i] = gts.ticks[j];
          gts.ticks[j] = tmplong;
            
          if (null != gts.locations) {
            tmplong = gts.locations[i];
            gts.locations[i] = gts.locations[j];
            gts.locations[j] = tmplong;          
          }
            
          if (null != gts.elevations) {
            tmplong = gts.elevations[i];
            gts.elevations[i] = gts.elevations[j];
            gts.elevations[j] = tmplong;          
          }
          
          i++;
          j--;
        }
      }
      
      // Recursion
      if (low < j) {
        //quicksortByValue(gts, low, j, reversed);
        ranges.add(new int[] { low, j });
      }
      if (i < high) {   
        //quicksortByValue(gts, i, high, reversed);
        ranges.add(new int[] { i, high });
      }
    }
  }

  /**
   * Sort GTS according to location, using HHCodes, between two indexes.
   * The ticks with no locations are considered the smallest.
   * 
   * @param gts GeoTimeSerie instance to sort.
   * @param low Lower index, only indexes higher or equal to this value will be sorted.
   * @param high Higher index, only indexes lower or equal to this value will be sorted.
   * @param reversed Whether to reverse the order of the resulting GTS or not.
   */
  private static final void quicksortByLocation(GeoTimeSerie gts, int low, int high, boolean reversed) { 
    
    if (0 == gts.values) {
      return;
    }
    
    if (null == gts.locations) {
      return;
    }
        
    List<int[]> ranges = new ArrayList<int[]>();
    
    ranges.add(new int[] { low, high });
    
    while(!ranges.isEmpty()) {
      int[] range = ranges.remove(0);
      low = range[0];
      high = range[1];

      int i = low, j = high;
      // Get the pivot element from the middle of the list
      long pivot = gts.locations[low + (high - low) / 2];
      
      long pivotTick = gts.ticks[low + (high - low) / 2];
      
      // Divide into two lists
      while (i <= j) {
        
        if (!reversed) {
          // If the current value from the left list is smaller
          // (or greater if reversed is true) than the pivot
          // element then get the next element from the left list        
          while ((pivot != GeoTimeSerie.NO_LOCATION && (gts.locations[i] == GeoTimeSerie.NO_LOCATION || gts.locations[i] < pivot))
              || (gts.locations[i] == pivot && gts.ticks[i] < pivotTick)) {
            i++;
          }

          // If the current value from the right list is larger (or lower if reversed is true)
          // than the pivot element then get the next element from the right list
          while ((gts.locations[j] != GeoTimeSerie.NO_LOCATION && (pivot == GeoTimeSerie.NO_LOCATION || gts.locations[j] > pivot))
              || (gts.locations[j] == pivot && gts.ticks[j] > pivotTick)) {
            j--;
          }
        } else {
          while ((gts.locations[i] != GeoTimeSerie.NO_LOCATION && (pivot == GeoTimeSerie.NO_LOCATION || gts.locations[i] > pivot))
              || (gts.locations[i] == pivot && gts.ticks[i] > pivotTick)) {
            i++;
          }
          while ((pivot != GeoTimeSerie.NO_LOCATION && (gts.locations[j] == GeoTimeSerie.NO_LOCATION || gts.locations[j] < pivot))
              || (gts.locations[j] == pivot && gts.ticks[j] < pivotTick)) {
            j--;
          }
        }
    
        // If we have found a values in the left list which is larger then
        // the pivot element and if we have found a value in the right list
        // which is smaller then the pivot element then we exchange the
        // values.
        // As we are done we can increase i and j
        if (i <= j) {
          if (TYPE.LONG == gts.type) {
            long tmplong = gts.longValues[i];
            gts.longValues[i] = gts.longValues[j];
            gts.longValues[j] = tmplong;          
          } else if (TYPE.DOUBLE == gts.type) {
            double tmpdouble = gts.doubleValues[i];
            gts.doubleValues[i] = gts.doubleValues[j];
            gts.doubleValues[j] = tmpdouble;            
          } else if (TYPE.STRING == gts.type) { 
            String tmpstring = gts.stringValues[i];
            gts.stringValues[i] = gts.stringValues[j];
            gts.stringValues[j] = tmpstring;            
          } else if (TYPE.BOOLEAN == gts.type) {
            boolean tmpboolean = gts.booleanValues.get(i);
            gts.booleanValues.set(i, gts.booleanValues.get(j));
            gts.booleanValues.set(j, tmpboolean);
          }

          long tmplong = gts.ticks[i];
          gts.ticks[i] = gts.ticks[j];
          gts.ticks[j] = tmplong;
            
          if (null != gts.locations) {
            tmplong = gts.locations[i];
            gts.locations[i] = gts.locations[j];
            gts.locations[j] = tmplong;          
          }
            
          if (null != gts.elevations) {
            tmplong = gts.elevations[i];
            gts.elevations[i] = gts.elevations[j];
            gts.elevations[j] = tmplong;          
          }
          
          i++;
          j--;
        }
      }
      
      // Recursion
      if (low < j) {
        //quicksortByLocation(gts, low, j, reversed);
        ranges.add(new int[] { low, j });
      }
      if (i < high) {   
        //quicksortByLocation(gts, i, high, reversed);
        ranges.add(new int[] { i, high });
      }      
    }
  }

  public static GeoTimeSerie locationSort(GeoTimeSerie gts) {
    gts.sorted = false;
    
    quicksortByLocation(gts,0,gts.values - 1,false);

    return gts;
  }

  /**
   * Compare data in a GTS at given indexes. Compare ticks, then if equal, values, then if equal locations then if equal elevations.
   * Be careful, no check is done on the validity of the indexes.
   * @param gts The GTS to get the data form.
   * @param index1 The first index to get the data at in the GTS.
   * @param index2 The second index to get the data at in the GTS.
   * @return -1 if the data at first given index is considered before, 1 if considered after else 0. Comparison is done using natural ordering and false before true.
   */
  public static int compareAllAtTick(GeoTimeSerie gts, int index1, int index2) {
    if (gts.ticks[index1] < gts.ticks[index2]) {
      return -1;
    } else if (gts.ticks[index1] > gts.ticks[index2]) {
      return 1;
    }

    // if ticks are equals, test values
    if (TYPE.LONG == gts.type) {
      if (gts.longValues[index1] < gts.longValues[index2]) {
        return -1;
      } else if (gts.longValues[index1] > gts.longValues[index2]) {
        return 1;
      }
    } else if (TYPE.DOUBLE == gts.type) {
      if (gts.doubleValues[index1] < gts.doubleValues[index2]) {
        return -1;
      } else if (gts.doubleValues[index1] > gts.doubleValues[index2]) {
        return 1;
      }
    } else if (TYPE.STRING == gts.type) {
      return gts.stringValues[index1].compareTo(gts.stringValues[index2]);
    } else if (TYPE.BOOLEAN == gts.type) {
      if (!gts.booleanValues.get(index1) && gts.booleanValues.get(index2)) {
        return -1;
      } else if (gts.booleanValues.get(index1) && !gts.booleanValues.get(index2)) {
        return 1;
      }
    }

    // if ticks and values are equals, test locations
    if (null != gts.locations) {
      if (gts.locations[index1] < gts.locations[index2]) {
        if (GeoTimeSerie.NO_LOCATION == gts.locations[index2]) {
          return 1;
        }
        return -1;
      } else if (gts.locations[index1] > gts.locations[index2]) {
        if (GeoTimeSerie.NO_LOCATION == gts.locations[index1]) {
          return -1;
        }
        return 1;
      }
    }

    // if ticks, values and locations are equal, test elevation
    if (null != gts.elevations) {
      if (gts.elevations[index1] < gts.elevations[index2]) {
        if (GeoTimeSerie.NO_ELEVATION == gts.elevations[index2]) {
          return 1;
        }
        return -1;
      } else if (gts.elevations[index1] > gts.elevations[index2]) {
        if (GeoTimeSerie.NO_ELEVATION == gts.elevations[index1]) {
          return -1;
        }
        return 1;
      }
    }

    return 0; // Equality
  }

  /**
   * Apply a quicksort on the given GTS instance using all the data at each tick to make the comparisons.
   * Use natural ordering to first order according to ticks then values, then locations, then elevations.
   * @param gts The GTS to be sorted, will be modified in place.
   * @param low Lowest considered index in the GTS.
   * @param high Highest considered index in the GTS.
   * @param reversed Whether to return a reversed GTS or not.
   */
  private static void fullquicksort(GeoTimeSerie gts, int low, int high, final boolean reversed) {

    if (0 == gts.values) {
      return;
    }

    List<int[]> ranges = new ArrayList<int[]>();

    ranges.add(new int[]{low, high});

    fullquicksort(gts, ranges, reversed);
  }

  /**
   * Apply a quicksort on the given GTS instance using all the data at each tick to make the comparisons.
   * Use natural ordering to first order according to ticks then values, then locations, then elevations.
   * @param gts The GTS to be sorted, will be modified in place.
   * @param ranges Ranges of indexes to sort in the GTS.
   * @param reversed Whether to return a reversed GTS or not.
   */
  private static void fullquicksort(GeoTimeSerie gts, List<int[]> ranges, final boolean reversed) {

    if (0 == gts.values) {
      return;
    }

    int low;
    int high;

    while (!ranges.isEmpty()) {
      int[] range = ranges.remove(0);
      low = range[0];
      high = range[1];

      int i = low, j = high;
      // Get the pivot element from the middle of the list
      int pivotIndex = low + (high - low) / 2;
      int reverseComp = reversed ? -1 : 1;

      // Divide into two lists
      while (i <= j) {
        // If the current value from the left list is smaller
        // (or greater if reversed is true) than the pivot
        // element then get the next element from the left list
        while (reverseComp * compareAllAtTick(gts, i, pivotIndex) < 0) {
          i++;
        }
        // If the current value from the right list is larger (or lower if reversed is true)
        // than the pivot element then get the next element from the right list
        while (reverseComp * compareAllAtTick(gts, j, pivotIndex) > 0) {
          j--;
        }

        // If we have found a value in the left list which is larger than
        // the pivot element and if we have found a value in the right list
        // which is smaller then the pivot element then we exchange the
        // values.
        // As we are done we can increase i and j
        if (i <= j) {
          if (i != j) {
            long tmplong = gts.ticks[i];
            gts.ticks[i] = gts.ticks[j];
            gts.ticks[j] = tmplong;

            if (null != gts.locations) {
              tmplong = gts.locations[i];
              gts.locations[i] = gts.locations[j];
              gts.locations[j] = tmplong;
            }

            if (null != gts.elevations) {
              tmplong = gts.elevations[i];
              gts.elevations[i] = gts.elevations[j];
              gts.elevations[j] = tmplong;
            }

            if (TYPE.LONG == gts.type) {
              tmplong = gts.longValues[i];
              gts.longValues[i] = gts.longValues[j];
              gts.longValues[j] = tmplong;
            } else if (TYPE.DOUBLE == gts.type) {
              double tmpdouble = gts.doubleValues[i];
              gts.doubleValues[i] = gts.doubleValues[j];
              gts.doubleValues[j] = tmpdouble;
            } else if (TYPE.STRING == gts.type) {
              String tmpstring = gts.stringValues[i];
              gts.stringValues[i] = gts.stringValues[j];
              gts.stringValues[j] = tmpstring;
            } else if (TYPE.BOOLEAN == gts.type) {
              boolean tmpboolean = gts.booleanValues.get(i);
              gts.booleanValues.set(i, gts.booleanValues.get(j));
              gts.booleanValues.set(j, tmpboolean);
            }

            // Update pivotIndex if either i or j
            if (pivotIndex == i) {
              pivotIndex = j;
            } else if (pivotIndex == j) {
              pivotIndex = i;
            }
          }

          i++;
          j--;
        }
      }

      // Recursion
      if (low < j) {
        //quicksort(gts, low, j, reversed);
        ranges.add(new int[]{low, j});
      }
      if (i < high) {
        //quicksort(gts, i, high, reversed);
        ranges.add(new int[]{i, high});
      }
    }
  }

  /**
   * Return the tick at a given index in a GeoTimeSerie.
   * 
   * @param gts GeoTimeSerie instance to get the tick from.
   * @param idx Index of the tick.
   * @return the tick value or Long.MIN_VALUE if no tick at that index.
   */
  public static long tickAtIndex(GeoTimeSerie gts, int idx) {
    if (0 > idx || idx >= gts.values) {
      return Long.MIN_VALUE;
    } else {
      return gts.ticks[idx];
    }
  }

  /**
   * Return a list with the ticks in a GeoTimeSerie.
   *
   * @return a list with the ticks in a GeoTimeSerie.
   */
  public static List<Long> tickList(GeoTimeSerie gts) {
    List<Long> ticks = new ArrayList<Long>(gts.values);

    if (gts.values > 0) {
      ticks.addAll(Arrays.asList(ArrayUtils.toObject(Arrays.copyOf(gts.ticks, gts.values))));
    }
    
    return ticks;
  }

  public static int indexAtTick(GeoTimeSerie gts, long tick) {
    
    if (0 == gts.values) {
      return -1;
    }
    
    sort(gts, false);
    
    //
    // Attempt to locate the tick
    //
    
    int idx = Arrays.binarySearch(gts.ticks, 0, gts.values, tick);

    if (idx < 0) {
      return -1;      
    }
    
    return idx;
  }
  
  /**
   * Return the value in a Geo Time Serie at a given timestamp.
   * 
   * The GeoTimeSerie instance will be sorted if it is not already
   * 
   * @param gts GeoTimeSerie instance from which to extract value
   * @param tick Timestamp at which to read the value
   * @return The value at 'tick' or null if none exists
   */
  public static Object valueAtTick(GeoTimeSerie gts, long tick) {
    
    if (0 == gts.values) {
      return null;
    }
    
    //
    // Force sorting in natural ordering of ticks
    //
    
    sort(gts, false);
    
    //
    // Attempt to locate the tick
    //
    
    int idx = Arrays.binarySearch(gts.ticks, 0, gts.values, tick);

    if (idx < 0) {
      return null;
    } else {
      if (TYPE.LONG == gts.type) {
        return gts.longValues[idx];
      } else if (TYPE.DOUBLE == gts.type) {
        return gts.doubleValues[idx];
      } else if (TYPE.STRING == gts.type) {
        return gts.stringValues[idx];
      } else if (TYPE.BOOLEAN == gts.type) {
        return gts.booleanValues.get(idx);
      } else {
        return null;
      }
    }        
  }
  
  /**
   * Return the value in a GTS instance at a given index.
   * Return null if no value exists for the given index.
   * 
   * @param gts GeoTimeSerie instance from which to extract the value.
   * @param idx Index at which to read the value.
   * @return The value at the given tick.
   */
  public static Object valueAtIndex(GeoTimeSerie gts, int idx) {
    if (idx >= gts.values) {
      return null;
    }
    if (TYPE.LONG == gts.type) {
      return gts.longValues[idx];
    } else if (TYPE.DOUBLE == gts.type) {
      return gts.doubleValues[idx];
    } else if (TYPE.STRING == gts.type) {
      return gts.stringValues[idx];
    } else if (TYPE.BOOLEAN == gts.type) {
      return gts.booleanValues.get(idx);
    } else {
      return null;
    }
  }
  
  /**
   * Return the location in a Geo Time Serie at a given timestamp.
   * 
   * The GeoTimeSerie instance will be sorted if it is not already.
   * 
   * @param gts GeoTimeSerie instance from which to extract location
   * @param tick Timestamp at which to read the location
   * @return The location at 'tick' (NO_LOCATION if none set).
   */
  public static long locationAtTick(GeoTimeSerie gts, long tick) {
    
    if (null == gts.locations) {
      return GeoTimeSerie.NO_LOCATION;
    }
    
    sort(gts, false);

    int idx = Arrays.binarySearch(gts.ticks, 0, gts.values, tick);
    
    if (idx < 0) {
      return GeoTimeSerie.NO_LOCATION;
    } else {
      return gts.locations[idx];
    }
  }

  /**
   * Return the location in a GeoTimeSerie at a given index.
   * 
   * @param gts GeoTimeSerie instance from which to extract the location.
   * @param idx Index at which to read the location.
   * @return The location in a GeoTimeSerie at a given index.
   */
  public static long locationAtIndex(GeoTimeSerie gts, int idx) {
    if (null == gts.locations || 0 > idx || idx >= gts.values) {
      return GeoTimeSerie.NO_LOCATION;
    } else {
      return gts.locations[idx];
    }    
  }

  /**
   * Set the location at a specific index in the GTS
   * 
   * @param gts GeoTimeSerie instance to be modified.
   * @param idx Index at which to write the location.
   * @param location Location as HHCode to write.
   */
  public static void setLocationAtIndex(GeoTimeSerie gts, int idx, long location) {
    if (idx >= gts.values) {
      return;
    }
    
    if (null != gts.locations) {
      gts.locations[idx] = location;
    } else {
      if (GeoTimeSerie.NO_LOCATION != location) {
        gts.locations = new long[gts.values];
        Arrays.fill(gts.locations, GeoTimeSerie.NO_LOCATION);
        gts.locations[idx] = location;
      }
    }
  }
  
  /**
   * Return the elevation in a Geo Time Serie at a given timestamp.
   * 
   * The GeoTimeSerie instance will be sorted if it is not already.
   * 
   * @param gts GeoTimeSerie instance from which to extract elevation
   * @param tick Timestamp at which to read the elevation
   * @return The elevation at 'tick' (NO_ELEVATION if none set).
   */
  public static long elevationAtTick(GeoTimeSerie gts, long tick) {
    
    if (null == gts.elevations) {
      return GeoTimeSerie.NO_ELEVATION;
    }
    
    sort(gts, false);

    int idx = Arrays.binarySearch(gts.ticks, 0, gts.values, tick);
    
    if (idx < 0) {
      return GeoTimeSerie.NO_ELEVATION;
    } else {
      return gts.elevations[idx];
    }
  }
  /**
   * Set the elevation at a specific index in the GTS
   * 
   * @param gts GeoTimeSerie instance to be modified.
   * @param idx Index at which to write the elevation.
   * @param elevation Elevation to write.
   */
  public static void setElevationAtIndex(GeoTimeSerie gts, int idx, long elevation) {
    if (idx >= gts.values) {
      return;
    }
    
    if (null != gts.elevations) {
      gts.elevations[idx] = elevation;
    } else {
      if (GeoTimeSerie.NO_ELEVATION != elevation) {
        gts.elevations = new long[gts.values];
        Arrays.fill(gts.elevations, GeoTimeSerie.NO_ELEVATION);
        gts.elevations[idx] = elevation;
      }
    }
  }
  
  /**
   * Return the elevation in a GeoTimeSerie at a given index
   * 
   * @param gts GeoTimeSerie instance from which to extract elevation.
   * @param idx Index at which to read the elevation
   * @return The elevation in the GeoTimeSerie at a given index (NO_ELEVATION if none set).
   */
  public static long elevationAtIndex(GeoTimeSerie gts, int idx) {
    if (null == gts.elevations || 0 > idx || idx >= gts.values) {
      return GeoTimeSerie.NO_ELEVATION;
    } else {
      return gts.elevations[idx];
    }    
  }
  
  /**
   * Remove a datapoint from a GTS.
   * 
   * @param gts The GTS to alter
   * @param timestamp The timestamp at which to remove the value
   * @param all Boolean indicating whether or not we should remove all occurrences or simply the first one found
   */
  public static void removeValue(GeoTimeSerie gts, long timestamp, boolean all) {    
    GeoTimeSerie altered = gts.cloneEmpty(gts.values);
    
    int todelete = Integer.MAX_VALUE;
    
    if (all) {
      todelete = 1;
    }
    
    for (int i = 0; i < gts.values; i++) {
      if (todelete > 1 && timestamp == gts.ticks[i]) {
        todelete--;
        continue;
      }
      GTSHelper.setValue(altered, gts.ticks[i], GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), GTSHelper.valueAtIndex(gts, i), false);
    }
  }

  /**
   * Add a measurement at the given timestamp/location/elevation
   * 
   * FIXME(hbs): If performance is bad due to autoboxing, we can always split addValue by type
   *             and have 4 methods instead of 1.
   *             
   * @param gts GeoTimeSerie instance to which the measurement must be added
   * @param timestamp Timestamp in microseconds of the measurement
   * @param geoxppoint Location of measurement as a GeoXPPoint
   * @param elevation Elevation of measurement in millimeters
   * @param value Value of measurement
   * @param overwrite Flag indicating whether or not to overwrite a previous measurement done at the same timestamp
   * 
   * @return The number of values in the GeoTimeSerie, including the one that was just added
   */
  
  public static final int setValue(GeoTimeSerie gts, long timestamp, long geoxppoint, long elevation, Object value, boolean overwrite) {
    
    //
    // Ignore nulls
    //
    
    if (null == value) {
      return gts.values;
    }
      
    if (value instanceof byte[]) {
      value = new String((byte[]) value, StandardCharsets.ISO_8859_1);
    }
    
    //
    // If 'overwrite' is true, check if 'timestamp' is already in 'ticks'
    // If so, record new value there.
    //

    int idx = gts.values;
    
    if (overwrite) {
      // Use binary search if possible
      if (gts.sorted) {
        int possibleIndex = binarySearchTick(gts, timestamp, BinarySearchTickChoice.FIRST);
        // If the tick is found, change idx
        if (0 <= possibleIndex) {
          idx = possibleIndex;
        }
      } else { // GTS is not sorted, scan the ticks
        for (int i = 0; i < gts.values; i++) {
          if (timestamp == gts.ticks[i]) {
            idx = i;
            break;
          }
        }
      }
    }
        
    //
    // Provision memory allocation for the new value.
    //

    if (gts.values == idx) {
      // Try to keep 'sorted' flag if possible
      if (2 > gts.values) { // Optimization to only make one check on most cases
        if (0 == gts.values) {
          gts.sorted = true;
          gts.reversed = false;
        } else { // 1 == gts.values
          gts.sorted = true;
          gts.reversed = gts.ticks[0] > timestamp;
        }
      } else if (gts.sorted) { // Simple check, if all values are equal we could keep checking
        if (gts.reversed) {
          gts.sorted = gts.ticks[gts.values - 1] >= timestamp;
        } else {
          gts.sorted = gts.ticks[gts.values - 1] <= timestamp;
        }
      }

      if (TYPE.UNDEFINED == gts.type || null == gts.ticks || gts.values >= gts.ticks.length || (null == gts.locations && GeoTimeSerie.NO_LOCATION != geoxppoint) || (null == gts.elevations && GeoTimeSerie.NO_ELEVATION != elevation)) {
        provision(gts, value, geoxppoint, elevation);
      }
    } else if ((null == gts.locations && GeoTimeSerie.NO_LOCATION != geoxppoint) || (null == gts.elevations && GeoTimeSerie.NO_ELEVATION != elevation)) {
      provision(gts, value, geoxppoint, elevation);      
    }
    
    //
    // Record timestamp, location, elevation
    //
  
    gts.ticks[idx] = timestamp;
    
    if (null != gts.locations) {
      gts.locations[idx] = geoxppoint;
    }
    
    if (null != gts.elevations) {
      gts.elevations[idx] = elevation;
    }
    
    //
    // Record value, doing conversions if need be
    //
    
    if (value instanceof Boolean) {
      if (TYPE.LONG == gts.type) {
        gts.longValues[idx] = ((Boolean) value).booleanValue() ? 1L : 0L;
      } else if (TYPE.DOUBLE == gts.type) {
        gts.doubleValues[idx] = ((Boolean) value).booleanValue() ? 1.0D : 0.0D;
      } else if (TYPE.STRING == gts.type) {
        gts.stringValues[idx] = ((Boolean) value).booleanValue() ? "T" : "F";
      } else if (TYPE.BOOLEAN == gts.type) {
        gts.booleanValues.set(idx, ((Boolean) value).booleanValue());
      }      
    } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
      if (TYPE.LONG == gts.type) {
        gts.longValues[idx] = ((Number) value).longValue(); 
      } else if (TYPE.DOUBLE == gts.type) {
        gts.doubleValues[idx] = ((Number) value).doubleValue();
      } else if (TYPE.STRING == gts.type) {
        gts.stringValues[idx] = ((Number) value).toString();
      } else if (TYPE.BOOLEAN == gts.type) {
        gts.booleanValues.set(idx, 0L != ((Number) value).longValue());
      }
    } else if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      if (TYPE.LONG == gts.type) {
        gts.longValues[idx] = ((Number) value).longValue(); 
      } else if (TYPE.DOUBLE == gts.type) {
        gts.doubleValues[idx] = ((Number) value).doubleValue();
      } else if (TYPE.STRING == gts.type) {
        gts.stringValues[idx] = value.toString();
      } else if (TYPE.BOOLEAN == gts.type) {
        gts.booleanValues.set(idx, 0.0D != ((Number) value).doubleValue());
      }      
    } else if (value instanceof String) {
      if (TYPE.LONG == gts.type) {
        try {
          gts.longValues[idx] = Long.parseLong((String) value);
        } catch (NumberFormatException nfe) {
          //
          // Attempt to parse a double
          //
          try {
            gts.longValues[idx] = (long) Double.parseDouble((String) value);
          } catch (NumberFormatException nfe2) {
            gts.longValues[idx] = 0L;            
          }          
        }
      } else if (TYPE.DOUBLE == gts.type) {
        try {
          gts.doubleValues[idx] = Double.parseDouble((String) value);
        } catch (NumberFormatException nfe) {
          try {
            gts.doubleValues[idx] = (double) Long.parseLong((String) value);
          } catch (NumberFormatException nfe2) {
            gts.doubleValues[idx] = 0.0D;
          }
        }
      } else if (TYPE.STRING == gts.type) {
        // Using intern is really CPU intensive
        gts.stringValues[idx] = (String) value; //.toString().intern();
      } else if (TYPE.BOOLEAN == gts.type) {
        gts.booleanValues.set(idx, !"".equals(value));
      }      
    } else {
      //
      // Ignore other types
      //
      
      return gts.values;
    }
    
    //
    // Increment number of stored values if we did not overwrite a previous one
    //
    
    if (gts.values == idx) {
      gts.values++;
    }

    return gts.values;
  }

  public static final int setValue(GeoTimeSerie gts, long timestamp, long geoxppoint, Object value) {
    return setValue(gts, timestamp, geoxppoint, GeoTimeSerie.NO_ELEVATION, value, false);
  }
  
  public static final int setValue(GeoTimeSerie gts, long timestamp, Object value) {
    return setValue(gts, timestamp, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, value, false);
  }
  
  /**
   * Allocate memory so we can add one value to the Geo Time Serie.
   * 
   * @param value The value that will be added, it is just used so we can allocate the correct container for the type.
   */
  private static final void provision(GeoTimeSerie gts, Object value, long location, long elevation) {
    //
    // Nothing to do if the ticks array is not full yet.
    //
    if (TYPE.UNDEFINED != gts.type && gts.values < gts.ticks.length) {
      if (GeoTimeSerie.NO_LOCATION == location && GeoTimeSerie.NO_ELEVATION == elevation) {
        return;
      }
      
      if (null == gts.locations && GeoTimeSerie.NO_LOCATION != location) {
        gts.locations = new long[gts.ticks.length];
        Arrays.fill(gts.locations, GeoTimeSerie.NO_LOCATION);
      }
      
      if (null == gts.elevations && GeoTimeSerie.NO_ELEVATION != elevation) {
        gts.elevations = new long[gts.ticks.length];
        Arrays.fill(gts.elevations, GeoTimeSerie.NO_ELEVATION);
      }
    } else if (TYPE.UNDEFINED != gts.type) {
      //
      // We need to grow 'ticks', 'locations', 'elevations' and associated value array.
      //
      
      int newlen = gts.ticks.length + (int) Math.min(GeoTimeSerie.MAX_ARRAY_GROWTH, Math.max(GeoTimeSerie.MIN_ARRAY_GROWTH, gts.ticks.length * GeoTimeSerie.ARRAY_GROWTH_FACTOR));

      if (newlen < gts.sizehint) {
        newlen = gts.sizehint;
      }
      
      gts.ticks = Arrays.copyOf(gts.ticks, newlen);
      if (null != gts.locations || GeoTimeSerie.NO_LOCATION != location) {
        if (null == gts.locations) {
          gts.locations = new long[gts.ticks.length];
          // Fill all values with NO_LOCATION since we are creating the array and thus
          // must consider all previous locations were undefined
          Arrays.fill(gts.locations, GeoTimeSerie.NO_LOCATION);
        } else {
          gts.locations = Arrays.copyOf(gts.locations, gts.ticks.length);
        }
      }
      if (null != gts.elevations || GeoTimeSerie.NO_ELEVATION != elevation) {
        if (null == gts.elevations) {
          gts.elevations = new long[gts.ticks.length];
          // Fill the newly allocated array with NO_ELEVATION since we must consider
          // all previous elevations were undefined
          Arrays.fill(gts.elevations, GeoTimeSerie.NO_ELEVATION);
        } else {
          gts.elevations = Arrays.copyOf(gts.elevations, gts.ticks.length);
        }
      }
      
      // BitSets grow automatically...
      if (TYPE.LONG == gts.type) {
        gts.longValues = Arrays.copyOf(gts.longValues, gts.ticks.length);
      } else if (TYPE.DOUBLE == gts.type) {
        gts.doubleValues = Arrays.copyOf(gts.doubleValues, gts.ticks.length);
      } else if (TYPE.STRING == gts.type) {
        gts.stringValues = Arrays.copyOf(gts.stringValues, gts.ticks.length);
      }
    } else { // TYPE.UNDEFINED == gts.type
      if (null == gts.ticks) {
        gts.ticks = new long[gts.sizehint > 0 ? gts.sizehint : GeoTimeSerie.MIN_ARRAY_GROWTH];
      }

      // Nullify location if no location is set (since the GTS is empty)
      if (GeoTimeSerie.NO_LOCATION == location) {
        gts.locations = null;
      } else if (null == gts.locations || gts.locations.length < gts.ticks.length) {
        gts.locations = new long[gts.ticks.length];
      }
            
      // Nullify elevation if no elevation is set (since the GTS is empty)
      if (GeoTimeSerie.NO_ELEVATION == elevation) {
        gts.elevations = null;
      } else if (null == gts.elevations || gts.elevations.length < gts.ticks.length) {
        gts.elevations = new long[gts.ticks.length];
      }
      
      if (value instanceof Boolean) {
        gts.type = TYPE.BOOLEAN;
        // BitSet capacity increases as booleans are added so there is no need to create another if it's too small
        if (null == gts.booleanValues) {
          gts.booleanValues = new BitSet(gts.ticks.length);
        }
      } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
        gts.type = TYPE.LONG;
        if (null == gts.longValues || gts.longValues.length < gts.ticks.length) {
          gts.longValues = new long[gts.ticks.length];
        }
      } else if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
        gts.type = TYPE.DOUBLE;
        if (null == gts.doubleValues || gts.doubleValues.length < gts.ticks.length) {
          gts.doubleValues = new double[gts.ticks.length];
        }
      } else if (value instanceof String) {
        gts.type = TYPE.STRING;
        if (null == gts.stringValues || gts.stringValues.length < gts.ticks.length) {
          gts.stringValues = new String[gts.ticks.length];
        }
      } else {
        //
        // Default type is boolean, this is so people will rapidly notice
        // this is not what they were expecting...
        //
        gts.type = TYPE.BOOLEAN;
        gts.booleanValues = new BitSet(gts.ticks.length);
      }
    }    
  }

  /**
   * Provision GTS arrays for several values. If the arrays have enough space left, no provision is done.
   * If arrays have not enough space left, a provision is done according to the given parameter.
   * @param gts The GTS instance to provision space for.
   * @param numberOfValuesToAdd Minimum available space in arrays for new data.
   * @param provisionSize Array length increase in case arrays are too small.
   */
  public static final void multiProvision(GeoTimeSerie gts, GeoTimeSerie.TYPE fallbackType, int numberOfValuesToAdd, int provisionSize) {
    if (0 < numberOfValuesToAdd) {
      int newSize = gts.values + provisionSize;

      if (null == gts.ticks) { // gts is empty
        gts.ticks = new long[newSize];

        if (TYPE.UNDEFINED != gts.type) {
          if (GeoTimeSerie.TYPE.LONG == gts.type) {
            gts.longValues = new long[newSize];
          } else if (GeoTimeSerie.TYPE.DOUBLE == gts.type) {
            gts.doubleValues = new double[newSize];
          } else if (GeoTimeSerie.TYPE.STRING == gts.type) {
            gts.stringValues = new String[newSize];
          } else if (TYPE.BOOLEAN == gts.type) {
            gts.booleanValues = new BitSet();
          }
        } else {
          if (GeoTimeSerie.TYPE.LONG == fallbackType) {
            gts.longValues = new long[newSize];
          } else if (GeoTimeSerie.TYPE.DOUBLE == fallbackType) {
            gts.doubleValues = new double[newSize];
          } else if (GeoTimeSerie.TYPE.STRING == fallbackType) {
            gts.stringValues = new String[newSize];
          } else { // TYPE.BOOLEAN == fallbackType || TYPE.UNDEFINED == fallbackType
            //
            // Default type is boolean, this is so people will rapidly notice
            // this is not what they were expecting...
            //
            gts.booleanValues = new BitSet();
          }
        }
      } else if (gts.ticks.length < gts.size() + numberOfValuesToAdd) { // gts is too small for added data
        gts.ticks = Arrays.copyOf(gts.ticks, newSize);

        if (GeoTimeSerie.TYPE.LONG == gts.type) {
          gts.longValues = Arrays.copyOf(gts.longValues, newSize);
        } else if (GeoTimeSerie.TYPE.DOUBLE == gts.type) {
          gts.doubleValues = Arrays.copyOf(gts.doubleValues, newSize);
        } else if (GeoTimeSerie.TYPE.STRING == gts.type) {
          gts.stringValues = Arrays.copyOf(gts.stringValues, newSize);
        }
        // else TYPE.BOOLEAN == gts.type // nothing to do because BitSet grows automatically

        // If gts has location info
        if (null != gts.locations) {
          gts.locations = Arrays.copyOf(gts.locations, newSize);
        }

        // If gts has elevation info
        if (null != gts.elevations) {
          gts.elevations = Arrays.copyOf(gts.elevations, newSize);
        }
      }
    }
  }

  /**
   * Return a new GeoTimeSerie instance containing only the value of 'gts'
   * which fall between 'starttimestamp' (inclusive) and 'stoptimestamp' (inclusive)
   * 
   * The resulting GTS instance will be sorted.
   * 
   * @param gts GeoTimeSerie from which to extract values.
   * @param starttimestamp Oldest timestamp to consider (in microseconds)
   * @param stoptimestamp Most recent timestamp to consider (in microseconds)
   * @param overwrite Should we overwrite measurements which occur at the same timestamp to only keep the last one added.
   * @param copyLabels If true, labels will be copied from the original GTS to the subserie.
   * 
   * @return The computed sub Geo Time Serie
   */
  public static final GeoTimeSerie subSerie(GeoTimeSerie gts, long starttimestamp, long stoptimestamp, boolean overwrite, boolean copyLabels, GeoTimeSerie subgts) {
    // FIXME(hbs): should a subserie of a bucketized GTS be also bucketized? With possible non existant values. This would impact Mappers/Bucketizers
    
    //
    // Create sub GTS
    //
    
    if (null == subgts) {
      subgts = new GeoTimeSerie(gts.sizehint);
      //
      // Copy name and labels
      //
      
      subgts.setName(gts.getName());
      
      if (copyLabels) {
        subgts.setLabels(gts.getLabels());
        subgts.getMetadata().setAttributes(new HashMap<String,String>(gts.getMetadata().getAttributes()));
      }
    } else {
      GTSHelper.reset(subgts);
    }
    
    if (null == gts.ticks || 0 == gts.values) {
      return subgts;
    }

    //
    // No value to return in the following case
    //

    if (starttimestamp > stoptimestamp) {
      return subgts;
    }

    //
    // Sort GTS so ticks are ordered
    //
    
    GTSHelper.sort(gts);
        
    //
    // Determine index to stop at
    //
    
    int lastidx = Arrays.binarySearch(gts.ticks, 0, gts.values, stoptimestamp);
    
    if (-1 == lastidx) {
      // The upper timestamp is less than the first tick, so subserie is necessarly empty
      return subgts;
    } else if (lastidx < 0) {

      // The upper timestamp is in between ticks, so we set the last index to the tick
      // just before the insertion point
      lastidx =  -lastidx - 1 - 1;

      if (lastidx >= gts.values) {
        lastidx = gts.values - 1;
      }
    } else {
      // We found the stop timestamp, we now must find the last occurrence of
      // it in case there are duplicates
      int lastlastidx = lastidx + 1;
      while(lastlastidx < gts.values && stoptimestamp == gts.ticks[lastlastidx]) {
        lastlastidx++;
      }
      
      lastidx = lastlastidx - 1;      
    }
    
    int firstidx = Arrays.binarySearch(gts.ticks, 0, lastidx + 1, starttimestamp);
    
    if (firstidx < 0) {
      // The first timestamp is in between existing ticks, so we set
      // the first index to the index of the insertion point
      firstidx = -firstidx - 1;

      // Start after the last tick of the GTS
      if (firstidx >= gts.values) {
        return subgts;
      }
    } else if (firstidx > 0) {
      // We found the start timestamp, we now must find the first occurrence of it
      // in case there are duplicates
      int firstfirstidx = firstidx - 1;
      
      while(firstfirstidx >= 0 && starttimestamp == gts.ticks[firstfirstidx]) {
        firstfirstidx--;
      }
      
      firstidx = firstfirstidx + 1;
    }
        
    //
    // Check that indices are ok with the requested time range
    //
    
    if (gts.ticks[firstidx] > stoptimestamp || gts.ticks[lastidx] < starttimestamp) {
      return subgts;
    }
    
    //
    // Extract values/locations/elevations that lie in the requested interval
    //

    // We know how many data will the new GTS so we provision arrays to receive the data.
    int count = lastidx - firstidx + 1;
    GTSHelper.multiProvision(subgts, gts.type, count, count);
    
    for (int i = firstidx; i <= lastidx; i++) {
      setValue(subgts, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, valueAtIndex(gts, i), overwrite);
    }

    return subgts;
  }

  public static final GeoTimeSerie subSerie(GeoTimeSerie gts, long starttimestamp, long stoptimestamp, boolean overwrite) {
    return subSerie(gts, starttimestamp, stoptimestamp, overwrite, true, null);
  }
  
  /**
   * Return a new GeoTimeSerie instance containing only the values of 'gts'
   * that fall under timestamps that are bound by the same modulo class.
   * 
   * The resulting GTS instance will be sorted and bucketized.
   * 
   * @param gts GeoTimeSerie from which to extract values. It must be bucketized.
   * @param lastbucket Most recent timestamp to consider (in microseconds)
   * @param buckets_per_period Number of buckets of the input gts that sum up to a bucket for the sub cycle gts
   * @param overwrite Should we overwrite measurements which occur at the same timestamp to only keep the last one added.
   * 
   * @return The computed sub cycle Geo Time Serie
   */
  public static final GeoTimeSerie subCycleSerie(GeoTimeSerie gts, long lastbucket, int buckets_per_period, boolean overwrite, GeoTimeSerie subgts) throws WarpScriptException {
    if (!isBucketized(gts)) {
      throw new WarpScriptException("GTS must be bucketized");
    }
    
    if (0 != (gts.lastbucket - lastbucket) % gts.bucketspan) {
      throw new WarpScriptException("lasbucket parameter of subCycleSerie method must fall on an actual bucket of the gts input");
    }
    
    //
    // Create sub GTS
    //
    
    // The size hint impacts performance, choose it wisely...
    if (null == subgts) {
      subgts = new GeoTimeSerie(lastbucket, (gts.bucketcount - (int) ((gts.lastbucket - lastbucket) / gts.bucketspan) - 1) / buckets_per_period + 1, gts.bucketspan * buckets_per_period, (int) Math.max(1.4 * gts.bucketcount, gts.sizehint) / buckets_per_period);
    } else {
      subgts.values = 0;
      subgts.type = TYPE.UNDEFINED;
      
      subgts.lastbucket = lastbucket;
      subgts.bucketcount = (gts.bucketcount - (int) ((gts.lastbucket - lastbucket) / gts.bucketspan) - 1) / buckets_per_period + 1;
      subgts.bucketspan = gts.bucketspan * buckets_per_period;      
    }
    
    if (null == gts.ticks || 0 == gts.values) {
      return subgts;
    }
    
    //
    // For each tick, search if tick is in gts, then copy it to subgts, else noop
    //    
    
    Iterator<Long> iter = tickIterator(subgts, true);
    long tick;
    sort(gts);
    int i = gts.values;
    while (iter.hasNext()) {
      
      tick = iter.next();
      i = Arrays.binarySearch(gts.ticks, 0, i, tick);
      
      if (i >= 0) {
        setValue(subgts, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, valueAtIndex(gts, i), overwrite);
      }
    }

    return subgts;
  }
  
  public static final GeoTimeSerie subCycleSerie(GeoTimeSerie gts, long lastbucket, int buckets_per_period, boolean overwrite) throws WarpScriptException {
    return subCycleSerie(gts, lastbucket, buckets_per_period, overwrite, null);
  }
  
  /**
   * Converts a Geo Time Serie into a bucketized version.
   * Bucketizing means aggregating values (with associated location and elevation) that lie
   * within a given interval into a single one. Intervals considered span a given
   * width called the bucketspan.
   * 
   * @param gts Geo Time Serie to bucketize
   * @param bucketspan Width of bucket (time interval) in microseconds
   * @param bucketcount Number of buckets to use
   * @param lastbucket Timestamp of the end of the last bucket of the resulting GTS. If 0, 'lastbucket' will be set to the last timestamp of 'gts'.
   * @param aggregator Function used to aggregate values/locations/elevations
   * @return A bucketized version of the GTS.
   */
  public static final GeoTimeSerie bucketize(GeoTimeSerie gts, long bucketspan, int bucketcount, long lastbucket, WarpScriptBucketizerFunction aggregator, long maxbuckets) throws WarpScriptException {
    return bucketize(gts, bucketspan, bucketcount, lastbucket, aggregator, maxbuckets, null);
  }
  
  public static final GeoTimeSerie bucketize(GeoTimeSerie gts, long bucketspan, int bucketcount, long lastbucket, Object aggregator, long maxbuckets, WarpScriptStack stack) throws WarpScriptException {

    //
    // If lastbucket is 0, compute it from the last timestamp
    // in gts and make it congruent to 0 modulus 'bucketspan'
    //
    
    long lasttick = GTSHelper.lasttick(gts);
    long firsttick = GTSHelper.firsttick(gts);

    boolean zeroLastBucket = 0 == lastbucket;
    boolean zeroBucketcount = 0 == bucketcount;
    
    //
    // If lastbucket AND bucketcount AND bucketspan are 0
    //
    
    if (0 == lastbucket) {
      lastbucket = lasttick;
    }

    //
    // If bucketspan is 0 but bucketcount is set, compute bucketspan so 'bucketcount' buckets
    // cover the complete set of values from firsttick to lastbucket
    //
    
    if (0 == bucketspan || -1 == bucketspan) {
      if(0 == bucketcount) {
        throw new WarpScriptException("One of bucketspan or bucketcount must be different from zero.");
      } else {
        if (lastbucket >= firsttick) {
          long delta;
          
          if (0 == bucketspan) {
            delta = lastbucket - firsttick + 1;
            bucketspan = delta / bucketcount;            
          } else {
            delta = lastbucket - firsttick;
            if (1 == bucketcount) {
              bucketspan = delta;
            } else {
              bucketspan = delta / (bucketcount - 1);
            }
          }

          //
          // Increase bucketspan by 1 so we cover the whole timespan
          //

          if (0 == bucketspan || (delta % bucketspan) != 0) {
            bucketspan++;
          }
        }
      }
    }
    
    if (bucketspan < 0) {
      bucketspan = 0;
    }
    
    //
    // If bucketcount is 0, compute it so the bucketized GTS covers all ticks
    //    
    
    if (0 == bucketcount) {      
      if (lastbucket >= firsttick) {
        long delta = lastbucket - firsttick;
        
        if (delta < bucketspan) {
          bucketcount = 1;
        } else {
          bucketcount = 1 + (int) (delta / bucketspan);
        }
      }
    }

    //
    // Now that bucketspan is computed, adjust lastbucket if the passed value was '0' so
    // it lies on a bucketspan boundary.
    //
    
    if (zeroLastBucket && zeroBucketcount) {     
      // Make sure lastbucket falls on a bucketspan boundary (only if bucketspan was specified)
      if (0 != lastbucket % bucketspan) {
        lastbucket = lastbucket - (lastbucket % bucketspan) + bucketspan;
        if (lastbucket - bucketcount * bucketspan >= firsttick) {
          bucketcount++;
        }
      }
    }
    
    if (bucketcount < 0 || bucketcount > maxbuckets) {
      throw new WarpScriptException("Bucket count (" + bucketcount + ") would exceed maximum value of " + maxbuckets);
    }

    if (0 == bucketspan) {
      throw new WarpScriptException("Undefined bucket span, check your GTS timestamps.");
    }

    //
    // If the bucketizer is null, it only sets lastbucket, bucketcount and bucketspan
    //

    if (null == aggregator) {
      gts.lastbucket = lastbucket;
      gts.bucketcount = bucketcount;
      gts.bucketspan = bucketspan;

      return gts;
    }
    
    //
    // Create the target Geo Time Serie (bucketized)
    //
    
    // Determine sizing hint. We evaluate the number of buckets that will be filled.
    
    int hint = Math.min(gts.values, (int) ((lasttick - firsttick) / bucketspan));
    
    GeoTimeSerie bucketized = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, hint);
    
    //
    // Copy name, labels and attributes
    //
    
    bucketized.setMetadata(new Metadata(gts.getMetadata()));

    Map<String,String> labels = gts.getLabels();
    
    //
    // Loop on all buckets
    //

    //
    // We can't skip buckets which are before the first tick or after the last one
    // because the bucketizer function might set a default value when it encounters an
    // empty sub serie
    //
      
    // Allocate a stable GTS instance which we will reuse when calling subserie
    GeoTimeSerie subgts = null;
    
    for (int i = 0; i < bucketcount; i++) {
      
      long bucketend = lastbucket - i * bucketspan;
      
      //
      // Extract GTS containing the values that fall in the bucket
      // Keep multiple values that fall on the same timestamp, the
      // aggregator functions will deal with them.
      //
      
      subgts = subSerie(gts, bucketend - bucketspan + 1, bucketend, false, false, subgts);

      if (0 == subgts.values) {
        continue;
      }
      
      Object[] aggregated = null;
      
      if (null != stack) {
        if (!(aggregator instanceof Macro)) {
          throw new WarpScriptException("Expected a macro as bucketizer.");
        }
        
        subgts.safeSetMetadata(bucketized.getMetadata());
        stack.push(subgts);
        stack.exec((Macro) aggregator);
        
        Object res = stack.peek();
        
        if (res instanceof List) {
          aggregated = MACROMAPPER.listToObjects((List<Object>) stack.pop());
        } else {
          aggregated = MACROMAPPER.stackToObjects(stack);
        }                
      } else {
        if (!(aggregator instanceof WarpScriptBucketizerFunction)) {
          throw new WarpScriptException("Invalid bucketizer function.");
        }
        //
        // Call the aggregation functions on this sub serie and add the resulting value
        //
        
        //
        // Aggregator functions have 8 parameters (so mappers or reducers can be used as aggregators)
        //
        // bucket timestamp: end timestamp of the bucket we're currently computing a value for
        // names: array of GTS names
        // labels: array of GTS labels
        // ticks: array of ticks being aggregated
        // locations: array of locations being aggregated
        // elevations: array of elevations being aggregated
        // values: array of values being aggregated
        // bucket span: width (in microseconds) of bucket
        //
        
        Object[] parms = new Object[8];

        int idx = 0;
        parms[idx++] = bucketend;
        parms[idx] = new String[1];
        ((String[]) parms[idx++])[0] = bucketized.getName();
        parms[idx] = new Map[1];
        ((Map[]) parms[idx++])[0] = labels;
        parms[idx++] = Arrays.copyOf(subgts.ticks, subgts.values);
        if (null != subgts.locations) {
          parms[idx++] = Arrays.copyOf(subgts.locations, subgts.values);
        } else {
          parms[idx++] = new long[subgts.values];
          Arrays.fill((long[]) parms[idx - 1], GeoTimeSerie.NO_LOCATION);
        }
        if (null != subgts.elevations) {
          parms[idx++] = Arrays.copyOf(subgts.elevations, subgts.values);
        } else {
          parms[idx++] = new long[subgts.values];
          Arrays.fill((long[]) parms[idx - 1], GeoTimeSerie.NO_ELEVATION);
        }
        parms[idx++] = new Object[subgts.values];
        parms[idx++] = new long[] { 0, -bucketspan, bucketend - bucketspan, bucketend };
        
        for (int j = 0; j < subgts.values; j++) {
          ((Object[]) parms[6])[j] = valueAtIndex(subgts, j);
        }

        aggregated = (Object[]) ((WarpScriptBucketizerFunction) aggregator).apply(parms);        
      }

      //
      // Only set value if it was non null
      //
      
      if (null != aggregated[3]) {
        setValue(bucketized, bucketend, (long) aggregated[1], (long) aggregated[2], aggregated[3], false);
      }
    }
    
    GTSHelper.shrink(bucketized);
    return bucketized;
  }

  public static void unbucketize(GeoTimeSerie gts) {
    gts.bucketcount = 0;
    gts.bucketspan = 0L;
    gts.lastbucket = 0L;
  }

  private static final Pattern MEASUREMENT_RE = Pattern.compile("^([0-9]+)?/(([0-9.-]+):([0-9.-]+))?/([0-9-]+)? +([^ ]+)\\{([^\\}]*)\\} +(.+)$");

  /**
   * Parses a string representation of a measurement and return a single valued GTS
   *
   * @param str String representation to parse
   * @return The resulting data in a GTSEncoder. The resulting encoder will not have classId/labelsId set.
   *
   * @throws ParseException if a parsing error occurred
   */
  public static GTSEncoder parse_regexp(GTSEncoder encoder, String str, Map<String,String> extraLabels) throws ParseException, IOException {
    Matcher matcher = MEASUREMENT_RE.matcher(str);
    
    if (!matcher.matches()) {
      throw new ParseException(str, 0);
    }
    
    //
    // Check name
    //
    
    String name = matcher.group(6);
    
    if (name.contains("%")) {
      try {      
        name = URLDecoder.decode(name, StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException uee) {
        // Can't happen, we're using UTF-8
      }      
    }
    
    //
    // Parse labels
    //
    
    Map<String,String> labels = parseLabels(matcher.group(7));
    
    //
    // Add any provided extra labels
    //
    
    if (null != extraLabels) {
      labels.putAll(extraLabels);
    }
    
    //
    // Extract timestamp, optional location and elevation
    //
    
    long timestamp;
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    try {
      
      if (null != matcher.group(1)) {
        timestamp = Long.parseLong(matcher.group(1));
      } else {
        // No timestamp provided, use 'now'
        timestamp = TimeSource.getTime();;
      }
      
      if (null != matcher.group(2)) {
        location = GeoXPLib.toGeoXPPoint(Double.parseDouble(matcher.group(3)), Double.parseDouble(matcher.group(4)));
      }
      
      if (null != matcher.group(5)) {
        elevation = Long.parseLong(matcher.group(5));
      }      
    } catch (NumberFormatException nfe) {
      throw new ParseException("", 0);
    }
    
    //
    // Extract value
    //
    
    
    String valuestr = matcher.group(8);

    Object value = parseValue_regexp(valuestr);
      
    if (null == value) {
      throw new ParseException("Unable to parse value '" + valuestr + "'", 0);
    }
    
    // Allocate a new Encoder if need be, with a base timestamp of 0L.
    if (null == encoder || !name.equals(encoder.getName()) || !labels.equals(encoder.getLabels())) {
      encoder = new GTSEncoder(0L);
      encoder.setName(name);
      encoder.setLabels(labels);
    }
    
    encoder.addValue(timestamp, location, elevation, value);

    return encoder;
  }

  public static GTSEncoder parseJSON(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now) throws IOException, ParseException {
    
    Map<String,Object> o = (Map<String,Object>) JsonUtils.jsonToObject(str);
    
    String name = (String) o.get("c");
    Map<String,String> labels = (Map<String,String>) o.get("l");
    
    //
    // Add any provided extra labels
    //
    
    if (null != extraLabels) {
      labels.putAll(extraLabels);
    
      //
      // Remove labels with null values
      //
      // FIXME(hbs): may be removed when dummy tokens have disappeared
      //
      
      if (extraLabels.containsValue(null)) {
        Set<Entry<String,String>> entries = extraLabels.entrySet();
        
        while(labels.containsValue(null)) {
          for (Entry<String,String> entry: entries) {
            if (null == entry.getValue()) {
              labels.remove(entry.getKey());
            }
          }
        }        
      }
    }

    Object ots = o.get("t");
    
    long ts = (null != ots ? ((Number) ots).longValue() : (null != now ? (long) now : TimeSource.getTime()));
    
    long location = GeoTimeSerie.NO_LOCATION;
    
    if (o.containsKey("lat") && o.containsKey("lon")) {
      double lat = (double) o.get("lat");
      double lon = (double) o.get("lon");
      
      location = GeoXPLib.toGeoXPPoint(lat, lon);
    }
    
    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    if (o.containsKey("elev")) {
      elevation = ((Number) o.get("elev")).longValue();
    }
    
    Object v = o.get("v");
    
    // Allocate a new Encoder if need be, with a base timestamp of 0L.
    if (null == encoder || !name.equals(encoder.getName()) || !labels.equals(encoder.getLabels())) {
      encoder = new GTSEncoder(0L);
      encoder.setName(name);
      encoder.setLabels(labels);
    }
    
    if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte || v instanceof BigInteger) {
      encoder.addValue(ts, location, elevation, ((Number) v).longValue());
    } else if (v instanceof Double || v instanceof Float) {
      encoder.addValue(ts, location, elevation, ((Number) v).doubleValue());      
    } else if (v instanceof BigDecimal) {
      encoder.addValue(ts, location, elevation, v);
    } else if (v instanceof Boolean || v instanceof String) {
      encoder.addValue(ts, location, elevation, v);
    } else {
      throw new ParseException("Invalid value.", 0);
    }
    
    return encoder;
  }
  
  private static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels) throws ParseException, IOException {
    return parse(encoder, str, extraLabels, null);
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now) throws ParseException, IOException {
    return parse(encoder, str, extraLabels, now, Long.MAX_VALUE, null);
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now, long maxValueSize) throws ParseException, IOException {
    return parse(encoder, str, extraLabels, now, maxValueSize, null);    
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now, long maxValueSize, AtomicBoolean parsedAttributes) throws ParseException, IOException {
    return parse(encoder, str, extraLabels, now, maxValueSize, parsedAttributes, null, null, null, false);
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str, Map<String,String> extraLabels, Long now, long maxValueSize, AtomicBoolean parsedAttributes, Long maxpast, Long maxfuture, AtomicLong ignoredCount, boolean deltaAttributes) throws ParseException, IOException {

    int idx = 0;
    
    int tsoffset = 0;
    
    if ('=' == str.charAt(0)) {
      if (null == encoder) {
        throw new ParseException("Invalid continuation.", 0);
      }
      tsoffset = 1;
    }
    
    //idx = str.indexOf("/");
    idx = UnsafeString.indexOf(str, '/');

    if (-1 == idx){
      throw new ParseException("Missing timestamp separator.", idx);
    }
    
    long timestamp;
    
    if (tsoffset == idx) {
      // No timestamp provided, use 'now'
      timestamp = null != now ? (long) now : TimeSource.getTime();
    } else {
      if ('T' == str.charAt(tsoffset)) {
        // Support T-XXX to record timestamps which are relative to 'now', useful for
        // devices with no time reference but only relative timestamps
        timestamp = (null != now ? (long) now : TimeSource.getTime()) + Long.parseLong(str.substring(1 + tsoffset, idx));
      } else {
        timestamp = Long.parseLong(str.substring(tsoffset,  idx));
      }
    }

    boolean ignored = false;
    
    if (null != maxpast && timestamp < maxpast) {
      if (null == ignoredCount) {
        throw new ParseException("Timestamp " + timestamp + " is too far in the past.", idx);
      } else {
        ignored = true;
      }
    } else if (null != maxfuture && timestamp > maxfuture) {
      if (null == ignoredCount) {
        throw new ParseException("Timestamp " + timestamp + " is too far in the future.", idx);
      } else {
        ignored = true;
      }
    }
    
    // Advance past the '/'
    idx++;
    
    //int idx2 = str.indexOf("/", idx);
    int idx2 = UnsafeString.indexOf(str, '/', idx);
    
    if (-1 == idx2){
      throw new ParseException("Missing location/elevation separator.", idx);
    }

    long location = GeoTimeSerie.NO_LOCATION;

    if (idx != idx2) {
      // We have a location (lat:lon)
      String latlon = str.substring(idx, idx2);
      // Advance past the second '/'    
      idx = idx2 + 1;
      //idx2 = latlon.indexOf(":");
      idx2 = UnsafeString.indexOf(latlon, ':');
            
      if (-1 != idx2) {
        location = GeoXPLib.toGeoXPPoint(Double.parseDouble(latlon.substring(0, idx2)), Double.parseDouble(latlon.substring(idx2 + 1)));
      } else {
        // Parse the location value as a Long
        location = Long.parseLong(latlon);        
      }
    } else {
      // Advance past the second '/'    
      idx = idx2 + 1;
    }
    
    //idx2 = str.indexOf(" ", idx);
    idx2 = UnsafeString.indexOf(str, ' ', idx);
    
    if (-1 == idx2){
      throw new ParseException(str, idx);
    }

    long elevation = GeoTimeSerie.NO_ELEVATION;
    
    if (idx != idx2) {
      // We have an elevation
      elevation = Long.parseLong(str.substring(idx, idx2));
    }

    // Advance past the ' '    
    idx = idx2 + 1;
    
    while (idx < str.length() && str.charAt(idx) == ' ') {
      idx++;
    }

    // If line started with '=', assume there is no class+labels component
    if (tsoffset > 0) {
      idx2 = -1;
    } else {
      //idx2 = str.indexOf("{", idx);
      idx2 = UnsafeString.indexOf(str, '{', idx);      
    }

    String name = null;
    Map<String,String> labels = null;
    Map<String,String>  attributes = null;
    
    boolean reuseLabels = false;
    
    if (-1 == idx2){
      // If we are over the end of the string, we're missing a value
      if (idx >= str.length()) {
        throw new ParseException("Missing value", idx);
      }
      // No class+labels, assume same class+labels as those in encoder, except
      // if encoder is null in which case we throw a parse exception
      if (null == encoder) {
        throw new ParseException(str, idx);
      }
      name = encoder.getMetadata().getName();
      labels = encoder.getMetadata().getLabels();
      reuseLabels = true;
    } else {
      name = str.substring(idx, idx2);
      
      name = WarpURLDecoder.decode(name, StandardCharsets.UTF_8);

      // Advance past the '{'
      idx = idx2 + 1;
      
      //idx2 = str.indexOf("}", idx);
      idx2 = UnsafeString.indexOf(str, '}', idx);
      
      if (-1 == idx2){
        throw new ParseException(str, idx);
      }
      
      //
      // Parse labels
      //
      
      labels = parseLabels(null != extraLabels ? extraLabels.size() : 0, str.substring(idx, idx2));           
      
      //
      // FIXME(hbs): parse attributes????
      //
      
      // Advance past the '}' and over spaces
      
      idx = idx2 + 1;

      // FIXME(hbs): should we skip over attributes if they are present?
      if (idx < str.length() && str.charAt(idx) == '{') {        
        idx++;
        int attrstart = idx;
        while(idx < str.length() && str.charAt(idx) != '}') {
          idx++;
        }
        if (null != parsedAttributes) {
          if (idx >= str.length()) {
            throw new ParseException("Missing attributes.", idx2);
          }
          attributes = parseLabels(str.substring(attrstart, idx));
          // Set the atomic boolean to true to indicate that attributes were parsed
          parsedAttributes.set(true);
        }
        idx++;
      }
      
      while (idx < str.length() && str.charAt(idx) == ' ') {
        idx++;
      }
      
      if (idx >= str.length()) {
        throw new ParseException("Missing value.", idx2);
      }
    }

    //
    // Add any provided extra labels
    //
    // WARNING(hbs): as we check reuseLabels, note that extraLabels won't be pushed onto the GTS labels
    // if reuseLabels is 'true', this means that if you call parse on a continuation line with different extraLabels
    // than for previous lines, the new extra labels won't be set. But this is not something that should be done anyway,
    // so it should not be considered a problem...
    //
    
    if (!reuseLabels && null != extraLabels) {
      labels.putAll(extraLabels);
    
      //
      // Remove labels with null values
      //
      // FIXME(hbs): may be removed when dummy tokens have disappeared
      //
      
      if (extraLabels.containsValue(null)) {
        Set<Entry<String,String>> entries = extraLabels.entrySet();
        
        while(labels.containsValue(null)) {
          for (Entry<String,String> entry: entries) {
            if (null == entry.getValue()) {
              labels.remove(entry.getKey());
            }
          }
        }        
      }
    }
    
    //
    // Extract value
    //
    
    String valuestr = str.substring(idx);
    
    Object value = parseValue(valuestr);

    if (null == value) {
      throw new ParseException("Unable to parse value '" + valuestr + "'", 0);
    }

    if ((value instanceof String  && value.toString().length() > maxValueSize) || (value instanceof byte[] && ((byte[]) value).length > maxValueSize)) {
      throw new ParseException("Value too large for GTS " + (null != encoder ? GTSHelper.buildSelector(encoder.getMetadata(), false) : ""), 0);
    }
    
    // Allocate a new Encoder if need be, with a base timestamp of 0L.
    if (null == encoder || !name.equals(encoder.getName()) || !labels.equals(encoder.getMetadata().getLabels())) {
      encoder = new GTSEncoder(0L);
      encoder.setName(name);
      encoder.getMetadata().setLabels(labels);
    }

    // Update the attributes if some were parsed
    if (null != attributes) {
      if (!deltaAttributes) {
        encoder.getMetadata().setAttributes(attributes);
      } else {
        if (0 == encoder.getMetadata().getAttributesSize()) {
          encoder.getMetadata().setAttributes(new HashMap<String,String>());
        }
        for (Entry<String,String> attr: attributes.entrySet()) {
          if ("".equals(attr.getValue())) {
            encoder.getMetadata().getAttributes().remove(attr.getKey());
          } else {
            encoder.getMetadata().putToAttributes(attr.getKey(), attr.getValue());
          }
        }
      }
    }

    if (!ignored) {
      encoder.addValue(timestamp, location, elevation, value);
    } else {
      ignoredCount.addAndGet(1);
    }
    
    // Check labels/attributes sizes, subtract 6 to account for '// {} '
    // Subtract value length
    if (str.length() - 6 - valuestr.length() > MetadataUtils.SIZE_THRESHOLD && !MetadataUtils.validateMetadata(encoder.getMetadata())) {
      throw new ParseException("Invalid metadata", 0);
    }
    
    return encoder;
  }
  
  public static GTSEncoder parse(GTSEncoder encoder, String str) throws ParseException, IOException {
    return parse(encoder, str, null);
  }
  
  public static Object parseValue(String valuestr) throws ParseException {
    
    Object value;
        
    try {
      char firstChar = valuestr.charAt(0);

      if (('\'' == firstChar && valuestr.endsWith("'"))
          || ('"' == firstChar && valuestr.endsWith("\""))) {
        value = valuestr.substring(1, valuestr.length() - 1);
        value = WarpURLDecoder.decode((String) value, StandardCharsets.UTF_8);
      } else if (('t' == firstChar || 'T' == firstChar) && (1 == valuestr.length() || "true".equalsIgnoreCase(valuestr))) {
        value = Boolean.TRUE;
      } else if (('f' == firstChar || 'F' == firstChar) && (1 == valuestr.length() || "false".equalsIgnoreCase(valuestr))) {
        value = Boolean.FALSE;
      } else if ('H' == firstChar && valuestr.startsWith("HH:")) {
        int colon = valuestr.indexOf(':',3);
        if (-1 == colon) {
          throw new ParseException("Invalid value for lat,lon conversion to HHCode.", 0);
        }
        double lat = Double.parseDouble(valuestr.substring(3, colon));
        double lon = Double.parseDouble(valuestr.substring(colon + 1));
        
        value = GeoXPLib.toGeoXPPoint(lat, lon);
      } else if ('Q' == firstChar && valuestr.startsWith("Q:")) {
        
        double[] q = new double[4];
        
        int idx = 2;
        int qidx = 0;
        
        while (qidx < q.length) {
          int colon = valuestr.indexOf(':', idx);
          
          if (-1 == colon) {
            throw new ParseException("Invalid value for Quaternion, expected Q:w:x:y:z", 0);
          }

          q[qidx++] = Double.parseDouble(valuestr.substring(idx, colon));
          idx = colon + 1;
          
          if (3 == qidx) {
            q[qidx++] = Double.parseDouble(valuestr.substring(idx));
          }
        }
        
        if (!DoubleUtils.isFinite(q[0]) || !DoubleUtils.isFinite(q[1]) || !DoubleUtils.isFinite(q[2]) || !DoubleUtils.isFinite(q[3])) {
          throw new ParseException("Quaternion values require finite elements.", 0);
        }
        
        value = TOQUATERNION.toQuaternion(q[0], q[1], q[2], q[3]);
      } else if ('[' == valuestr.charAt(0)) {        
        
        // Value is a nested set of lists, each one being a space separated list of tokens of the form enclosed in [ ... ]
        // VALUE
        // TS/VALUE
        // TS/LAT:LON/VALUE
        // TS//ELEV/VALUE
        // TS/LAT:LON/ELEV/VALUE
        //
        // Elements between two '/' can be empty
        
        GTSEncoder encoder = new GTSEncoder();

        // Start index of the token to parse. Will advance through the token to parse each slash-separated field.
        int idxTokenStart = 1;
        
        boolean comp = true;
        // Handle the case where the value starts with [! which means to not compress the resulting encoder
        if ('!' == valuestr.charAt(1)) {
          comp = false;
          idxTokenStart = 2;
        }
        
        int idxTokenSlash = 0; // Used to find '/' inside the token

        // Last index (excluded) to be considered in valuestr
        int idxValueEnd = valuestr.length() - 1;

        // Find the last closing bracket which should match the opening bracket, ignoring trailing spaces.
        while (idxValueEnd >= idxTokenStart) {
          char c = valuestr.charAt(idxValueEnd);
          if (']' == c) {
            break;
          } else if (' ' == c) {
            idxValueEnd--;
          } else {
            throw new ParseException("Missing closing bracket.", 0);
          }
        }

        if (idxValueEnd < idxTokenStart) {
          throw new ParseException("Missing closing bracket.", 0);
        }
        
        while(idxTokenStart < idxValueEnd) {
          // Ignore leading spaces
          while(idxTokenStart < idxValueEnd && ' ' == valuestr.charAt(idxTokenStart)) {
            idxTokenStart++;
          }

          if (idxTokenStart >= idxValueEnd) {
            break;
          }
          
          // Find end of current token
          int idxTokenEnd = idxTokenStart + 1;
          
          while(idxTokenEnd < idxValueEnd && ' ' != valuestr.charAt(idxTokenEnd)) {
            idxTokenEnd++;
          }
          
          // Current token is between idxTokenStart (included) and idxTokenEnd (excluded)
          
          // Find out if there is a first '/'
          idxTokenSlash = idxTokenStart + 1;
          
          while(idxTokenSlash < idxTokenEnd && '/' != valuestr.charAt(idxTokenSlash)) {
            idxTokenSlash++;
          }
          
          long ts;
          long location = GeoTimeSerie.NO_LOCATION;
          long elevation = GeoTimeSerie.NO_ELEVATION;
              
          Object val;
          
          // No '/', we simply have a value, we'll store it at ts 0
          
          if (idxTokenSlash == idxTokenEnd) {
            ts = 0L;
            // Advance to the closing ']' if the value starts with '['
            if ('[' == valuestr.charAt(idxTokenStart)) {
              int closing = idxTokenStart + 1;
              int opening = 1;
              while(closing < idxValueEnd && opening > 0) {
                char lead = valuestr.charAt(closing);
                if (']' == lead) {
                  opening--;
                } else if ('[' == lead) {
                  opening++;
                }
                closing++;
              }
              idxTokenEnd = closing; // idxTokenEnd points to the character just after ']' or after the last character of valuestr if opening > 0.
              // In that latter case, the parsing will fail when recursively calling GTSHelper.parseValue because at one time it will be given a token without closing bracket at the end.
            }
            
            val = GTSHelper.parseValue(valuestr.substring(idxTokenStart, idxTokenEnd));
            encoder.addValue(ts, location, elevation, val);
            idxTokenStart = idxTokenEnd;
            continue;
          }
          
          //
          // Parse the timestamp
          //
          ts = Long.parseLong(valuestr.substring(idxTokenStart, idxTokenSlash));
          
          // Advance idxTokenSlash after the first '/'
          idxTokenSlash++;
          idxTokenStart = idxTokenSlash;
          
          // Identify a possible second '/'
          while(idxTokenSlash < idxTokenEnd && '/' != valuestr.charAt(idxTokenSlash)) {
            idxTokenSlash++;
          }
          
          // No second '/', we have TS/VALUE
          if (idxTokenSlash == idxTokenEnd) {
            // Advance to the closing ']' if the value starts with '['
            if ('[' == valuestr.charAt(idxTokenStart)) {
              int closing = idxTokenStart + 1;
              int opening = 1;
              while(closing < idxValueEnd && opening > 0) {
                char lead = valuestr.charAt(closing);
                if (']' == lead) {
                  opening--;
                } else if ('[' == lead) {
                  opening++;
                }
                closing++;
              }
              idxTokenEnd = closing; // idxTokenEnd points to the character just after ']' or after the last character of valuestr if opening > 0.
              // In that latter case, the parsing will fail when recursively calling GTSHelper.parseValue because at one time it will be given a token without closing bracket at the end.
            }
            val = GTSHelper.parseValue(valuestr.substring(idxTokenStart, idxTokenEnd));
            encoder.addValue(ts, location, elevation, val);
            idxTokenStart = idxTokenEnd;
            continue;
          }
          
          // Parse lat:lon or HHCode
          // Identify ':'
          
          int idxTokenSemiCol = idxTokenStart;
          
          // idxTokenSlash is the index of the second '/' we found
          while(idxTokenSemiCol < idxTokenSlash && ':' != valuestr.charAt(idxTokenSemiCol)) {
            idxTokenSemiCol++;
          }
          
          // No ':', we have TS/HHCode/... or TS//...
          if (idxTokenSemiCol == idxTokenSlash) {
            if (idxTokenSemiCol > idxTokenStart) {
              location = Long.parseLong(valuestr.substring(idxTokenStart, idxTokenSemiCol));
            }
          } else {       
            // Parse LAT:LON
            double lat = Double.parseDouble(valuestr.substring(idxTokenStart, idxTokenSemiCol));
            double lon = Double.parseDouble(valuestr.substring(idxTokenSemiCol + 1, idxTokenSlash));
            location = GeoXPLib.toGeoXPPoint(lat, lon);
          }

          idxTokenSlash++;
          idxTokenStart = idxTokenSlash;

          // Identify a possible third '/'
          while(idxTokenSlash < idxTokenEnd && '/' != valuestr.charAt(idxTokenSlash)) {
            idxTokenSlash++;
          }
          
          // No '/', we have TS/LAT:LON/VALUE
          if (idxTokenSlash == idxTokenEnd) {
            // Advance to the closing ']' if the value starts with '['
            if ('[' == valuestr.charAt(idxTokenStart)) {
              int closing = idxTokenStart + 1;
              int opening = 1;
              while(closing < idxValueEnd && opening > 0) {
                char lead = valuestr.charAt(closing);
                if (']' == lead) {
                  opening--;
                } else if ('[' == lead) {
                  opening++;
                }
                closing++;
              }
              idxTokenEnd = closing; // idxTokenEnd points to the character just after ']' or after the last character of valuestr if opening > 0.
              // In that latter case, the parsing will fail when recursively calling GTSHelper.parseValue because at one time it will be given a token without closing bracket at the end.
            }
            val = GTSHelper.parseValue(valuestr.substring(idxTokenStart, idxTokenEnd));
            encoder.addValue(ts, location, elevation, val);
            idxTokenStart = idxTokenEnd;
            continue;
          }
          
          // Extract elevation
          if (idxTokenSlash > idxTokenStart) {
            elevation = Long.parseLong(valuestr.substring(idxTokenStart, idxTokenSlash));
          }
          
          // Advance to the closing ']' if the value starts with '['
          if ('[' == valuestr.charAt(idxTokenSlash + 1)) {
            int closing = idxTokenSlash + 2;
            int opening = 1;
            while(closing < idxValueEnd && opening > 0) {
              char lead = valuestr.charAt(closing);
              if (']' == lead) {
                opening--;
              } else if ('[' == lead) {
                opening++;
              }
              closing++;
            }
            idxTokenEnd = closing; // idxTokenEnd points to the character just after ']' or after the last character of valuestr if opening > 0.
            // In that latter case, the parsing will fail when recursively calling GTSHelper.parseValue because at one time it will be given a token without closing bracket at the end.
          }

          val = GTSHelper.parseValue(valuestr.substring(idxTokenSlash + 1, idxTokenEnd));
          encoder.addValue(ts, location, elevation, val);
          
          idxTokenStart = idxTokenEnd + 1;
        }
        
        // Wrap GTSEncoder and encode result, we don't set the count in the wrapper to save some
        // space
        GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, comp, GTSWrapperHelper.DEFAULT_COMP_RATIO_THRESHOLD, Integer.MAX_VALUE, false);
        
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        
        byte[] ser = serializer.serialize(wrapper);
        
        return ser;
      } else if ('b' == firstChar && valuestr.startsWith("b64:")) {
        value = Base64.decodeBase64(valuestr.substring(4));
      } else if ('h' == firstChar && valuestr.startsWith("hex:")) {
        value = Hex.decodeHex(valuestr.substring(4).toCharArray());
      } else if (':' == firstChar) {
        //
        // Custom encoders support values prefixed with ':' + a custom prefix, i.e. ':xxx:VALUE'.
        //
        
        value = ValueEncoder.parse(valuestr);
      } else {
        //boolean likelydouble = UnsafeString.isDouble(valuestr);
        boolean likelylong = UnsafeString.isLong(valuestr);
        
        //if (!likelydouble) {
        if (likelylong) {
          value = Long.parseLong(valuestr);
        } else {
          //
          // If the double does not contain an 'E' or an 'N' or an 'n' (scientific notation or NaN or Infinity)
          // we use the following heuristic to determine if we should return a Double or a BigDecimal,
          // since we encode a mantissa of up to 46 bits, we will return a BigDecimal only if the size of the
          // input is <= 15 digits (log(46)/log(10) = 13.84 plus one digit for the dot plus one to tolerate values starting with low digits).
          // This will have the following caveats:
          //    * leading or trailing 0s should be discarded, this heuristic won't do that
          //    * negative values with 14 or more digits will be returned as Double, but we don't want to parse the
          //    first char.
          //    
          if (valuestr.length() <= 15 && UnsafeString.mayBeDecimalDouble(valuestr)) {
            value = new BigDecimal(valuestr);
          } else {
            value = Double.parseDouble(valuestr);
          }
        }
      }      
    } catch (Exception e) {
      ParseException pe = new ParseException(valuestr, 0);
      pe.initCause(e);
      throw pe;
    }
    
    return value;
  }
  
  private static final Pattern STRING_VALUE_RE = Pattern.compile("^['\"].*['\"]$");
  private static final Pattern BOOLEAN_VALUE_RE = Pattern.compile("^(T|F|true|false)$", Pattern.CASE_INSENSITIVE);
  private static final Pattern LONG_VALUE_RE = Pattern.compile("^[+-]?[0-9]+$");
  private static final Pattern DOUBLE_VALUE_RE = Pattern.compile("^[+-]?([0-9]+)\\.([0-9]+)$");  

  public static Object parseValue_regexp(String valuestr) throws ParseException {
    
    Object value;
    
    Matcher valuematcher = DOUBLE_VALUE_RE.matcher(valuestr);

    if (valuematcher.matches()) {
      // FIXME(hbs): maybe find a better heuristic to determine if we should
      // create a BigDecimal or a Double. BigDecimal is only meaningful if its encoding
      // will be less than 8 bytes.
      
      if (valuematcher.group(1).length() < 10 && valuematcher.group(2).length() < 10) {
        value = new BigDecimal(valuestr);
      } else {
        value = Double.parseDouble(valuestr);
      }
    } else {
      valuematcher = LONG_VALUE_RE.matcher(valuestr);
      
      if (valuematcher.matches()) {
        value = Long.parseLong(valuestr);
      } else {
        valuematcher = STRING_VALUE_RE.matcher(valuestr);
        
        if (valuematcher.matches()) {
          value = valuestr.substring(1, valuestr.length() - 1);
        } else {
          valuematcher = BOOLEAN_VALUE_RE.matcher(valuestr);
          
          if (valuematcher.matches()) {
            if ('t' == valuestr.charAt(0) || 'T' == valuestr.charAt(0)) {
              value = Boolean.TRUE;
            } else {
              value = Boolean.FALSE;
            }
          } else {
            throw new ParseException(valuestr, 0);
          }
        }
      }
    }

    return value;
  }
  /**
   * Parses the string representation of labels.
   * The representation is composed of name=value pairs separated by commas (',').
   * Values are expected to be UTF-8 strings percent-encoded.
   * 
   * @param str
   * @return
   * @throws ParseException if a label name is incorrect.
   */
  public static Map<String,String> parseLabels(int initialCapacity, String str) throws ParseException {
    
    //
    // Parse as selectors. We'll throw an exception if we encounter a regexp selector
    //
    
    Map<String,String> selectors = parseLabelsSelectors(str);
    Map<String,String> labels = new HashMap<String,String>(selectors.size() + initialCapacity);

    for (Entry<String, String> entry: selectors.entrySet()) {
      //
      // Check that selector is an exact match which would means
      // the syntax was label=value
      //
      
      if ('=' != entry.getValue().charAt(0)) {
        throw new ParseException(entry.getValue(), 0);
      }
      
      labels.put(entry.getKey(), entry.getValue().substring(1));
    }
    
    return labels;
  }
  
  public static Map<String,String> parseLabels(String str) throws ParseException {
    return parseLabels(0, str);
  }
  
  /**
   * Compute a class Id (metric name Id) using SipHash and the given key.
   * 
   * We compute the class Id in two phases.
   * 
   * During the first phase we compute the SipHash of the class name and
   * the SipHash of the reversed class name.
   * 
   * Then we compute the SipHash of the concatenation of those two hashes.
   * 
   * This is done to mitigate potential collisions in SipHash. We assume that
   * if a collision occurs for two class names CLASS0 and CLASS1, no collision will
   * occur for the reversed class names (i.e. 0SSALC and 1SSALC), and that no collision
   * will occur on concatenations of hash and hash of reversed input.
   * 
   * We waste some CPU cycles compared to just computing a SipHash of the class name
   * but this will ensure we do not risk synthezied collisions in classIds.
   * 
   * In the highly unlikely event of a collision, we'll offer 12 months of free service to the
   * customer who finds it!.
   * 
   * @param key 128 bits SipHash key to use
   * @param name Name of metric to hash
   */
  public static final long classId(byte[] key, String name) {
    long[] sipkey = SipHashInline.getKey(key);
    return classId(sipkey, name);
  }
  
  public static final long classId(long[] key, String name) {
    return classId(key[0], key[1], name);
  }
  
  public static final long classId(long k0, long k1, String name) {
    CharsetEncoder ce = StandardCharsets.UTF_8.newEncoder();

    ce.onMalformedInput(CodingErrorAction.REPLACE)
    .onUnmappableCharacter(CodingErrorAction.REPLACE)
    .reset();

    byte[] ba = new byte[(int) ((double) ce.maxBytesPerChar() * name.length())];

    int blen = ((ArrayEncoder)ce).encode(UnsafeString.getChars(name), UnsafeString.getOffset(name), name.length(), ba);

    return SipHashInline.hash24_palindromic(k0, k1, ba, 0, blen);
  }
  
  /**
   * Compute a gts Id from given classId/labelsId.
   * 
   * We compute the palindromic SipHash of 'GTS:' + <classId> + ':' + <labelsId>
   * 
   * @param classId
   * @param labelsId
   * @return
   */
  public static final long gtsId(long[] key, long classId, long labelsId) {
    byte[] buf = new byte[8 + 8 + 2 + 3];
    buf[0] = 'G';
    buf[1] = 'T';
    buf[2] = 'S';
    buf[3] = ':';
    for (int i = 0; i < 8; i++) {
      buf[11-i] = (byte) (classId & 0xffL);
      classId >>>= 8;
    }
    
    buf[12] = ':';
    
    for (int i = 0; i < 8; i++) {
      buf[20-i] = (byte) (labelsId & 0xffL);
      labelsId >>>= 8;
    }
    
    return SipHashInline.hash24_palindromic(key[0], key[1], buf);
  }
  
  /**
   * Compute the class Id of a Geo Time Serie instance.
   * 
   * @param key 128 bits SipHash key to use
   * @param gts GeoTimeSerie instance for which to compute the classId
   * @return
   */
  public static final long classId(byte[] key, GeoTimeSerie gts) {
    return classId(key, gts.getName());
  }
  
  /**
   * Compute a labels Id (unique hash of labels) using SipHash and the given key.
   * 
   * The label Id is computed in two phases.
   * 
   * During the first phase, we compute the SipHash of both label names and values.
   * We then sort those pairs of hashes in ascending order (name hashes, then associated values).
   * Note that unless there is a collision in name hashes, we should only have different name hashes
   * since a label name can only appear once in the labels map.
   * 
   * Once the hashes are sorted, we compute the SipHash of the ids array.
   * 
   * In case of label names collisions (label0 and label1), the labels Id will be identical for the value set of label0 and label1,
   * i.e. the labels Id will be the same for label0=X,label1=Y and label0=Y,label1=X.
   * 
   * In case of label values collisions(X and Y), the labels Id will be identical when the colliding values are affected to the same label,
   * i.e. the labels Id of label0=X will be the same as that of label0=Y.
   * 
   * Even though those cases have a non zero probability of happening, we deem them highly improbable as there is a strong probability
   * that the collision happens with some random data which would probably not be used as a label name or value which tend
   * to have meaning.
   * 
   * And if our hypothesis on randomness is not verified, we'll have the pleasure of showing the world a legible SipHash
   * collision :-) And we'll offer 6 month of subscription to the customer who finds it!
   * 
   * @param key 128 bit SipHash key to use
   * @param labels Map of label names to label values
   */
  
  public static final long labelsId(byte[] key, Map<String,String> labels) {
    long[] sipkey = SipHashInline.getKey(key);
    return labelsId(sipkey, labels);
  }
  
  public static final long labelsId(long[] sipkey, Map<String,String> labels) {
    return labelsId(sipkey[0], sipkey[1], labels);
  }
  
  public static final long labelsId(long sipkey0, long sipkey1, Map<String,String> labels) {
    
    //
    // Allocate a CharsetEncoder.
    // Implementation is a sun.nio.cs.UTF_8$Encoder which implements ArrayEncoder
    //
    
    CharsetEncoder ce = StandardCharsets.UTF_8.newEncoder();
    
    //
    // Allocate arrays large enough for most cases
    //
    
    int calen = 64;
    byte[] ba = new byte[(int) ((double) ce.maxBytesPerChar() * calen)];
    //char[] ca = new char[64];
    
    
    //
    // Allocate an array to hold both name and value hashes
    //
    
    long[] hashes = new long[labels.size() * 2];
    
    //
    // Compute hash for each name and each value
    //
    
    int idx = 0;
    
    for (Entry<String, String> entry: labels.entrySet()) {
      String ekey = entry.getKey();
      String eval = entry.getValue();
      
      int klen = ekey.length();
      int vlen = eval.length();
      
      if (klen > calen || vlen > calen) {
        calen = Math.max(klen, vlen);
        ba = new byte[(int) ((double) ce.maxBytesPerChar() * calen)];
      }
      
      ce.onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE)
      .reset();

      int blen = ((ArrayEncoder)ce).encode(UnsafeString.getChars(ekey), UnsafeString.getOffset(ekey), klen, ba);

      hashes[idx] = SipHashInline.hash24_palindromic(sipkey0, sipkey1, ba, 0, blen);
      
      ce.onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE)
      .reset();

      blen = ((ArrayEncoder)ce).encode(UnsafeString.getChars(eval), UnsafeString.getOffset(eval), vlen, ba);
      
      hashes[idx+1] = SipHashInline.hash24_palindromic(sipkey0, sipkey1, ba, 0, blen);
      idx+=2;
    }
    
    //
    // Sort array by label hash then value hash.
    // Given the normally reduced number of labels, we can use a simple bubble sort...
    //
    
    for (int i = 0; i < labels.size() - 1; i++) {
      int ii = i << 1;
      int iip = ii + 1;
      for (int j = i + 1; j < labels.size(); j++) {
        int jj = j << 1;
        int jjp = jj + 1;
        if (hashes[ii] > hashes[jj]) {
          //
          // We need to swap name and value ids at i and j
          //
          long tmp = hashes[jj];
          hashes[jj] = hashes[ii];
          hashes[ii] = tmp;
          tmp = hashes[jjp];
          hashes[jjp] = hashes[iip];
          hashes[iip] = tmp;                    
        } else if (hashes[ii] == hashes[jj] && hashes[iip] > hashes[jjp]) {
          //
          // We need to swap value ids at i and j
          //
          long tmp = hashes[jjp];
          hashes[jjp] = hashes[iip];
          hashes[iip] = tmp;
        }
      }
    }
    
    //
    // Now compute the SipHash of all the longs in the order we just determined
    // 
    
    byte[] buf = new byte[hashes.length * 8];

    idx = 0;
    
    for (long hash: hashes) {
      buf[idx++] = (byte) ((hash >> 56) & 0xffL);
      buf[idx++] = (byte) ((hash >> 48) & 0xffL);
      buf[idx++] = (byte) ((hash >> 40) & 0xffL);
      buf[idx++] = (byte) ((hash >> 32) & 0xffL);
      buf[idx++] = (byte) ((hash >> 24) & 0xffL);
      buf[idx++] = (byte) ((hash >> 16) & 0xffL);
      buf[idx++] = (byte) ((hash >> 8) & 0xffL);
      buf[idx++] = (byte) (hash & 0xffL);
    }
    
    return SipHashInline.hash24_palindromic(sipkey0, sipkey1, buf, 0, buf.length);
  }

  public static final long labelsId_slow(byte[] key, Map<String,String> labels) {
    //
    // Allocate an array to hold both name and value hashes
    //
    
    long[] hashes = new long[labels.size() * 2];
    
    //
    // Compute hash for each name and each value
    //
    
    int idx = 0;
    
    long[] sipkey = SipHashInline.getKey(key);
    
    for (Entry<String, String> entry: labels.entrySet()) {
      hashes[idx] = SipHashInline.hash24_palindromic(sipkey[0], sipkey[1], entry.getKey().getBytes(StandardCharsets.UTF_8));
      hashes[idx+1] = SipHashInline.hash24_palindromic(sipkey[0], sipkey[1], entry.getValue().getBytes(StandardCharsets.UTF_8));
      idx+=2;
    }
    
    //
    // Sort array by label hash then value hash.
    // Given the normally reduced number of labels, we can use a simple bubble sort...
    //
    
    for (int i = 0; i < labels.size() - 1; i++) {
      for (int j = i + 1; j < labels.size(); j++) {
        if (hashes[i * 2] > hashes[j * 2]) {
          //
          // We need to swap name and value ids at i and j
          //
          long tmp = hashes[j * 2];
          hashes[j * 2] = hashes[i * 2];
          hashes[i * 2] = tmp;
          tmp = hashes[j * 2 + 1];
          hashes[j * 2 + 1] = hashes[i * 2 + 1];
          hashes[i * 2 + 1] = tmp;                    
        } else if (hashes[i * 2] == hashes[j * 2] && hashes[i * 2 + 1] > hashes[j * 2 + 1]) {
          //
          // We need to swap value ids at i and j
          //
          long tmp = hashes[j * 2 + 1];
          hashes[j * 2 + 1] = hashes[i * 2 + 1];
          hashes[i * 2 + 1] = tmp;
        }
      }
    }
    
    //
    // Now compute the SipHash of all the longs in the order we just determined
    // 
    
    byte[] buf = new byte[hashes.length * 8];
    ByteBuffer bb = ByteBuffer.wrap(buf);
    bb.order(ByteOrder.BIG_ENDIAN);

    for (long hash: hashes) {
      bb.putLong(hash);
    }
    
    return SipHashInline.hash24_palindromic(sipkey[0], sipkey[1], buf, 0, buf.length);
  }
  
  /**
   * Compute the labels Id of a GeoTimeSerie instance.
   * 
   * @param key 128 bits SipHash key to use
   * @param gts GeoTimeSerie instance for which to compute the labels Id
   * @return Labels ID of a GeoTimeSerie instance
   */
  public static final long labelsId(byte[] key, GeoTimeSerie gts) {
    return labelsId(key, gts.getLabels());
  }

  /**
   * Convert a GTS Id packed as a BigInteger into an array of bytes
   * containing classId/labelsId in big endian representation
   * @param bi Packed GTS ID.
   * @return array of bytes containing classId/labelsId in big endian representation
   */
  public static byte[] unpackGTSId(BigInteger bi) {
    byte[] bytes = bi.toByteArray();
    
    if (bytes.length < 16) {
      byte[] tmp = new byte[16];
      if (bi.signum() < 0) {
        Arrays.fill(tmp, (byte) 0xff);
      }
      System.arraycopy(bytes, 0, tmp, tmp.length - bytes.length, bytes.length);
      return tmp;
    } else {
      return bytes;
    }
  }

  public static byte[] unpackGTSId(String s) {
    byte[] bytes = new byte[16];
    
    if (8 != s.length()) {
      return bytes;
    }
    
    char[] c = UnsafeString.getChars(s);

    for (int i = 0; i < 8; i++) {
      bytes[i * 2] = (byte) ((c[i] >> 8) & 0xFF);
      bytes[1 + i * 2] = (byte) (c[i] & 0xFF);
    }
    
    return bytes;
  }
  
  public static long[] unpackGTSIdLongs(String s) {
    long[] clslbls = new long[2];

    char[] c = UnsafeString.getChars(s);
    for (int i = 0; i < 4; i++) {
      clslbls[0] <<= 16;
      clslbls[0] |= (c[i] & 0xFFFFL) & 0xFFFFL;
      clslbls[1] <<= 16;
      clslbls[1] |= (c[i + 4] & 0xFFFFL) & 0xFFFFL;
    }


    return clslbls;
  }
  
  public static String gtsIdToString(long classId, long labelsId) {
    return gtsIdToString(classId, labelsId, true);
  }
  
  public static String gtsIdToString(long classId, long labelsId, boolean intern) {
    String s = new String("01234567");
    
    // This way we don't create a char array twice...
    char[] c = UnsafeString.getChars(s);
    
    long x = classId;
    long y = labelsId;
    
    for (int i = 3; i >= 0; i--) {
      c[i] = (char) (x & 0xffffL);
      c[i+4] = (char) (y & 0xffffL); 
      x >>>= 16;
      y >>>= 16;
    }
    
    if (intern) {
      return s.intern();
    } else {
      return s;
    }
  }
  
  public static long[] stringToGTSId(String s) {    
    char[] c = s.toCharArray();
    
    long x = 0L;
    long y = 0L;
    
    for (int i = 0; i < 4; i++) {
      x <<= 16;
      x |= (c[i] & 0xffffL);
      y <<= 16;
      y |= (c[i + 4] & 0xffffL);
    }
    
    long[] clslbls = new long[2];
    clslbls[0] = x;
    clslbls[1] = y;

    return clslbls;
  }
  
  public static void fillGTSIds(byte[] bytes, int offset, long classId, long labelsId) {   
    long id = classId;
    int idx = offset;
    
    bytes[idx++] = (byte) ((id >> 56) & 0xff);
    bytes[idx++] = (byte) ((id >> 48) & 0xff);
    bytes[idx++] = (byte) ((id >> 40) & 0xff);
    bytes[idx++] = (byte) ((id >> 32) & 0xff);
    bytes[idx++] = (byte) ((id >> 24) & 0xff);
    bytes[idx++] = (byte) ((id >> 16) & 0xff);
    bytes[idx++] = (byte) ((id >> 8) & 0xff);
    bytes[idx++] = (byte) (id & 0xff);
    
    id = labelsId;

    bytes[idx++] = (byte) ((id >> 56) & 0xff);
    bytes[idx++] = (byte) ((id >> 48) & 0xff);
    bytes[idx++] = (byte) ((id >> 40) & 0xff);
    bytes[idx++] = (byte) ((id >> 32) & 0xff);
    bytes[idx++] = (byte) ((id >> 24) & 0xff);
    bytes[idx++] = (byte) ((id >> 16) & 0xff);
    bytes[idx++] = (byte) ((id >> 8) & 0xff);
    bytes[idx++] = (byte) (id & 0xff);
  }
  
  /**
   * Parse labels selectors and return a map of label name to selector.
   * 
   * The syntax of the 'selectors' String is:
   * 
   * NAME&lt;TYPE&gt;VALUE,NAME&lt;TYPE&gt;VALUE,...
   * 
   * where &lt;TYPE&gt; is either '=' for exact matches or '~' for regular expression matches.
   * NAME and VALUE are percent-encoded UTF-8 Strings.
   * 
   * @param selectors Selectors following the syntax NAME&lt;TYPE&gt;VALUE,NAME&lt;TYPE&gt;VALUE,... to be parsed.
   * @return A map from label name to selector.
   */
  public static final Map<String,String> parseLabelsSelectors(String selectors) throws ParseException {
    //
    // Split selectors on ',' boundaries
    //
    
    //String[] tokens = selectors.split(",");
    String[] tokens = UnsafeString.split(selectors, ',');
    
    //
    // Loop over the tokens
    //
    
    Map<String,String> result = new HashMap<String,String>(tokens.length);
    
    for (String token: tokens) {
      
      token = token.trim();
      
      if ("".equals(token)) {
        continue;
      }
      
      //
      // Split on '=' or '~'
      //
      
      boolean exact = true;
      
      String[] subtokens;
      
      if (token.contains("=")) {
        exact = true;
        subtokens = UnsafeString.split(token, '=');
      } else if (token.contains("~")){
        exact = false;
        subtokens = UnsafeString.split(token, '~');
      } else {
        throw new ParseException(token,0);
      }
      
      String name = subtokens[0];
      String value = subtokens.length > 1 ? subtokens[1] : "";

      try {
        name = WarpURLDecoder.decode(name, StandardCharsets.UTF_8);
        value = WarpURLDecoder.decode(value, StandardCharsets.UTF_8);
      } catch (UnsupportedEncodingException uee) {
        // Can't happen since we are using a standard JVM charset
      }
            
      result.put(name, (exact ? "=" : "~") + value);
    }
    
    return result;
  }
  
  /**
   * Return patterns for matching class and labels
   * The class selector is associated with the 'null' key.
   * The labels selectors are associated with the label name they're associated with
   * 
   * @param classLabelsSelectionString A string representation of a class/labels selector
   * @return Patterns for matching class and labels.
   * @throws ParseException when classLabelsSelectionString is not representing a selector.
   */
  public static Map<String,Pattern> patternsFromSelectors(String classLabelsSelectionString) throws ParseException {
    String classSelector = classLabelsSelectionString.replaceAll("\\{.*$", "");
    String labelsSelectorsString = classLabelsSelectionString.replaceAll("^.*\\{","").replaceAll("\\}.*$", "");
    
    Map<String,String> labelsSelectors = parseLabelsSelectors(labelsSelectorsString);
    
    Map<String,Pattern> patterns = new HashMap<String,Pattern>();
    
    try {
      classSelector = WarpURLDecoder.decode(classSelector, StandardCharsets.UTF_8);
    } catch (UnsupportedEncodingException uee) {
      // Can't happen since we are using a standard JVM charset
    }
    
    if ('=' == classSelector.charAt(0)) {
      patterns.put(null, Pattern.compile(Pattern.quote(classSelector.substring(1))));            
    } else if ('~' == classSelector.charAt(0)) {
      patterns.put(null, Pattern.compile(classSelector.substring(1)));      
    } else {
      patterns.put(null, Pattern.compile(Pattern.quote(classSelector)));      
    }
    
    for (Entry<String,String> entry: labelsSelectors.entrySet()) {
      if ('=' == entry.getValue().charAt(0)) {
        patterns.put(entry.getKey(), Pattern.compile(Pattern.quote(entry.getValue().substring(1))));
      } else {
        patterns.put(entry.getKey(), Pattern.compile(entry.getValue().substring(1)));
      }
    }
    
    return patterns;
  }
  
  public static Iterator<String> valueRepresentationIterator(final GeoTimeSerie gts) {
    return new Iterator<String>() {
      int idx = 0;
      
      @Override
      public boolean hasNext() {
        return idx < gts.values;
      }
      @Override
      public String next() {
        Object value;
        if (TYPE.LONG == gts.type) {
          value = gts.longValues[idx];
        } else if (TYPE.DOUBLE == gts.type) {
          value = gts.doubleValues[idx];
        } else if (TYPE.STRING == gts.type) {
          value = gts.stringValues[idx];
        } else if (TYPE.BOOLEAN == gts.type) {
          value = gts.booleanValues.get(idx);
        } else {
          value = null;
        }

        String repr = tickToString(gts.ticks[idx], null != gts.locations ? gts.locations[idx] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[idx] : GeoTimeSerie.NO_ELEVATION, value);
        idx++;
        
        return repr;
      }
      @Override
      public void remove() {}
    };
  }
  
  /**
   * Return a string representation of a GTS measurement at 'tick'
   * 
   * @return String representation of a GTS measurement at 'tick'.
   */
  public static String tickToString(StringBuilder clslbls, long timestamp, long location, long elevation, Object value) {
    try {
      StringBuilder sb = new StringBuilder();
      
      sb.append(timestamp);
      sb.append("/");
      if (GeoTimeSerie.NO_LOCATION != location) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(location);
        sb.append(latlon[0]);
        sb.append(":");
        sb.append(latlon[1]);
      }
      sb.append("/");
      if (GeoTimeSerie.NO_ELEVATION != elevation) {
        sb.append(elevation);
      }
      sb.append(" ");
      
      if (null != clslbls && clslbls.length() > 0) {
        sb.append(clslbls);
        sb.append(" ");
      }
      
      encodeValue(sb, value);
      return sb.toString();
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static String tickToString(long timestamp, long location, long elevation, Object value) {
    return tickToString(null, timestamp, location, elevation, value);
  }

  public static void encodeValue(StringBuilder sb, Object value) {
    if (value instanceof Long || value instanceof Double) {
      sb.append(value);
    } else if (value instanceof BigDecimal) {
      sb.append(((BigDecimal) value).toPlainString());
      if (((BigDecimal) value).scale() <= 0) {
        sb.append(".0");
      }
    } else if (value instanceof Boolean) {
      sb.append(((Boolean) value).equals(Boolean.TRUE) ? "T" : "F");
    } else if (value instanceof String) {
      sb.append("'");
      try {
        String encoded = WarpURLEncoder.encode((String) value, StandardCharsets.UTF_8);
        sb.append(encoded);
      } catch (UnsupportedEncodingException uee) {
        // Won't happen
      }
      sb.append("'");
    } else if (value instanceof byte[]) {
      sb.append("b64:");
      sb.append(Base64.encodeBase64URLSafeString((byte[]) value));
    }
  }
  
  public static void encodeName(StringBuilder sb, String name) {
    if (null == name) {
      return;
    }
    try {
      String encoded = WarpURLEncoder.encode(name, StandardCharsets.UTF_8);
      sb.append(encoded);
    } catch (UnsupportedEncodingException uee) {      
    }
  }
  
  /**
   * Merge 'gts' into 'base' the slow way, i.e. copying values one at a time.
   * 
   * If 'gts' and 'base' differ in type, no merging takes place, unless 'base' is empty.
   * Values/locations/elevations in gts override those in base if they occur at the same tick.
   * 
   * No check of name or labels is done, the name and labels of 'base' are not modified.
   * 
   * @param base GTS instance into which other values should be merged
   * @param gts GTS instance whose values/locations/elevations should be merged.
   * @param overwrite Flag indicating whether or not to overwrite existing value at identical timestamp
   * 
   * @return base
   */
  public static GeoTimeSerie slowmerge(GeoTimeSerie base, GeoTimeSerie gts, boolean overwrite) {
    GeoTimeSerie.TYPE baseType = base.getType();
    GeoTimeSerie.TYPE gtsType = gts.getType();
    
    if (TYPE.UNDEFINED.equals(baseType) || baseType.equals(gtsType)) {
      for (int i = 0; i < gts.values; i++) {
        GTSHelper.setValue(base, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, valueAtIndex(gts, i), overwrite);        
      }
    }

    return base;
  }

  /**
   * Merge 'gts' into 'base'.
   * 
   * If 'gts' and 'base' differ in type, no merging takes place, unless 'base' is empty.
   * No deduplication is done if the same tick is present in 'base' and 'gts'.
   * 
   * No check of name or labels is done, the name and labels of 'base' are not modified.
   * 
   * @param base GTS instance into which other values should be merged
   * @param gts GTS instance whose values/locations/elevations should be merged.
   * 
   * @return base
   */

  public static GeoTimeSerie merge(GeoTimeSerie base, GeoTimeSerie gts) {
    GeoTimeSerie.TYPE baseType = base.getType();
    GeoTimeSerie.TYPE gtsType = gts.getType();
    
    if (TYPE.UNDEFINED.equals(baseType) || baseType.equals(gtsType)) {
      
      if (0 == gts.values) {
        return base;
      }
      
      if (null == base.ticks) {
        base.ticks = Arrays.copyOf(gts.ticks, gts.values);
      } else {
        if (base.ticks.length < base.values + gts.values) {
          base.ticks = Arrays.copyOf(base.ticks, base.values + gts.values);
        }
        System.arraycopy(gts.ticks, 0, base.ticks, base.values, gts.values);
      }

      if (null == base.locations) {
        if (null != gts.locations) {
          base.locations = new long[base.ticks.length];
          Arrays.fill(base.locations, GeoTimeSerie.NO_LOCATION);
          System.arraycopy(gts.locations, 0, base.locations, base.values, gts.values);
        }
      } else {
        if (base.locations.length < base.values + gts.values) {
          base.locations = Arrays.copyOf(base.locations, base.values + gts.values);
        }
        if (null != gts.locations) {
          System.arraycopy(gts.locations, 0, base.locations, base.values, gts.values);
        } else {
          Arrays.fill(base.locations, base.values, base.values + gts.values, GeoTimeSerie.NO_LOCATION);
        }
      }

      if (null == base.elevations) {
        if (null != gts.elevations) {
          base.elevations = new long[base.ticks.length];
          Arrays.fill(base.elevations, GeoTimeSerie.NO_ELEVATION);
          System.arraycopy(gts.elevations, 0, base.elevations, base.values, gts.values);
        }
      } else {
        if (base.elevations.length < base.values + gts.values) {
          base.elevations = Arrays.copyOf(base.elevations, base.values + gts.values);
        }
        if (null != gts.elevations) {
          System.arraycopy(gts.elevations, 0, base.elevations, base.values, gts.values);
        } else {
          Arrays.fill(base.elevations, base.values, base.values + gts.values, GeoTimeSerie.NO_ELEVATION);
        }
      }

      switch (gtsType) {
        case LONG:
          base.type = TYPE.LONG;
          if (null == base.longValues) {
            base.longValues = Arrays.copyOf(gts.longValues, gts.values);
          } else {
            if (base.longValues.length < base.values + gts.values) {
              base.longValues = Arrays.copyOf(base.longValues, base.values + gts.values);
            }
            System.arraycopy(gts.longValues, 0, base.longValues, base.values, gts.values);
          }
          break;
        case DOUBLE:
          base.type = TYPE.DOUBLE;
          if (null == base.doubleValues) {
            base.doubleValues = Arrays.copyOf(gts.doubleValues, gts.values);
          } else {
            if (base.doubleValues.length < base.values + gts.values) {
              base.doubleValues = Arrays.copyOf(base.doubleValues, base.values + gts.values);
            }
            System.arraycopy(gts.doubleValues, 0, base.doubleValues, base.values, gts.values);
          }
          break;
        case STRING:
          base.type = TYPE.STRING;
          if (null == base.stringValues) {
            base.stringValues = Arrays.copyOf(gts.stringValues, gts.values);
          } else {
            if (base.stringValues.length < base.values + gts.values) {
              base.stringValues = Arrays.copyOf(base.stringValues, base.values + gts.values);
            }
            System.arraycopy(gts.stringValues, 0, base.stringValues, base.values, gts.values);
          }
          break;
        case BOOLEAN:
          base.type = TYPE.BOOLEAN;
          if (null == base.booleanValues) {
            base.booleanValues = (BitSet) gts.booleanValues.clone();
          } else {
            for (int i = 0; i < gts.values; i++) {
              base.booleanValues.set(base.values + i, gts.booleanValues.get(i));
            }
          }
          break;
      }
      
    } else {
      throw new RuntimeException("Merge cannot proceed with incompatible GTS types.");
    }

    // Try to keep sorted and reversed information on the base GTS
    if (0 == base.values) {
      base.sorted = gts.sorted;
      base.reversed = gts.reversed;
    } else if (base.sorted && gts.sorted && base.reversed == gts.reversed) {
      // We're already sure 0 != gts.values (checked at the beginning of the function) and 0 != base.values (checked above)
      if (base.reversed) {
        base.sorted = base.ticks[base.values - 1] >= gts.ticks[0];
      } else {
        base.sorted = base.ticks[base.values - 1] <= gts.ticks[0];
      }
    } else {
      base.sorted = false;
    }

    base.values = base.values + gts.values;

    return base;
  }

  /**
   * Merge GeoTimeSerie instances using GTSEncoders.
   * This is noticeably faster than what 'merge' is doing.
   * 
   * @param series List of series to merge, the first one will be considered the base. Its metadata will be used.
   * @return The merged series.
   */
  public static GeoTimeSerie mergeViaEncoders(List<GeoTimeSerie> series) throws IOException {
    //
    // We merge the GTS but do not use GTSHelper#merge
    // as it is less efficient than the use of GTSDecoder
    //
    
    GTSEncoder encoder = new GTSEncoder(0L);

    try {
      for (int i = 0; i < series.size(); i++) {
        GeoTimeSerie gts = series.get(i);

        if (0 == i) {
          encoder.setMetadata(gts.getMetadata());
        }
        encoder.encode(gts);
      }
    } catch (IOException ioe) {
      throw new IOException(ioe);
    }
    
    //return encoder.getUnsafeDecoder(false).decode();
    return encoder.getDecoder(true).decode();
  }

  /**
   * Merge 2 GTSs into one sorted GTS, with values from gts overwriting values from base.
   * If both GTSs are without duplicate, the result is also without duplicate.
   * If there are some duplicates in base or gts, the result will contain some duplicates so it's best to avoid this situation.
   *
   * The returned GTS has the same classname, labels and attributes as the base GTS.
   *
   * @param base The GTS used as a reference
   * @param gts  The GTS whose data is to be merged with that of base.
   * @return A new instance of GTS, sorted and without duplicates if base and gts are without duplicates.
   * @throws RuntimeException when GTSs are not of the same type and base is not empty.
   */
  public static GeoTimeSerie sortedMerge(GeoTimeSerie base, GeoTimeSerie gts) {
    // No values to merge, return clone of base
    if (0 == gts.values) {
      return base.clone();
    }

    GeoTimeSerie.TYPE baseType = base.getType();
    GeoTimeSerie.TYPE gtsType = gts.getType();

    if (!GeoTimeSerie.TYPE.UNDEFINED.equals(baseType) && !baseType.equals(gtsType)) {
      throw new RuntimeException("merge cannot proceed with incompatible GTS types.");
    }

    GeoTimeSerie merged = base.cloneEmpty();
    merged.type = gtsType; // Make sure the type is set in case the base is UNDEFINED


    // GTSs must be sorted
    sort(base);
    sort(gts);

    //
    // Initialize arrays
    //

    merged.ticks = new long[base.values + gts.values]; // If there are dups it may be more than enough but it's cheaper that way

    if (null != base.locations || null != gts.locations) {
      merged.locations = new long[merged.ticks.length];
    }

    if (null != base.elevations || null != gts.elevations) {
      merged.elevations = new long[merged.ticks.length];
    }

    if (GeoTimeSerie.TYPE.LONG == merged.type) {
      merged.longValues = new long[base.values + gts.values];
    } else if (GeoTimeSerie.TYPE.DOUBLE == merged.type) {
      merged.doubleValues = new double[base.values + gts.values];
    } else if (GeoTimeSerie.TYPE.STRING == merged.type) {
      merged.stringValues = new String[base.values + gts.values];
    } else { // TYPE.BOOLEAN == merged.type
      merged.booleanValues = new BitSet();
    }

    int baseIndex = 0;
    int gtsIndex = 0;
    int mergedIndex = 0;

    while (base.values > baseIndex && gts.values > gtsIndex) {

      if (base.ticks[baseIndex] < gts.ticks[gtsIndex]) {
        merged.ticks[mergedIndex] = base.ticks[baseIndex];
        if (null != merged.locations) {
          merged.locations[mergedIndex] = null == base.locations ? GeoTimeSerie.NO_LOCATION : base.locations[baseIndex];
        }
        if (null != merged.locations) {
          merged.locations[mergedIndex] = null == base.locations ? GeoTimeSerie.NO_LOCATION : base.locations[baseIndex];
        }
        if (GeoTimeSerie.TYPE.LONG == merged.type) {
          merged.longValues[mergedIndex] = base.longValues[baseIndex];
        } else if (GeoTimeSerie.TYPE.DOUBLE == merged.type) {
          merged.doubleValues[mergedIndex] = base.doubleValues[baseIndex];
        } else if (GeoTimeSerie.TYPE.STRING == merged.type) {
          merged.stringValues[mergedIndex] = base.stringValues[baseIndex];
        } else { // TYPE.BOOLEAN == merged.type
          merged.booleanValues.set(mergedIndex, base.booleanValues.get(baseIndex));
        }

        baseIndex++;
      } else {
        if (base.ticks[baseIndex] == gts.ticks[gtsIndex]) {
          baseIndex++;
        }

        merged.ticks[mergedIndex] = gts.ticks[gtsIndex];
        if (null != merged.locations) {
          merged.locations[mergedIndex] = null == gts.locations ? GeoTimeSerie.NO_LOCATION : gts.locations[gtsIndex];
        }
        if (null != merged.locations) {
          merged.locations[mergedIndex] = null == gts.locations ? GeoTimeSerie.NO_LOCATION : gts.locations[gtsIndex];
        }
        if (GeoTimeSerie.TYPE.LONG == merged.type) {
          merged.longValues[mergedIndex] = gts.longValues[gtsIndex];
        } else if (GeoTimeSerie.TYPE.DOUBLE == merged.type) {
          merged.doubleValues[mergedIndex] = gts.doubleValues[gtsIndex];
        } else if (GeoTimeSerie.TYPE.STRING == merged.type) {
          merged.stringValues[mergedIndex] = gts.stringValues[gtsIndex];
        } else { // TYPE.BOOLEAN == merged.type
          merged.booleanValues.set(mergedIndex, gts.booleanValues.get(gtsIndex));
        }

        gtsIndex++;
      }

      mergedIndex++;
    }

    if (base.values > baseIndex) {
      int length = base.values - baseIndex;
      // Safe to use copy0 because all checks have been done on array existence and size
      copy0(base, baseIndex, merged, mergedIndex, length);
      mergedIndex += length;
    } else if (gts.values > gtsIndex) {
      int length = gts.values - gtsIndex;
      // Safe to use copy0 because all checks have been done on array existence and size
      copy0(gts, gtsIndex, merged, mergedIndex, length);
      mergedIndex += length;
    }

    merged.values = mergedIndex;
    merged.sorted = true;

    return merged;
  }
  
  /**
   * Fill missing values/locations/elevations in a bucketized GTS with the previously
   * encountered one.
   * 
   * Returns a filled clone of 'gts'. 'gts' is not modified.
   * 
   * @param gts GeoTimeSerie to fill
   * @return
   */
  public static GeoTimeSerie fillprevious(GeoTimeSerie gts) {
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not bucketized, do nothing
    //
        
    if (!isBucketized(filled)) {
      return filled;
    }
    
    //
    // Sort GTS in natural ordering of ticks
    //
    
    sort(filled, false);
    
    //
    // Loop on ticks
    //
    
    int nticks = filled.values;
    
    //
    // Modify size hint so we only allocate once
    //

    if (0 != nticks) {
      long firsttick = filled.ticks[0];
      filled.setSizeHint(1 + (int) ((filled.lastbucket - firsttick) / filled.bucketspan));
    }
    
    int idx = 0;
    int bucketidx = filled.bucketcount - 1;
    long bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
    
    Object prevValue = null;
    long prevLocation = GeoTimeSerie.NO_LOCATION;
    long prevElevation = GeoTimeSerie.NO_ELEVATION;
    
    long bucketoffset = filled.lastbucket % filled.bucketspan;
    
    while (bucketidx >= 0) {
      //
      // Only consider ticks which are valid buckets. We need to do
      // this test because values could have been added at invalid bucket
      // timestamps after the GTS has been bucketized.
      //
      
      while(idx < nticks && bucketoffset != (filled.ticks[idx] % filled.bucketspan)) {
        idx++;
      }
      
      if (idx >= nticks) {
        break;
      }
      
      while(bucketidx >= 0 && filled.ticks[idx] > bucketts) {
        if (null != prevValue) {
          setValue(filled, bucketts, prevLocation, prevElevation, prevValue, false);
        }
        bucketidx--;
        bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
      }
      
      //
      // We need to decrease bucketidx as ticks[idx] == bucketts
      // otherwise we would duplicate the existing values.
      //
      
      bucketidx--;
      bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
      
      prevValue = valueAtIndex(filled, idx);
      prevLocation = null != filled.locations ? filled.locations[idx] : GeoTimeSerie.NO_LOCATION;
      prevElevation = null != filled.elevations ? filled.elevations[idx] : GeoTimeSerie.NO_ELEVATION;
      
      idx++;
    }
    
    //
    // Finish filling up past the last seen value
    //

    while(bucketidx >= 0) {
      if (null != prevValue) {
        setValue(filled, bucketts, prevLocation, prevElevation, prevValue, false);
      }
      bucketidx--;
      bucketts = filled.lastbucket - bucketidx * filled.bucketspan;      
    }

    return filled;
  }

  /**
   * Fill missing values/locations/elevations in a bucketized GTS with the next
   * encountered one.
   * 
   * @param gts GeoTimeSerie to fill
   * @return A filled clone of 'gts'.
   */
  public static GeoTimeSerie fillnext(GeoTimeSerie gts) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not bucketized, do nothing
    //
        
    if (!isBucketized(filled)) {
      return filled;
    }
    
    //
    // Sort GTS in reverse ordering of ticks
    //
    
    sort(filled, true);
    
    //
    // Loop on ticks
    //
    
    int nticks = filled.values;

    //
    // Change size hint so we only do one allocation
    //
    
    if (0 != nticks) {
      long lasttick = filled.ticks[0];
      filled.setSizeHint(1 + (int) ((lasttick - (filled.lastbucket - filled.bucketcount * filled.bucketspan)) / filled.bucketspan));
    }

    int idx = 0;
    int bucketidx = 0;
    long bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
    
    Object prevValue = null;
    long prevLocation = GeoTimeSerie.NO_LOCATION;
    long prevElevation = GeoTimeSerie.NO_ELEVATION;
    
    long bucketoffset = filled.lastbucket % filled.bucketspan;

    while (bucketidx < filled.bucketcount) {
      //
      // Only consider ticks which are valid buckets. We need to do
      // this test because values could have been added at invalid bucket
      // timestamps after the GTS has been bucketized.
      //
      
      while(idx < nticks && bucketoffset != (filled.ticks[idx] % filled.bucketspan)) {
        idx++;
      }
      
      if (idx >= nticks) {
        break;
      }
      
      while(bucketidx >= 0 && filled.ticks[idx] < bucketts) {
        if (null != prevValue) {
          setValue(filled, bucketts, prevLocation, prevElevation, prevValue, false);
        }
        bucketidx++;
        bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
      }
      
      //
      // We need to increase bucketidx as ticks[idx] == bucketts
      // otherwise we would duplicate the existing values.
      //
      
      bucketidx++;
      bucketts = filled.lastbucket - bucketidx * filled.bucketspan;
      
      prevValue = valueAtIndex(filled, idx);
      prevLocation = null != filled.locations ? filled.locations[idx] : GeoTimeSerie.NO_LOCATION;
      prevElevation = null != filled.elevations ? filled.elevations[idx] : GeoTimeSerie.NO_ELEVATION;
      
      idx++;
    }
    
    //
    // Finish filling up past the last seen value
    //

    while(bucketidx < filled.bucketcount) {
      if (null != prevValue) {
        setValue(filled, bucketts, prevLocation, prevElevation, prevValue, false);
      }
      bucketidx++;
      bucketts = filled.lastbucket - bucketidx * filled.bucketspan;      
    }

    return filled;
  }

  /**
   * Fill missing values/locations/elevations in a bucketized GTS with the
   * given location/elevation/value.
   * 
   * @param gts GeoTimeSerie to fill
   * @param location Location to use for filling
   * @param elevation Elevation to use for filling
   * @param value Value to use for filling 'gts'
   * 
   * @return A filled clone of 'gts'.
   */
  public static GeoTimeSerie fillvalue(GeoTimeSerie gts, long location, long elevation, Object value) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not bucketized or value is null, do nothing
    //
        
    if (!isBucketized(filled) || null == value) {
      return filled;
    }

    //
    // Force size hint since we know the GTS will contain 'bucketcount' ticks
    //
    
    filled.setSizeHint(filled.bucketcount);
    
    //
    // Sort 'filled'
    //
    
    sort(filled);
    
    int bucket = filled.bucketcount - 1;
    int idx = 0;
    int nticks = filled.values;
    
    while (bucket >= 0) {
      long bucketts = filled.lastbucket - bucket * filled.bucketspan;
      
      while((idx < nticks && filled.ticks[idx] > bucketts) || (idx >= nticks && bucket >= 0)) {
        setValue(filled, bucketts, location, elevation, value, false);
        bucket--;
        bucketts = filled.lastbucket - bucket * filled.bucketspan;
      }
    
      idx++;
      bucket--;
    }
  
    return filled;
  }

  /**
   * Fill the given ticks in a GTS with the elements provided.
   * 
   * If the GTS is bucketized, do nothing.
   * 
   * @param gts GeoTimeSerie instance to had its values filled.
   * @param location Location data to use for the filling.
   * @param elevation Elevation data to use for the filling.
   * @param value Value data to use for the filling.
   * @param ticks Ticks to be filled.
   * @return A cloned and filled GTS or a cloned GTS if bucketized.
   */
  public static GeoTimeSerie fillticks(GeoTimeSerie gts, long location, long elevation, Object value, long[] ticks) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If value is null or GTS bucketized, do nothing
    //
        
    if (null == value || GTSHelper.isBucketized(filled)) {
      return filled;
    }
    
    //
    // Extract ticks from the GTS and sort them, we don't want to sort the values or location info
    //
    
    long[] gticks = filled.values > 0 ? Arrays.copyOf(filled.ticks, filled.values) : new long[0];
    
    Arrays.sort(gticks);
    
    //
    // Sort ticks
    //
    
    Arrays.sort(ticks);
    
    int gtsidx = 0;
    int tickidx = 0;
    
    int nvalues = filled.values;
        
    while(gtsidx < nvalues) {
      long tick = gticks[gtsidx];
      
      while(tickidx < ticks.length && ticks[tickidx] < tick) {
        GTSHelper.setValue(filled, ticks[tickidx], location, elevation, value, false);
        tickidx++;
      }
      
      gtsidx++;
    }
        
    while(tickidx < ticks.length) {
      GTSHelper.setValue(filled, ticks[tickidx], location, elevation, value, false);
      tickidx++;
    }
  
    return filled;
  }

  /**
   * This function fills the gaps in two GTS so they end up with identical ticks
   * 
   * @param gtsa First GTS to fill
   * @param gtsb Second GTS to fill
   * @param filler Instance of filler to use for filling the gaps.
   */
  public static final List<GeoTimeSerie> fill(GeoTimeSerie gtsa, GeoTimeSerie gtsb, WarpScriptFillerFunction filler) throws WarpScriptException {
    //
    // Ensure the two original GTS are sorted
    //
    
    sort(gtsa, false);
    sort(gtsb, false);

    //
    // Clone the Geo Time Series, we will fill ga and gb
    //
    
    GeoTimeSerie ga = gtsa.clone();
    GeoTimeSerie gb = gtsb.clone();    
    
    int idxa = 0;
    int idxb = 0;
    
    Long curTickA = null;
    Long curTickB = null;
    
    String classA = ga.getName();
    String classB = gb.getName();
    
    Map<String,String> labelsA = Collections.unmodifiableMap(ga.getLabels());
    Map<String,String> labelsB = Collections.unmodifiableMap(gb.getLabels());
    
    Map<String,String> attrA = Collections.unmodifiableMap(ga.getMetadata().getAttributes());
    Map<String,String> attrB = Collections.unmodifiableMap(gb.getMetadata().getAttributes());
    
    //
    // We use a sweeping line algorithm to go over all the ticks
    //

    int prewindow = filler.getPreWindow() >= 0 ? filler.getPreWindow() : 0;
    int postwindow = filler.getPostWindow() >= 0 ? filler.getPostWindow() : 0;
    
    Object[] meta = new Object[2];
    Object[][] prev = new Object[prewindow][];
    for (int i = 0; i < prewindow; i++) {
      prev[i] = new Object[4];
    }
    Object[][] next = new Object[postwindow][];
    for (int i = 0; i < postwindow; i++) {
      next[i] = new Object[4];
    }
    Object[] other = new Object[4];
    Object[][] params = new Object[2 + prewindow + postwindow][];
    
    while(idxa < gtsa.values || idxb < gtsb.values) {

      curTickA = null;
      curTickB = null;
      
      if (idxa < gtsa.values) {
        curTickA = gtsa.ticks[idxa];
      }
      
      if (idxb < gtsb.values) {
        curTickB = gtsb.ticks[idxb];
      }
      
      //
      // If both ticks are identical, advance the indices until the next timestamp
      //

      if (null != curTickA && curTickA.equals(curTickB)) {
        idxa++;
        idxb++;
        
        if ((idxa < gtsa.values && curTickA == gtsa.ticks[idxa])
            || (idxb < gtsb.values && curTickB == gtsb.ticks[idxb])) {
          throw new WarpScriptException("Cannot fill Geo Time Series™ with duplicate timestamps.");
        }
        continue;
      }

      //
      // Determine if we should fill GTS A or GTS B
      //

      for (int i = 0; i < prewindow; i++) {
        prev[i][0] = null;
        prev[i][1] = null;
        prev[i][2] = null;
        prev[i][3] = null;
      }

      for (int i = 0; i < postwindow; i++) {
        next[i][0] = null;
        next[i][1] = null;
        next[i][2] = null;
        next[i][3] = null;
      }

      Object otherValue = null;
      Long otherTick = null;
      Long otherLocation = null;
      Long otherElevation = null;
      
      Metadata ourMeta = new Metadata();
      Metadata otherMeta = new Metadata();
      
      String ourClass = null;
      Map<String,String> ourLabels = null;
      Map<String,String> ourAttr = null;
      
      String otherClass = null;
      Map<String,String> otherLabels = null;
      Map<String,String> otherAttr = null;      
      
      GeoTimeSerie filled = null;
      
      if (curTickA == null || (null != curTickB && curTickA > curTickB)) {
        // We should fill GTS A
        
        filled = ga;

        for (int i = prewindow - 1; i >= 0; i--) {
          int ia = idxa - prewindow + i;
          if (ia >= 0) {
            prev[i][0] = gtsa.ticks[ia];
            prev[i][1] = locationAtIndex(gtsa, ia);
            prev[i][2] = elevationAtIndex(gtsa, ia);
            prev[i][3] = valueAtIndex(gtsa, ia);
          } else {
            break; // No more element to add
          }
        }

        for (int i = 0; i < postwindow; i++) {
          int ia = idxa + i;
          if (ia < gtsa.values) {
            next[i][0] = gtsa.ticks[ia];
            next[i][1] = locationAtIndex(gtsa, ia);
            next[i][2] = elevationAtIndex(gtsa, ia);
            next[i][3] = valueAtIndex(gtsa, ia);
          } else {
            break; // No more element to add
          }
        }
      
        otherValue = valueAtIndex(gtsb, idxb);
        otherTick = gtsb.ticks[idxb];
        otherLocation = locationAtIndex(gtsb, idxb);
        otherElevation = elevationAtIndex(gtsb, idxb);
        
        ourClass = classA;
        ourLabels = labelsA;
        ourAttr = attrA;
        
        otherClass = classB;
        otherLabels = labelsB;
        otherAttr = attrB;
        
        idxb++;        
      } else {
        // We should fill GTS B
      
        filled = gb;

        for (int i = prewindow - 1; i >= 0; i--) {
          int ib = idxb - prewindow + i;
          if (ib >= 0) {
            prev[i][0] = gtsb.ticks[ib];
            prev[i][1] = locationAtIndex(gtsb, ib);
            prev[i][2] = elevationAtIndex(gtsb, ib);
            prev[i][3] = valueAtIndex(gtsb, ib);
          } else {
            break; // No more element to add
          }
        }

        for (int i = 0; i < postwindow; i++) {
          int ib = idxb + i;
          if (ib < gtsb.values) {
            next[i][0] = gtsb.ticks[ib];
            next[i][1] = locationAtIndex(gtsb, ib);
            next[i][2] = elevationAtIndex(gtsb, ib);
            next[i][3] = valueAtIndex(gtsb, ib);
          } else {
            break; // No more element to add
          }
        }
        
        otherValue = valueAtIndex(gtsa, idxa);
        otherTick = gtsa.ticks[idxa];
        otherLocation = locationAtIndex(gtsa, idxa);
        otherElevation = elevationAtIndex(gtsa, idxa);
        
        ourClass = classB;
        ourLabels = labelsB;
        ourAttr = attrB;
        
        otherClass = classA;
        otherLabels = labelsA;
        otherAttr = attrA;
        
        idxa++;
      }
      
      other[0] = otherTick;
      other[1] = otherLocation;
      other[2] = otherElevation;
      other[3] = otherValue;
      
      ourMeta.setName(ourClass);
      ourMeta.setLabels(ourLabels);
      ourMeta.setAttributes(ourAttr);
      meta[0] = ourMeta;
      
      otherMeta.setName(otherClass);
      otherMeta.setLabels(otherLabels);
      otherMeta.setAttributes(otherAttr);
      meta[1] = otherMeta;
            
      params[0] = meta;
      for (int i = 0; i < prewindow; i++) {
        params[1 + i] = prev[i];
      }
      params[prewindow + 1] = other;
      for (int i = 0; i < postwindow; i++) {
        params[2 + prewindow + i] = next[i];
      }

      //
      // Call the filler
      //
      
      Object[] result = filler.apply(params);
      
      if (null != result[3]) {
        long tick = ((Number) result[0]).longValue();
        long location = ((Number) result[1]).longValue();
        long elevation = ((Number) result[2]).longValue();
        Object value = result[3];
        
        GTSHelper.setValue(filled, tick, location, elevation, value, false);        
      }
    }
    
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    results.add(ga);
    results.add(gb);
    
    return results;
  }
  
  /**
   * Compensate resets by computing an offset each time a value decreased between two ticks.
   * 
   * If we have the following TS (from most recent to most ancient) :
   * 
   * 10 5 1 9 4 3 7 2 1
   * 
   * we detect 2 resets (one between values 9 and 1, another one between 7 and 3).
   * 
   * The compensated GTS will be
   * 
   * 31 21 17 16 11 10 7 2 1
   * 
   * @param gts GTS instance to compensate resets for
   * @param resethigher If true, indicates that resets will have higher values than counter value (i.e. counter decreases)
   * @return A cloned GTS with compensated resets.
   */
  public static GeoTimeSerie compensateResets(GeoTimeSerie gts, boolean resethigher) {
    //
    // Clone gts
    //
    
    GeoTimeSerie filled = gts.clone();
    
    //
    // If gts is not of type LONG or DOUBLE, do noting
    //
        
    TYPE type = gts.getType();
    
    if (TYPE.LONG != type && TYPE.DOUBLE != type) {
      return filled;
    }
    
    //
    // Sort filled so ticks are in chronological orders
    //
    
    sort(filled);
    
    long lastl = TYPE.LONG == type ? filled.longValues[0] : 0L;
    long offsetl = 0L;
    
    double lastd = TYPE.DOUBLE == type ? filled.doubleValues[0] : 0.0D;
    double offsetd = 0.0D;
    
    for (int i = 1; i < filled.values; i++) {
      if (TYPE.LONG == type) {
        long value = filled.longValues[i];
        if (!resethigher) { // A reset is when we go from a value to a lower value
          if (value < lastl) {
            offsetl += lastl;
          }
          lastl = value;
        } else { // A reset is when we go from a value to a higher value
          if (value > lastl) {
            offsetl += lastl;
          }
          lastl = value;
        }
        filled.longValues[i] = value + offsetl;
      } else {
        double value = filled.doubleValues[i];
        if (!resethigher) { // A reset is when we go from a value to a lower value
          if (value < lastd) {
            offsetd += lastd; 
          }          
          lastd = value;
        } else { // A reset is when we go from a value to a higher value
          if (value > lastd) {
            offsetd += lastd; 
          }
          lastd = value;
        }
        filled.doubleValues[i] = value + offsetd;
      }
    }
    
    return filled;
  }
  
  public static boolean isBucketized(GeoTimeSerie gts) {    
    return 0 != gts.bucketcount && 0L != gts.bucketspan && 0L != gts.lastbucket; 
  }
  
  /**
   * Split a GTS into multiple GTS by cutting in 'quiet zones', i.e. intervals
   * of 'quietperiod' or more during which there were no measurements.
   *
   * If 'gts' has no values or if 'label' is already part of the labels of 'gts', then
   * the resulting list of GTS will only contain a clone of 'gts'.
   * 
   * @param gts GTS instance to split
   * @param quietperiod Minimum number of microseconds without values to consider a split. The previous value must be at least 'quietperiod' us ago.
   * @param minvalues Only produce GTS with more than 'minvalues' values, this is to ignore lone values
   * @param labelname Name to use for a label containing the GTS sequence (oldest GTS is 1, next is 2, ....). 'label' MUST NOT exist among the labels of 'gts'.
   * 
   * @return The list of resulting GTS.
   */
  public static List<GeoTimeSerie> timesplit(GeoTimeSerie gts, long quietperiod, int minvalues, String labelname) {
    
    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

    //
    // If the input GTS already contains the sequence label or has no value, return it as is.
    //
    
    if (0 == gts.values || gts.hasLabel(labelname)) {
      series.add(gts.clone());
      return series;
    }
        
    //
    // Sort 'gts'
    //
    
    sort(gts, false);
    
    
    long lasttick = gts.ticks[0];
    
    int idx = 0;
        
    int gtsid = 1;
    
    //
    // Loop over ticks
    //
    
    GeoTimeSerie serie = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, 4);
    serie.setName(gts.getName());
    Map<String,String> labels = new HashMap<String,String>();
    labels.putAll(gts.getLabels());
    labels.put(labelname, Integer.toString(gtsid));
    serie.setLabels(labels);
    if (gts.getMetadata().getAttributesSize() > 0) {
      serie.getMetadata().setAttributes(new HashMap<String,String>(gts.getMetadata().getAttributes()));
    }
    
    while (idx < gts.values) {
      //
      // If current tick is further than 'quietperiod' away
      // from the previous one, init a new GTS.
      // If the current GTS contains at least 'minvalues' values,
      // add it as a result GTS.
      //
      
      if (gts.ticks[idx] - lasttick >= quietperiod) {
        if (serie.values > 0 && serie.values >= minvalues) {
          series.add(serie);
        }
        serie = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, 4);
        serie.setName(gts.getName());
        labels = new HashMap<String,String>();
        labels.putAll(gts.getLabels());
        gtsid++;
        labels.put(labelname, Integer.toString(gtsid));
        serie.setLabels(labels);
        if (gts.getMetadata().getAttributesSize() > 0) {
          serie.getMetadata().setAttributes(new HashMap<String,String>(gts.getMetadata().getAttributes()));
        }
      }

      Object value = GTSHelper.valueAtIndex(gts, idx);      
      GTSHelper.setValue(serie, gts.ticks[idx], null != gts.locations ? gts.locations[idx] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[idx] : GeoTimeSerie.NO_ELEVATION, value, false);
      
      lasttick = gts.ticks[idx];
      idx++;
    }
    
    if (serie.values > 0 && serie.values >= minvalues) {
      series.add(serie);
    }
    
    return series;
  }
  
  /**
   * Crops a bucketized GTS so it spans the largest interval with actual values.
   * This method simply clones non bucketized GTS.
   * For bucketized GTS, only values at bucket boundaries are kept.
   * 
   * @param gts GTS instance to crop.
   * 
   * @return A cropped version of GTS or a clone thereof if GTS was not bucketized.
   */
  public static GeoTimeSerie crop(GeoTimeSerie gts) {
    if (!isBucketized(gts)) {
      return gts.clone();
    }
    
    //
    // Sort GTS
    //
    
    sort(gts, false);
    
    long firstbucket = gts.lastbucket - (gts.bucketcount - 1) * gts.bucketspan;
    
    GeoTimeSerie cropped = new GeoTimeSerie(4);
    cropped.setMetadata(new Metadata(gts.getMetadata()));
    
    for (int i = 0; i < gts.values; i++) {
      if (gts.ticks[i] >= firstbucket
          && gts.ticks[i] <= gts.lastbucket
          && (gts.ticks[i] % gts.bucketspan == gts.lastbucket % gts.bucketspan)) {
        setValue(cropped, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, valueAtIndex(gts, i), false);
      }
    }
    
    //
    // Compute lastbucket
    //
    
    cropped.bucketspan = gts.bucketspan;
    cropped.lastbucket = cropped.ticks[cropped.values - 1];
    cropped.bucketcount = 1 + (int) ((cropped.lastbucket - cropped.ticks[0]) / cropped.bucketspan);
    
    return cropped;
  }
  
  /**
   * Shifts a GTS instance by the given time delta.
   * All ticks will be shifted by 'delta'.
   * 
   * For bucketized GTS instances, all buckets will be shifted by 'delta'.
   * 
   * @param gts GTS instance to shift
   * @param delta Number of microsecondes to shift ticks by.
   * @return A shifted version of 'gts'.
   */
  public static GeoTimeSerie timeshift(GeoTimeSerie gts, long delta) {
    //
    // Clone gts.
    //
    
    GeoTimeSerie shifted = gts.clone();
    
    //
    // Shift ticks
    //
    
    for (int i = 0; i < shifted.values; i++) {
      shifted.ticks[i] = shifted.ticks[i] + delta;
    }
    
    //
    // Shift lastbucket if GTS is bucketized
    //
    
    if (isBucketized(shifted)) {
      shifted.lastbucket = shifted.lastbucket + delta;
    }
    
    return shifted;
  }
  
  /**
   * Produces a GTS instance similar to the original one except
   * that its ticks will have been modified to reflect their index
   * int the sequence, starting at 0.
   * 
   * @param gts
   * @return
   */
  public static GeoTimeSerie tickindex(GeoTimeSerie gts) {
    GeoTimeSerie indexed = gts.clone();
    
    for (int i = 0; i < indexed.values; i++) {
      indexed.ticks[i] = i;
    }

    //
    // Result cannot be bucketized
    //
    
    indexed.bucketcount = 0;
    indexed.bucketspan = 0;
    indexed.lastbucket = 0;
    
    return indexed;
  }
  
  public static GTSEncoder tickindex(GTSEncoder encoder) throws IOException {
    long index = 0;
    
    GTSDecoder decoder = encoder.getDecoder(true);
    GTSEncoder newencoder = new GTSEncoder(0L);
    
    while(decoder.next()) {
      newencoder.addValue(index++, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
    }
    
    return newencoder;
  }
  
  /**
   * Removes all values from a GTS instance by setting its value count to 0
   * and its type to UNDEFINED
   * 
   * @param gts GTS instance to clear.
   * 
   */
  public static void clear(GeoTimeSerie gts) {
    gts.values = 0;
    gts.type = TYPE.UNDEFINED;
    gts.booleanValues = null;
    gts.locations = null;
    gts.elevations = null;
    gts.ticks = null;
    gts.doubleValues = null;
    gts.longValues = null;
    gts.stringValues = null;
  }
  
  public static void reset(GeoTimeSerie gts) {
    gts.values = 0;
    gts.type = TYPE.UNDEFINED;
    
    unbucketize(gts);
  }
  
  /**
   * Apply a mapper on a GeoTimeSerie instance and produce a new
   * GTS instance with the result of the mapper application.
   * 
   * @param gts GTS instance to apply the mapper on
   * @param mapper Mapper Function to use
   * @param prewindow Number of ticks or time interval to consider BEFORE each tick for which the computation is done.
   *                  If number is 0, don't consider any ticks prior to the current one.
   *                  If number is > 0, consider that many ticks prior to the current one.
   *                  If number is < 0, consider that number negated of microseconds prior to the current tick.
   *                  A delta comparison from the previous value will need a prewindow of 1 (1 tick before the current one).   *                  
   * @param postwindow Same meaning as 'prewindow' but for the interval AFTER each tick.
   * @param occurrences Number of times to apply map, 0 means apply it for each tick. This is useful for some computations like
   *                    sums where the only result that might matter is that of the latest tick
   * @param reversed Compute ticks backwards, starting from most recent one
   * @param step How many ticks to move the sliding window after each mapper application (>=1)
   * @param overrideTick If true, use the tick returned by the mapper instead of the current tick. This may lead to duplicate ticks, need to run DEDUP.
   *                    
   * @return A new GTS instance with the result of the Mapper.
   */
  public static List<GeoTimeSerie> map(GeoTimeSerie gts, WarpScriptMapperFunction mapper, long prewindow, long postwindow, long occurrences, boolean reversed, int step, boolean overrideTick) throws WarpScriptException {
    return map(gts, mapper, prewindow, postwindow, occurrences, reversed, step, overrideTick, null);
  }

  public static List<GeoTimeSerie> map(GeoTimeSerie gts, Object mapper, long prewindow, long postwindow, long occurrences, boolean reversed, int step, boolean overrideTick, WarpScriptStack stack) throws WarpScriptException {
    return map(gts, mapper, prewindow, postwindow, occurrences, reversed, step, overrideTick, stack, null);
  }

  public static List<GeoTimeSerie> map(GeoTimeSerie gts, Object mapper, long prewindow, long postwindow, long occurrences, boolean reversed, int step, boolean overrideTick, WarpScriptStack stack,
                                       List<Long> outputTicks) throws WarpScriptException {

    //
    // Make sure step is positive
    //

    if (step <= 0) {
      step = 1;
    }

    //
    // Limit pre/post windows and occurrences to Integer.MAX_VALUE
    // as this is as many indices we may have at most in a GTS
    //
    
    if (prewindow > Integer.MAX_VALUE) {
      prewindow = Integer.MAX_VALUE;
    }
    if (postwindow > Integer.MAX_VALUE) {
      postwindow = Integer.MAX_VALUE;
    }
    if (occurrences > Integer.MAX_VALUE) {
      occurrences = Integer.MAX_VALUE;
    }
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();

    //
    // Clone gts
    //

    GeoTimeSerie mapped = gts.clone();

    //
    // Do nothing if there are no values and gts was not bucketized
    //

    if (0 == mapped.values && !isBucketized(mapped)) {
      results.add(mapped);
      return results;
    }

    // Sort ticks
    sort(mapped, reversed);
    // Retrieve ticks if GTS is not bucketized.
    long[] ticks = isBucketized(gts) ? null : Arrays.copyOf(mapped.ticks, gts.values);

    // Sort outputTicks
    if (null != outputTicks) {
      if (reversed) {
        Collections.sort(outputTicks, Collections.<Long>reverseOrder());
      } else {
        Collections.sort(outputTicks);
      }
    }

    // Clear clone
    GTSHelper.clear(mapped);

    // If @param outputTicks is not specified, then leftIdx and rightIdx are not used.
    int idx = 0;
    int rightIdx = 0; // used only if (!reversed)
    int leftIdx = 0; // used only if (reversed)

    //
    // idx is the index of the current output tick,
    // if (!reversed), rightIdx is the index of the tick of the input gts that is the least greater or equal than the current output tick.
    // if (reversed), leftIdx is the index of the tick of the input gts that is the least lesser or equal than the current output tick.
    // Note that they may not exist if all values are at the left (or right).
    //

    // Number of ticks for which to run the mapper
    int nticks = null != ticks ? ticks.length : mapped.bucketcount;
    if (null != outputTicks) {
      nticks = outputTicks.size();
    }

    // Call getLabels once so we don't waste CPU cycles, this will create a clone of the labels map
    Map<String, String> labels = gts.getLabels();

    long tick = 0;

    GeoTimeSerie subgts = null;

    boolean hasOccurrences = (0 != occurrences);

    Map<String, GeoTimeSerie> multipleMapped = new TreeMap<String, GeoTimeSerie>();

    boolean hasSingleResult = true;

    while (idx < nticks) {

      if (hasOccurrences && 0 == occurrences) {
        break;
      }

      if (null == outputTicks) {

        if (reversed) {
          tick = null != ticks ? ticks[idx] : mapped.lastbucket - idx * mapped.bucketspan;
        } else {
          tick = null != ticks ? ticks[idx] : mapped.lastbucket - (mapped.bucketcount - 1 - idx) * mapped.bucketspan;
        }
      } else {
        tick = outputTicks.get(idx);

        if (null != ticks) { // means input gts is not bucketized

          // calculate leftIdx and rightIdx
          if (reversed) {
            while (leftIdx < ticks.length && ticks[leftIdx] > tick) {
              leftIdx++;
            }
          } else {
            while (rightIdx < ticks.length && ticks[rightIdx] < tick) {
              rightIdx++;
            }
          }
        }
      }

      //
      // Determine start/stop timestamp for extracting subserie
      //

      long start = tick;
      long stop = tick;

      if (prewindow < 0) {
        start = tick + prewindow;
      } else if (prewindow > 0) {
        // window is a number of ticks
        if (null == ticks) {
          if (null == outputTicks || !reversed) {
            start = prewindow <= mapped.bucketcount ? tick - prewindow * mapped.bucketspan : Long.MIN_VALUE;
          } else {
            start = prewindow - 1 <= mapped.bucketcount ? tick - (prewindow - 1) * mapped.bucketspan : Long.MIN_VALUE;
          }
        } else {
          if (null == outputTicks) {
            if (reversed) {
              start = idx + prewindow < ticks.length ? (ticks[idx + (int) prewindow]) : Long.MIN_VALUE;
            } else {
              start = idx - prewindow >= 0 ? (ticks[idx - (int) prewindow]) : Long.MIN_VALUE;
            }
          } else {

            //
            // Note that if output ticks are provided,
            // then if the current output tick matches an input tick,
            // then if (reversed)
            // then prewindow counts the current tick
            // else it is postwindow that counts the current tick
            //

            if (reversed) {
              start = leftIdx - 1 + prewindow < ticks.length ? (ticks[leftIdx - 1 + (int) prewindow]) : Long.MIN_VALUE;
            } else {
              start = rightIdx - prewindow >= 0 ? (ticks[rightIdx - (int) prewindow]) : Long.MIN_VALUE;
            }
          }
        }
      } else {
        if (null != outputTicks && reversed) {
          start = tick + 1;
        }
      }

      if (postwindow < 0) {
        stop = tick - postwindow;
      } else if (postwindow > 0) {
        // window is a number of ticks
        if (null == ticks) {
          if (null == outputTicks || reversed) {
            stop = postwindow <= mapped.bucketcount ? tick + postwindow * mapped.bucketspan : Long.MAX_VALUE;
          } else {
            stop = postwindow - 1 <= mapped.bucketcount ? tick + (postwindow - 1) * mapped.bucketspan : Long.MAX_VALUE;
          }
        } else {
          if (null == outputTicks) {
            if (reversed) {
              stop = idx - postwindow >= 0 ? (ticks[idx - (int) postwindow]) : Long.MAX_VALUE;
            } else {
              stop = idx + postwindow < ticks.length ? (ticks[idx + (int) postwindow]) : Long.MAX_VALUE;
            }
          } else {
            if (reversed) {
              stop = leftIdx - postwindow >= 0 ? (ticks[leftIdx - (int) postwindow]) : Long.MAX_VALUE;
            } else {
              stop = rightIdx - 1 + postwindow < ticks.length ? (ticks[rightIdx - 1 + (int) postwindow]) : Long.MAX_VALUE;
            }
          }
        }
      } else {
        if (!(null == outputTicks) && !reversed) {
          stop = tick - 1;
        }
      }

      //
      // Extract values
      //

      subgts = GTSHelper.subSerie(gts, start, stop, false, false, subgts);

      Object mapResult = null;

      if (null != stack) {
        if (mapper instanceof Macro) {
          subgts.safeSetMetadata(mapped.getMetadata());
          stack.push(subgts);
          stack.exec((Macro) mapper);
          Object res = stack.peek();

          if (res instanceof List) {
            stack.drop();

            mapResult = MACROMAPPER.listToObjects((List) res);
          } else if (res instanceof Map) {
            stack.drop();

            for (Map.Entry<?, ?> keyAndValue: ((Map<?, ?>) res).entrySet()) {
              Object[] ores2 = MACROMAPPER.listToObjects((List) keyAndValue.getValue());
              ((Map) res).put(keyAndValue.getKey(), ores2);
            }

            mapResult = res;
          } else {
            //
            // Retrieve result
            //

            mapResult = MACROMAPPER.stackToObjects(stack);
          }

        } else {
          throw new WarpScriptException("Invalid mapper function.");
        }
      } else {
        if (!(mapper instanceof WarpScriptMapperFunction)) {
          throw new WarpScriptException("Expected a mapper function.");
        }
        //
        // Mapper functions have 8 parameters
        //
        // tick: timestamp we're computing the value for
        // names: array of names (for reducer compatibility)
        // labels: array of labels (for reducer compatibility)
        // ticks: array of ticks being aggregated
        // locations: array of locations being aggregated
        // elevations: array of elevations being aggregated
        // values: array of values being aggregated
        // window: An array with the window parameters [ prewindow, postwindow, start, stop, tick index ] on which the mapper runs
        //
        // 'window' nullity should be checked prior to using to allow mappers to be used as reducers.
        //
        // They return an array of 4 values:
        //
        // timestamp, location, elevation, value
        //
        // timestamp: an indication relative to timestamp (may be the timestamp at which the returned value was observed).
        //            it is usually not used (the returned value will be set at 'tick') but must be present.
        // location: location associated with the returned value
        // elevation: elevation associated with the returned value
        // value: computed value
        //

        Object[] parms = new Object[8];

        int i = 0;
        parms[i++] = tick;

        //
        // All arrays are allocated each time, so we don't risk
        // having a rogue mapper modify them.
        //

        parms[i++] = new String[subgts.values];
        Arrays.fill((Object[]) parms[i - 1], gts.getName());

        parms[i++] = new Map[subgts.values];
        Arrays.fill((Object[]) parms[i - 1], labels);

        parms[i++] = subgts.values > 0 ? Arrays.copyOf(subgts.ticks, subgts.values) : new long[0];
        if (null != subgts.locations) {
          parms[i++] = subgts.values > 0 ? Arrays.copyOf(subgts.locations, subgts.values) : new long[0];
        } else {
          if (subgts.values > 0) {
            parms[i++] = new long[subgts.values];
            Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
          } else {
            parms[i++] = new long[0];
          }
        }
        if (null != subgts.elevations) {
          parms[i++] = subgts.values > 0 ? Arrays.copyOf(subgts.elevations, subgts.values) : new long[0];
        } else {
          if (subgts.values > 0) {
            parms[i++] = new long[subgts.values];
            Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
          } else {
            parms[i++] = new long[0];
          }
        }
        parms[i++] = new Object[subgts.values];

        int tickidx = -1;

        for (int j = 0; j < subgts.values; j++) {
          ((Object[]) parms[6])[j] = valueAtIndex(subgts, j);
          if (-1 == tickidx && tick == tickAtIndex(subgts, j)) {
            tickidx = j;
          }
        }

        parms[i++] = new long[] {prewindow, postwindow, start, stop, tickidx};

        mapResult = ((WarpScriptMapperFunction) mapper).apply(parms);
      }

      if (mapResult instanceof Map) {
        hasSingleResult = false;
        for (Entry<Object, Object> entry : ((Map<Object, Object>) mapResult).entrySet()) {
          GeoTimeSerie mgts = multipleMapped.get(entry.getKey().toString());
          if (null == mgts) {
            mgts = mapped.cloneEmpty();
            if (null != outputTicks ) {
              unbucketize(mgts);
            }

            mgts.setName(entry.getKey().toString());
            multipleMapped.put(entry.getKey().toString(), mgts);
          }

          Object[] result = (Object[]) entry.getValue();
          if (null != result[3]) {
            GTSHelper.setValue(mgts, overrideTick ? (long) result[0] : tick, (long) result[1], (long) result[2], result[3], false);
          }
        }
      } else {
        Object[] result = mapResult instanceof List ? ((List) mapResult).toArray() : (Object[]) mapResult;

        //
        // Set value if it was not null. Don't overwrite, we scan ticks only once
        //



        if (null != result[3]) {
          GTSHelper.setValue(mapped, overrideTick ? (long) result[0] : tick, (long) result[1], (long) result[2], result[3], false);
        }
      }

      idx += step;
      occurrences--;
    }

    if (hasSingleResult) {

      if (null != outputTicks) {
        unbucketize(mapped);
      }
      results.add(mapped);
    }

    if (!multipleMapped.isEmpty()) {
      results.addAll(multipleMapped.values());
    }

    return results;
  }

  public static List<GeoTimeSerie> map(GeoTimeSerie gts, WarpScriptMapperFunction mapper, long prewindow, long postwindow, long occurrences, boolean reversed) throws WarpScriptException {
    return map(gts, mapper, prewindow, postwindow, occurrences, reversed, 1, false);
  }

  public static List<GeoTimeSerie> map(GeoTimeSerie gts, WarpScriptMapperFunction mapper, long prewindow, long postwindow) throws WarpScriptException {
    return map(gts, mapper, prewindow, postwindow, 0, false);
  }
  
  /**
   * Modify the labels of a GTS instance.
   * If a label appears in 'newlabels', the associated value will be used in 'gts', unless
   * the associated value is the empty string or null in which case the said label will be removed
   * from 'gts'.
   * If a null key is present in the 'newlabels' map, the labels will replace those of 'gts'
   * instead of modifying them.
   * 
   * @param gts GTS instance whose labels must be modified.
   * @param newlabels Map of label names to label values.
   */
  public static GeoTimeSerie relabel(GeoTimeSerie gts, Map<String,String> newlabels) {
    Map<String,String> labels = relabel(gts.getMetadata(), newlabels);
    
    gts.getMetadata().setLabels(labels);
    
    return gts;
  }

  public static GTSEncoder relabel(GTSEncoder encoder, Map<String,String> newlabels) {
    Map<String,String> labels = relabel(encoder.getMetadata(), newlabels);
    
    encoder.getMetadata().setLabels(labels);
    
    return encoder;
  }

  private static Map<String,String> relabel(Metadata metadata, Map<String,String> newlabels) {
    Map<String,String> labels = new HashMap<String,String>();
    
    if (!newlabels.containsKey(null)) {
      labels.putAll(metadata.getLabels());
    }
    
    for (Entry<String, String> nameAndValue: newlabels.entrySet()) {
      String name = nameAndValue.getKey();
      String value = nameAndValue.getValue();
      if (null == value || "".equals(value)) {
        labels.remove(name);
        continue;
      }
    
      if (null != name) {
        labels.put(name, value);
      }
    }
    
    return labels;
  }
  
  /**
   * Rename the given Geo Time Serie instance.
   * 
   * @param gts GTS instance to rename.
   * @param name New name to give the GTS, or suffix to add to its current name if 'name' starts with '+' (the '+' will be trimmed).
   */
  public static GeoTimeSerie rename(GeoTimeSerie gts, String name) {
    
    String newname = null;
    
    if (name.startsWith("+")) {
      newname = gts.getName() + name.substring(1); 
    } else {
      newname = name;
    }
    
    gts.setName(newname);
    
    gts.setRenamed(true);
    
    return gts;
  }
  
  /**
   * Partition a collection of Geo Time Serie instances into equivalence classes.
   * 
   * Member of each equivalence class have at least a common set of values for 'bylabels', they
   * may share common values for other labels too.
   * 
   * @param series Collection of GTS to partition
   * @param bylabels Collection of label names to use for partitioning or null to use all labels of each GTS
   * 
   * @return A map of label values to GTS instances
   */
  public static Map<Map<String,String>, List<GeoTimeSerie>> partition(Collection<GeoTimeSerie> series, Collection<String> bylabels) {
    
    Map<Map<String,String>, List<GeoTimeSerie>> classes = new HashMap<Map<String,String>, List<GeoTimeSerie>>();
    Map<Map<String,String>, Map<String,String>> labelsbyclass = new HashMap<Map<String,String>, Map<String,String>>();
    
    //
    // Loop over the GTS instances
    //
    
    for (GeoTimeSerie gts: series) {
      //
      // Construct the equivalence class key
      //
      
      Map<String,String> eqcls = new HashMap<String,String>();

      //
      // If 'bylabels' is null, consider that all labels determine the equivalence class
      //
      
      if (null == bylabels) {
        eqcls.putAll(gts.getMetadata().getLabels());
      } else {
        for (String label: bylabels) {
          if (gts.hasLabel(label)) {
            eqcls.put(label, gts.getLabel(label));
          }
        }        
      }
            
      if(!classes.containsKey(eqcls)) {
        //
        // This equivalence class is not yet known, create an initial list of its members
        // and an initial Map of common labels
        //
        
        classes.put(eqcls, new ArrayList<GeoTimeSerie>());
        classes.get(eqcls).add(gts);
        labelsbyclass.put(eqcls, new HashMap<String,String>());
        labelsbyclass.get(eqcls).putAll(gts.getLabels());
      } else {
        //
        // Add current GTS to its class
        //
        
        classes.get(eqcls).add(gts);
        
        //
        // Remove from equivalence class labels those which 'gts' does not have
        //

        List<String> labelstoremove = new ArrayList<String>();
        
        Map<String,String> gtsLabels = gts.getMetadata().getLabels();
        
        for (Entry<String, String> labelAndValue: labelsbyclass.get(eqcls).entrySet()) {
          String label = labelAndValue.getKey();
          if (!labelAndValue.getValue().equals(gtsLabels.get(label))) {
            labelstoremove.add(label);
          }
        }
        
        for (String label: labelstoremove) {
          labelsbyclass.get(eqcls).remove(label);
        }
      }
    }
    
    Map<Map<String,String>, List<GeoTimeSerie>> partition = new HashMap<Map<String,String>, List<GeoTimeSerie>>();
    
    for (Entry<Map<String, String>, List<GeoTimeSerie>> keyAndValue: classes.entrySet()) {
      partition.put(labelsbyclass.get(keyAndValue.getKey()), keyAndValue.getValue());
    }
    return partition;
  }

  /**
   * Return the first tick in the GTS instance.
   * 
   * @param gts GeoTimeSerie to return the first tick for.
   * @return The first tick or Long.MAX_VALUE if 'gts' is not bucketized and has no values.
   */
  public static long firsttick(GeoTimeSerie gts) {
    if (isBucketized(gts)) {
      return gts.lastbucket - (gts.bucketcount - 1) * gts.bucketspan;
    } else {
      long firsttick = Long.MAX_VALUE;
      
      if (gts.sorted && gts.values > 0) {
        if (!gts.reversed) {
          firsttick = gts.ticks[0];
        } else {
          firsttick = gts.ticks[gts.values - 1];
        }
      } else {
        for (int i = 0; i < gts.values; i++) {
          if (gts.ticks[i] < firsttick) {
            firsttick = gts.ticks[i];
          }
        }        
      }
      
      return firsttick;
    }
  }

  /**
   * Return the last tick in the GTS instance.
   * 
   * @param gts GeoTimeSerie to return the last tick for.
   * 
   * @return The last tick or Long.MIN_VALUE if 'gts' is not bucketized and has no values.
   */
  public static long lasttick(GeoTimeSerie gts) {
    if (isBucketized(gts)) {
      return gts.lastbucket;
    } else {
      long lasttick = Long.MIN_VALUE;
      
      if (gts.sorted && gts.values > 0) {
        if (!gts.reversed) {
          lasttick = gts.ticks[gts.values - 1];
        } else {
          lasttick = gts.ticks[0];
        }
      } else {
        for (int i = 0; i < gts.values; i++) {
          if (gts.ticks[i] > lasttick) {
            lasttick = gts.ticks[i];
          }
        }
      }
      
      return lasttick;
    }
  }

  /**
   * Return the number of ticks in a GTS instance.
   * If the GTS is bucketized, the number of buckets is returned, not the number of actual values.
   * 
   * @param gts GeoTimeSerie instance of which to count ticks.
   * 
   * @return Number of ticks in GTS
   */
  public static int nticks(GeoTimeSerie gts) {
    if (isBucketized(gts)) {
      return gts.bucketcount;
    } else {
      return gts.values;
    }
  }
  
  /**
   * Return the number of values in a GTS instance.
   * 
   * @param gts GeoTimeSerie instance of which to count values.
   * 
   * @return Number of values in GTS.
   */
  
  public static int nvalues(GeoTimeSerie gts) {
    return gts.values;
  }

  /**
   * Tell if a GTS instance is considered sorted.
   * If this method returns true, the GTS instance is sorted but if it returns false the GTS instance may or may not be sorted.
   *
   * @param gts GeoTimeSerie instance to consider.
   * @return true if the GTS instance is considered sorted, false otherwise.
   */
  public static boolean isSorted(GeoTimeSerie gts) {
    return gts.sorted;
  }

  /**
   * Tell if a GTS instance is considered reversed. This has only a meaning if the GTS instance is considered sorted.
   * If this method returns true, the GTS instance is reversed but if it returns false the GTS instance may or may not be reversed.
   *
   * @param gts GeoTimeSerie instance to consider.
   * @return true if the GTS instance is considered reversed, false otherwise.
   */
  public static boolean isReversed(GeoTimeSerie gts) {
    return gts.reversed;
  }
  
  /**
   * Handy method to compute the max value of a GTS instance.
   * 
   * It uses 'map_max' under the hood.
   * 
   * @param gts GTS to compute the max of
   * 
   * @return The computed max value
   */
  public static Object max(GeoTimeSerie gts) throws WarpScriptException {    
    Object[] parms = new Object[8];
    
    int i = 0;
    parms[i++] = 0L;
    parms[i++] = null;
    parms[i++] = null;
    parms[i++] = Arrays.copyOf(gts.ticks, gts.values);
    if (null != gts.locations) {
      parms[i++] = Arrays.copyOf(gts.locations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
    }
    if (null != gts.elevations) {
      parms[i++] = Arrays.copyOf(gts.elevations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
    }
    parms[i++] = new Object[gts.values];
    parms[i++] = null;
    
    for (int j = 0; j < gts.values; j++) {
      ((Object[]) parms[6])[j] = valueAtIndex(gts, j);
    }

    Object[] result = (Object[]) ((WarpScriptAggregatorFunction) WarpScriptLib.getFunction(WarpScriptLib.MAPPER_MAX)).apply(parms);

    return result[3];
  }
  
  /**
   * Handy method to compute the min value of a GTS instance.
   * 
   * It uses 'map_min' under the hood.
   * 
   * @param gts GTS to compute the max of
   * 
   * @return The computed min value
   */
  public static Object min(GeoTimeSerie gts) throws WarpScriptException {    
    Object[] parms = new Object[8];
    
    int i = 0;
    parms[i++] = 0L;
    parms[i++] = null;
    parms[i++] = null;
    parms[i++] = Arrays.copyOf(gts.ticks, gts.values);
    if (null != gts.locations) {
      parms[i++] = Arrays.copyOf(gts.locations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
    }
    if (null != gts.elevations) {
      parms[i++] = Arrays.copyOf(gts.elevations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
    }
    parms[i++] = new Object[gts.values];
    parms[i++] = null;
    
    for (int j = 0; j < gts.values; j++) {
      ((Object[]) parms[6])[j] = valueAtIndex(gts, j);
    }

    Object[] result = (Object[]) ((WarpScriptAggregatorFunction) WarpScriptLib.getFunction(WarpScriptLib.MAPPER_MIN)).apply(parms);

    return result[3];
  }

  /**
   * Handy method to compute the minimum elevation of a GTS instance.
   * 
   * It uses 'map_lowest' under the hood.
   * 
   * @param gts GTS to compute the minimum elevation on
   * 
   * @return The computed lowest elevation
   */
  public static Object lowest(GeoTimeSerie gts) throws WarpScriptException {    
    Object[] parms = new Object[6];
    
    int i = 0;
    parms[i++] = 0L;
    parms[i++] = 0L;
    parms[i++] = Arrays.copyOf(gts.ticks, gts.values);
    if (null != gts.locations) {
      parms[i++] = Arrays.copyOf(gts.locations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
    }
    if (null != gts.elevations) {
      parms[i++] = Arrays.copyOf(gts.elevations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
    }
    parms[i++] = new Object[gts.values];
    
    for (int j = 0; j < gts.values; j++) {
      ((Object[]) parms[5])[j] = valueAtIndex(gts, j);
    }

    Object[] result = (Object[]) ((WarpScriptAggregatorFunction) WarpScriptLib.getFunction(WarpScriptLib.MAPPER_LOWEST)).apply(parms);

    return result[3];
  }
  
  /**
   * Handy method to compute the maximum elevation of a GTS instance.
   * 
   * It uses 'map_highest' under the hood.
   * 
   * @param gts GTS to compute the maximum elevation on
   * 
   * @return The computed highest elevation
   */
  public static Object highest(GeoTimeSerie gts) throws WarpScriptException {    
    Object[] parms = new Object[6];
    
    int i = 0;
    parms[i++] = 0L;
    parms[i++] = 0L;
    parms[i++] = Arrays.copyOf(gts.ticks, gts.values);
    if (null != gts.locations) {
      parms[i++] = Arrays.copyOf(gts.locations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_LOCATION);
    }
    if (null != gts.elevations) {
      parms[i++] = Arrays.copyOf(gts.elevations, gts.values);
    } else {
      parms[i++] = new long[gts.values];
      Arrays.fill((long[]) parms[i - 1], GeoTimeSerie.NO_ELEVATION);
    }
    parms[i++] = new Object[gts.values];
    
    for (int j = 0; j < gts.values; j++) {
      ((Object[]) parms[5])[j] = valueAtIndex(gts, j);
    }

    Object[] result = (Object[]) ((WarpScriptAggregatorFunction) WarpScriptLib.getFunction(WarpScriptLib.MAPPER_HIGHEST)).apply(parms);

    return result[3];
  }
  
  public static long[] longValues(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.LONG != gts.type) {
      throw new WarpScriptException("Invalid type, expected LONG.");
    }
    
    return Arrays.copyOfRange(gts.longValues, 0, gts.values);
  }

  public static double[] doubleValues(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.DOUBLE != gts.type) {
      throw new WarpScriptException("Invalid type, expected DOUBLE.");
    }
    
    return Arrays.copyOfRange(gts.doubleValues, 0, gts.values);
  }

  public static BitSet booleanValues(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.BOOLEAN != gts.type) {
      throw new WarpScriptException("Invalid type, expected BOOLEAN.");
    }
    
    return gts.booleanValues.get(0, gts.values);
  }
  
  public static String[] stringValues(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.STRING != gts.type) {
      throw new WarpScriptException("Invalid type, expected STRING.");
    }
    
    return Arrays.copyOfRange(gts.stringValues, 0, gts.values);
  }
  
  /**
   * Remove duplicate ticks from the GeoTimeSerie instance.
   * 
   * Only the last value found for a given timestamp will be kept.
   * 
   * @param gts GeoTimeSerie instance from which to remove duplicates.
   * @return A clone of the given GeoTimeSerie without duplicate ticks.
   */
  public static GeoTimeSerie dedup(GeoTimeSerie gts) {
    //
    // Start by cloning gts
    //
    
    GeoTimeSerie clone = gts.clone();
    
    //
    // If there is only one tick, there can't be duplicates
    //
    
    if (clone.values < 2) {
      return clone;
    }
    
    //
    // Now the real work begins...
    // We can't trivially sort 'clone' and scan its ticks
    // to remove duplicates, the dedup process retains
    // the LAST value encountered at a given tick, so we
    // can have compacted data AND individual measurements that
    // came afterwards and still retain the correct value
    //
    
    
    //
    // Extract and sort tick.
    //
    
    long[] ticks = Arrays.copyOf(clone.ticks, clone.values);
    
    Arrays.sort(ticks);
    
    //
    // Extract the set of duplicate ticks, maintaining a counter
    // of occurrences for each one
    //
    
    Map<Long,AtomicInteger> duplicates = new HashMap<Long,AtomicInteger>();
    
    int idx = 0;
    int idx2 = 1;
    
    while(idx2 < clone.values) {
      while(idx2 < clone.values && ticks[idx] == ticks[idx2]) {
        idx2++;
      }
      
      if (idx2 - idx > 1) {
        duplicates.put(ticks[idx], new AtomicInteger(idx2 - idx));
      }
      idx = idx2++;
    }
    
    //
    // If no duplicates were found, return 'clone' as is
    //

    if (0 == duplicates.size()) {
      return clone;
    }
    
    //
    // The duplicates map contains the timestamps of all duplicate ticks as keys and the number of
    // occurrences of each duplicate tick
    //
    
    // Index in the values array
    
    idx = 0;

    //
    // Loop over the original ticks/locations/elevations/values arrays and
    // if a tick is a duplicate and not the last occurrence of it, remove it
    // and decrement the duplicate count
    //
    
    int offset = 0;
      
    while(idx + offset < clone.values) {
      while (duplicates.containsKey(clone.ticks[idx+offset]) && duplicates.get(clone.ticks[idx+offset]).decrementAndGet() > 0) {
        offset++;
      }
      if (offset > 0) {
        clone.ticks[idx] = clone.ticks[idx + offset];
        if (null != clone.locations) {
          clone.locations[idx] = clone.locations[idx + offset];
        }
        if (null != clone.elevations) {
          clone.elevations[idx] = clone.elevations[idx + offset];
        }
        switch (clone.type) {
          case LONG:              
            clone.longValues[idx] = clone.longValues[idx + offset];
            break;
          case DOUBLE:
            clone.doubleValues[idx] = clone.doubleValues[idx + offset];
            break;
          case STRING:
            clone.stringValues[idx] = clone.stringValues[idx + offset];
            break;
          case BOOLEAN:
            clone.booleanValues.set(idx, clone.booleanValues.get(idx + offset));
            break;
        }          
      }
      idx++;
    }

    clone.values -= offset;

    //
    // If we removed more than 1000 duplicates, shrink GTS
    //
    
    if (offset > 1000) {
      GTSHelper.shrink(clone);
    }

    return clone;
  }
  
  /**
   * Removes non-bucket points of a bucketized GTS
   * 
   * @param gts GeoTimeSerie instance to clean.
   * @return  A clone of the given GeoTimeSerie without non-bucket points or a clone if not bucketized.
   */
  public static GeoTimeSerie onlybuckets(GeoTimeSerie gts) {
   
    GeoTimeSerie clone = gts.clone();
    
    // nothing to do if not bucketized
    if (!isBucketized(gts)) {
      return clone;
    }
    
    boolean setLocations = gts.locations != null;
    boolean setElevations = gts.elevations != null;
    
    int i = 0;
    while (i < clone.values) {
      
      // is current tick a bucket?
      long q = (gts.lastbucket - gts.ticks[i]) / gts.bucketspan;
      long r = (gts.lastbucket - gts.ticks[i]) % gts.bucketspan;
      if (0 == r && q >= 0 && q < gts.bucketcount) {
      
        // if yes then go to next tick
        i++;
      
        // else we will swap it with the bucket that has the biggest index
      } else {
        
        // searching this index and removing non-bucket ticks that have bigger index
        q = (gts.lastbucket - gts.ticks[clone.values - 1]) / gts.bucketspan;
        r = (gts.lastbucket - gts.ticks[clone.values - 1]) % gts.bucketspan;
        while (clone.values - 1 > i && !(0 == r && q >= 0 && q < gts.bucketcount)) {
          clone.values--;
          q = (gts.lastbucket - gts.ticks[clone.values - 1]) / gts.bucketspan;
          r = (gts.lastbucket - gts.ticks[clone.values - 1]) % gts.bucketspan;
        }
        
        if (clone.values - 1 == i) {
          // if this index does not exist, we just remove the last point
          clone.values--;
        } else {
        
          clone.ticks[i] = clone.ticks[clone.values - 1];
          if (setLocations) {
            clone.locations[i] = clone.locations[clone.values - 1];
          }
          if (setElevations) {
            clone.elevations[i] = clone.elevations[clone.values - 1];
          }
          
          switch(clone.type) {
            case LONG:
              clone.longValues[i] = clone.longValues[clone.values - 1];
              break;
            case DOUBLE:
              clone.doubleValues[i] = clone.doubleValues[clone.values - 1];
              break;
            case STRING:
              clone.stringValues[i] = clone.stringValues[clone.values - 1];
              break;
            case BOOLEAN:
              clone.booleanValues.set(i, clone.booleanValues.get(clone.values - 1));
              break;
          }
          
          // we remove the last point
          clone.values--;
          
          // then we go to the next tick
          i++;
        }        
      }
    }
    
    //
    // If we removed more than 1000 non-bucket ticks, shrink GTS
    //
    
    if (gts.values - clone.values > 1000) {
      GTSHelper.shrink(clone);
    }
    
    return clone;
  }
  
  /**
   * Apply a function (either a filter or n-ary op) onto collections of GTS instances.
   * 
   * Let's assume there are N collections in 'series'.
   *
   * The GTS instances from collections among those N that
   * have more than 1 element will be partitioned. For each
   * partition P, the function F will be called with arguments
   * 
   * F(labels, S(Ci,Pi), S(C1,P2), ... S(Ci,Pi), ...., S(CN,PN))
   * 
   * where S(Ci,Pi) is either the collection Ci if card(Ci) == 1
   * or only those series from Pi which belong to Ci.
   * 
   * 'labels' is the set of common labels in the partition.
   * 
   * @param function The function to apply, either an WarpScriptFilterFunction or WarpScriptNAryFunction
   * @param bylabels Labels to use for partitioning the GTS instances
   * @param series Set of GTS instances collections
   * @return A list of GeoTimeSeries, result of the application of the function
   * @throws WarpScriptException if the function is invalid.
   */
  @SafeVarargs
  public static List<GeoTimeSerie> partitionAndApply(Object function, WarpScriptStack stack, Macro validator, Collection<String> bylabels, List<GeoTimeSerie>... series) throws WarpScriptException {
    Map<Map<String,String>,List<GeoTimeSerie>> unflattened = partitionAndApplyUnflattened(function, stack, validator, bylabels, series);
    
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    
    for (List<GeoTimeSerie> l: unflattened.values()) {
      results.addAll(l);
    }
    
    return results;
  }
  
  /**
   * Apply a function or filter GTS and keep the results ventilated per equivalence class
   * 
   * This function has the side effect of unsetting classId/labelsId
   *
   * @param function The function to apply, either an WarpScriptFilterFunction or WarpScriptNAryFunction
   * @param bylabels Labels to use for partitioning the GTS instances
   * @param series Set of GTS instances collections
   * @return A list of GeoTimeSeries, result of the application of the function
   * @throws WarpScriptException if the function is invalid.
   */
  @SafeVarargs
  public static Map<Map<String,String>,List<GeoTimeSerie>> partitionAndApplyUnflattened(Object function, WarpScriptStack stack, Macro validator, Collection<String> bylabels, List<GeoTimeSerie>... series) throws WarpScriptException {

    //
    // Gather all GTS instances together so we can partition them
    // We omit the GTS instances which belong to collections with a single
    // GTS instance, instead those will systematically be added as parameters
    //
    //
    
    Collection<GeoTimeSerie> allgts = new LinkedHashSet<GeoTimeSerie>(series.length);

    boolean hasNonSingleton = false;
    
    for (Collection<GeoTimeSerie> serie: series) {
      if (serie.size() > 1) {
        hasNonSingleton = true;
        allgts.addAll(serie);
      }
    }

    //
    // If all GTS lists are singletons, add the first GTS to allgts
    // so partition is not empty and thus the for loop does nothing.
    //
    
    if (!hasNonSingleton) {
      allgts.addAll(series[0]);
    }
    
    //
    // Partition the GTS instances
    //
    
    Map<Map<String,String>, List<GeoTimeSerie>> partition = GTSHelper.partition(allgts, bylabels);
    
    Map<Map<String,String>, List<GeoTimeSerie>> results = new LinkedHashMap<Map<String,String>,List<GeoTimeSerie>>();

    //
    // We force a dummy classId so we can easily perform a binary search
    // on the lists of GTS    
    //

    long idx = 0L;
    
    for (int i = 0; i < series.length; i++) {
      for (GeoTimeSerie gts: series[i]) {
        gts.getMetadata().setClassId(idx++);
        gts.getMetadata().setLabelsId(0L);
      }
      
      Collections.sort(series[i], GTSIdComparator.COMPARATOR);      
    }
    //Collections.sort(series[i], METASORT.META_COMPARATOR);

    try {
      //
      // Loop on each partition
      //
      
      for (Entry<Map<String, String>, List<GeoTimeSerie>> partitionlabelsAndGtss: partition.entrySet()) {

        Map<String,String> commonlabels = Collections.unmodifiableMap(partitionlabelsAndGtss.getKey());
        
        List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();

        //
        // Make N (cardinality of 'series') sublists of GTS instances.
        //
        
        List<GeoTimeSerie>[] subseries = new List[series.length];
        for (int i = 0; i < series.length; i++) {
          
          subseries[i] = new ArrayList<GeoTimeSerie>();
         
          //
          // Treat the case when the original series had a cardinality of 1
          // as a special case by adding the original series unconditionally
          //
          
          if (1 == series[i].size()) {
            subseries[i].add(series[i].iterator().next());
          } else {
            // The series appear in the order they are in the original list due to 'partition' using a List
            for (GeoTimeSerie serie: partitionlabelsAndGtss.getValue()) {
              //if (Collections.binarySearch(series[i], serie, METASORT.META_COMPARATOR) >= 0) {
              // We perform a binary search to determine if the GTS 'serie' is in series[i]
              if (Collections.binarySearch(series[i], serie, GTSIdComparator.COMPARATOR) >= 0) {
                subseries[i].add(serie);
              }
            }          
          }        
        }
        
        //
        // Call the function
        //
        
        if (function instanceof WarpScriptFilterFunction) {
          List<GeoTimeSerie> filtered = ((WarpScriptFilterFunction) function).filter(commonlabels, subseries);
          if (null != filtered) {
            result.addAll(filtered);
          }
        } else if (function instanceof WarpScriptNAryFunction) {
          //
          // If we have a stack and a validator, push the commonlabels and the list of subseries onto the stack,
          // call the validator and check if it left true or false onto the stack.
          //
          
          boolean proceed = true;
          
          if (null != stack && null != validator) {
            stack.push(Arrays.asList(subseries));
            stack.push(commonlabels);
            stack.exec(validator);
            if (!Boolean.TRUE.equals(stack.pop())) {
              proceed = false;
            }
          }
          
          if (proceed) {
            result.add(GTSHelper.applyNAryFunction((WarpScriptNAryFunction) function, commonlabels, subseries));
          }
        } else {
          throw new WarpScriptException("Invalid function to apply.");
        }
        
        results.put(commonlabels, result);
      }
      
      //
      // Check that all resulting GTS instances were in allgts
      //

      //if (function instanceof WarpScriptFilterFunction) {
      //  for (GeoTimeSerie gts: result) {
      //    if (!allgts.contains(gts)) {
      //      throw new WarpScriptException("Some filtered Geo Time Series were not in the original set.");
      //    }
      //  }      
      //}
      
      return results;      
    } finally {
      // Unset classId/labelsId since we modified them for efficient binary search
      for (int i = 0; i < series.length; i++) {
        for (GeoTimeSerie gts: series[i]) {
          gts.getMetadata().unsetClassId();
          gts.getMetadata().unsetLabelsId();
        }
      }
    }
  }

  @SafeVarargs
  public static GeoTimeSerie applyNAryFunction(WarpScriptNAryFunction function, Map<String,String> commonlabels, List<GeoTimeSerie>... subseries) throws WarpScriptException {
    
    commonlabels = Collections.unmodifiableMap(commonlabels);
    
    //
    // Determine if target should be bucketized.
    // Target will be bucketized if x and y have the same bucketspan,
    // and if their lastbucket values are congruent modulo bucketspan
    //
    
    long lastbucket = 0L;
    long firstbucket = Long.MAX_VALUE;
    long bucketspan = 0L;
    int bucketcount = 0;
    
    boolean done = false;
    
    for (List<GeoTimeSerie> subserie: subseries) {
      for (GeoTimeSerie serie: subserie) {
        // If we encountered a non bucketized GTS instance after
        // encountering a bucketized one, result won't be bucketized
        if (!isBucketized(serie) && bucketspan > 0) {
          done = true;
          break;
        }
        
        //
        // Skip the rest of the processing if GTS is not bucketized
        //
        
        if (!isBucketized(serie)) {
          continue;
        }
        
        //
        // If bucketspan differs from previous bucketspan, result
        // won't be bucketized
        //
        if (bucketspan > 0 && serie.bucketspan != bucketspan) {
          done = true;
          break;
        } else if (0L == bucketspan) {
          bucketspan = serie.bucketspan;
          lastbucket = serie.lastbucket;
        }
        
        //
        // If the lastbucket of this serie is not congruent to the
        // current lastbucket modulus 'bucketspan', result won't be
        // bucketized
        //
        
        if ((serie.lastbucket % bucketspan) != (lastbucket % bucketspan)) {
          done = true;
          break;
        }
        
        lastbucket = Math.max(serie.lastbucket, lastbucket);
        firstbucket = Math.min(firstbucket, serie.lastbucket - serie.bucketcount * bucketspan);
      }
      
      if (done) {
        break;
      }
    }

    //
    // We exited early or no GTS were bucketized, result won't be bucketized
    //
    
    if (done || 0 == bucketspan) {
      bucketspan = 0L;
      lastbucket = 0L;
      bucketcount = 0;
    } else {
      bucketcount = (int) ((lastbucket - firstbucket) / bucketspan);
    }

    //
    // Create target GTS
    //
    
    GeoTimeSerie gts = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, 16);
    gts.setName("");
    gts.setLabels(commonlabels);
    
    //
    // Sort all GTS instances so we can access their values cheaply
    // As GTS instances will be marked as sorted, sorting won't be done
    // multiple times
    //
    
    int idx[][] = new int[subseries.length][];
    int nseries = 0;
    
    // Keep a matrix with GTS labels, so we can avoid calling getLabels repeatedly
    Map<String,String>[][] partlabels = new Map[subseries.length][];
    
    for (int i = 0; i < subseries.length; i++) {
      idx[i] = new int[subseries[i].size()];
      partlabels[i] = new Map[subseries[i].size()];
      for (int j = 0; j < subseries[i].size(); j++) {
        partlabels[i][j] = subseries[i].get(j).getLabels();
        GTSHelper.sort(subseries[i].get(j));
        // Force name of target GTS, this is a brute force way to set it to the last name encountered...
        gts.setName(subseries[i].get(j).getName());
        nseries++;
      }
    }
    
    //
    // Allocate arrays
    //
    
    long[] ticks = new long[nseries];
    String[] names = new String[nseries];
    // Allocate one extra slot for common labels
    Map<String,String>[] labels = new Map[nseries + 1];
    long[] locations = new long[nseries];
    long[] elevations = new long[nseries];
    Object[] values = new Object[nseries];
    // Index of collection the value is from, if there is a gap in the index, a collection had no member in the partition
    // The last value is the initial number of collections
    int[] collections = new int[nseries + 1];
    collections[nseries] = subseries.length;
    
    Object[] params = new Object[8];
    
    // Do a sweeping line algorithm from oldest tick to newest
    
    while(true) {
      //
      // Determine the tick span at the given indices
      //

      long smallest = Long.MAX_VALUE;
      
      for (int i = 0; i < subseries.length; i++) {
        for (int j = 0; j < idx[i].length; j++) {
          GeoTimeSerie serie = subseries[i].get(j); 
          if (idx[i][j] < serie.values) {
            if (serie.ticks[idx[i][j]] < smallest) {
              smallest = serie.ticks[idx[i][j]];
            }
          }
        }
      }

      //
      // No smallest tick, this means we've exhausted all values
      //
      
      if (Long.MAX_VALUE == smallest) {
        break;
      }
           
      //
      // Now fill the locations/elevations/values arrays for all GTS
      // instances whose current tick is 'smallest'
      //
      
      int k = 0;
      
      for (int i = 0; i < subseries.length; i++) {
        for (int j = 0; j < idx[i].length; j++) {          
          GeoTimeSerie serie = subseries[i].get(j);
          
          names[k] = serie.getName();
          labels[k] = partlabels[i][j];
          
          // Tick records id of partition.
          ticks[k] = i;
          
          if (idx[i][j] < serie.values && smallest == serie.ticks[idx[i][j]]) {
            locations[k] = null != serie.locations ? serie.locations[idx[i][j]] : GeoTimeSerie.NO_LOCATION;
            elevations[k] = null != serie.elevations ? serie.elevations[idx[i][j]] : GeoTimeSerie.NO_ELEVATION;
            values[k] = GTSHelper.valueAtIndex(serie, idx[i][j]);
            idx[i][j]++;
          } else {
            locations[k] = GeoTimeSerie.NO_LOCATION;
            elevations[k] = GeoTimeSerie.NO_ELEVATION;
            values[k] = null;
          }
          collections[k] = i;
          k++;
        }
      }
    
      // Set common labels
      labels[k] = commonlabels;
      
      //
      // Call the reducer for the current tick
      //
      // Return value will be an array [tick, location, elevation, value]
      //
      
      params[0] = smallest;
      params[1] = names;
      params[2] = labels;
      params[3] = ticks;
      params[4] = locations;
      params[5] = elevations;
      params[6] = values;
      params[7] = collections;
      
      Object[] result = (Object[]) function.apply(params);

      if (null != result[3]) {
        GTSHelper.setValue(gts, smallest, (long) result[1], (long) result[2], result[3], false);
      }
    }

    return gts;
  }
  
  /**
   * Apply a binary op to pairs drawn from two collections of GTS instances.
   * 
   * If op1 has one element, op will be applied to (op1,y) where y belongs to op2. Result name and labels will be those of y.
   * If op2 has one element, op will be applied to (x,op2) where x belongs to op1. Result name and labels will be those of x.
   * 
   * If cardinalities of both op1 and op2 are > 1, the set {op1 + op2} will be partitioned using 'bylabels' (@see #partition).
   * If each partition contains 2 GTS instances, op will be applied on each partition. The result will have name and labels of the
   * partition element which belongs to op1.
   * 
   * @param op Binary op to apply.
   * @param op1 Collection of GTS instances for operand1 of op
   * @param op2 Collection of GTS instances for operand2 of op
   * @param bylabels Label names to use for partitioning op1+op2
   * 
   * @return A list of resulting GTS instances.
   * 
   * @throws WarpScriptException If partitioning could not be done
   */
  @Deprecated
  public static List<GeoTimeSerie> apply(WarpScriptBinaryOp op, Collection<GeoTimeSerie> op1, Collection<GeoTimeSerie> op2, Collection<String> bylabels) throws WarpScriptException {
    Collection<GeoTimeSerie> allgts = new ArrayList<GeoTimeSerie>();
    allgts.addAll(op1);
    allgts.addAll(op2);

    //
    // Handle the case when either op1 or op2 have a cardinality of 1, in which case
    // the semantics differ slightly, i.e. one GTS will be returned per one present in op1 or op2 (depending
    // which one has a single element).
    // In this case no partition is computed.
    //
    
    boolean oneToMany = false;
    
    if (1 == op1.size() || 1 == op2.size()) {
      oneToMany = true;
    }
    
    // FIXME(hbs): should we make sure op1 and op2 have no overlap
   
    //
    // Compute a partition of all GeoTimeSerie instances
    //
    
    Map<Map<String,String>, List<GeoTimeSerie>> partition = null;
    
    if (!oneToMany) {
      partition = partition(allgts, bylabels);
    } else {
      partition = new HashMap<Map<String,String>, List<GeoTimeSerie>>();
      if (1 == op1.size()) {
        for (GeoTimeSerie gts: op2) {
          Map<String,String> labels = gts.getLabels();
          if (!partition.containsKey(labels)) {
            partition.put(labels, new ArrayList<GeoTimeSerie>());
            partition.get(labels).add(op1.iterator().next());
          }
          partition.get(labels).add(gts);
        }
      } else {
        for (GeoTimeSerie gts: op1) {
          Map<String,String> labels = gts.getLabels();
          if (!partition.containsKey(labels)) {
            partition.put(labels, new ArrayList<GeoTimeSerie>());
            partition.get(labels).add(op2.iterator().next());
          }
          partition.get(labels).add(gts);
        }        
      }
    }

    //
    // Check that each partition contains 2 elements, one from 'op1' and one from 'op2'
    //
    
    if (!oneToMany) {
      for (List<GeoTimeSerie> series: partition.values()) {
        if (2 != series.size()) {
          throw new WarpScriptException("Unable to partition operands coherently.");
        }
        if (!(op1.contains(series.get(0)) && op2.contains(series.get(1))) && !(op2.contains(series.get(0)) && op1.contains(series.get(1)))) {
          throw new WarpScriptException("Unable to partition operands coherently.");
        }
      }      
    }
        
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    
    //
    // For each partition, compute op(x,y) where x is in op1 and y in op2
    //
    
    if (!oneToMany) {
      for (Entry<Map<String,String>, List<GeoTimeSerie>> entry: partition.entrySet()) {
        
        List<GeoTimeSerie> series = entry.getValue();
        Map<String,String> commonlabels = ImmutableMap.copyOf(entry.getKey());
        
        //
        // Extract x and y
        //
        
        GeoTimeSerie x = op1.contains(series.get(0)) ? series.get(0) : series.get(1);
        GeoTimeSerie y = op2.contains(series.get(1)) ? series.get(1) : series.get(0);
        
        
        results.add(applyBinOp(op, x.getName(), commonlabels, x, y));
      }      
    } else {
      for (Entry<Map<String,String>, List<GeoTimeSerie>> entry: partition.entrySet()) {
        
        List<GeoTimeSerie> series = entry.getValue();
        Map<String,String> commonlabels = ImmutableMap.copyOf(entry.getKey());
        
        //
        // Extract x and y
        //
        
        GeoTimeSerie x = series.get(0);
        
        for (int i = 1; i < series.size(); i++) {
          GeoTimeSerie y = series.get(i);        
        
          if (op1.size() == 1) {
            results.add(applyBinOp(op, y.getName(), commonlabels, x, y));            
          } else {
            results.add(applyBinOp(op, y.getName(), commonlabels, y, x));
          }
        }
      }            
    }
    
    return results;
  }
  
  /**
   * Apply a binary op to two GTS instances
   * 
   * @param op Binary op to apply
   * @param name Name of resulting GTS
   * @param labels Labels of resulting GTS
   * @param x First operand GTS
   * @param y Second operand GTS
   * @return A new GeoTimeSerie containing the result of the binary op applied on the two GeoTimeSerie instances.
   */
  private static GeoTimeSerie applyBinOp(WarpScriptBinaryOp op, String name, Map<String,String> labels, GeoTimeSerie x, GeoTimeSerie y) {
    //
    // Determine if target should be bucketized.
    // Target will be bucketized if x and y have the same bucketspan,
    // and if their lastbucket values are congruent modulo bucketspan
    //
    
    long lastbucket = 0L;
    long bucketspan = 0L;
    int bucketcount = 0;
    
    if (isBucketized(x) && isBucketized(y)) {
      if (x.bucketspan == y.bucketspan) {
        if ((x.lastbucket % x.bucketspan) == (y.lastbucket % y.bucketspan)) {
          lastbucket = Math.max(x.lastbucket, y.lastbucket);
          bucketspan = x.bucketspan;
          long firstbucket = Math.min(x.lastbucket - x.bucketcount * bucketspan, y.lastbucket - y.bucketcount * bucketspan);
          bucketcount = (int) ((lastbucket - firstbucket) / bucketspan);
        }
      }
    }
    
    //
    // Create target GTS
    //
    
    GeoTimeSerie gts = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, 16);
    gts.setName(name);
    gts.setLabels(labels);
    
    //
    // Sort x and y so we can scan the ticks cheaply
    //
    
    sort(x);
    sort(y);
    
    int xidx = 0;
    int yidx = 0;
    
    //
    // Binary ops have 8 parameters like mappers/reducers/aggregators
    //
    // tick -> the tick for which a value is computed
    // names -> array of GTS names (index 0 is x, 1 is y)
    // labels -> array of GTS labels (those from the original GTS) + 1 for the common labels (the last element of the array)
    // ticks -> array of ticks
    // locations -> array of locations
    // elevations -> array of elevations
    // values -> array of values
    //
    
    Object[] params = new Object[7];
    params[1] = new String[2];
    params[2] = new Map[3];
    params[3] = new long[2];
    params[4] = new long[2];
    params[5] = new long[2];
    params[6] = new Object[2];
    
    Map<String,String> xlabels = ImmutableMap.copyOf(x.getLabels());
    Map<String,String> ylabels = ImmutableMap.copyOf(y.getLabels());
          
    labels = ImmutableMap.copyOf(labels);
    
    
    // FIXME(hbs): should the type be determined by the first call with a non null value for X?
    
    while(xidx < x.values || yidx < y.values) {
      
      //
      //  Advance yidx until y tick catches x tick
      //
      
      while(yidx < y.values && x.ticks[xidx] >= y.ticks[yidx]) {
        
        long tick = y.ticks[yidx];
        Object xelt = x.ticks[xidx] == y.ticks[yidx] ? GTSHelper.valueAtIndex(x, xidx) : null;
        Object yelt = GTSHelper.valueAtIndex(y, yidx);
        
        params[0] = tick;
        ((String[]) params[1])[0] = x.getName();
        ((Map[]) params[2])[0] = xlabels;
        ((long[]) params[3])[0] = tick;
        
        if (null == xelt) {
          ((long[]) params[4])[0] = GeoTimeSerie.NO_LOCATION;
          ((long[]) params[5])[0] = GeoTimeSerie.NO_ELEVATION;
        } else {
          ((long[]) params[4])[0] = GTSHelper.locationAtIndex(x, xidx);
          ((long[]) params[5])[0] = GTSHelper.elevationAtIndex(x, xidx);
        }
        
        ((Object[]) params[6])[0] = xelt;            
        
        ((String[]) params[1])[1] = y.getName();
        ((Map[]) params[2])[1] = ylabels;
        ((long[]) params[3])[1] = tick;
        ((long[]) params[4])[1] = GTSHelper.locationAtIndex(y, yidx);
        ((long[]) params[5])[1] = GTSHelper.elevationAtIndex(y, yidx);
        ((Object[]) params[6])[1] = yelt;
        
        ((Map[]) params[2])[2] = labels;

        Object[] result = (Object[]) op.apply(params);

        if(null != result) {
          Object value = result[3];

          if (null != value) {
            long location = (long) result[1];
            long elevation = (long) result[2];
            GTSHelper.setValue(gts, tick, location, elevation, value, false);
          }
        }
        
        yidx++;
      }
    
      //
      // Advance the x index since the y tick is now > than the x tick
      //
      
      xidx++;
      
      while(xidx < x.values && ((x.ticks[xidx] < y.ticks[yidx]) || yidx >= y.values)) {

        long tick = x.ticks[xidx];
        Object xelt = GTSHelper.valueAtIndex(x, xidx);
        // y has no elements since x tick is lagging behind y tick.
        Object yelt = null;
        
        params[0] = tick;
        ((String[]) params[1])[0] = x.getName();
        ((Map[]) params[2])[0] = xlabels;
        ((long[]) params[3])[0] = tick;
        
        if (null == xelt) {
          ((long[]) params[4])[0] = GeoTimeSerie.NO_LOCATION;
          ((long[]) params[5])[0] = GeoTimeSerie.NO_ELEVATION;
        } else {
          ((long[]) params[4])[0] = GTSHelper.locationAtIndex(x, xidx);
          ((long[]) params[5])[0] = GTSHelper.elevationAtIndex(x, xidx);
        }
        
        ((Object[]) params[6])[0] = xelt;            

        
        ((String[]) params[1])[1] = y.getName();
        ((Map[]) params[2])[1] = ylabels;
        ((long[]) params[3])[1] = tick;
        ((long[]) params[4])[1] = GeoTimeSerie.NO_LOCATION;
        ((long[]) params[5])[1] = GeoTimeSerie.NO_ELEVATION;
        ((Object[]) params[6])[1] = yelt; // null
        
        ((Map[]) params[2])[2] = labels;
        
        Object[] result = (Object[]) op.apply(params);

        if(null != result) {
          Object value = result[3];

          if (null != value) {
            long location = (long) result[1];
            long elevation = (long) result[2];

            GTSHelper.setValue(gts, tick, location, elevation, value, false);
          }
        }
        xidx++;
      }        
    }

    return gts;
  }

  public static List<GeoTimeSerie> reduce(WarpScriptReducerFunction reducer, Collection<GeoTimeSerie> series, Collection<String> bylabels) throws WarpScriptException {
    return reduce(reducer, series, bylabels, false);
  }

  public static List<GeoTimeSerie> reduce(WarpScriptReducerFunction reducer, Collection<GeoTimeSerie> series, Collection<String> bylabels, boolean overrideTick) throws WarpScriptException {
    Map<Map<String,String>,List<GeoTimeSerie>> unflattened = reduceUnflattened(reducer, series, bylabels, overrideTick);
    
    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
    
    for (List<GeoTimeSerie> l: unflattened.values()) {
      results.addAll(l);
    }
    
    return results;
  }

  public static Map<Map<String, String>, List<GeoTimeSerie>> reduceUnflattened(WarpScriptReducerFunction reducer, Collection<GeoTimeSerie> series, Collection<String> bylabels) throws WarpScriptException {
    return reduceUnflattened(reducer, series, bylabels, false);
  }

  public static Map<Map<String, String>, List<GeoTimeSerie>> reduceUnflattened(WarpScriptReducerFunction reducer, Collection<GeoTimeSerie> series, Collection<String> bylabels, boolean overrideTick) throws WarpScriptException {
    //
    // Partition the GTS instances using the given labels
    //
    
    Map<Map<String,String>, List<GeoTimeSerie>> partitions = partition(series, bylabels);
    
    Map<Map<String,String>,List<GeoTimeSerie>> results = new LinkedHashMap<Map<String,String>, List<GeoTimeSerie>>();
    
    
    for (Entry<Map<String, String>, List<GeoTimeSerie>> partitionLabelsAndGtss: partitions.entrySet()) {
      boolean singleGTSResult = false;

      Map<String, String> partitionLabels = partitionLabelsAndGtss.getKey();
      List<GeoTimeSerie> partitionSeries = partitionLabelsAndGtss.getValue();
      
      //
      // Extract labels and common labels
      //
      
      Map[] partlabels = new Map[partitionSeries.size() + 1];
      
      for (int i = 0; i < partitionSeries.size(); i++) {
        partlabels[i] = partitionSeries.get(i).getLabels();
      }
      
      partlabels[partitionSeries.size()] = Collections.unmodifiableMap(partitionLabelsAndGtss.getKey());
      
      //
      // Determine if result should be bucketized or not.
      // Result will be bucketized if all GTS instances in the partition are
      // bucketized, have the same bucketspan and have congruent lastbucket values
      //
      
      long lastbucket = Long.MIN_VALUE;
      long startbucket = Long.MAX_VALUE;
      long bucketspan = 0L;      
      
      for (GeoTimeSerie gts: partitionSeries) {
        // One GTS instance is not bucketized, result won't be either
        if (!isBucketized(gts)) {
          bucketspan = 0L;          
          break;
        }
        if (0L == bucketspan) {
          bucketspan = gts.bucketspan;
        } else if (bucketspan != gts.bucketspan) {
          // GTS has a bucketspan which differs from the previous one,
          // so result won't be bucketized.
          bucketspan = 0L;
          break;
        }
        if (Long.MIN_VALUE == lastbucket) {
          lastbucket = gts.lastbucket;
        }
        if (lastbucket % bucketspan != gts.lastbucket % gts.bucketspan) {
          // GTS has a lastbucket value which is not congruent to the other
          // lastbucket values, so result GTS won't be bucketized.
          bucketspan = 0L;
          break;
        }
        
        //
        // Update start/end bucket
        //
        
        if (gts.lastbucket > lastbucket) {
          lastbucket = gts.lastbucket;
        }
        if (gts.lastbucket - gts.bucketcount * gts.bucketspan < startbucket) {
          startbucket = gts.lastbucket - gts.bucketcount * gts.bucketspan;
        }
      }
      
      //
      // Determine bucketcount if result is to be bucketized
      // startbucket is the end of the first bucket not considered
      //
      
      int bucketcount = 0;
      
      if (0L != bucketspan) {
        bucketcount = (int) ((lastbucket - startbucket) / bucketspan);
      }

      //
      // Create target GTS
      //
      
      GeoTimeSerie result;
      
      if (0L != bucketspan) {
        result = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, 0);
      } else {
        result = new GeoTimeSerie();
      }

      result.setName("");
      result.setLabels(partitionLabels);
      
      //
      // Sort all series in the partition so we can scan their ticks in order
      //
        
      String resultName = null;
      
      for (GeoTimeSerie gts: partitionSeries) {
        sort(gts, false);
        if (null == resultName) {
          resultName = gts.getName();
        } else if (!resultName.equals(gts.getName())) {
          resultName = "";
        }
      }
      
      result.setName(resultName);
      
      Map<String,GeoTimeSerie> multipleResults = new TreeMap<String,GeoTimeSerie>();
      
      //
      // Initialize indices for each serie
      //
      
      int[] idx = new int[partitionSeries.size()];

      //
      // Initialize names/labels/location/elevation/value arrays
      //
      
      long[] ticks = new long[idx.length];
      String[] names = new String[idx.length];
      // Allocate 1 more slot for labels so we can store the common labels at the end of the array
      Map<String,String>[] lbls = Arrays.copyOf(partlabels, partlabels.length);
      
      long[] locations = new long[idx.length];
      long[] elevations = new long[idx.length];
      Object[] values = new Object[idx.length];
      
      //
      // Reducers have 7 parameters (similar to those of binary ops and mappers)
      //
      // tick for which value is computed
      // array of ticks
      // array of names
      // array of labels
      // array of locations
      // array of elevations
      // array of values
      //
      
      Object[] params = new Object[7];
      
      while(true) {
        //
        // Determine the tick span at the given indices
        //

        long smallest = Long.MAX_VALUE;
        
        for (int i = 0; i < idx.length; i++) {
          GeoTimeSerie gts = partitionSeries.get(i); 
          if (idx[i] < gts.values) {
            if (gts.ticks[idx[i]] < smallest) {
              smallest = gts.ticks[idx[i]];
            }
          }
        }

        //
        // No smallest tick, this means we've exhausted all values
        //
        
        if (Long.MAX_VALUE == smallest) {
          break;
        }
        
        //
        // Now fill the locations/elevations/values arrays for all GTS
        // instances whose current tick is 'smallest'
        //
        
        for (int i = 0; i < idx.length; i++) {
          GeoTimeSerie gts = partitionSeries.get(i); 
          if (idx[i] < gts.values && smallest == gts.ticks[idx[i]]) {
            ticks[i] = smallest;
            names[i] = gts.getName();

            locations[i] = null != gts.locations ? gts.locations[idx[i]] : GeoTimeSerie.NO_LOCATION;
            elevations[i] = null != gts.elevations ? gts.elevations[idx[i]] : GeoTimeSerie.NO_ELEVATION;
            values[i] = GTSHelper.valueAtIndex(gts, idx[i]);
            // Advance idx[i] since it was the smallest tick.
            idx[i]++;
          } else {
            ticks[i] = Long.MIN_VALUE;
            names[i] = gts.getName();

            locations[i] = GeoTimeSerie.NO_LOCATION;
            elevations[i] = GeoTimeSerie.NO_ELEVATION;
            values[i] = null;
          }
        }
        
        //
        // Call the reducer for the current tick
        //
        // Return value will be an array [tick, location, elevation, value]
        //
        
        // TODO(hbs): extend reducers to use a window instead of a single value when reducing.
        //            ticks/locations/elevations/values would be arrays of arrays and an 8th param
        //            could contain the values.
        
        params[0] = smallest;
        params[1] = names;
        params[2] = lbls;
        params[3] = ticks;
        params[4] = locations;
        params[5] = elevations;
        params[6] = values;
                
        Object reducerResult = reducer.apply(params);
        
        if (reducerResult instanceof Map) {
          for (Entry<Object,Object> entry: ((Map<Object,Object>) reducerResult).entrySet()) {
            GeoTimeSerie gts = multipleResults.get(entry.getKey().toString());
            if (null == gts) {
              if (0L != bucketspan) {
                gts = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, 0);
              } else {
                gts = new GeoTimeSerie();
              }

              gts.setName(entry.getKey().toString());
              gts.setLabels(partitionLabels);
              multipleResults.put(entry.getKey().toString(), gts);
            }
            
            Object[] reduced = (Object[]) entry.getValue();
            
            if (null != reduced[3]) {
              GTSHelper.setValue(gts, overrideTick ? (long) reduced[0] : smallest, (long) reduced[1], (long) reduced[2], reduced[3], false);
            }
          }
        } else {
          Object[] reduced = (Object[]) reducerResult;
          singleGTSResult = true;
          if (null != reduced[3]) {
            GTSHelper.setValue(result, overrideTick ? (long) reduced[0] : smallest, (long) reduced[1], (long) reduced[2], reduced[3], false);
          }
        }
        
      }
      
      if (!results.containsKey(partitionLabels)) {
        results.put(partitionLabels, new ArrayList<GeoTimeSerie>());
      }

      if (singleGTSResult) {
        results.get(partitionLabels).add(result);
      }

      if (!multipleResults.isEmpty()) {
        results.get(partitionLabels).addAll(multipleResults.values());
      }            
    }
    
    return results;
  }
  
  /**
   * Return the value of the most recent tick in this GTS.
   * 
   * @param gts GTS instance to extract value from.
   * 
   * @return The value of the most recent tick in the given GeoTimeSerie instance.
   */
  public static Object getLastValue(GeoTimeSerie gts) {
    
    // Easy one, if the GTS has no values then value is null
    
    if (0 == gts.values) {
      return null;
    }
    
    if (isBucketized(gts)) {
      for (int i = 0; i < gts.values; i++) {
        if (gts.lastbucket == gts.ticks[i]) {
          return GTSHelper.valueAtIndex(gts, i);
        }
      }
      return null;
    } else {
      long ts = Long.MIN_VALUE;
      int idx = -1;
      
      for (int i = 0; i < gts.values; i++) {
        if (gts.ticks[i] > ts) {
          ts = gts.ticks[i];
          idx = i;
        }
      }
      
      if (-1 != idx) {
        return GTSHelper.valueAtIndex(gts, idx);
      } else {
        return null;
      }
    }
  }
  
  /**
   * Return an array of indices sorted in the order of the matching value in the 'values' array.
   *  
   * @param values Values whose order must be checked
   * @param reversed If true, sort indices so 'values' is in descending order
   * 
   * @return An array of indices sorted in the order of the matching value in the 'values' array.
   */
  public static int[] sortIndices(final long[] values, final boolean reversed) {
    Integer[] indices = new Integer[values.length];
    
    for (int i = 0; i < values.length; i++) {
      indices[i] = i;
    }
    
    Arrays.sort(indices, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        if (values[o1] < values[o2]) {
          return reversed ? 1 : -1;
        } else if (values[o1] == values[o2]) {
          return 0;
        } else {
          return reversed ? -1 : 1;
        }
      }
    });
    
    int[] sorted = new int[indices.length];
    
    for (int i = 0; i < sorted.length; i++) {
      sorted[i] = indices[i];
    }
    
    return sorted;
  }
  
  public static TYPE getValueType(Object value) {
    if (value instanceof Long) {
      return TYPE.LONG;
    } else if (value instanceof Double) {
      return TYPE.DOUBLE;
    } else if (value instanceof String) {
      return TYPE.STRING;
    } else if (value instanceof Boolean) {
      return TYPE.BOOLEAN;
    } else {
      return TYPE.UNDEFINED;
    }
  }
  
  /**
   * Shrink the internal arrays of this GTS so their length matches
   * the number of values.
   * 
   * @param gts GTS instance to shrink
   */
  public static void shrink(GeoTimeSerie gts) {
    shrink(gts, 1.0D);
  }    
  
  public static void shrink(GeoTimeSerie gts, double ratio) {
    if (0 == gts.values) {
      gts.ticks = null;
      gts.locations = null;
      gts.elevations = null;
      gts.longValues = null;
      gts.doubleValues = null;
      gts.stringValues = null;
      gts.booleanValues = null;
      return;
    }
  
    // Do nothing if array size / value count is <= ratio
    if ((double) gts.ticks.length / (double) gts.values <= ratio) {
      return;
    }
    
    if (gts.ticks.length > gts.values) { // When 0 < gts.values we're sure null != gts.ticks
      gts.ticks = Arrays.copyOf(gts.ticks, gts.values);
    }
    if (null != gts.locations && gts.locations.length > gts.values) {
      gts.locations = Arrays.copyOf(gts.locations, gts.values);
    }
    if (null != gts.elevations && gts.elevations.length > gts.values) {
      gts.elevations = Arrays.copyOf(gts.elevations, gts.values);
    }
    switch(gts.type) {
      case UNDEFINED:
        gts.longValues = null;
        gts.doubleValues = null;
        gts.stringValues = null;
        gts.booleanValues = null;
        break;
      case LONG:
        gts.longValues = null != gts.longValues && gts.longValues.length > gts.values ? Arrays.copyOf(gts.longValues, gts.values) : gts.longValues;
        gts.doubleValues = null;
        gts.stringValues = null;
        gts.booleanValues = null;
        break;
      case DOUBLE:
        gts.longValues = null;
        gts.doubleValues = null != gts.doubleValues && gts.doubleValues.length > gts.values ? Arrays.copyOf(gts.doubleValues, gts.values) : gts.doubleValues;
        gts.stringValues = null;
        gts.booleanValues = null;
        break;
      case STRING:
        gts.longValues = null;
        gts.doubleValues = null;
        gts.stringValues = null != gts.stringValues && gts.stringValues.length > gts.values ? Arrays.copyOf(gts.stringValues, gts.values) : gts.stringValues;
        gts.booleanValues = null;
        break;
      case BOOLEAN:
        gts.longValues = null;
        gts.doubleValues = null;
        gts.stringValues = null;
        if (null != gts.booleanValues && gts.booleanValues.size() > gts.values) {
          gts.booleanValues = gts.booleanValues.get(0, gts.values);
        }
        break;
    }
  }
  
  /**
   * Compact a GeoTimeSerie instance by removing measurements which have the same
   * value/location/elevation as the previous one.
   * We retain the last value so the resulting GTS instance spans the same time
   * interval as the original one. Otherwise for example in the case of a constant
   * GTS, there would be only a single value which would means we would not know when
   * the last value was.
   * 
   * @param gts GTS instance to compress
   * @param preserveRanges Flag indicating if we should preserve range extrema or not
   * 
   * @return a new GTS instance which is a compressed version of the input one
   * 
   */
  public static GeoTimeSerie compact(GeoTimeSerie gts, boolean preserveRanges) {
    //
    // Clone gts
    //
    
    GeoTimeSerie clone = gts.clone();
    
    //
    // Sort so the ticks are in chronological order
    //
    
    GTSHelper.sort(clone);
    
    if (2 >= clone.values) {
      return clone;
    }
    
    //
    // Now scan the ticks and remove duplicate value/location/elevation tuples
    //
    
    int idx = 0;
    int offset = 0;
    // Start at index 1 so we keep the first value
    int compactIdx = 1;
    
    while(idx < clone.values - 1) {
      while(idx + 1 + offset < clone.values - 1
          && locationAtIndex(clone, idx + 1 + offset) == locationAtIndex(clone, idx)
          && elevationAtIndex(clone, idx + 1 + offset) == elevationAtIndex(clone, idx)
          && valueAtIndex(clone, idx + 1 + offset).equals(valueAtIndex(clone, idx))) {
        offset++;
      }
      
      //
      // If we preserve ranges and we reached clone.values - 1 and the last value/location/elevation is identical,
      // continue the loop as the last value will be the end of the range
      //

      boolean last = false;
      
      if (preserveRanges && idx + 1 + offset == clone.values - 1
          && locationAtIndex(clone, clone.values - 1) == locationAtIndex(clone, idx)
          && elevationAtIndex(clone, clone.values - 1) == elevationAtIndex(clone, idx)
          && valueAtIndex(clone, clone.values - 1).equals(valueAtIndex(clone, idx))) {        
        offset++;
        last = true;
      }
      
      //
      // Record the end of the range if preserveRanges is true
      //
      
      if (preserveRanges && offset > 0) {
        clone.ticks[compactIdx] = clone.ticks[idx + offset];
        if (null != clone.locations) {
          clone.locations[compactIdx] = clone.locations[idx + offset];
        }
        if (null != clone.elevations) {
          clone.elevations[compactIdx] = clone.elevations[idx + offset];
        }
        switch (clone.type) {
          case LONG:
            clone.longValues[compactIdx] = clone.longValues[idx + offset];
            break;
          case DOUBLE:
            clone.doubleValues[compactIdx] = clone.doubleValues[idx + offset];
            break;
          case BOOLEAN:
            clone.booleanValues.set(compactIdx, clone.booleanValues.get(idx + offset));
            break;
          case STRING:
            clone.stringValues[compactIdx] = clone.stringValues[idx + offset];
            break;
        }
        if (!last) {
          compactIdx++;
        }
      }

      //
      // Record the new value if idx + offset + 1 < clone.values
      //
      
      if (idx + offset + 1 < clone.values) {
        clone.ticks[compactIdx] = clone.ticks[idx + offset + 1];
        if (null != clone.locations) {
          clone.locations[compactIdx] = clone.locations[idx + offset + 1];
        }
        if (null != clone.elevations) {
          clone.elevations[compactIdx] = clone.elevations[idx + offset + 1];
        }
        switch (clone.type) {
          case LONG:
            clone.longValues[compactIdx] = clone.longValues[idx + offset + 1];
            break;
          case DOUBLE:
            clone.doubleValues[compactIdx] = clone.doubleValues[idx + offset + 1];
            break;
          case BOOLEAN:
            clone.booleanValues.set(compactIdx, clone.booleanValues.get(idx + offset + 1));
            break;
          case STRING:
            clone.stringValues[compactIdx] = clone.stringValues[idx + offset + 1];
            break;
        }        
      }
        
      //
      // Advance indices
      //
      idx = idx + offset + 1;
      compactIdx++;
      offset = 0;
    }

    clone.values = compactIdx;
    
    GTSHelper.shrink(clone);
    
    return clone;
  }
  
  /**
   * Normalize a GTS, replacing X by (X-MIN)/(MAX-MIN) or 1.0
   * @param gts GeoTimeSerie instance to be normalized.
   * @return A clone of the given GTS, normalized only if it contains numeric values.
   */
  public static GeoTimeSerie normalize(GeoTimeSerie gts) {
    //
    // Return immediately if GTS is not numeric or has no values
    //
    if ((TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) || 0 == gts.values) {
      return gts.clone();
    }

    //
    // Extract min/max
    //
    
    double dmin = Double.POSITIVE_INFINITY;
    double dmax = Double.NEGATIVE_INFINITY;
    
    long lmin = Long.MAX_VALUE;
    long lmax = Long.MIN_VALUE;
    
    if (TYPE.LONG == gts.getType()) {
      for (int i = 0; i < gts.values; i++) {
        long value = ((Number) GTSHelper.valueAtIndex(gts, i)).longValue();
        
        if (value > lmax) {
          lmax = value;
        }
        if (value < lmin) {
          lmin = value;
        }
      }      
    } else {
      for (int i = 0; i < gts.values; i++) {
        double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
        
        if (value > dmax) {
          dmax = value;
        }
        if (value < dmin) {
          dmin = value;
        }
      }
    }
    
    boolean constant = false;
    
    if (lmin == lmax || dmin == dmax) {
      constant = true;
    }
    
    //
    // Don't use clone or cloneEmpty, this would force the type to that of 'gts'
    
    GeoTimeSerie normalized = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    normalized.setMetadata(new Metadata(gts.getMetadata()));
    
    for (int i = 0; i < gts.values; i++) {
      Object value;
      
      if (constant) {
        value = 1.0D;
      } else if (TYPE.LONG == gts.getType()) {
        value = (((Number) GTSHelper.valueAtIndex(gts, i)).longValue() - lmin) / (double) (lmax - lmin);
      } else {
        value = (((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() - dmin) / (double) (dmax - dmin);
      }
      
      GTSHelper.setValue(normalized, gts.ticks[i], GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
    }
    
    return normalized;
  }

  /**
   * Normalize a GTS, replacing X by (X-MEAN)/(MAX-MIN) or 1.0
   * @param gts GeoTimeSerie instance to be isonormalized.
   * @return A clone of the given GTS, isonormalized only if it contains numeric values.
   */
  public static GeoTimeSerie isonormalize(GeoTimeSerie gts) {
    //
    // Return immediately if GTS is not numeric or has no values
    //
    if ((TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) || 0 == gts.values) {
      return gts.clone();
    }

    //
    // Extract min/max
    //
    
    double sum = 0.0D;
    
    double dmin = Double.POSITIVE_INFINITY;
    double dmax = Double.NEGATIVE_INFINITY;
    
    long lmin = Long.MAX_VALUE;
    long lmax = Long.MIN_VALUE;
    
    if (TYPE.LONG == gts.getType()) {
      for (int i = 0; i < gts.values; i++) {
        long value = ((Number) GTSHelper.valueAtIndex(gts, i)).longValue();
        
        if (value > lmax) {
          lmax = value;
        }
        if (value < lmin) {
          lmin = value;
        }
        
        sum += value;
      }      
    } else {
      for (int i = 0; i < gts.values; i++) {
        double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
        
        if (value > dmax) {
          dmax = value;
        }
        if (value < dmin) {
          dmin = value;
        }
        
        sum += value;
      }
    }
    
    boolean constant = false;
    
    if (lmin == lmax || dmin == dmax) {
      constant = true;
    }
    
    //
    // Don't use clone or cloneEmpty, this would force the type to that of 'gts'
    
    GeoTimeSerie isonormalized = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    isonormalized.setMetadata(new Metadata(gts.getMetadata()));
    
    double mean = sum / gts.values;
    
    for (int i = 0; i < gts.values; i++) {
      Object value;
      
      if (constant) {
        value = 1.0D;
      } else if (TYPE.LONG == gts.getType()) {
        value = (((Number) GTSHelper.valueAtIndex(gts, i)).longValue() - mean) / (double) (lmax - lmin);
      } else {
        value = (((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() - mean) / (double) (dmax - dmin);
      }
      
      GTSHelper.setValue(isonormalized, gts.ticks[i], GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
    }
    
    return isonormalized;
  }

  /**
   * Standardize a numeric GeoTimeSerie
   *
   * @param gts GeoTimeSerie instance to be standardized.
   * @return A clone of the given GTS, standardized only if it contains numeric values.
   */
  public static GeoTimeSerie standardize(GeoTimeSerie gts) {
    //
    // Return immediately if GTS is not numeric or has no values
    //
    if ((TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) || 0 == gts.values) {
      return gts.clone();
    }
    
    //
    // Compute sum of values and sum of squares
    //
    
    double sum = 0.0D;
    double sumsq = 0.0D;
    
    for (int i = 0; i < gts.values; i++) {
      double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
      sum += value;
      sumsq += value * value;
    }
    
    //
    // Compute mean and standard deviation
    //
    
    double mean = sum / (double) gts.values;
    
    double variance = (sumsq / (double) gts.values) - (sum * sum) / ((double) gts.values * (double) gts.values);
    
    //
    // Apply Bessel's correction
    // @see http://en.wikipedia.org/wiki/Bessel's_correction
    //
    
    if (gts.values > 1) {
      variance = variance * ((double) gts.values) / (gts.values - 1.0D);
    }

    double sd = Math.sqrt(variance);

    return standardize(gts, mean, sd);
  }
  
  public static GeoTimeSerie standardize(GeoTimeSerie gts, double mean, double sd) {
    GeoTimeSerie standardized = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    standardized.setMetadata(new Metadata(gts.getMetadata()));

    for (int i = 0; i < gts.values; i++) {
      double value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
      // Subtract mean
      value = value - mean;
      // Divide by sd if sd is not null
      if (0.0D != sd) {
        value = value / sd;
      }
      GTSHelper.setValue(standardized, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, value, false);
    }
    
    return standardized;
  }
  
  /**
   * Produces a String GTS whose values are the encoded bSAX words of the sequence starting at each tick.
   * 
   * It only works on numeric bucketized GTS instances.
   * 
   * @param gts GeoTimeSerie instance on which to apply the bSAX algorithm, must be bucketized, filled and numeric.
   * @param alphabetSize Size of the alphabet used by bSAX, must be a power of two.
   * @param wordLen Size of the words to be generated by the bSAX algorithm.
   * @param windowLen Window size which is the width of each interval on which the pattern extraction should be performed.
   * @param standardizePAA Whether to standardize or not the piecewise aggregate approximation.
   * @return A new GeoTimeSerie instance containing the bSAX result.
   *
   * @throws WarpScriptException if the GTS is not numeric, bucketized and filled or if parameters are incorrect.
   */
  public static GeoTimeSerie bSAX(GeoTimeSerie gts, int alphabetSize, int wordLen, int windowLen, boolean standardizePAA) throws WarpScriptException {
    
    if (!GTSHelper.isBucketized(gts) || (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type)) {
      throw new WarpScriptException("Function can only be applied to numeric, bucketized, filled Geo Time Series.");
    }
    
    if (windowLen % wordLen != 0) {
      throw new WarpScriptException("Wordlen MUST divide windowlen.");
    }
    
    //
    // Check if alphabetSize is a power of 2
    //
    
    int levels = 0;
    
    if (0 == alphabetSize) {
      throw new WarpScriptException("Alphabet size MUST be a power of two.");      
    }
    
    while(0 == (alphabetSize & 1)) {
      levels++;
      alphabetSize >>>= 1;
    }
    
    if (1 != alphabetSize) {
      throw new WarpScriptException("Alphabet size MUST be a power of two.");      
    }

    if (levels < 1 || levels > SAXUtils.SAX_MAX_LEVELS) {
      throw new WarpScriptException("Alphabet size MUST be a power of two between 2 and 2^" + SAXUtils.SAX_MAX_LEVELS);
    }

    // Compute number of values to aggregate using PAA to obtain the correct number of sax symbols per word given the sliding window length
    int paaLen = windowLen / wordLen;
 
    //
    // Sort GTS
    //
    
    GTSHelper.sort(gts);
    
    GeoTimeSerie saxGTS = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    saxGTS.setMetadata(gts.getMetadata());
    
    int[] symbols = new int[wordLen];
    double paaSum[] = new double[wordLen];
    
    for (int i = 0; i < gts.values - windowLen + 1; i++) {
      //
      // Apply PAA
      //

      double sum = 0.0D;
      double sumsq = 0.0D;
      
      
      for (int w = 0; w < wordLen; w++) {
        paaSum[w] = 0.0D;
        
        for (int k = 0; k < paaLen; k++) {
          paaSum[w] += TYPE.LONG == gts.type ? gts.longValues[i + w * paaLen + k] : gts.doubleValues[i + w * paaLen + k];
        }
              
        if (!standardizePAA) {
          continue;
        }

        double mean = paaSum[w] / paaLen;
        sum += mean;
        sumsq += mean * mean;
      }
      
      //
      // Normalize window
      //
      
      double mu = 0.0D;
      
      double variance = 0.0D;
      double sigma = 0.0D;
      
      if (standardizePAA) {
        mu = sum / wordLen;
        variance = (sumsq / wordLen) - (sum * sum) / ((double) wordLen * (double) wordLen);
        //
        // Apply Bessel's correction
        // @see http://en.wikipedia.org/wiki/Bessel's_correction
        //
        
        if (wordLen > 1) {
          variance = variance * wordLen / (wordLen - 1.0D);
        }
        
        sigma =  Math.sqrt(variance);
      }
      
      for (int w = 0; w < wordLen; w++) {
        // Compute value to use for generating SAX symbol
        if (standardizePAA) {
          symbols[w] = SAXUtils.SAX(levels, sigma != 0D ? ((paaSum[w] / paaLen) - mu) / sigma : ((paaSum[w] / paaLen) - mu));
        } else {
          symbols[w] = SAXUtils.SAX(levels, paaSum[w] / paaLen);         
        }
      }

      //
      // Generate bSAX words
      //
      
      String word = new String(OrderPreservingBase64.encode(SAXUtils.bSAX(levels, symbols)), StandardCharsets.US_ASCII);
      
      GTSHelper.setValue(saxGTS, gts.ticks[i], word);      
    }
    
    return saxGTS;
  }
  
  /**
   * Perform exponential smoothing on a numeric GTS.
   * 
   * @param gts GeoTimeSerie instance on which to apply the exponential smoothing.
   * @param alpha Smoothing Factor (0 < alpha < 1)
   * @return A clone of the given instance with single exponential smoothing applied.
   *
   * @throws WarpScriptException if the GTS is not numeric, has less than 2 values or alpha is not in ]0;1[.
   */
  public static GeoTimeSerie singleExponentialSmoothing(GeoTimeSerie gts, double alpha) throws WarpScriptException {
    
    //
    // Alpha must be between 0 and 1
    //
    
    if (alpha <= 0.0D || alpha >= 1.0D) {
      throw new WarpScriptException("The smoothing factor must be in 0 < alpha < 1.");
    }
    
    //
    // Exponential smoothing only works on numeric GTS with at least two values
    //
    
    if (TYPE.LONG != gts.type && TYPE.DOUBLE != gts.type) {
      throw new WarpScriptException("Can only perform exponential smoothing on numeric Geo Time Series.");
    }
    
    if (gts.values < 2) {
      throw new WarpScriptException("Can only perform exponential smoothing on Geo Time Series containing at least two values.");
    }
    
    GeoTimeSerie s = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    s.setMetadata(new Metadata(gts.getMetadata()));
    
    //
    // Sort input GTS
    //
    
    GTSHelper.sort(gts);
    
    double smoothed = ((Number) GTSHelper.valueAtIndex(gts, 0)).doubleValue();
    double oneminusalpha = 1.0D - alpha;
    
    GTSHelper.setValue(s, gts.ticks[0], null != gts.locations ? gts.locations[0] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[0] : GeoTimeSerie.NO_ELEVATION, smoothed, false);
    
    for (int i = 1; i < gts.values; i++) {
      smoothed = alpha * ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() + oneminusalpha * smoothed;
      
      GTSHelper.setValue(s, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, smoothed, false);
    }
    
    return s;
  }
  
  /**
   * Perform a double exponential smoothing on the input GTS.
   *
   * @param gts GeoTimeSerie instance on which to apply the exponential smoothing.
   * @param alpha Smoothing Factor (0 < alpha < 1)
   * @param beta Trend smoothing Factor (0 < beta < 1)
   * @return A clone of the given instance with double exponential smoothing applied.
   *
   * @throws WarpScriptException if the GTS is not numeric, has less than 2 values or alpha or beta is not in ]0;1[.
   */
  public static List<GeoTimeSerie> doubleExponentialSmoothing(GeoTimeSerie gts, double alpha, double beta) throws WarpScriptException {
    
    //
    // Alpha and Beta must be between 0 and 1
    //
    
    if (alpha <= 0.0D || alpha >= 1.0D) {
      throw new WarpScriptException("The data smoothing factor must be in 0 < alpha < 1.");
    }

    if (beta <= 0.0D || beta >= 1.0D) {
      throw new WarpScriptException("The trend smoothing factor must be in 0 < beta < 1.");
    }

    //
    // Exponential smoothing only works on numeric GTS with at least two values
    //
    
    if (TYPE.LONG != gts.type && TYPE.DOUBLE != gts.type) {
      throw new WarpScriptException("Can only perform exponential smoothing on numeric Geo Time Series.");
    }
    
    if (gts.values < 2) {
      throw new WarpScriptException("Can only perform exponential smoothing on Geo Time Series containing at least two values.");
    }

    //
    // Allocate a list for returning smoothed GTS and best estimate GTS
    //
    
    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
    
    // Smoothed GTS
    GeoTimeSerie s = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, gts.values);
    s.setMetadata(new Metadata(gts.getMetadata()));

    // Best estimate GTS
    GeoTimeSerie b = s.clone();
    
    GTSHelper.sort(gts);
    
    double smoothed = ((Number) GTSHelper.valueAtIndex(gts, 1)).doubleValue();
    double bestestimate = smoothed - ((Number) GTSHelper.valueAtIndex(gts, 0)).doubleValue();
    double oneminusalpha = 1.0D - alpha;
    double oneminusbeta = 1.0D - beta;
        
    GTSHelper.setValue(s, gts.ticks[1], null != gts.locations ? gts.locations[1] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[1] : GeoTimeSerie.NO_ELEVATION, smoothed, false);
    GTSHelper.setValue(b, gts.ticks[1], null != gts.locations ? gts.locations[1] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[1] : GeoTimeSerie.NO_ELEVATION, bestestimate, false);

    for (int i = 2; i < gts.values; i++) {
      double newsmoothed = alpha * ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() + oneminusalpha * (smoothed + bestestimate);
      bestestimate = beta * (newsmoothed - smoothed) + oneminusbeta * bestestimate;
      smoothed = newsmoothed;
      GTSHelper.setValue(s, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, smoothed, false);
      GTSHelper.setValue(b, gts.ticks[i], null != gts.locations ? gts.locations[i] : GeoTimeSerie.NO_LOCATION, null != gts.elevations ? gts.elevations[i] : GeoTimeSerie.NO_ELEVATION, bestestimate, false);
    }
    
    result.add(s);
    result.add(b);
    
    return result;
  }
  
  /**
   * Build an occurrence count by value for the given time serie.
   */
  public static Map<Object,Long> valueHistogram(GeoTimeSerie gts) {
    //
    // Sort gts
    //
    
    Map<Object, Long> occurrences = new HashMap<Object, Long>();
    
    //
    // Count the actual values
    //
    
    for (int i = 0; i < gts.values; i++) {
      Object value = GTSHelper.valueAtIndex(gts, i);
      
      if (!occurrences.containsKey(value)) {
        occurrences.put(value, 1L);
      } else {        
        occurrences.put(value, 1L + occurrences.get(value));        
      }
    }
    
    //
    // If the GTS is bucketized, add the count of empty values
    //
    
    if (GTSHelper.isBucketized(gts) && gts.bucketcount != gts.values) {
      occurrences.put(null, (long) (gts.bucketcount - gts.values));
    }
    
    return occurrences;
  }
  
  /**
   * Build an occurrence count by value for the given GTS Encoder.
   */
  public static Map<Object,Long> valueHistogram(GTSEncoder encoder) {
    Map<Object, Long> occurrences = new HashMap<Object, Long>();

    GTSDecoder decoder = encoder.getDecoder();
    
    while(decoder.next()) {
      Object value = decoder.getValue();
      
      if (!occurrences.containsKey(value)) {
        occurrences.put(value, 1L);
      } else {        
        occurrences.put(value, 1L + occurrences.get(value));        
      }
    }
    
    return occurrences;
  }
  
  /**
   * Detect patterns in a Geo Time Serie instance. Return a modified version of the original
   * GTS instance where only the values which are part of one of the provided patterns are kept.
   * 
   * @param gts GeoTimeSerie in which to detect patterns.
   * @param alphabetSize Size of the alphabet used by bSAX, must be a power of two.
   * @param wordLen Size of the words to be generated by the bSAX algorithm.
   * @param windowLen Window size which is the width of each interval on which the pattern extraction should be performed.
   * @param standardizePAA Whether to standardize or not the piecewise aggregate approximation.
   * @return A new GeoTimeSerie instance where only the values which are part of one of the provided patterns are kept.
   *
   * @throws WarpScriptException if the GTS is not numeric, bucketized and filled or if bSAX parameters are incorrect.
   */
  public static GeoTimeSerie detect(GeoTimeSerie gts, int alphabetSize, int wordLen, int windowLen, Collection<String> patterns, boolean standardizePAA) throws WarpScriptException {
    //
    // Generate patterns for the provided GTS
    //
    
    GeoTimeSerie gtsPatterns = GTSHelper.bSAX(gts, alphabetSize, wordLen, windowLen, standardizePAA);
    
    //
    // Sort gtsPatterns
    //
    
    GTSHelper.sort(gtsPatterns);
    
    //
    // Scan the ticks, 
    //
    
    GeoTimeSerie detected = new GeoTimeSerie(gts.lastbucket, gts.bucketcount, gts.bucketspan, 16);
    detected.setMetadata(gts.getMetadata());
    
    // Last index included, used to speed up things
    int lastidx = -1;
    
    for (int i = 0; i < gtsPatterns.values; i++) {
      if (!patterns.contains(gtsPatterns.stringValues[i])) {
        continue;
      }
      
      //
      // The pattern at the current tick is one we want to detect, include 'windowLen' ticks into the 'detected' GTS
      //
      
      for (int j = 0; j < windowLen; j++) {
        if (i + j > lastidx) {
          lastidx = i + j;
          GTSHelper.setValue(detected, GTSHelper.tickAtIndex(gts, lastidx), GTSHelper.locationAtIndex(gts, lastidx), GTSHelper.elevationAtIndex(gts, lastidx), GTSHelper.valueAtIndex(gts, lastidx), false);
        }
      }
    }
    
    return detected;
  }
  
  public static int getBucketCount(GeoTimeSerie gts) {
    return gts.bucketcount;
  }
  
  public static long getBucketSpan(GeoTimeSerie gts) {
    return gts.bucketspan;
  }
  
  public static long getLastBucket(GeoTimeSerie gts) {
    return gts.lastbucket;
  }

  public static void setBucketCount(GeoTimeSerie gts, int bucketcount) {
    gts.bucketcount = bucketcount;
  }
  
  public static void setBucketSpan(GeoTimeSerie gts, long bucketspan) {
    gts.bucketspan = bucketspan;
  }
  
  public static void setLastBucket(GeoTimeSerie gts, long lastbucket) {
    gts.lastbucket = lastbucket;
  }

  //public static void metadataToString(StringBuilder sb, String name, Map<String,String> labels) {
  //  metadataToString(sb, name, labels, false);
  //}
  
  public static void metadataToString(StringBuilder sb, String name, Map<String,String> labels, boolean expose) {
    GTSHelper.encodeName(sb, name);
    
    labelsToString(sb, labels, expose);
  }
  //public static void labelsToString(StringBuilder sb, Map<String,String> labels) {
  //  labelsToString(sb, labels, false);
  //}
  
  public static void labelsToString(StringBuilder sb, Map<String,String> labels, boolean expose) {
    sb.append("{");
    boolean first = true;
    
    if (null != labels) {
      for (Entry<String, String> entry: labels.entrySet()) {
        //
        // Skip owner/producer labels and any other 'private' labels
        //
        if (!expose && !Constants.EXPOSE_OWNER_PRODUCER) {
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
    }
    
    sb.append("}");
  }
  
  public static String buildSelector(GeoTimeSerie gts, boolean forSearch) {
    return buildSelector(gts.getMetadata(), forSearch);
  }
  
  /**
   * Build a string representation of Metadata suitable for selection (via FIND/FETCH).
   * 
   * @param metadata Metadata to represent
   * @param forSearch Set to true if the result is for searching, in that case for empty values of labels, '~$' will be produced, otherwise '='
   * @return
   */
  public static String buildSelector(Metadata metadata, boolean forSearch) {
    StringBuilder sb = new StringBuilder();

    String name = metadata.getName();

    if(name.length() > 0) {
      char nameFirstChar = name.charAt(0);
      if ('=' == nameFirstChar || '~' == nameFirstChar) {
        sb.append("="); // Prepend '=' for the special character not to be interpreted
      }
    }
    encodeName(sb, name);

    sb.append("{");
    TreeMap<String,String> labels = new TreeMap<String,String>(metadata.getLabels());
    boolean first = true;
    for (Entry<String,String> entry: labels.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      encodeName(sb, entry.getKey());
      if (forSearch && Constants.ABSENT_LABEL_SUPPORT && "".equals(entry.getValue())) {
        sb.append("~$");
      } else {
        sb.append("=");
        encodeName(sb, entry.getValue());
      }
      first = false;
    }
    sb.append("}");
    
    return sb.toString();
  }
  
  /**
   * Return a GTS which is a subset of the input GTS with only those ticks which
   * fall between start and end (both inclusive).
   * 
   * @param gts GeoTimeSerie instance to clip.
   * @param start Oldest tick (inclusive) to keep.
   * @param end Youngest tick (inclusive) to keep.
   * @return A new GeoTimeSerie instance which is a subset of the input GTS with only those ticks which fall between start and end (both inclusive).
   */
  public static GeoTimeSerie timeclip(GeoTimeSerie gts, long start, long end) {
    // If the GTS is sorted, use subSerie which is vastly faster
    if (gts.sorted) {
      return subSerie(gts, start, end, false);
    }

    // GTS is not sorted, scan the GTS and add tick in [start;end] to an empty GTS
    GeoTimeSerie clipped = gts.cloneEmpty();

    for (int idx = 0; idx < gts.values; idx++) {
      long ts = GTSHelper.tickAtIndex(gts, idx);

      if (ts >= start && ts <= end) {
        GTSHelper.setValue(clipped, ts, GTSHelper.locationAtIndex(gts, idx), GTSHelper.elevationAtIndex(gts, idx), GTSHelper.valueAtIndex(gts, idx), false);
      }
    }
    
    return clipped;
  }

  /**
   * Clip a GTSEncoder
   * 
   * @param start lower timestamp to consider (inclusive)
   * @param end upper timestamp to consider (inclusive)
   * @return A new GTSEncoder instance which is a subset of the input GTSEncoder with only those ticks which fall between start and end (both inclusive).
   */
  public static GTSEncoder timeclip(GTSEncoder encoder, long start, long end) {
    
    GTSEncoder clipped = new GTSEncoder(0L);
    clipped.setMetadata(encoder.getMetadata());
    GTSDecoder decoder = encoder.getUnsafeDecoder(false);
    while(decoder.next()) {
      long timestamp = decoder.getTimestamp();
      if (timestamp < start || timestamp > end) {
        continue;
      }
      try {
        clipped.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    
    return clipped;
  }
  
  /**
   * 'Integrate' a GTS, considering the value at each tick is a rate of change per second.
   * 
   * @param gts GeoTimeSerie instance to integrate.
   * @param initialValue Initial value of the resulting GTS.
   * @return A new GeoTimeSerie instance which is the integration of the given GeoTimeSerie.
   */
  public static GeoTimeSerie integrate(GeoTimeSerie gts, double initialValue) {
    GeoTimeSerie integrated = gts.cloneEmpty();
    
    //
    // Sort GTS so ticks are in ascending order
    //
    
    GTSHelper.sort(gts);
    
    double value = initialValue;
    
    GTSHelper.setValue(integrated, GTSHelper.tickAtIndex(gts, 0), GTSHelper.locationAtIndex(gts, 0), GTSHelper.elevationAtIndex(gts,  0), value, false);
    
    for (int i = 1; i < gts.values; i++) {
      double deltaT = (double) (gts.ticks[i] - gts.ticks[i - 1]) / (double) Constants.TIME_UNITS_PER_S;
      
      double rateOfChange = ((Number) GTSHelper.valueAtIndex(gts, i - 1)).doubleValue();
      
      value = value + rateOfChange * deltaT;
      
      GTSHelper.setValue(integrated, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts,  i), value, false);
    }
    
    return integrated;
  }
  
  /**
   * Reduce the number of values in a GTS. The underlying arrays are not shrunk.
   * If the new size is far lower than the old size and should stay that way, consider applying GTSHelper.shrink to avoid wasting memory.
   * 
   * @param gts Geo Time Series to reduce
   * @param newsize New number of values
   * @return The given GeoTimeSerie instance with at more newsize values. If newsize>=0, return the GTS unmodified.
   */
  public static GeoTimeSerie shrinkTo(GeoTimeSerie gts, int newsize) {
    if (newsize >= 0 && newsize < gts.values) {
      gts.values = newsize;
    }
    
    return gts;
  }
  
  /**
   * Shrink an encoder to at most a given number of values.
   * @param encoder
   * @param newsize
   * @return
   * @throws IOException
   */
  public static GTSEncoder shrinkTo(GTSEncoder encoder, int newsize) throws IOException {
    GTSEncoder enc = null;
        
    // We cannot check the number of datapoints of the encoder as it might be false
    // if the decoder was created from a buffer and not by adding values.
    if (0 == newsize) {
      return encoder.cloneEmpty();
    } else if (newsize > 0) {
      enc = encoder.cloneEmpty();
      
      GTSDecoder decoder = encoder.getDecoder(true);
      
      int count = 0;
      
      // Iterate over the elements, stopping when we reached the requested new size
      while (count < newsize && decoder.next()) {
        enc.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
        count++;
      }
      
      return enc;
    } else {
      return encoder;
    }
  }
  
  /**
   * Splits a GTS in 'chunks' of equal time length.
   * 
   * @param gts
   * @param lastchunk End timestamp of the most recent chunk. Use 0 to adjust automatically on 'chunkwidth' boundary
   * @param chunkwidth Width of each chunk in time units
   * @param chunkcount Number of chunks to generate. Use 0 to generate as many chunks as needed to cover the GTS
   * @param chunklabel Name of the label to use for storing the chunkid
   * @param keepempty Should we keed empty chunks
   * @return a List of GeoTimeSerie, each GTS covering one chunk.
   *
   * @throws WarpScriptException if parameters are invalid.
   */
  public static List<GeoTimeSerie> chunk(GeoTimeSerie gts, long lastchunk, long chunkwidth, long chunkcount, String chunklabel, boolean keepempty) throws WarpScriptException {
    return chunk(gts, lastchunk, chunkwidth, chunkcount, chunklabel, keepempty, 0L);    
  }
  
  public static List<GeoTimeSerie> chunk(GeoTimeSerie gts, long lastchunk, long chunkwidth, long chunkcount, String chunklabel, boolean keepempty, long overlap) throws WarpScriptException {
    
    if (overlap < 0 || overlap > chunkwidth) {
      throw new WarpScriptException("Overlap cannot exceed chunk width.");
    }
    
    //
    // Check if 'chunklabel' exists in the GTS labels
    //
    
    Metadata metadata = gts.getMetadata();
    
    if(metadata.getLabels().containsKey(chunklabel)) {
      throw new WarpScriptException("Cannot operate on Geo Time Series which already have a label named '" + chunklabel + "'");
    }

    TreeMap<Long, GeoTimeSerie> chunks = new TreeMap<Long,GeoTimeSerie>();
    
    //
    // If GTS is bucketized, make sure bucketspan is less than chunkwidth
    //
    
    boolean bucketized = GTSHelper.isBucketized(gts);
    
    if (bucketized) {
      if (gts.bucketspan > chunkwidth) {
        throw new WarpScriptException("Cannot operate on Geo Time Series with a bucketspan greater than the chunk width.");
      }
    } else {
      // GTS is not bucketized and has 0 values, if lastchunk was 0, return an empty list as we
      // are unable to produce chunks
      if (0 == gts.values && 0L == lastchunk) {
        return new ArrayList<GeoTimeSerie>();
      }
    }
    
    //
    // Set chunkcount to Integer.MAX_VALUE if it's 0
    //
    
    boolean zeroChunkCount = false;
    
    if (0 == chunkcount) {
      chunkcount = Integer.MAX_VALUE;
      zeroChunkCount = true;
    }
    
    //
    // Sort timestamps in reverse order so we can produce all chunks in O(n)
    //
    
    GTSHelper.sort(gts, true);

    //
    // Loop on the chunks
    //
    
    // Index in the timestamp array
    int idx = 0;

    long bucketspan = gts.bucketspan;
    int bucketcount = gts.bucketcount;
    long lastbucket = gts.lastbucket;
    
    //
    // If lastchunk is 0, use lastbucket or the most recent tick
    //
    
    if (0 == lastchunk) {
      if (isBucketized(gts)) {
        lastchunk = lastbucket;
      } else {
        // Use the most recent tick
        lastchunk = gts.ticks[0]; 
        // Make sure lastchunk is aligned on 'chunkwidth' boundary
        if (0 != (lastchunk % chunkwidth)) {
          lastchunk = lastchunk - (lastchunk % chunkwidth) + chunkwidth;
        }
      }            
    }

    // If we have overlap add extra chunks at the beginning and end to compute overlap
    if (overlap > 0) {
      chunkcount += 2;
      lastchunk += chunkwidth;
    }

    //
    // Compute size hints so allocations are faster.
    // We heuristically consider that each chunk will contain an equal share of
    // the original points. We do the same for the overlaps.
    //
    
    int hint = (int) (chunkcount > 0 ? gts.values / chunkcount : 0);
    int overlaphint = (int) (overlap > 0 ? gts.values / overlap : 0);
    
    //
    // The final hint takes intoaccount the overlaps
    //
    
    if (hint > 0) {
      hint += 2 * overlaphint;
    }
    
    for (long i = 0; i < chunkcount; i++) {
      // If we have no more values and were not specified a chunk count, exit the loop, we're done
      if (idx >= gts.values && zeroChunkCount) {
        break;
      }
      
      // Compute chunk bounds
      long chunkend = lastchunk - i * chunkwidth;
      long chunkstart = chunkend - chunkwidth + 1;

      GeoTimeSerie chunkgts = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, hint);
      
      // Set metadata for the GTS
      chunkgts.setMetadata(metadata);
     // Add 'chunklabel'
      chunkgts.getMetadata().putToLabels(chunklabel, Long.toString(chunkend));
            
      if (bucketized) {
        // Chunk is outside the GTS, it will be empty 
        if (lastbucket < chunkstart || chunkend <= lastbucket - (bucketcount * bucketspan)) {
          // Add the (empty) chunk if keepempty is true
          if (keepempty || overlap > 0) {
            chunks.put(chunkend,chunkgts);
          }
          continue;
        }

        // Set the bucketized parameters in the GTS
        
        // If bucketspan does not divide chunkwidth, chunks won't be bucketized
        
        if (0 == chunkwidth % bucketspan) {
          chunkgts.bucketspan = bucketspan;
          chunkgts.lastbucket = chunkend;
          chunkgts.bucketcount = (int) ((chunkend - chunkstart + 1) / bucketspan);
        } else {
          chunkgts.bucketspan = 0L;
          chunkgts.lastbucket = 0L;
          chunkgts.bucketspan = 0;
        }
      }
      
      //
      // Add the datapoints which fall within the current chunk
      //
      
      // Advance until the current tick is before 'chunkend'       
      while (idx < gts.values && gts.ticks[idx] > chunkend) {
        idx++;
      }

      // We've exhausted the values
      if (idx >= gts.values) {
        // only add chunk if it's not empty or empty with 'keepempty' set to true
        if (0 != chunkgts.values || (keepempty || overlap > 0)) {
          chunks.put(chunkend, chunkgts);
        }
        continue;
      }
      
      // The current tick is before the beginning of the current chunk
      if (gts.ticks[idx] < chunkstart) {
        // only add chunk if it's not empty or empty with 'keepempty' set to true
        if (0 != chunkgts.values || (keepempty || overlap > 0)) {
          chunks.put(chunkend, chunkgts);
        }
        continue;
      }
      
      while(idx < gts.values && gts.ticks[idx] >= chunkstart) {
        GTSHelper.setValue(chunkgts, GTSHelper.tickAtIndex(gts, idx), GTSHelper.locationAtIndex(gts, idx), GTSHelper.elevationAtIndex(gts, idx), GTSHelper.valueAtIndex(gts, idx), false);
        idx++;
      }
      
      // only add chunk if it's not empty or empty with 'keepempty' set to true
      if (0 != chunkgts.values || (keepempty || overlap > 0)) {
        chunks.put(chunkend,chunkgts);
      }
      
      //
      // If there is no overlap and the chunk has empty slots, shrink it now so we save memory.
      // If using overlap and the chunk has more empty array slots than 2 * overlaphint, shrink it.
      // This will slow down the overlap computation but will save memory.
      //
      
      if (0 == chunkgts.values) {
        GTSHelper.shrink(chunkgts);
      } else if (overlap <= 0 && gts.values > 0 && chunkgts.ticks.length - chunkgts.values > 0) {
        GTSHelper.shrink(chunkgts);
      } else if (overlap > 0 && chunkgts.values > 0 && chunkgts.ticks.length - chunkgts.values > 2 * overlaphint) {
        GTSHelper.shrink(chunkgts);
      }
      
      //
      // Adapt the hint
      //
      
      if (chunkgts.values > hint) {
        hint = chunkgts.values + 2 * overlaphint;
      } else if (chunkgts.values < hint) {
        hint = ((hint + chunkgts.values) / 2) + 2 * overlaphint;
      }
    }
    
    //
    // Handle overlapping is need be.
    // We need to iterate over all ticks and add datapoints to each GTS they belong to
    //
    
    if (overlap > 0) {
      //
      // Put all entries in a list so we can access them randomly
      //
      
      List<Entry<Long,GeoTimeSerie>> allchunks = new ArrayList<Entry<Long,GeoTimeSerie>>(chunks.entrySet());

      int[] currentSizes = new int[allchunks.size()];
            
      for (int i = 0; i < currentSizes.length; i++) {
        currentSizes[i] = allchunks.get(i).getValue().values;
      }
      
      //
      // Iterate over chunks, completing with prev and next overlaps
      // Remember the timestamps are in reverse order so far.
      //
      
      for (int i = 0; i < allchunks.size(); i++) {
        GeoTimeSerie current = allchunks.get(i).getValue();
        long lowerBound = allchunks.get(i).getKey() - chunkwidth + 1 - overlap;
        long upperBound = allchunks.get(i).getKey() + overlap;
        if (i > 0) {
          GeoTimeSerie prev = allchunks.get(i - 1).getValue();
          for (int j = 0; j < currentSizes[i - 1]; j++) {
            long timestamp = GTSHelper.tickAtIndex(prev, j);
            if (timestamp < lowerBound) {
              break;
            }
            GTSHelper.setValue(current, timestamp, GTSHelper.locationAtIndex(prev, j), GTSHelper.elevationAtIndex(prev, j), GTSHelper.valueAtIndex(prev, j), false);
          }
        }
        if (i < allchunks.size() - 1) {
          GeoTimeSerie next = allchunks.get(i + 1).getValue();
          for (int j = currentSizes[i + 1] - 1; j >=0; j--) {
            long timestamp = GTSHelper.tickAtIndex(next, j);
            if (timestamp > upperBound) {
              break;
            }
            GTSHelper.setValue(current, timestamp, GTSHelper.locationAtIndex(next, j), GTSHelper.elevationAtIndex(next, j), GTSHelper.valueAtIndex(next, j), false);
          }
        }
      }

      // Remove extra chunks at the beginning and end used to compute overlap
      chunks.remove(lastchunk);
      chunks.remove(lastchunk - (chunkcount - 1) * chunkwidth);
    }
    
    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
    
    for (GeoTimeSerie g: chunks.values()) {
      if (!keepempty && 0 == g.values) {
        continue;
      }
      
      //
      // Shrink the GTS if the underlying storage is unused for more than 10% of the capacity
      //
      
      if (g.values > 0 && g.ticks.length - g.values > g.values * 0.1) {
        GTSHelper.shrink(g);
      }
      
      result.add(g);
    }

    return result;
  }

  public static List<GTSEncoder> chunk(GTSEncoder encoder, long lastchunk, long chunkwidth, long chunkcount, String chunklabel, boolean keepempty, long overlap) throws WarpScriptException {

    if (overlap < 0 || overlap > chunkwidth) {
      throw new WarpScriptException("Overlap cannot exceed chunk width.");
    }

    //
    // Check if 'chunklabel' exists in the GTS labels
    //

    Metadata metadata = encoder.getMetadata();

    if(metadata.getLabels().containsKey(chunklabel)) {
      throw new WarpScriptException("Cannot operate on encoders which already have a label named '" + chunklabel + "'");
    }

    // Store and associate chunks with their id.
    HashMap<Long, GTSEncoder> chunks = new HashMap<Long, GTSEncoder>();

    // Encoder has 0 values, if lastchunk was 0, return an empty list as we are unable to produce chunks
    if (0 == encoder.getCount() && 0 == encoder.size() && 0L == lastchunk) {
      return new ArrayList<GTSEncoder>();
    }

    //
    // Set chunkcount to Integer.MAX_VALUE if it's 0
    //

    boolean zeroChunkCount = false;

    if (0 == chunkcount) {
      chunkcount = Integer.MAX_VALUE;
      zeroChunkCount = true;
    }

    //
    // Loop on the chunks
    //

    GTSDecoder decoder = encoder.getUnsafeDecoder(false);

    long oldestChunk = Long.MAX_VALUE;
    long newestChunk = Long.MIN_VALUE;

    try {
      while(decoder.next()) {
        long timestamp = decoder.getTimestamp();

        // Compute chunkid for the current timestamp (the end timestamp of the chunk timestamp is in)

        long chunkid = 0L;

        // Compute delta from 'lastchunk'

        long delta = timestamp - lastchunk;

        // Compute chunkid

        if (delta < 0) { // timestamp is before 'lastchunk'
          if (0 != -delta % chunkwidth) {
            delta += (-delta % chunkwidth);
          }
          chunkid = lastchunk + delta;
        } else if (delta > 0) { // timestamp if after 'lastchunk'
          if (0 != delta % chunkwidth) {
            delta = delta - (delta % chunkwidth) + chunkwidth;
          }
          chunkid = lastchunk + delta;
        } else {
          chunkid = lastchunk;
        }

        // Add datapoint in the chunk it belongs to

        GTSEncoder chunkencoder = chunks.get(chunkid);

        if (null == chunkencoder) {
          chunkencoder = new GTSEncoder(0L);
          chunkencoder.setMetadata(encoder.getMetadata());
          chunkencoder.getMetadata().putToLabels(chunklabel, Long.toString(chunkid));
          chunks.put(chunkid, chunkencoder);
        }

        chunkencoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());

        // Add datapoint to adjacent chunk if overlap is > 0

        if (overlap > 0) {
          // Check next chunk
          if (timestamp >= chunkid + 1 - overlap) {
            chunkencoder = chunks.get(chunkid + chunkwidth);
            if (null == chunkencoder) {
              chunkencoder = new GTSEncoder(0L);
              chunkencoder.setMetadata(encoder.getMetadata());
              chunkencoder.getMetadata().putToLabels(chunklabel, Long.toString(chunkid + chunkwidth));
              chunks.put(chunkid + chunkwidth, chunkencoder);
            }
            chunkencoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());
          }

          // Check previous chunk
          if (timestamp <= chunkid - chunkwidth + overlap) {
            chunkencoder = chunks.get(chunkid - chunkwidth);
            if (null == chunkencoder) {
              chunkencoder = new GTSEncoder(0L);
              chunkencoder.setMetadata(encoder.getMetadata());
              chunkencoder.getMetadata().putToLabels(chunklabel, Long.toString(chunkid - chunkwidth));
              chunks.put(chunkid - chunkwidth, chunkencoder);
            }
            chunkencoder.addValue(timestamp, decoder.getLocation(), decoder.getElevation(), decoder.getValue());
          }
        }

        oldestChunk = Math.min(oldestChunk, chunkid);
        newestChunk = Math.max(newestChunk, chunkid);
      }
    } catch (IOException ioe) {
      throw new WarpScriptException("Encountered an error while creating chunks.", ioe);
    }

    ArrayList<GTSEncoder> encoders = new ArrayList<GTSEncoder>();

    // Now retain only the chunks we want according to chunkcount and lastchunk.

    CapacityExtractorOutputStream extractor = new CapacityExtractorOutputStream();

    long firstchunkid = oldestChunk;
    if (!zeroChunkCount) {
      firstchunkid = lastchunk - (chunkcount - 1) * chunkwidth;
    }

    long lastchunkid = lastchunk;
    if (0 == lastchunk) {
      lastchunkid = newestChunk;
    }

    // Scan chunkIDs backward to early abort in case chunkcount is reached.
    for (long chunkid = lastchunkid; chunkid >= firstchunkid; chunkid -= chunkwidth) {

      // Stop if chunkcount is reached. We can't rely on the size of the encoders list because we may have skipped empty encoders
      if (!zeroChunkCount && (lastchunkid - chunkid) / chunkwidth >= chunkcount) {
        break;
      }

      GTSEncoder enc = chunks.get(chunkid);

      if (null == enc) {
        // If there is no encoder for this chunk, add an empty one if requested, or skip to next chunkid.
        if (keepempty) {
          enc = new GTSEncoder();
          enc.setMetadata(encoder.getMetadata());
          enc.getMetadata().putToLabels(chunklabel, Long.toString(chunkid));
        } else {
          continue;
        }
      } else {
        // Shrink encoder if it has more than 10% unused memory
        try {
          enc.writeTo(extractor);
          if (extractor.getCapacity() > 1.1 * enc.size()) {
            enc.resize(enc.size());
          }
        } catch (IOException ioe) {
          throw new WarpScriptException("Encountered an error while optimizing chunks.", ioe);
        }
      }

      encoders.add(enc);
    }

    // Reverse result list so chunk ids are in ascending order, consistent with chunk on GTSs.
    Collections.reverse(encoders);

    return encoders;
  }
  
  public static GeoTimeSerie fuse(Collection<GeoTimeSerie> chunks) throws WarpScriptException {
    //
    // Check if all chunks are of the same type
    //

    if (!chunks.isEmpty()) {

      TYPE type = null;

      int size = 0;

      for (GeoTimeSerie chunk: chunks) {
        // Set the type of the result from the type of the first chunk with
        // a defined type.
        if (null == type) {
          if (TYPE.UNDEFINED != chunk.type) {
            type = chunk.type;
          }
          continue;
        }
        if (0 != chunk.values && type != chunk.type) {
          throw new WarpScriptException("Inconsistent types for chunks to fuse.");
        }
        size += chunk.values;
      }

      //
      // Determine if we have compatible bucketization parameters.
      // bucketspan should be the same for all chunks and lastbucket should
      // be congruent to the same value modulo bucketspan
      //

      long lastbucket = Long.MIN_VALUE;
      long bucketspan = 0L;
      long firstbucket = Long.MAX_VALUE;

      for (GeoTimeSerie chunk: chunks) {
        // If one chunk is not bucketized, exit
        if (!isBucketized(chunk)) {
          bucketspan = 0L;
          break;
        }
        if (0L == bucketspan) {
          bucketspan = chunk.bucketspan;
          lastbucket = chunk.lastbucket;
          firstbucket = lastbucket - bucketspan * (chunk.bucketcount - 1);
        } else {
          // If bucketspan is not the same as the previous one, exit, result won't be bucketized
          if (bucketspan != chunk.bucketspan) {
            bucketspan = 0L;
            break;
          }
          // Check if lastbucket and chunk.lastbucket are congruent to the same value modulo the bucketspan, if not result is not bucketized
          if ((lastbucket % bucketspan) != (chunk.lastbucket % bucketspan)) {
            bucketspan = 0L;
            break;
          }
          // Update lastbucket and firstbucket
          if (chunk.lastbucket > lastbucket) {
            lastbucket = chunk.lastbucket;
          }
          if (chunk.lastbucket - (chunk.bucketcount - 1) * chunk.bucketspan < firstbucket) {
            firstbucket = chunk.lastbucket - (chunk.bucketcount - 1) * chunk.bucketspan;
          }
        }
      }

      int bucketcount = 0;

      if (0L == bucketspan) {
        lastbucket = 0L;
      } else {
        // Compute bucketcount
        bucketcount = (int) (1 + ((lastbucket - firstbucket) / bucketspan));
      }

      // Create the fused GTS
      GeoTimeSerie fused = new GeoTimeSerie(lastbucket, bucketcount, bucketspan, size);

      // Merge the datapoints and jointly determine class and labels

      String classname = null;
      boolean hasClass = false;
      Map<String, String> labels = null;

      for (GeoTimeSerie chunk: chunks) {

        if (null == classname) {
          classname = chunk.getMetadata().getName();
          hasClass = true;
        } else if (!classname.equals(chunk.getMetadata().getName())) {
          hasClass = false;
        }

        Map<String, String> chunklabels = chunk.getMetadata().getLabels();

        if (null == labels) {
          labels = new HashMap<String, String>();
          labels.putAll(chunklabels);
        } else {
          // Determine the common labels of chunks
          for (Entry<String, String> entry: chunklabels.entrySet()) {
            if (!entry.getValue().equals(labels.get(entry.getKey()))) {
              labels.remove(entry.getKey());
            }
          }
        }

        for (int i = 0; i < chunk.values; i++) {
          setValue(fused, GTSHelper.tickAtIndex(chunk, i), GTSHelper.locationAtIndex(chunk, i), GTSHelper.elevationAtIndex(chunk, i), GTSHelper.valueAtIndex(chunk, i), false);
        }
      }

      //
      // Set labels and class
      //

      if (hasClass) {
        fused.setName(classname);
      }

      fused.setLabels(labels);

      return fused;

    }

    return new GeoTimeSerie();
  }

  /**
   * Multiply the ticks of a GTS instance by a factor. Cannot be applied to a bucketized GTS instance as it can cause rounding errors.
   *
   * @param gts   The GTS instance whose ticks are to be scaled.
   * @param scale The factor to apply to the ticks.
   * @return A copy of the given GTS instance with its tick scaled.
   * @throws WarpScriptException If the GTS instance is bucketized.
   */
  public static GeoTimeSerie timescale(GeoTimeSerie gts, double scale) throws WarpScriptException {
    if (isBucketized(gts)) {
      throw new WarpScriptException("Cannot apply timescale on a bucketized GTS. Unbucketize it first.");
    }

    GeoTimeSerie scaled = gts.clone();

    for (int i = 0; i < scaled.values; i++) {
      scaled.ticks[i] = (long) (scaled.ticks[i] * scale);
    }

    if (scaled.sorted && scale < 0.0D) {
      scaled.reversed = !scaled.reversed;
    }

    return scaled;
  }
  
  /**
   * Determine if a GTS' values are normally distributed.
   * Works for numerical GTS only

   * @param gts GTS to check
   * @param buckets Number of buckets to distribute the values in
   * @param pcterror Maximum acceptable percentage of deviation from the mean of the bucket sizes
   * @param bessel Should we apply Bessel's correction when computing sigma
   * 
   * @return true if the GTS is normally distributed (no bucket over pcterror from the mean bucket size)
   */
  public static boolean isNormal(GeoTimeSerie gts, int buckets, double pcterror, boolean bessel) {
    //
    // Return true if GTS has no values
    //
    
    if (0 == gts.values) {
      return true;
    }
    
    //
    // Return false if GTS is not of type LONG or DOUBLE
    //
    
    if (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type) {
      return false;
    }
    
    double[] musigma = musigma(gts, bessel);
    double mu = musigma[0];
    double sigma = musigma[1];
    
    //
    // Constant GTS are not gaussian
    //
    
    if (0.0D == sigma) {
      return false;
    }
    
    //
    // Retrieve bounds
    //
    
    double[] bounds = SAXUtils.getBounds(buckets);
    
    int[] counts = new int[bounds.length + 1];
    
    //
    // Loop over the values, counting the number of occurrences in each interval
    //
    
    for (int i = 0; i < gts.values; i++) {
      double v = (((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() - mu) / sigma;
      
      int insertion = Arrays.binarySearch(bounds, v);
      
      //(-(insertion_point) - 1);
      
      if (insertion >= 0) {
        counts[insertion]++;
      } else {
        counts[-(1 + insertion)]++;
      }
    }
    
    //
    // Compute the mean bucket size
    //
    
    double mean = (double)gts.values / counts.length;

    //
    // Check that each bucket is with 'pcterror' of the mean
    //
    
    for (int count: counts) {
      if (Math.abs(1.0D - (count / mean)) > pcterror) {
        return false;
      }
    }
    
    return true;
  }
  
  public static double[] musigma(GeoTimeSerie gts, boolean bessel) {
    //
    // Compute mu and sigma
    //
    
    double sum = 0.0D;
    double sumsq = 0.0D;
    
    double[] musigma = new double[2];
    
    if (TYPE.DOUBLE == gts.type) {
      for (int i = 0; i < gts.values; i++) {
        sum += gts.doubleValues[i];
        sumsq += gts.doubleValues[i] * gts.doubleValues[i];
      }
    } else {
      for (int i = 0; i < gts.values; i++) {
        double v = (double) gts.longValues[i]; 
        sum += v;
        sumsq += v * v;
      }      
    }
    
    musigma[0] = sum / gts.values;
    double variance = (sumsq / gts.values) - (sum * sum / ((double) gts.values * (double) gts.values));
    
    if (bessel && gts.values > 1) {
      variance = variance * gts.values / ((double) gts.values - 1.0D);
    }
    
    musigma[1] = Math.sqrt(variance);
    
    return musigma;
  }
  
  public static GeoTimeSerie quantize(GeoTimeSerie gts, double[] bounds, Object[] values) throws WarpScriptException {
    
    // FIXME(hbs): we could support String types too
    if (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type) {
      throw new WarpScriptException("Can only quantify numeric Geo Time Series.");
    }
    
    GeoTimeSerie quantified = gts.cloneEmpty();
    
    //
    // Loop over the values, counting the number of occurrences in each interval
    //
    
    for (int i = 0; i < gts.values; i++) {
      double v = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
      
      int insertion = Arrays.binarySearch(bounds, v);
      
      //(-(insertion_point) - 1);
      
      if (null == values) {
        if (insertion >= 0) {
          GTSHelper.setValue(quantified, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), insertion, false);
        } else {
          GTSHelper.setValue(quantified, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), -(1 + insertion), false);
        }        
      } else {
        if (insertion >= 0) {
          GTSHelper.setValue(quantified, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), values[insertion], false);
        } else {
          GTSHelper.setValue(quantified, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), values[-(1 + insertion)], false);
        }
      }
    }

    return quantified;
  }
  
  /**
   * Return an array with a copy of the ticks of the given GTS.
   * 
   * @param gts GeoTime Serie instance to get the ticks from.
   * @return A copy of the array of ticks.
   */
  public static long[] getTicks(GeoTimeSerie gts) {
    return Arrays.copyOf(gts.ticks, gts.values);
  }
  
  /**
   * Return an array with a copy of the locations of the given GTS.
   * Allocation a new array if no location was specified in the GTS.
   * 
   * @param gts GeoTime Serie instance to get the locations from.
   * @return A copy of the array of locations.
   */
  public static long[] getLocations(GeoTimeSerie gts) {
    if (null != gts.locations) {
      return Arrays.copyOf(gts.locations, gts.values);
    } else {
      long[] locations = new long[gts.values];
      Arrays.fill(locations, GeoTimeSerie.NO_LOCATION);
      return locations;
    }
  }
  
  public static long[] getOriginalLocations(GeoTimeSerie gts) {
    return gts.locations;
  }
  
  public static long[] getOriginalElevations(GeoTimeSerie gts) {
    return gts.elevations;
  }

  /**
   * Return an array with a copy of the locations of the given GTS.
   * Allocation a new array if no location was specified in the GTS.
   * 
   * @param gts GeoTime Serie instance to get the elevations from.
   * @return A copy of the array of elevations.
   */
  public static long[] getElevations(GeoTimeSerie gts) {
    if (null != gts.elevations) {
      return Arrays.copyOf(gts.elevations, gts.values);
    } else {
      long[] elevations = new long[gts.values];
      Arrays.fill(elevations, GeoTimeSerie.NO_ELEVATION);
      return elevations;
    }
  }
  
  /**
   * Return an array with a copy of the GTS values as doubles
   * 
   * @param gts GeoTime Serie instance to get the doubles from.
   * @return A copy of the array of doubles values.
   * @throws WarpScriptException if the GeoTimeSerie instance does not contain doubles.
   */
  public static double[] getValuesAsDouble(GeoTimeSerie gts) throws WarpScriptException {
    if (TYPE.DOUBLE == gts.type) {
      return Arrays.copyOf(gts.doubleValues, gts.values);
    } else if (TYPE.LONG == gts.type) {
      double[] values = new double[gts.values];
      for (int i = 0; i < gts.values; i++) {
        values[i] = gts.longValues[i];
      }
      return values;
    } else {
      throw new WarpScriptException("Invalid Geo Time Series type.");
    }
  }

  /**
   * Transform a Metadata instance by 'intern'ing all of its strings.
   * 
   * @param meta Metadata to be internalized.
   */
  public static void internalizeStrings(Metadata meta) {
    String name = meta.getName();
    
    if (null != name) {
      meta.setName(name.intern());
    }

    //
    // The approach we take is to create a new instance of Map, filling
    // it with the interned keys and values and replacing labels/attributes.
    //
    // This approach is safe as we do not modify the map underlying the entrySet
    // we iterate over.
    // 
    // Modifying the original map while iterating over it is prone to throwing ConcurrentModificationException
    // in the case when a bin contains multiple nodes since we MUST do a remove then a put to intern both
    // the key AND the value.
    //
    // The ideal solution would be to have a modified HashMap which exposes an 'internalize' method which would
    // call intern for every String in keys and values
    //
    
    if (meta.getLabelsSize() > 0) {
      Map<String,String> newlabels = new HashMap<String, String>();
      for (Entry<String,String> entry: meta.getLabels().entrySet()) {
        String key = entry.getKey().intern();
        String value = entry.getValue().intern();
        newlabels.put(key, value);
      }
      meta.setLabels(newlabels);
    }
    
    if (meta.getAttributesSize() > 0) {
      Map<String,String> newattributes = new HashMap<String, String>();
      for (Entry<String,String> entry: meta.getAttributes().entrySet()) {
        String key = entry.getKey().intern();
        String value = entry.getValue().intern();
        newattributes.put(key, value);
      }
      meta.setAttributes(newattributes);
    }
  }
  
  /**
   * Compute local weighted regression at given tick
   * 
   * @param gts      : input GTS
   * @param idx      : considered as index of the first non-null neighbour at the right
   * @param tick     : tick at which lowess is achieved
   * @param q        : bandwitdth, i.e. number of nearest neighbours to consider
   * @param p        : degree of polynomial fit
   * @param weights  : optional array that store the weights
   * @param rho      : optional array that store the robustness weights
   * @param beta     : optional array that store the regression parameters
   * @param reversed : should idx be considered to be at the left instead
   * @return the local weighted regression at given tick.
   *
   * @throws WarpScriptException if array lengths are not coherent.
   */
  public static double pointwise_lowess(GeoTimeSerie gts, int idx, long tick, int q, int p, double[] weights, double[] rho, double[] beta, boolean reversed) throws WarpScriptException {
    
    if (null != weights && q > weights.length || (null != rho && gts.values > rho.length) || (null != beta && p + 1 > beta.length) ) {
      throw new WarpScriptException("Incoherent array lengths as input of pointwise_lowess");
    }
    /*
     * FIXME(JCV):
     * q = 3: 22 errors out of 100 points (at these points the value is almost equal to the value at q=2)
     * q = 4: 2 errors out of 100 points (at these points the value is almost equal to the value at q=3)
     * other q: 0 error
     * But it's not meant to be used with low value of q anyway.
     */
    
    //
    // Determine the 'q' closest values
    // We use two indices i and j for that, i moves forward, j moves backward from idx, until we
    // identified 'q' values (or 'n' if q > n)
    //
    
    int i = idx;
    int j = idx - 1;
    
    if (reversed) {
      i += 1;
      j += 1;
    }
    
    
    int count = 0;
    
    while(count < q) {
      long idist = Long.MAX_VALUE;
      long jdist = Long.MAX_VALUE;
      
      if (i < gts.values) {
        idist = Math.abs(tickAtIndex(gts, i) - tick);
      }
      
      if (j >= 0) {
        jdist = Math.abs(tickAtIndex(gts, j) - tick);
      }
      
      // If we exhausted the possible values
      if (Long.MAX_VALUE == idist && Long.MAX_VALUE == jdist) {
        break;
      }
      
      if (idist < jdist) {
        i++;
      } else {
        j--;
      }
      
      count++;
    }

    // The 'q' nearest values are between indices j and i (excluded)
    
    // Compute the maximum distance from 'tick'
    
    double maxdist = Math.max(j < -1 ? 0.0D : Math.abs(tickAtIndex(gts, j + 1) - tick), i <= 0 ? 0.0D : Math.abs(tickAtIndex(gts, i - 1) - tick));

    // Adjust maxdist if q > gtq.values
    
    if (q > gts.values) {
      maxdist = (maxdist * q) / gts.values;
    }
          
    // Compute the weights
    
    // Reset the weights array
    if (null == weights) {
      weights = new double[q];
    } else {
      Arrays.fill(weights, 0.0D);
    }
      
    int widx = 0;
    
    double wsum = 0.0D;
    
    for (int k = j + 1; k < i; k++) {
      if (0 == maxdist) {
        weights[widx] = 1.0D;
      } else {         
        double u = Math.abs(gts.ticks[k] - tick) / maxdist;
        
        if (u >= 1.0) {
          weights[widx] = 0.0D;
        } else {
          
          weights[widx] = 1.0D - u * u * u;
          
          double rho_ = 1.0D;
          if (null != rho) {
            // In some cases, "all rho are equal to 0.0", which should be translated to "all rho are equal" (or else there is no regression at all)
            // So if rho equals zero we set it to a certain value which is low enough not to bias the result in case all rho are not all equals.
            rho_ = 0.0D != rho[k] ? rho[k] : 0.000001D;
          }          
          weights[widx] = rho_ * weights[widx] * weights[widx] * weights[widx];
        }
      }
      wsum += weights[widx];
      widx++;
    }
    
    // Regression parameters
    if (null == beta) {
      beta = new double[p + 1];
    }

    //
    // Linear polynomial fit
    //
    
    if (1 == p){
      
      //
      // Compute weighted centroids for ticks and values
      //
      
      widx = 0;
      
      double ctick = 0.0D;
      double cvalue = 0.0D;
          
      for (int k = j + 1; k < i; k++) {
        ctick = ctick + weights[widx] * gts.ticks[k];
        cvalue = cvalue + weights[widx] * ((Number) valueAtIndex(gts, k)).doubleValue();
        widx++;
      }
      
      ctick = ctick / wsum;
      cvalue = cvalue / wsum;
      
      //
      // Compute weighted covariance and variance
      //
      
      double covar = 0.0D;
      double var = 0.0D;
      
      widx = 0;
      
      for (int k = j + 1; k < i; k++) {
        covar = covar + weights[widx] * (gts.ticks[k] - ctick) * (((Number) valueAtIndex(gts, k)).doubleValue() - cvalue);
        var = var + weights[widx] * (gts.ticks[k] - ctick) * (gts.ticks[k] - ctick);
        widx++;
      }
      
      covar = covar / wsum;
      var = var / wsum;
      
      //
      // Compute regression parameters
      //
      
      beta[1] = 0 == var ? 0.0D : covar / var;
      beta[0] = cvalue - ctick * beta[1];
      
    } else {
    
    //
    // Quadratic-or-more polynomial fit
    //
      
      // filling the container with the points
      
      List<WeightedObservedPoint> observations = new ArrayList<WeightedObservedPoint>();
      
      widx = 0;
      for (int k = j + 1; k < i; k++) {
        WeightedObservedPoint point = new WeightedObservedPoint(weights[widx], (double) gts.ticks[k], ((Number) valueAtIndex(gts, k)).doubleValue());
        observations.add(point);
        widx++;
      }
      
      PolynomialCurveFitter fitter = PolynomialCurveFitter.create(p);
      beta = fitter.fit(observations);
      observations.clear();
    
    }
    
    //
    // Compute value at 'tick'
    //
    
    double estimated = beta[0];
    double tmp = 1.0D;
    for (int u = 1; u < p + 1; u++){
      tmp *= tick;
      estimated += tmp * beta[u];  
    }
    
    return estimated;
  }
  
  public static double pointwise_lowess(GeoTimeSerie gts, int idx, long tick, int q, int p, double[] weights, double[] rho, double[] beta) throws WarpScriptException {
    return pointwise_lowess(gts, idx, tick, q, p, weights, rho, beta, false);
  }
  
  /**
   * Compute fast and robust version of LOWESS on a Geo Time Series,
   * with a polynomial fit of degree p > 0.
   * 
   * @see <a href="https://pdfs.semanticscholar.org/414e/5d1f5a75e2327d99b5bbb93f2e4e241c5acc.pdf">Robust Locally Weighted Regression and Smoothing Scatterplots</a>
   * 
   * @param gts Input GTS
   * @param q   Bandwith, i.e. number of nearest neighbours to consider when applying LOWESS
   * @param r   Robustness, i.e. number of robustifying iterations
   * @param d   Delta in µs, i.e. acceptable neighbourhood radius within which LOWESS is not recomputed
   *            close points are approximated by polynomial interpolation, d should remain < 0.1*(lasttick-firstick) in most cases
   * @param p   Degree, i.e. degree of the polynomial fit
   *            best usage p=1 or p=2 ; it will likely return an overflow error if p is too big
   * 
   * @param weights  : optional array that store the weights
   * @param rho      : optional array that store the robustness weights
   * @param inplace  : should the gts returned be the same object than the input
   * 
   * @return a smoothed GTS
   * @throws WarpScriptException if parameters are incorrect.
   */
  public static GeoTimeSerie rlowess(GeoTimeSerie gts, int q, int r, long d, int p, double[] weights, double[] rho, boolean inplace) throws WarpScriptException {
    if (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type) {
      throw new WarpScriptException("Can only smooth numeric Geo Time Series.");
    }
    
    if (q < 1) {
      throw new WarpScriptException("Bandwidth parameter must be greater than 0");
    }
    
    if (r < 0) {
      throw new WarpScriptException("Robustness parameter must be greater or equal to 0");
    }
    
    if (d < 0) {
      throw new WarpScriptException("Delta parameter must be greater or equal to 0");
    }
    
    if (p < 1) {
      throw new WarpScriptException("Degree of polynomial fit must be greater than 0");
    }
    
    if (p > 9) {
      throw new WarpScriptException("Degree of polynomial fit should remain small (lower than 10)");
    }
    
    //
    // Sort the ticks
    //
    
    sort(gts, false);
    
    //
    // Check if number of missing values is reasonable
    // Note that (bucketcount - values) is not the number of missing values but a minorant
    //
    
    if (gts.bucketcount - gts.values > 500000) {
      throw new WarpScriptException("More than 500000 missing values");
    }    
        
    //
    // Check for duplicate ticks
    //
    
    long previous = -1;
    for (int t = 0; t < gts.values; t++) {
      long current = gts.ticks[t];
      if (previous == current){
        throw new WarpScriptException("Can't be applied on GTS with duplicate ticks");
      }
      previous = current;
    }
    
    //
    // At each step of robustifying operations,
    // we compute values in transient_smoothed using updated weights rho * weights
    //
    
    int size = isBucketized(gts) ? gts.bucketcount : gts.values;
    int sizehint = Math.max(gts.sizehint, Math.round(1.1f * size));
    double[] transient_smoothed = new double[sizehint];
    
    int nvalues = q < size ? q : size;
    
    // Allocate an array for weights
    if (null == weights) {
      weights = new double[nvalues];
    } else {
      if (weights.length < nvalues) {
        throw new WarpScriptException("in rlowess weights array too small");
      }
    }
    
    // Allocate an array to update the weights through the robustness iterations and another for the absolute of the residuals
    // The weights used will be rho*weights
    double[] residual;
    if (r > 0){
      if (null == rho) {
        rho = new double[gts.values];
        Arrays.fill(rho, 1.0D);
      } else {
        if (rho.length < nvalues) {
          throw new WarpScriptException("in rlowess rho array too small");
        }
      }
      residual = new double[gts.values];
    } else {
      residual = null;
    }
    
    // Regression parameters
    double[] beta = new double[p + 1];
    
    //
    // Robustifying iterations
    //

    int r_iter = 0;
    while (r_iter < r + 1) {
      
      //
      // In order to speed up the computations,
      // we will skip some points pointed by iter.
      // We use iter_follower to interpolate the skipped points afterward.
      //
      
      Iterator<Long> iter = tickIterator(gts, false);
      Iterator<Long> iter_follower;
      if (0.0 == d){
        iter_follower = null;
      } else {
        iter_follower = tickIterator(gts,false);
      }
      
      // Index in the ticks/values array of the input gts
      int idx = 0;
      
      // Index in the ticks/values array of the output gts (values are also estimated for null points of the input)
      int ridx = 0;
      int ridx_last = 0;
      
      // Last tick estimated (set to -d-1 so (tick-last)>d at first iter) and its index in the result
      long last = d * (-1) - 1;
      int idx_last = 0;
      
      //
      // When we find a tick that is not within distance d of the last estimated tick,
      // then either we estimate it,
      // or if at least one tick has been skipped just before,
      // then we estimate the last skipped one and interpolate the others.
      // We then take back the loop from the former last skipped tick.
      //
      
      long last_skipped = 0;
      boolean skip = false;
      
      // Have skipped ticks been interpolated in last loop ?
      boolean resolved = false;
      
      // Current tick
      long tick = 0;
      
      while(iter.hasNext() || resolved) {
        
        if (!resolved) {
          tick = iter.next();
        } else {
          resolved = false;
        }
        
        // Skip points that are too close from the previous estimated one, unless its the last
        if (iter.hasNext() && (tick - last <= d)) {
          
          last_skipped = tick;
          skip = true;
          ridx++;
          
        } else {
          
          if (!skip) {
            
            // advance idx to the first neighbour at the right whose value is not null
            while(idx < gts.values - 1 && tick > tickAtIndex(gts, idx)) {
              idx++;
            }
            
            // compute value at tick
            transient_smoothed[ridx] = pointwise_lowess(gts, idx, tick, nvalues, p, weights, rho, beta);
            
            // update residual if tick had a non-null value
            if (r_iter < r && tick == tickAtIndex(gts, idx)) {
              residual[idx] = Math.abs(((Number) valueAtIndex(gts,idx)).doubleValue()-transient_smoothed[ridx]);
            }
            
            if (null != iter_follower) {
              iter_follower.next();
              last = tick;
              idx_last = idx;
              ridx_last = ridx;
            }
            ridx++;
            
          } else {
            
            if (!iter.hasNext() && (tick - last <= d)) {
              last_skipped = tick;
              ridx++;
            }
            
            // advance idx to the first neighbour at the right whose value is not null
            while(idx < gts.values - 1 && last_skipped > tickAtIndex(gts, idx)) {
              idx++;
            }
            
            // compute value at last_skipped tick
            transient_smoothed[ridx - 1] = pointwise_lowess(gts, idx, last_skipped, nvalues, p, weights, rho, beta);
            
            // update residual if tick had a non-null value
            if (r_iter < r && last_skipped == tickAtIndex(gts, idx)) {
              residual[idx] = Math.abs(((Number) valueAtIndex(gts,idx)).doubleValue()-transient_smoothed[ridx - 1]);
            }

            //
            // Linear interpolation of skipped points
            //
            
            double denom = last_skipped - last;
            long skipped = iter_follower.next();
            int ridx_s = ridx_last + 1;
            while (last_skipped > skipped) {
              
              // interpolate
              double alpha = (skipped - last) / denom;
              transient_smoothed[ridx_s] = alpha * transient_smoothed[ridx - 1] + (1 - alpha) * transient_smoothed[ridx_last];
              
              // update residual if tick had a non-null value
              int sidx;
              if (r_iter < r && 0 < (sidx = Arrays.binarySearch(gts.ticks, idx_last, idx, skipped))) {
                residual[sidx] = Math.abs(((Number) valueAtIndex(gts,sidx)).doubleValue()-transient_smoothed[ridx_s]);
              }
              
              skipped = iter_follower.next();
              ridx_s++;
              
            }
            
            if (iter.hasNext() || (tick - last > d)) {
              //updates
              skip = false;
              resolved = true;
              last = last_skipped;
              idx_last = idx;
              ridx_last = ridx - 1;
            }
            
          }
        }
      }

      //
      // Update robustifying weights (except last time or if r is 0)
      //

      if (r_iter < r) {
        
        // rho's values will be recomputed anyway so sorted can borrow its body
        double[] sorted = rho;
        sorted = Arrays.copyOf(residual, gts.values);
        Arrays.sort(sorted);
        
        // compute median of abs(residual)
        double median;
        if (gts.values % 2 == 0) {
          median = (sorted[gts.values/2] + sorted[gts.values/2 - 1])/2;
        } else {
          median = sorted[gts.values/2];
        }
        
        // compute h = 6 * median and rho = bisquare(|residual|/h)
        double h = 6 * median;
        
        for (int k = 0; k < gts.values; k++) {
          if (0 == h){
            rho[k] = 1.0D;
          } else{
            double u = residual[k] / h;
            
            if (u >= 1.0) {
              rho[k] = 0.0D;
            } else {
              rho[k] = 1.0D - u * u;
              rho[k] = rho[k] * rho[k];
            }
          }
        }
      }
            
      r_iter++;
    }
    
    //
    // Copying result to output
    //
    
    boolean hasLocations = null != gts.locations;
    boolean hasElevations = null != gts.elevations;
    
    //
    // We separate case without or with missing values.
    //
    
    if (!isBucketized(gts) || (gts.values == gts.bucketcount && gts.lastbucket == gts.ticks[gts.values - 1] && gts.lastbucket - gts.bucketspan * (gts.bucketcount - 1) == gts.ticks[0])) {
      if (inplace) {

        if (TYPE.LONG == gts.type) {
          gts.longValues = null;
          gts.type = TYPE.DOUBLE;
        }

        gts.doubleValues = transient_smoothed;
        return gts;
        
      } else {
        
        GeoTimeSerie smoothed = gts.cloneEmpty(sizehint);
        try {
          smoothed.reset(Arrays.copyOf(gts.ticks,sizehint), hasLocations ? Arrays.copyOf(gts.locations,sizehint) : null, hasElevations ? Arrays.copyOf(gts.elevations,sizehint) : null, transient_smoothed, size);
        } catch (IOException ioe) {
          throw new WarpScriptException("IOException in reset method.", ioe);
        }
        
        return smoothed;
        
      }
    } else {
      
      //
      // Case with missing values
      //
      
      if (inplace) {
        
        // TODO(JCV): need to unit test location and elevation settings in this case
        
        if (hasLocations && gts.locations.length != sizehint) {
          gts.locations = Arrays.copyOf(gts.locations, sizehint);
        }
        
        if (hasElevations && gts.elevations.length != sizehint) {
          gts.elevations = Arrays.copyOf(gts.elevations, sizehint);
        }
        
        if (gts.ticks.length != sizehint) {
          gts.ticks = Arrays.copyOf(gts.ticks, sizehint);
        }
        
        // We try to allocate the lesser additional memory so we
        // fill locations and elevations backward with values upfront in the same array
        
        if (hasLocations || hasElevations) {
          
          Iterator<Long> iter = tickIterator(gts, true);
          int idx = gts.values - 1;
          
          int idx_new = gts.bucketcount;
          while (iter.hasNext()) {
            
            long tick = iter.next();
            idx_new--;
            
            // search if tick was present in original gts
            while (idx > 0 && tick < gts.ticks[idx]) {
              idx--;
            }
          
            if (hasLocations) {
              gts.locations[idx_new] = tick == gts.ticks[idx] ? gts.locations[idx] : GeoTimeSerie.NO_LOCATION;
            }
            
            if (hasElevations) {
              gts.elevations[idx_new] = tick == gts.ticks[idx] ? gts.locations[idx] : GeoTimeSerie.NO_ELEVATION;
            }
            
            gts.ticks[idx_new] = tick;
            
          }
        }

        if (TYPE.LONG == gts.type) {
          gts.longValues = null;
          gts.type = TYPE.DOUBLE;
        }
        gts.doubleValues = transient_smoothed;
        gts.values = size;
        return gts;
        
      } else {
        
        GeoTimeSerie smoothed = gts.cloneEmpty(sizehint);
        
        // ticks
        long[] ticks = new long[sizehint];
        int idx = 0;
        Iterator<Long> iter = tickIterator(gts, false);
        while (iter.hasNext()){
          ticks[idx] = iter.next();
          idx++;
        }
        
        // locations
        int v = 0;
        long[] locations = null;
        if (hasLocations) {
          locations = new long[sizehint];
          for (int u = 0; u < size; u++) {
            v = Arrays.binarySearch(gts.ticks, v, gts.values, smoothed.ticks[u]);
            locations[u] = v < 0 ? GeoTimeSerie.NO_LOCATION : gts.locations[v];
          }
        }
        
        // elevations
        v = 0;
        long[] elevations = null;
        if (hasElevations) {
          elevations = new long[sizehint];
          for (int u = 0; u < size; u++) {
            v = Arrays.binarySearch(gts.ticks, v, gts.values, smoothed.ticks[u]);
            elevations[u] = v < 0 ? GeoTimeSerie.NO_ELEVATION : gts.elevations[v];
          }
        }
        
        try {
          smoothed.reset(ticks, locations, elevations, transient_smoothed, size);
        } catch (IOException ioe) {
          throw new WarpScriptException("IOException in reset method.", ioe);
        }
        
        return smoothed;
        
      }
    }
  }

  public static GeoTimeSerie rlowess(GeoTimeSerie gts, int q, int r, long d, int p) throws WarpScriptException {
    return rlowess(gts, q, r, d, p, null, null, false);
  }
  
  /**
   * Version of LOWESS used in stl
   * 
   * @param fromGTS    : GTS from which data used for estimation is taken
   * @param toGTS      : GTS where results are saved 
   * @param neighbours : number of nearest neighbours to take into account when computing lowess (bandwitdh)
   * @param degree     : degree of polynomial fit in lowess
   * @param jump       : number of bucket to skip when computing lowess (to speed it up)
   * @param weights    : optional array that store the weights
   * @param rho        : optional array that store the robustness weights
   */
  public static void lowess_stl(GeoTimeSerie fromGTS, GeoTimeSerie toGTS, int neighbours, int degree, int jump, double[] weights, double[] rho) throws WarpScriptException {
    if (!isBucketized(fromGTS)) {
      throw new WarpScriptException("lowess_stl method works with bucketized gts only");
    }
    
    if (fromGTS == toGTS) {
      throw new WarpScriptException("in lowess_stl method, fromGTS and toGTS can't be the same object. Please consider using rlowess method instead");
    }
    
    sort(fromGTS);

    // if neighbours < 0, do not do lowess smoothing but instead take the mean
    if (neighbours < 0) {
      double mean = GTSHelper.musigma(fromGTS, false)[0];
      for (int j = 0; j < fromGTS.bucketcount; j ++) {
        long tick = fromGTS.lastbucket - j * fromGTS.bucketspan;
        setValue(toGTS, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, mean, true);
      }

    } else {

      // estimate all points but skip jump_s points between each
      // (we are starting at lastbucket and are going backward)
      int idx = fromGTS.values - 1;

      // we want to end on the oldest bucket
      int rest = (fromGTS.bucketcount - 1) % (jump + 1);
      for (int j = 0; j <= (fromGTS.bucketcount - 1) / (jump + 1); j++) {

        // calculate tick
        long tick = fromGTS.lastbucket - (j * (jump + 1) + rest) * fromGTS.bucketspan;

        // take back idx to the first neighbour at the left whose value is not null
        while (idx > -1 && tick < tickAtIndex(fromGTS, idx)) {
          idx--;
        }

        // estimate value
        double estimated = pointwise_lowess(fromGTS, idx, tick, neighbours, degree, weights, rho, null, true);
        setValue(toGTS, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, estimated, true);

      }

      // interpolate skipped points
      for (int j = 0; j < (fromGTS.bucketcount - 1) / (jump + 1); j++) {

        int right = j * (jump + 1) + rest;
        int left = (j + 1) * (jump + 1) + rest;
        double denom = left - right;
        long righttick = fromGTS.lastbucket - right * fromGTS.bucketspan;
        long lefttick = fromGTS.lastbucket - left * fromGTS.bucketspan;

        for (int r = 1; r < jump + 1; r++) {

          int middle = r + j * (jump + 1) + rest;
          long tick = fromGTS.lastbucket - middle * fromGTS.bucketspan;

          double alpha = (middle - right) / denom;
          double interpolated = alpha * ((Number) valueAtTick(toGTS, lefttick)).doubleValue() + (1 - alpha) * ((Number) valueAtTick(toGTS, righttick)).doubleValue();
          setValue(toGTS, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, interpolated, true);

        }
      }

      // estimate the most recent point in case it has been jumped
      if (0 != rest) {

        // take back idx to the first neighbour at the left whose value is not null if any
        idx = fromGTS.values - 1;
        while (idx > -1 && fromGTS.lastbucket < tickAtIndex(fromGTS, idx)) {
          idx--;
        }

        // estimate value
        double estimated = pointwise_lowess(fromGTS, idx, fromGTS.lastbucket, neighbours, degree, weights, rho, null, true);
        setValue(toGTS, fromGTS.lastbucket, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, estimated, true);

        // interpolate skipped points
        int right = 0;
        int left = rest;
        double denom = left - right;
        long lefttick = fromGTS.lastbucket - left * fromGTS.bucketspan;

        for (int r = 1; r < rest; r++) {
          long tick = fromGTS.lastbucket - r * fromGTS.bucketspan;

          double alpha = (r - right) / denom;
          double interpolated = alpha * ((Number) valueAtTick(toGTS, lefttick)).doubleValue() + (1 - alpha) * estimated;
          setValue(toGTS, tick, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, interpolated, true);
        }
      }
    }
  }
  
  /**
   * Compute STL i.e. Seasonal-Trend decomposition procedure based on LOWESS
   * @see <a href="http://www.wessa.net/download/stl.pdf">STL: A Seasonal-Trend Decomposition Procedure Based on Loess</a>
   * 
   * Global parameters:
   * @param gts                : Input GTS, must be bucketized
   * @param buckets_per_period : number of buckets for one period of the seasonality
   * @param inner Precision    : number of inner loops (to make the decomposition)
   * @param outer Robustness   : number of outer loops (to alleviate the impact of outliers upon the decomposition)
   *
   * Optional sets of parameters shared by call of lowess of the same kind:
   * @param neighbour_s        : (for the seasonal extracting step) Bandwidth, i.e. number of nearest neighbours to consider when applying LOWESS. If negative, approximation by the mean is used instead of LOWESS.
   * @param degree_s           : (for the seasonal extracting step) Degree, i.e. degree of the polynomial fit
   * @param jump_s             : (for the seasonal extracting step) Jump, i.e. number of bucket to skip to speed up computation. These buckets are interpolated afterward.
   *
   * @param neighbour_l        : (for the low frequency filtering step) Bandwidth, i.e. number of nearest neighbours to consider when applying LOWESS
   * @param degree_l           : (for the low frequency filtering step) Degree, i.e. degree of the polynomial fit
   * @param jump_l             : (for the low frequency filtering step) Jump, i.e. number of bucket to skip to speed up computation. These buckets are interpolated afterward.
   *
   * @param neighbour_t        : (for the trend extracting step) Bandwidth, i.e. number of nearest neighbours to consider when applying LOWESS
   * @param degree_t           : (for the trend extracting step) Degree, i.e. degree of the polynomial fit
   * @param jump_t             : (for the trend extracting step) Jump, i.e. number of bucket to skip to speed up computation. These buckets are interpolated afterward.
   *
   * @param neighbour_p        : (for the post seasonal smoothing step) Bandwidth, i.e. number of nearest neighbours to consider when applying LOWESS
   * @param degree_p           : (for the post seasonal smoothing step) Degree, i.e. degree of the polynomial fit
   * @param jump_p             : (for the post seasonal smoothing step) Jump, i.e. number of bucket to skip to speed up computation. These buckets are interpolated afterward.
   * 
   * @return a list of 2 GTS consisting of the seasonal and trend part of the decomposition.
   */
  public static List<GeoTimeSerie> stl(
      GeoTimeSerie gts,
      
      // big picture STL parameters
      int buckets_per_period,
      int inner,
      int outer,
      
      // lowess parameters for seasonal extract
      int neighbour_s,
      int degree_s,
      int jump_s,
            
      // lowess parameters for low-pass filtering
      int neighbour_l,
      int degree_l,
      int jump_l,
      
      // lowess parameters for trend extract
      int neighbour_t,
      int degree_t,
      int jump_t,
      
      // lowess parameters for seasonal post-smoothing
      int neighbour_p,
      int degree_p,
      int jump_p
            
      ) throws WarpScriptException {
    if (TYPE.DOUBLE != gts.type && TYPE.LONG != gts.type) {
      throw new WarpScriptException("Can only be applied on numeric Geo Time Series.");
    }
    
    if (!isBucketized(gts)) {
      throw new WarpScriptException("Can only be applied on bucketized Geo Time Series");
    }
    
    //
    // Sort the GTS
    //
    
    sort(gts, false);
    
    //
    // Allocate tables: Y = S + T + R
    // Y: initial GTS
    // S: seasonal
    // T: trend
    // R: residual
    //
    
    int nonnull = gts.values;
    int size = gts.bucketcount;
    
    // limit size at the moment
    if (size - nonnull > 500000) {
      throw new WarpScriptException("More than 500000 missing values");
    }
        
    // At each iteration of inner loop, seasonal will be augmented of 2*bpp buckets.
    // The current implementation fill every gap in seasonal with a value estimated by lowess.
    // Hence sizehint choice.
    // TODO(JCV): maybe rethink STL so that it can handle missing values better.
    int sizehint = size + 2 * buckets_per_period;
    GeoTimeSerie seasonal = gts.cloneEmpty(sizehint);
    try {
      seasonal.reset(Arrays.copyOf(gts.ticks,sizehint), null, null, new double[sizehint], nonnull);
    } catch (IOException ioe) {
      throw new WarpScriptException("IOException in reset method.", ioe);
    }
    
    
    GeoTimeSerie trend = gts.cloneEmpty(sizehint);
    try {
      trend.reset(Arrays.copyOf(gts.ticks,sizehint), null, null, new double[sizehint], nonnull);
    } catch (IOException ioe) {
      throw new WarpScriptException("IOException in reset method.", ioe);
    }
    
    // lowpassed will borrow the body of trend in step 3
    GeoTimeSerie lowpassed = trend;
    
    // Allocate an array to update the weights through the outer loop
    // The weights used will be rho*weights
    double[] rho = new double[nonnull];
    Arrays.fill(rho, 1.0D);
    
    // residual will borrow body of rho
    double[] residual = rho;
    
    // Weights used in lowess computations
    int nei = Math.max(Math.max(neighbour_s, neighbour_l), neighbour_t);
    double[] weights = new double[nei];
    // FIXME(JCV): maybe smarter size to put in here ?
    
    //
    // Outer loop
    //
    
    for (int s = 0; s < outer + 1; s++) {
      
      //
      // Inner loop
      //
      
      for (int k = 0 ; k < inner; k++) {
        
        //
        // Step 1: Detrending
        //
        
        int idx_t = 0;
        for (int idx = 0 ; idx < nonnull; idx++){
          idx_t = Arrays.binarySearch(trend.ticks, idx_t, nonnull, gts.ticks[idx]);
          seasonal.doubleValues[idx] = ((Number) valueAtIndex(gts,idx)).doubleValue() - trend.doubleValues[idx_t];
        }
        
        //
        // Step 2: Cycle-subseries smoothing
        //
        
        GeoTimeSerie subCycle = null;
        GeoTimeSerie subRho = null;
        
        // smoothing of each cycle-subserie
        for (int c = 0; c < buckets_per_period; c++ ) {
          
          // extracting a cycle-subserie and extending by one value at both ends
          subCycle = subCycleSerie(seasonal, seasonal.lastbucket - c * seasonal.bucketspan, buckets_per_period, true, subCycle);
          subCycle.lastbucket += subCycle.bucketspan;
          subCycle.bucketcount += 2;
          
          // extracting subRho
          if (s > 0) {
            double[] tmp = seasonal.doubleValues;
            seasonal.doubleValues = rho;
            subRho = subCycleSerie(seasonal, seasonal.lastbucket - c * seasonal.bucketspan, buckets_per_period, true, subRho);
            seasonal.doubleValues = tmp;
          }
          
          // applying lowess
          lowess_stl(subCycle, seasonal, neighbour_s, degree_s, jump_s, weights, s > 0 ? subRho.doubleValues : rho);
        }
        
        /*
         * Concretely, with the added points, seasonal.lastbucket is 1 period behind the reality,
         * and it has twice bpp more buckets than its bucketcount.
         */
        
        // Updating bucket parameters of seasonal for clarity
        seasonal.lastbucket += seasonal.bucketspan * buckets_per_period;
        seasonal.bucketcount += 2 * buckets_per_period;        
        
        //
        // Step 3: Low-Pass Filtering of Smoothed Cycle-subseries
        //

        sort(seasonal);
        
        // FIXME(JCV): what happens if buckets_per_period < bucketcount / buckets_per_period ?
        
        // Applying first moving average of size bpp
        
        // First average
        double sum = 0;
        for (int r = 0; r < buckets_per_period; r++) {
          sum += seasonal.doubleValues[r];
        }
        lowpassed.doubleValues[0] = sum / buckets_per_period;
        
        // other averages
        for (int r = 1; r < seasonal.bucketcount - buckets_per_period + 1; r++) {
          sum += seasonal.doubleValues[r + buckets_per_period - 1] - seasonal.doubleValues[r - 1];
          lowpassed.doubleValues[r] = sum / buckets_per_period;
        }
        
        // Applying 2nd moving average of size bpp
        sum = 0;
        for (int r = 0; r < buckets_per_period; r++) {
          sum += lowpassed.doubleValues[r];
        }
        double tmp = lowpassed.doubleValues[0];
        lowpassed.doubleValues[0] = sum / buckets_per_period;
        for (int r = 1; r <= seasonal.bucketcount - 2 * buckets_per_period + 1; r++) {
          sum += lowpassed.doubleValues[r + buckets_per_period - 1] - tmp;
          tmp = lowpassed.doubleValues[r];
          lowpassed.doubleValues[r] = sum / buckets_per_period;
        }
        
        // Applying 3rd moving average of size 3
        for (int r = 0; r < seasonal.bucketcount - 2 * buckets_per_period; r++) {
          lowpassed.doubleValues[r] += lowpassed.doubleValues[r + 1] + lowpassed.doubleValues[r + 2];
          lowpassed.doubleValues[r] /= 3;
        }
        
        // Update size of gts
        lowpassed.bucketcount = seasonal.bucketcount - 2 * buckets_per_period;
        lowpassed.lastbucket = seasonal.lastbucket - buckets_per_period * seasonal.bucketspan;
        lowpassed.values = lowpassed.bucketcount;

        // Lowess_l
        lowpassed = rlowess(lowpassed, neighbour_l, 0, (jump_l + 1) * lowpassed.bucketspan, degree_l, weights, null, true);

        //
        // Step 4: Detrending of Smoothed Cycle-Subseries
        //
        
        // shifting seasonal bucket parameters back
        seasonal.lastbucket -= seasonal.bucketspan * buckets_per_period;
        seasonal.bucketcount -= 2 * buckets_per_period;        
        
        if (seasonal.bucketcount != lowpassed.values) {
          throw new WarpScriptException("stl impl error #1: " + seasonal.values + " vs " + lowpassed.values);
        }
        
        for (int r = 0; r < seasonal.bucketcount; r++) {
          seasonal.doubleValues[r] = seasonal.doubleValues[r + buckets_per_period] - lowpassed.doubleValues[r];
          seasonal.ticks[r] = seasonal.ticks[r + buckets_per_period];
        }
        
        // trim seasonal back
        seasonal.values = seasonal.bucketcount;

        //
        // Step 5: Deseasonalizing
        //
        
        int idx_s = 0;
        for (int idx = 0 ; idx < nonnull; idx++){
          idx_s = Arrays.binarySearch(seasonal.ticks, idx_s, nonnull, gts.ticks[idx]);
          trend.doubleValues[idx] = ((Number) valueAtIndex(gts,idx)).doubleValue() - seasonal.doubleValues[idx_s];
        }
        
        trend.values = nonnull;
        trend.lastbucket = gts.lastbucket;
        trend.bucketspan = gts.bucketspan;
        trend.bucketcount = size;

        //
        // Step 6: Trend smoothing
        //
        
        // FIXME(JCV): currently all buckets are estimated or interpolated, but it is not necessary to estimate ones with no value unless it is the last iteration
        // But won't gain that much in speed if enough points are already interpolated.
        
        trend = rlowess(trend, neighbour_t, 0, (jump_t + 1) * trend.bucketspan, degree_t, weights, rho, true);
      }
      
      //
      // Robustifying operations of outer loop (except last time)
      //

      if (s < outer) {
        
        // compute residual
        int idx_s = 0;
        int idx_t = 0;
        for (int idx = 0 ; idx < nonnull; idx++){
          idx_s = Arrays.binarySearch(seasonal.ticks, idx_s, nonnull, gts.ticks[idx]);
          //idx_t = Arrays.binarySearch(trend.ticks, idx_t, nonnull, gts.ticks[idx]);
          // we assume idx_t == idx_s
          idx_t = idx_s;
          residual[idx] = Math.abs(((Number) valueAtIndex(gts,idx)).doubleValue() - seasonal.doubleValues[idx_s] - trend.doubleValues[idx_t]);
        }
        
        //
        // Update robustifying weights 
        //
                
        // compute median of abs(residual)
        double median;
        double[] sorted = Arrays.copyOf(residual, gts.values);
        Arrays.sort(sorted);
        if (gts.values % 2 == 0) {
          median = (sorted[gts.values/2] + sorted[gts.values/2 - 1])/2;
        } else {
          median = sorted[gts.values/2];
        }
        
        // compute h = 6 * median and rho = bisquare(|residual|/h)
        double h = 6 * median;
        
        for (int k = 0; k < gts.values; k++) {
          if (0 == h){
            rho[k] = 1.0D;
          } else{
            double u = residual[k] / h;
            
            if (u >= 1.0) {
              rho[k] = 0.0D;
            } else {
              rho[k] = 1.0D - u * u;
              rho[k] = rho[k] * rho[k];
            }
          }
        }
      }
    }
    
    //
    // Post seasonal smoothing
    //
    
    if (neighbour_p > 0) {
      seasonal = rlowess(seasonal, neighbour_p, 0, (jump_p + 1) * seasonal.bucketspan, degree_p);
    }
    
    //
    // Locations and elevations
    //
    
    int v = 0;
    if (null != gts.locations) {
      for (int u = 0; u < size; u++) {
        v = Arrays.binarySearch(gts.ticks, v, nonnull, seasonal.ticks[u]);
        seasonal.locations[u] = v < 0 ? GeoTimeSerie.NO_LOCATION : gts.locations[v];
        trend.locations[u] = seasonal.locations[u];
      }
    } else {
      seasonal.locations = null;
      trend.locations = null;
    }
    
    v = 0;
    if (null != gts.elevations) {
      for (int u = 0; u < size; u++) {
        v = Arrays.binarySearch(gts.ticks, v, nonnull, seasonal.ticks[u]);
        seasonal.elevations[u] = v < 0 ? GeoTimeSerie.NO_ELEVATION : gts.elevations[v];
        trend.elevations[u] = seasonal.elevations[u];
      }
    } else {
      seasonal.elevations = null;
      trend.elevations = null;
    }    
    
    //
    // Output
    //
    
    String prefix = (null == gts.getName()) || (0 == gts.getName().length()) ? "" : gts.getName() + "_";
    seasonal.setName(prefix + "seasonal");
    trend.setName(prefix + "trend");
    
    List<GeoTimeSerie> output = new ArrayList<GeoTimeSerie>();
    output.add(seasonal);
    output.add(trend);
    
    return output;
  }

  /**
   * Copy the specified part of a GTS to an other specified part of GTS.
   *
   * @param src The source GTS
   * @param srcPos The starting index in src
   * @param dest The destination GTS
   * @param destPos The starting index in dest
   * @param length The number of points to copy
   */
  public static void copy(GeoTimeSerie src, int srcPos, GeoTimeSerie dest, int destPos, int length) {
    if (!GeoTimeSerie.TYPE.UNDEFINED.equals(dest.type) && !dest.type.equals(src.type)) {
      throw new RuntimeException("Combine cannot proceed with incompatible GTS types.");
    }

    // Make sure dest is not UNDEFINED
    dest.type = src.type;

    // Make sure all receiving arrays are initialized and big enough
    int destMinLength = destPos + length;

    if(null == dest.ticks){ // dest is empty
      dest.ticks = new long[destMinLength];

      if (GeoTimeSerie.TYPE.LONG == dest.type) {
        dest.longValues = new long[destMinLength];
      } else if (GeoTimeSerie.TYPE.DOUBLE == dest.type) {
        dest.doubleValues = new double[destMinLength];
      } else if (GeoTimeSerie.TYPE.STRING == dest.type) {
        dest.stringValues = new String[destMinLength];
      } else { // TYPE.BOOLEAN == dest.type
        dest.booleanValues = new BitSet();
      }
    } else if(dest.ticks.length < destMinLength){ // dest is too small to contain new data
      dest.ticks = Arrays.copyOf(dest.ticks, destMinLength);

      if (GeoTimeSerie.TYPE.LONG == dest.type) {
        dest.longValues = Arrays.copyOf(dest.longValues, destMinLength);
      } else if (GeoTimeSerie.TYPE.DOUBLE == dest.type) {
        dest.doubleValues = Arrays.copyOf(dest.doubleValues, destMinLength);
      } else if (GeoTimeSerie.TYPE.STRING == dest.type) {
        dest.stringValues = Arrays.copyOf(dest.stringValues, destMinLength);
      }
      // else TYPE.BOOLEAN == dest.type // nothing to do because BitSet grows automatically
    }

    // If any of dest or src have location info
    if (null != dest.locations || null != src.locations) {
      if(null == dest.locations){
        dest.locations = new long[destMinLength];
        Arrays.fill(dest.locations, GeoTimeSerie.NO_LOCATION);
      } else if(dest.locations.length < destMinLength){
        dest.locations = Arrays.copyOf(dest.locations, destMinLength);
        Arrays.fill(dest.locations, dest.values, dest.values + destMinLength, GeoTimeSerie.NO_LOCATION);
      }
    }

    // If any of dest or src have elevation info
    if (null != dest.elevations || null != src.elevations) {
      if(null == dest.elevations){
        dest.elevations = new long[destMinLength];
        Arrays.fill(dest.elevations, GeoTimeSerie.NO_ELEVATION);
      } else if(dest.elevations.length < destMinLength){
        dest.elevations = Arrays.copyOf(dest.elevations, destMinLength);
        Arrays.fill(dest.elevations, dest.values, dest.values + destMinLength, GeoTimeSerie.NO_ELEVATION);
      }
    }

    // Actual copy
    copy0(src, srcPos, dest, destPos, length);

    dest.values = Math.max(dest.values, destMinLength);
  }

  /**
   * CAREFUL, no check done in this method.
   * Copy the specified part of a GTS to an other specified part of GTS.
   * Very similar to System.arraycopy.
   *
   * @param src The source GTS
   * @param srcPos The starting index in src
   * @param dest The destination GTS
   * @param destPos The starting index in dest
   * @param length The number of points to copy
   */
  private static void copy0(GeoTimeSerie src, int srcPos, GeoTimeSerie dest, int destPos, int length){
    System.arraycopy(src.ticks, srcPos, dest.ticks, destPos, length);
    if (null != src.locations) {
      System.arraycopy(src.locations, srcPos, dest.locations, destPos, length);
    }
    if (null != src.elevations) {
      System.arraycopy(src.elevations, srcPos, dest.elevations, destPos, length);
    }
    if (GeoTimeSerie.TYPE.LONG == dest.type) {
      System.arraycopy(src.longValues, srcPos, dest.longValues, destPos, length);
    } else if (GeoTimeSerie.TYPE.DOUBLE == dest.type) {
      System.arraycopy(src.doubleValues, srcPos, dest.doubleValues, destPos, length);
    } else if (GeoTimeSerie.TYPE.STRING == dest.type) {
      System.arraycopy(src.stringValues, srcPos, dest.stringValues, destPos, length);
    } else { // TYPE.BOOLEAN == dest.type
      for(int i = 0; i < length; i++) {
        dest.booleanValues.set(destPos + i, src.booleanValues.get(srcPos + i));
      }
    }
  }
  
  /**
   * Copy geo infos (location + elevation) from GTS 'from' into GTS 'to'
   * Only infos for matching ticks with values in 'to' will be copied
   * 
   * @param from Source GeoTimeSerie instance from which geo infos will be copied.
   * @param to Destination GeoTimeSerie instance to which geo infos will be copied.
   */
  public static void copyGeo(GeoTimeSerie from, GeoTimeSerie to) {
    // Sort both GTS
    GTSHelper.sort(from, false);
    GTSHelper.sort(to, false);
    
    int fromidx = 0;
    int toidx = 0;
    
    while(toidx < to.values && fromidx < from.values) {
      long fromtick = GTSHelper.tickAtIndex(from, fromidx);
      long totick = GTSHelper.tickAtIndex(to, toidx);
      
      // Advance fromidx until fromtick >= totick
      while(fromidx < from.values && fromtick < totick) {
        fromidx++;
        fromtick = GTSHelper.tickAtIndex(from, fromidx);
      }
      
      if (fromidx >= from.values) {
        break;
      }
      
      // Advance toidx until totick == fromtick
      while(toidx < to.values && totick < fromtick) {
        toidx++;
        totick = GTSHelper.tickAtIndex(to, toidx);
      }
      
      if (toidx >= to.values) {
        break;
      }
      
      if (totick == fromtick) {
        long location = GTSHelper.locationAtIndex(from, fromidx);
        long elevation = GTSHelper.elevationAtIndex(from, fromidx);
        
        // Set location + elevation at 'toidx'
        GTSHelper.setLocationAtIndex(to, toidx, location);
        GTSHelper.setElevationAtIndex(to, toidx, elevation);
        fromidx++;
      }
    }
  }

  public static int[] getFirstLastTicks(long[] ticks) {
    //
    // Determine first and last ticks
    //
    
    long firsttick = Long.MAX_VALUE;
    long lasttick = Long.MIN_VALUE;
    
    int firstidx = -1;
    int lastidx = -1;
    
    for (int i = 0; i < ticks.length; i++) {
      if (ticks[i] < firsttick) {
        firstidx = i;
        firsttick = ticks[i];
      }
      if (ticks[i] > lasttick) {
        lastidx = i;
        lasttick = ticks[i];
      }
    }
  
    return new int[] { firstidx, lastidx };
  }
  
  public static boolean geowithin(GeoXPShape shape, GeoTimeSerie gts) {
    boolean hasLocations = false;
    for (int i = 0; i < gts.values; i++) {      
      long location = GTSHelper.locationAtIndex(gts, i);
      if (GeoTimeSerie.NO_LOCATION != location) {
        hasLocations = true;
        if (!GeoXPLib.isGeoXPPointInGeoXPShape(location, shape)) {
          return false;
        }
      }
    }
    return hasLocations;
  }

  public static boolean geointersects(GeoXPShape shape, GeoTimeSerie gts) {
    for (int i = 0; i < gts.values; i++) {      
      long location = GTSHelper.locationAtIndex(gts, i);
      if (GeoTimeSerie.NO_LOCATION != location) {
        if (GeoXPLib.isGeoXPPointInGeoXPShape(location, shape)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Return shrinked versions of the input GTS containing only the ticks which have
   * values in all input GTS.
   * 
   * @param series List of GeoTimeSerie instance to get values with common ticks from.
   * @return A new List of cloned GeoTimeSerie containing ony common ticks.
   */  
  public static List<GeoTimeSerie> commonTicks(List<GeoTimeSerie> series) {
    
    if (1 == series.size()) {
      GeoTimeSerie serie = series.get(0).clone();
      List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
      result.add(serie);
      return result;
    }
    
    //
    // Start by sorting the GTS in chronological order
    //
    
    for (GeoTimeSerie serie: series) {
      GTSHelper.sort(serie);
    }
    
    //
    // Now run a sweeping line algorithm
    //
    
    int[] idx = new int[series.size()];
    
    //
    // Determine which GTS has the least number of ticks, it will be the leader
    //
    
    int minticks = Integer.MAX_VALUE;
    int leader = -1;
    
    for (int i = 0; i < series.size(); i++) {
      if (series.get(i).values < minticks) {
        leader = i;
        minticks = series.get(i).values;
      }
    }
    
    GeoTimeSerie leadergts = series.get(leader);
    
    // Build resulting GTS
    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();
    
    for (GeoTimeSerie gts: series) {
      result.add(gts.cloneEmpty(gts.values / 2));
    }
    
    //
    // Return immediately if minticks is 0, meaning that there is one empty GTS
    //
    
    if (0 == minticks) {
      return result;
    }
    
    while(true) {
      //
      // Advance all indices until the timestamp is >= that of the leader
      //
      
      // Set the target tick to the current one of the leader
      long target = leadergts.ticks[idx[leader]];

      boolean match = true;
      
      for (int i = 0; i < series.size(); i++) {
        if (i == leader) {
          continue;
        }

        GeoTimeSerie serie = series.get(i);
        
        //
        // Advance index until we reach or pass target
        //
        
        while(idx[i] < serie.values && serie.ticks[idx[i]] < target) {
          idx[i]++;
        }
                
        //
        // We've reached the end of one of the GTS, we know we're done
        //
        
        if (idx[i] >= serie.values) {
          return result;
        }
        
        //
        // We've reached a tick which is greater than the leader's current tick, we need to move the leader now
        //
        
        if (serie.ticks[idx[i]] > target) {
          //
          // Advance the leader
          //
          
          while(idx[leader] < leadergts.values && leadergts.ticks[idx[leader]] < serie.ticks[idx[i]]) {
            idx[leader]++;
          }
          
          if (idx[leader] >= leadergts.values) {
            return result;
          }
          match = false;
          break; 
        }
      }
      
      //
      // We have a match of all the ticks, add the datapoints with the current tick to the resulting GTS
      //
      
      if (match) {        
        for (int i = 0; i < series.size(); i++) {
          GeoTimeSerie serie = series.get(i);
          
          int tidx = idx[i];
          
          while (tidx < serie.values && target == serie.ticks[tidx]) {
            GTSHelper.setValue(result.get(i),
                GTSHelper.tickAtIndex(serie, tidx),
                GTSHelper.locationAtIndex(serie, tidx),
                GTSHelper.elevationAtIndex(serie, tidx),
                GTSHelper.valueAtIndex(serie, tidx),
                false);
            tidx++;
          }
          
          if (tidx < serie.values) {
            tidx--;
          }
          
          idx[i] = tidx;
        }
        
        idx[leader]++;
        if (idx[leader] >= leadergts.values) {
          return result;
        }
      }
    }
  }
  
  public static List<Number> bbox(GeoTimeSerie gts) {
    double maxlat = -90.0D;
    double minlat = 90.0D;
    double minlon = 180.0D;
    double maxlon = -180.0D;

    List<Number> bbox = new ArrayList<Number>();
    
    if (null == gts.locations) {
      bbox.add(-90.0D);
      bbox.add(-180.0D);
      bbox.add(90.0D);
      bbox.add(180.0D);
      return bbox;
    }
    
    for (int i = 0; i < gts.values; i++) {
      if (GeoTimeSerie.NO_LOCATION == gts.locations[i]) {
        continue;
      }
      
      double[] latlon = GeoXPLib.fromGeoXPPoint(gts.locations[i]);
      
      if (latlon[0] < minlat) {
        minlat = latlon[0];
      }
      if (latlon[0] > maxlat) {
        maxlat = latlon[0];
      }
      if (latlon[1] < minlon) {
        minlon = latlon[1];
      }
      if (latlon[1] > maxlon) {
        maxlon = latlon[1];
      }
    }
    
    bbox.add(minlat);
    bbox.add(minlon);
    bbox.add(maxlat);
    bbox.add(maxlon);

    return bbox;
  }
 
  /**
   * Compute conditional probabilities given a GTS considering the values as the concatenation
   * of given events plus the event for which we want to compute the probability, separated by 'separator'. i.e.
   * given1&lt;SEP&gt;given2&lt;SEP&gt;....&lt;SEP&gt;event
   * 
   * If 'separator' is null then we simply compute the probability of values instead of conditional probabilities
   * 
   * @param gts GeoTimeSerie instance representing the sequence of events.
   * @param separator a String separator between events or null to simply compute the probability of values.
   * @return a new GeoTimeSerie instance containing the probability P(X=event|A=given1,B=given2,...), the value of each random variable being those of the corresponding tick in the given GTS.
   */
  public static GeoTimeSerie cprob(GeoTimeSerie gts, String separator) throws WarpScriptException {
    
    Map<Object,AtomicInteger> histogram = new HashMap<Object, AtomicInteger>();

    GeoTimeSerie prob = gts.cloneEmpty();

    if (null == separator) {      
      long total = 0L;
      
      for (int i = 0; i < gts.values; i++) {
        Object value = GTSHelper.valueAtIndex(gts, i);
        AtomicInteger count = histogram.get(value);
        if (null == count) {
          count = new AtomicInteger(0);
          histogram.put(value, count);
        }
        count.addAndGet(1);
        total++;
      }
      
      for (int i = 0; i < gts.values; i++) {
        long timestamp = GTSHelper.tickAtIndex(gts, i);
        long geoxppoint = GTSHelper.locationAtIndex(gts, i);
        long elevation = GTSHelper.elevationAtIndex(gts, i);
        Object value = GTSHelper.valueAtIndex(gts, i);
        double p = histogram.get(value).doubleValue() / total;
        GTSHelper.setValue(prob, timestamp, geoxppoint, elevation, p, false);
      }
      
      return prob;
    }
    
    //
    // Sort 'gts' by value so we group the 'given' events by common set of values
    //
    
    GTSHelper.valueSort(gts);
    
    int idx = 0;
    
    while(idx < gts.values) {
      //
      // Extract 'given' events
      //
      
      Object val = GTSHelper.valueAtIndex(gts, idx);
      
      if (!(val instanceof String)) {
        throw new WarpScriptException("Can only compute conditional probabilities for String Geo Time Series.");
      }
      
      int lastsep = val.toString().lastIndexOf(separator);
      
      if (-1 == lastsep) {
        throw new WarpScriptException("Separator not found, unable to isolate given events.");
      }
      
      String given = val.toString().substring(0, lastsep);
     
      histogram.clear();
      long total = 0;
      
      int subidx = idx;
      
      while(subidx < gts.values) {
        
        val = GTSHelper.valueAtIndex(gts, subidx);
        
        lastsep = val.toString().lastIndexOf(separator);
        
        if (-1 == lastsep) {
          throw new WarpScriptException("Separator not found, unable to isolate given events.");
        }
        
        String givenEvents = val.toString().substring(0, lastsep);
        
        if (!givenEvents.equals(given)) {
          break;
        }
        
        String event = val.toString().substring(lastsep + separator.length()).trim();
        
        AtomicInteger count = histogram.get(event);
        
        if (null == count) {
          count = new AtomicInteger(0);
          histogram.put(event, count);
        }
        
        count.addAndGet(1);
        total++;
        
        subidx++;
      }
      
      for (int i = idx; i < subidx; i++) {
        val = GTSHelper.valueAtIndex(gts, i);
        
        lastsep = val.toString().lastIndexOf(separator);
        
        String event = val.toString().substring(lastsep + separator.length());

        long timestamp = GTSHelper.tickAtIndex(gts, i);
        long location = GTSHelper.locationAtIndex(gts, i);
        long elevation = GTSHelper.elevationAtIndex(gts, i);
        
        double p = histogram.get(event).doubleValue() / total;
        
        GTSHelper.setValue(prob, timestamp, location, elevation, p, false);
      }
      
      idx = subidx;
    } 
    
    return prob;
  }
  
  /**
   * Convert a GTS into a GTS of the probability associated with the value present at each tick.
   * 
   * @param gts GeoTimeSerie instance for which to get the value probabilities.
   * @return a new GeoTimeSerie instance containing the probability P(X=event), the value 'event' being that of the corresponding tick in the given GTS.
   */
  public static GeoTimeSerie prob(GeoTimeSerie gts) {
    Map<Object, Long> histogram = valueHistogram(gts);
    
    GeoTimeSerie prob = gts.cloneEmpty(gts.values);
    
    for (int i = 0; i < gts.values; i++) {
      long timestamp = tickAtIndex(gts, i);
      long geoxppoint = locationAtIndex(gts, i);
      long elevation = elevationAtIndex(gts, i);
      Object value = valueAtIndex(gts, i);
      
      setValue(prob, timestamp, geoxppoint, elevation, histogram.get(value).doubleValue() / gts.values, false);
    }
    
    return prob;
  }
  
  public static GeoTimeSerie lttb(GeoTimeSerie gts, int threshold, boolean timebased) throws WarpScriptException {
    //
    // If the GTS has less than threshold values, return it as is
    //
    
    if (gts.values <= threshold - 2) {
      return gts;
    }
        
    //
    // Ditto if the GTS is a STRING or BOOLEAN one
    //
    
    if (TYPE.STRING == gts.type || TYPE.BOOLEAN == gts.type) {
      return gts;
    }
    
    if (threshold < 3) {
      throw new WarpScriptException("Threshold MUST be >= 3.");
    }
    
    double bucketsize = (double) gts.values / (double) (threshold - 1);
    
    // Sort GTS
    GTSHelper.sort(gts);

    long timebucket = (long) Math.ceil((gts.ticks[gts.values - 1] - gts.ticks[0] - 2) / (double) (threshold - 2)); 
    long firsttick = gts.ticks[0];
    
    List<Integer> buckets = null;
    
    //
    // If timebased, determine the valid buckets
    //
    
    if (timebased) {
      // Allocate a list to keep track of buckets boundaries
      buckets = new ArrayList<Integer>();
      
      long lowerts = firsttick + 1;
     
      buckets.add(0); // Start of first bucket
      
      long lastbucket = 0;
      
      for (int i = 1; i < gts.values - 1; i++) {
        long bucket = 1 + (gts.ticks[i] - lowerts) / timebucket;
        
        if (bucket != lastbucket) {
          buckets.add(i - 1); // End of previous bucket
          buckets.add(i); // Start of current bucket
          lastbucket = bucket;
        }
      }
      
      buckets.add(gts.values - 2);
      buckets.add(gts.values - 1);
      buckets.add(gts.values - 1);
      
      // Adjust number of buckets
      threshold = buckets.size() / 2;
    }
    
    GeoTimeSerie sampled = gts.cloneEmpty(threshold);

    // Add first datapoint
    GTSHelper.setValue(sampled, GTSHelper.tickAtIndex(gts,0), GTSHelper.locationAtIndex(gts, 0), GTSHelper.elevationAtIndex(gts, 0), GTSHelper.valueAtIndex(gts, 0), false);

    int refidx = 0;
    
    int firstinrange = 1;
    int lastinrange = -1;
    
    //
    // Loop over the number of requested values - 2 (as we retain both ends)
    //
    
    for (int i = 0; i < threshold - 2; i++) {
      
      //
      // Determine the ticks to consider when computing the next datapoint
      //
      
      if (timebased) {
        firstinrange = buckets.get(2 * (i + 2));
        lastinrange = buckets.get(2 * (i + 2) + 1) + 1; // Add '1' as we use a strict comparison when scanning
      } else {
        //
        // Determine the index range of the bucket following the current one
        //
        
        firstinrange = 1 + (int) Math.floor((i + 1) * bucketsize);
        lastinrange = 1 + (int) Math.floor((i + 2) * bucketsize);
        
        if (firstinrange >= gts.values) {
          firstinrange = gts.values - 1;
        }
        
        if (lastinrange >= gts.values) {
          lastinrange = gts.values - 1;
        }
      }
      
      //
      // Compute the average value on the range and the average tick
      //
      
      double ticksum = 0.0D;
      double valuesum = 0.0D;
      
      for (int j = firstinrange; j < lastinrange; j++) {
        ticksum += gts.ticks[j];
        valuesum += ((Number) GTSHelper.valueAtIndex(gts, j)).doubleValue();
      }
      
      double tickavg = ticksum / (lastinrange - firstinrange + 1);
      double valueavg = valuesum / (lastinrange - firstinrange + 1);
      
      //
      // Now compute the triangle area and retain the point in the current bucket which maximizes it
      //
      
      double maxarea = -1.0D;
      
      double refvalue = ((Number) GTSHelper.valueAtIndex(gts, refidx)).doubleValue();
      double reftick = gts.ticks[refidx];
      
      int nextref = -1;

      if (timebased) {
        firstinrange = buckets.get(2 * (i + 1));
        lastinrange = buckets.get(2 * (i + 1) + 1) + 1; // Add '1' as we use a strict comparison when scanning
      } else {
        //
        // Compute the index range of the current bucket
        //
        firstinrange = 1 + (int) Math.floor(i * bucketsize);
        lastinrange = 1 + (int) Math.floor((i + 1) * bucketsize);
        if (firstinrange >= gts.values - 1) {
          firstinrange = gts.values - 2;
        }
        if (lastinrange >= gts.values - 1) {
          lastinrange = gts.values - 1;
        }        
      }
      
      for (int j = firstinrange; j < lastinrange; j++) {
        double tick = gts.ticks[j];
        double value = ((Number) GTSHelper.valueAtIndex(gts, j)).doubleValue();
        double area = 0.5D * Math.abs(((reftick - tickavg) * (value - refvalue)) - (reftick - tick) * (valueavg - refvalue)); 
               
        if (area > maxarea) {
          maxarea = area;
          nextref = j;
        }
      }
      
      GTSHelper.setValue(sampled, GTSHelper.tickAtIndex(gts, nextref), GTSHelper.locationAtIndex(gts, nextref), GTSHelper.elevationAtIndex(gts, nextref), GTSHelper.valueAtIndex(gts, nextref), false);
    }
    
    // Add last datapoint
    GTSHelper.setValue(sampled, GTSHelper.tickAtIndex(gts,gts.values - 1), GTSHelper.locationAtIndex(gts, gts.values - 1), GTSHelper.elevationAtIndex(gts, gts.values - 1), GTSHelper.valueAtIndex(gts, gts.values - 1), false);
    
    return sampled;
  }
  
  public static void dump(GTSEncoder encoder, PrintWriter pw) {
    StringBuilder sb = new StringBuilder(" ");
    Metadata meta = encoder.getMetadata();
    
    GTSHelper.encodeName(sb, meta.getName());
    if (meta.getLabelsSize() > 0) {
      sb.append("{");
      boolean first = true;
      for (Entry<String,String> entry: meta.getLabels().entrySet()) {
        if (!first) {
          sb.append(",");
        }
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        first = false;
      }
      sb.append("}");      
    } else {
      sb.append("{}");
    }
    
    if (meta.getAttributesSize() > 0) {
      sb.append("{");
      boolean first = true;    
      for (Entry<String,String> entry: meta.getAttributes().entrySet()) {
        if (!first) {
          sb.append(",");
        }
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        first = false;
      }
      sb.append("}");
    } else {
      sb.append("{}");
    }
    
    sb.append(" ");
    
    String clslbs = sb.toString();
    
    GTSDecoder decoder = encoder.getUnsafeDecoder(false);
    
    boolean first = true;
    while(decoder.next()) {      
      if (!first) {
        pw.print("=");
      }
      pw.print(decoder.getTimestamp());
      pw.print("/");
      long location = decoder.getLocation();
      if (GeoTimeSerie.NO_LOCATION != location) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(location);
        pw.print(latlon[0]);
        pw.print(":");
        pw.print(latlon[1]);
      }
      pw.print("/");
      long elevation = decoder.getElevation();
      if (GeoTimeSerie.NO_ELEVATION != elevation) {
        pw.print(elevation);
      }
      if (first) {
        pw.print(clslbs);
      } else {
        pw.print(" ");
      }
      sb.setLength(0);
      GTSHelper.encodeValue(sb, decoder.getBinaryValue());
      pw.print(sb.toString());
      pw.print("\r\n");
      first = false;
    }    
  }
  
  public static double standardizedMoment(int moment, GeoTimeSerie gts, boolean bessel) throws WarpScriptException {
    double sum = 0.0D;
    double sumsq = 0.0D;
    
    int n = gts.values;

    if (TYPE.LONG == gts.type) {
      for (int i = 0; i < n; i++) {
        sum = sum + gts.longValues[i];
        sumsq = sumsq + (gts.longValues[i] * gts.longValues[i]);
      }
    } else if (TYPE.DOUBLE == gts.type) {
      for (int i = 0; i < n; i++) {
        sum = sum + gts.doubleValues[i];
        sumsq = sumsq + (gts.doubleValues[i] * gts.doubleValues[i]);
      }      
    } else {
      throw new WarpScriptException("Non numeric Geo Time Series.");
    }
        
    //
    // Compute mean and standard deviation
    //
    
    double mean = sum / (double) n;
    
    double variance = (sumsq / (double) n) - (sum * sum) / ((double) n * (double) n);
    
    //
    // Apply Bessel's correction
    // @see http://en.wikipedia.org/wiki/Bessel's_correction
    //
    
    if (n > 1 && bessel) {
      variance = variance * ((double) n) / (n - 1.0D);
    }

    double sd = Math.sqrt(variance);
    
    double momentValue = 0.0D;
    
    if (TYPE.LONG == gts.type) {
      for (int i = 0; i < n; i++) {
        momentValue += Math.pow((gts.longValues[i] - mean) / sd, moment);
      }      
    } else {
      for (int i = 0; i < n; i++) {
        momentValue += Math.pow((gts.doubleValues[i] - mean) / sd, moment);
      }      
    }
    
    momentValue = momentValue / n;
    
    return momentValue;
  }
  
  public static double kurtosis(GeoTimeSerie gts, boolean bessel) throws WarpScriptException {
    return standardizedMoment(4, gts, bessel);
  }
  
  public static double skewness(GeoTimeSerie gts, boolean bessel) throws WarpScriptException {
    return standardizedMoment(3, gts, bessel);
  }

  public static void booleanNot(GeoTimeSerie gts) throws WarpScriptException {
    if (GeoTimeSerie.TYPE.BOOLEAN == gts.getType()) {
      gts.booleanValues.flip(0, gts.booleanValues.length());
    } else {
      throw new WarpScriptException("Non boolean Geo Time Series.");
    }
  }
}
