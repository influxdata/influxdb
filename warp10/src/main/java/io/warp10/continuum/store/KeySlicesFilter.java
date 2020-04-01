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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Filter which filters keys based on slices thereof.
 * 
 * This filter operates on byte[] independently of any underlying storage system.
 * 
 * A slice of key is defined by a start and end index in the key.
 * If the key contains less data than what the slices need, key will be filtered.
 * 
 * Once the slices are extracted from the key, their merged contents are compared with a list
 * of ranges to determine if they lie in one of them. If not, the key is filtered.
 * 
 * This filter is handy to select rows whose key is a compound one and which must have
 * some parts of the key in specific ranges.

 */
public class KeySlicesFilter {
  
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  
  /**
   * Pairs of bounds for each slice to be extracted
   */
  private int[] bounds;
  
  /**
   * Cumulative length of slices
   */
  private int slicesLength;
  
  /**
   * Ranges in which the slice must fall
   * NOTE: the end keys are included in their respective ranges.
   */
  private List<Pair<byte[],byte[]>> ranges;

  /**
   * Packed range keys
   */
  private byte[] rangekeys = null;
  
  /**
   * Flag indicating that the first slice starts at key offset 0 and therefore the filter
   * can provide next key hinting when encountering a key out of all ranges.
   */
  private boolean hasHinting = false;
  
  /**
   * Stable array for extracting slices
   */
  private byte[] keyslices = null;
  
  /**
   * Number of ranges in 'rangekeys'
   */
  private int nranges;
  
  /**
   * Constructor for a KeySlicesFilter.
   * 
   * @param bounds Bounds of the key slices to extract.
   * @param ranges Ranges which the key slices must be included in for a key to be retained
   * @param singleThread Flag indicating that the filter will be called by a single thread, this is used to optimize array allocation.
   */
  public KeySlicesFilter(int[] bounds, List<Pair<byte[], byte[]>> ranges, boolean singleThread) {
    
    //
    // Check that there is an even number of bounds
    //
    
    if (0 != bounds.length % 2) {
      throw new RuntimeException("Odd number of slice bounds.");
    }
    
    int idx = 0;
    
    slicesLength = 0;

    //
    // Compute cumulative length of slices
    //
    
    int lastbound = -1;
    
    while (idx < bounds.length) {
      if (bounds[idx] > bounds[idx + 1]) {
        int tmp = bounds[idx];
        bounds[idx] = bounds[idx + 1];
        bounds[idx + 1] = tmp;
      }
      
      //
      // Make sure slices do not overlap or are out of order
      //
      
      if (bounds[idx] <= lastbound) {
        throw new RuntimeException("Out of order or overlapping slice bounds.");
      }
      
      lastbound = bounds[idx + 1];
      
      slicesLength += bounds[idx + 1] - bounds[idx] + 1;
      idx += 2;
    }
    
    //
    // Merge adjacent slices.
    // This is important to speed up slice extraction and to provide
    // more accurate hints when the first slice starts at offset 0.
    //
    
    this.bounds = new int[bounds.length];
    
    int idx2 = 0;
    
    idx = 0;
    
    while(idx < bounds.length) {
      // Check the end of a slice to see if it matches the start of the next one
      if (idx % 1 == 1 && idx < bounds.length - 1) {
        // Merge adjacent slices
        if (bounds[idx + 1] == bounds[idx + 2]) {
          idx += 2;
          continue;
        }
      }
      this.bounds[idx2++] = bounds[idx++];
    }

    // Shorten 'bounds' if slices were merged.
    if (idx2 != this.bounds.length) {
      this.bounds = Arrays.copyOf(this.bounds, idx2);
    }
    
    //
    // Check all pairs, if one has a null as first or second value, replace
    // the null with the other non null value, this is a singleton.
    // Also swap if upper/lower bounds are reversed.
    //
    
    for (Pair<byte[], byte[]> pair: ranges) {
      if (null == pair.getFirst()) {
        pair.setFirst(pair.getSecond());
      } else if (null == pair.getSecond()) {
        pair.setSecond(pair.getFirst());
      } else if (null != pair.getFirst() && null != pair.getSecond()) {
        if (Bytes.compareTo(pair.getFirst(), pair.getSecond()) > 0) {
          byte[] tmp = pair.getFirst();
          pair.setFirst(pair.getSecond());
          pair.setSecond(tmp);
        }      
      }
      //
      // Make sure both extrema are of 'slicesLength' length
      //
      if (slicesLength != pair.getFirst().length || slicesLength != pair.getSecond().length) {
        throw new RuntimeException("Invalid length for range extremum, expected " + slicesLength);
      }
    }
    
    //
    // Remove occurrences of <null,null>
    //
    
    Pair<byte[], byte[]> nullpair = new Pair<byte[], byte[]>(null, null);
    
    while(ranges.remove(nullpair)) {      
    }
   
    //
    // Sort the pairs in ascending order of lower bound then of upper bound
    //
    
    Collections.sort(ranges, new Comparator<Pair<byte[], byte[]>> () {
      @Override
      public int compare(Pair<byte[], byte[]> o1, Pair<byte[], byte[]> o2) {
        int lowerBoundComparison = Bytes.compareTo(o1.getFirst(), o2.getFirst());  
        if (0 == lowerBoundComparison) {
          // Lower bounds are equal, compare upper bounds, replacing nulls with the
          // lower bound
          return (Bytes.compareTo(null == o1.getSecond() ? o1.getFirst() : o1.getSecond(),
                                  null == o2.getSecond() ? o2.getFirst() : o2.getSecond()));
        } else {
          return lowerBoundComparison;
        }
      }
    });
    
    //
    // Pack all ranges in a single byte array 
    //
    
    int currentidx = -1;
    int rangeidx =  0;
    
    byte[] byteranges = new byte[ranges.size() * 2 * slicesLength];
    
    while(rangeidx < ranges.size()) {
      byte[] low = ranges.get(rangeidx).getFirst();
      byte[] high = ranges.get(rangeidx).getSecond();
      
      if (currentidx >= 0 && Bytes.compareTo(low, 0, slicesLength, byteranges, currentidx * slicesLength, slicesLength) <= 0) {
        if (Bytes.compareTo(high, 0, slicesLength, byteranges, currentidx * slicesLength, slicesLength) > 0) {
          //
          // If current range overlaps the previous one, simply replace the end key if it is > to the current one
          // Otherwise, do nothing as the current range is included in the previous one
          System.arraycopy(high, 0, byteranges, currentidx * slicesLength, slicesLength);        
        }
      } else {
        //
        // Store low/high keys of range
        //
        currentidx++;
        System.arraycopy(low, 0, byteranges, currentidx * slicesLength, slicesLength);
        currentidx++;
        System.arraycopy(high, 0, byteranges, currentidx * slicesLength, slicesLength);        
      }       
      
      rangeidx++;
    }

    currentidx++;
      
    //
    // Some ranges were merged, reduce byteranges size
    //
    
    if (currentidx < ranges.size() * 2) {
      byteranges = Arrays.copyOf(byteranges, currentidx * slicesLength);
    }
    
    this.rangekeys = byteranges;
    this.nranges = this.rangekeys.length / this.slicesLength;
    
    //
    // If the first slice starts at offset 0 then we will be able to provide a key hint
    //
    
    if (0 == bounds[0]) {
      hasHinting = true;
    } else {
      hasHinting = false;
    }
    
    if (singleThread) {
      this.keyslices = new byte[this.slicesLength];
    }
  }
  
  /**
   * Determine if a key should be filtered or not.
   * 
   * This method returns a byte array which indicates how the key should be dealt with.
   * 
   * If the return value is 'buffer', the key should be include.
   * If the return value is null, the key should be skipped and no next key hint is available
   * If the return value is a non empty byte array (different from 'buffer'), the key should be skipped and the return value used as the next key hint.
   * If the return value is an empty byte array, the key is passed the last range and scanning should end
   * 
   * @param buffer
   * @param offset
   * @param len
   * @return
   */
  public byte[] filter(byte[] buffer, int offset, int len) {
    //
    // Extract slices
    //
    
    byte[] slices = getSlices(buffer, offset, len);
    
    // getSlices returned null, skip to the next row
    if (null == slices) {
      return null;
    }
    
    //
    // Determine the insertion point of the slices in the range keys
    //
    
    int insertionPoint = findInsertionPoint(slices);
    
    //
    // If the insertion point is >= 0 then we know the key is included as it
    // lies on a range key
    //
    
    if (insertionPoint >= 0) {
      return buffer;
    }
    
    //
    // If slice should be inserted before the first range, then we know it's not
    // included in any range
    //
    
    if (-1 == insertionPoint) {
      if (hasHinting) {
        // Hint is the first slice of the first range key
        return Arrays.copyOf(this.rangekeys, this.bounds[1]);
      } else {
        return null;
      }
    }
    
    //
    // If the insertion point is -nranges - 1 this means the slice lies after the last range
    //
    
    if (-nranges - 1 == insertionPoint) {
      if (hasHinting) {
        // Indicate we've reached the end of the ranges
        return EMPTY_BYTE_ARRAY;
      } else {
        return null;
      }
    }
    
    //
    // If the insertion point is even, this means the slice lies between valid ranges, so the row should be excluded
    //
    
    if (-(insertionPoint + 1) % 2 == 0) {
      if (hasHinting) {
        // hintOffset will be the start of the next range
        int hintOffset = this.slicesLength * (-(insertionPoint + 1));
        return Arrays.copyOfRange(this.rangekeys, hintOffset,  this.bounds[1]);        
      } else {
        return null;
      }
    }
    
    //
    // By default include the key
    //
    
    return buffer;
  }
  
  /**
   * Extract slices from a row key
   * 
   * @param row
   * @param offset
   * @param length
   * @return
   */
  private byte[] getSlices(byte[] row, int offset, int length) {
        
    /**
     * There is not enough data in 'row' to extract all slices
     */
    if (length <= this.bounds[this.bounds.length - 1]) {
      return null;
    }

    //
    // Allocate an array or use the stable one for extracting slices.
    //
    
    byte[] slices = null != this.keyslices ? this.keyslices : new byte[this.slicesLength];
    
    //
    // We first extract the slices, if there is not enough data to extract the
    // slices, ignore the row
    //
    
    int boundsidx = 0;
    int sliceidx = 0;
    
    while (boundsidx < this.bounds.length) {
      int off = bounds[boundsidx];
      int len = bounds[boundsidx + 1] - bounds[boundsidx] + 1;
      
      if (length < off + len) {
        return null;
      }
      
      System.arraycopy(row, offset + off, slices, sliceidx, len);
      sliceidx += len;
      boundsidx += 2;      
    }
    
    return slices;
  }
  
  /**
   * Find the insertion point among the filter ranges for the given subkey
   * 
   * @param subkey concatenated key slices
   * @return
   */
  private int findInsertionPoint(byte[] subkey) {    
    //
    // Attempt to find the insertion point
    //
    
    int nranges = this.rangekeys.length / this.slicesLength;
    int insertionPoint = nranges;
    
    int left = 0;
    int right = insertionPoint - 1;
    
    while(true) {
      
      if (left > right) {
        left = right;
      } else if (right < left) {
        right = left;
      }
      
      int mid = (left + right) / 2;
            
      int res = Bytes.compareTo(subkey, 0, subkey.length, this.rangekeys, mid * this.slicesLength, this.slicesLength);

      if (0 == res) {
        insertionPoint = mid;
        break;
      } else if (res < 0) {
        // If left==right this means the insertion point is before 'right'
        if (right == left) {
          insertionPoint = -(right) - 1;
          break;
        }
        right = mid - 1;
      } else if (res > 0) {
        // If left==right this means the insertion point is after left
        if (right == left) {
          insertionPoint = -(right + 1) - 1;
          break;
        }
        left = mid + 1;
      }
    }
    
    return insertionPoint;
  }
}
