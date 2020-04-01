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

package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Filters data based on slices of row key.
 * 
 * A slice of row key is defined by a start and end index in the row key.
 * If the key contains less data than what the slices need, row will be filtered.
 * 
 * Once the slices are extracted from the key, their merged contents are compared with a list
 * of ranges to determine if they lie in one of them. If not, the row is filtered.
 * 
 * This filter is handy to select rows whose key is a compound one and which must have
 * some parts of the key in specific ranges.
 * 
 */
public class SlicedRowFilter extends FilterBase {
  
  /**
   * Pairs of bounds for each slice to be extracted.
   * Slice 0 goes from byte bounds[0] to byte bounds[1]
   * Slice 1 goes from byte bounds[2] to byte bounds[3]
   * ...
   */
  private int[] bounds;
  
  /**
   * Cumulative length of slices in bytes
   */
  private int slicesLength;
  
  /**
   * Ranges in which the concatenation of slices must fall
   * NOTE: the end keys are included in their respective ranges.
   */
  private List<Pair<byte[],byte[]>> ranges;

  /**
   * Comparator to find insertion point of slice in a list of ranges
   */
  private Comparator<Pair<byte[], byte[]>> SLICE_RANGE_COMPARATOR = new Comparator<Pair<byte[],byte[]>>() {
    @Override
    public int compare(Pair<byte[], byte[]> o1, Pair<byte[], byte[]> o2) {
      return Bytes.compareTo(o1.getFirst(), o2.getFirst());
    }
  };
  
  /**
   * Flag indicating we're done filtering rows since we've encountered a slice starting at offset 0
   * and which was past the end of the last range
   */
  private boolean done = false;
  
  /**
   * Stable container for extracting slices
   */
  private byte[] slice;
  
  private byte[] rangekeys;
  
  /**
   * Is the filter instance able to provide next key hints.
   * This is only possible if the first slice starts at 0.
   */
  private boolean hasHinting;
  
  /**
   * Offset in 'rangekeys' which should be used as the seek hint
   */
  private int hintOffset = -1;
  
  /**
   * Boolean to fast track the columns inclusion once the row has been deemed valid
   */
  private boolean includeRow = false;
  
  /**
   * Boolean to track row exclusion status from call to filterRow
   */
  private boolean excludeRow = false;
  
  /**
   * Number of values to consider in each range
   */
  private long count = Long.MAX_VALUE;
  
  /**
   * Number of values left to consider in the current range
   */
  private long nvalues = Long.MAX_VALUE;
  
  /**
   * Index of the current key range the last row key fell within. This is used to determine if the
   * current row is in a different range or not
   */
  private int currentRange = -1;
  
  /**
   * Minimum range we expect to be in, if before that range, row should be excluded and SEEK_NEXT_USING_HINT suggested.
   * This is to overcome a problem encountered once, where, it seems, getNextKeyHint was not called.
   */
  private int minRange = -1;
  
//  private long nano;
//  private long resetCount = 0;
//  private long resetTime = 0;
//  private long lastReset = -1;
//  private long hintCount = 0;
//  private long includeCount = 0;
  
  public SlicedRowFilter() {
//    nano = System.nanoTime();
  }
  
//  @Override
//  public void reset() throws IOException {
//    super.reset();
//    //resetCount++;
//    long n = System.nanoTime();
//    if (-1 != lastReset) {
//      resetTime += (n - lastReset);
//    }
//    lastReset = n;
//  }
//  
//  @Override
//  protected void finalize() throws Throwable {
//    super.finalize();
//    System.out.println("FINALIZE FILTER AFTER " + ((System.nanoTime() - nano) / 1000000.0D) + " ms");
//    System.out.println("# of resets: " + resetCount);
//    System.out.println("Time between resets: " + (resetTime / 1000000.0D) + "ms");
//    System.out.println("# of includes: " + includeCount);
//    System.out.println("# of hints : " + hintCount);
//    System.out.println("Size: " + rangekeys.length);
//  }
  
  public SlicedRowFilter(int[] bounds, List<Pair<byte[], byte[]>> ranges) {
    this(bounds, ranges, Long.MAX_VALUE);
  }
  
  public SlicedRowFilter(int[] bounds, List<Pair<byte[], byte[]>> ranges, long count) {
    
    //
    // Check that there is an even number of bounds
    //
    
    if (0 != bounds.length % 2) {
      throw new RuntimeException("Odd number of slice bounds.");
    }
    
    int idx = 0;
    
    slicesLength = 0;

    //
    // Compute slices length
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
      if (bounds[idx] <= lastbound) {
        throw new RuntimeException("Out of order or overlapping slice bounds.");
      }
      
      lastbound = bounds[idx + 1];
      
      slicesLength += bounds[idx + 1] - bounds[idx] + 1;
      idx += 2;
    }
    
    this.bounds = bounds;
        
    //
    // Allocate an array where slices will be extracted
    //
    
    this.slice = new byte[slicesLength];
    
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
    
    //
    // If the first slice starts at offset 0 then we will be able to provide a key hint
    //
    
    if (0 == bounds[0]) {
      hasHinting = true;
    } else {
      hasHinting = false;
    }
    
    //
    // Initialize count
    //
    
    this.count = count;
    this.nvalues = this.count;
    
    //
    // If we don't have hinting but a count different from Long.MAX_VALUE, throw a RuntimeException
    // as we would be unable to count valid values
    //
    
    if (!hasHinting && Long.MAX_VALUE != this.count) {
      throw new RuntimeException("Slices are incompatible with count based filtering.");
    }
  }
  
  
  /**
   * @see HBASE-9717 for an API change suggestion that would speed up scanning.
   */
      
  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (done) {
      return true;
    }
    
    // Reset hintOffset so we know we've encountered a new row
    hintOffset = -1;
    includeRow = false;
    excludeRow = false;

    //
    // If this filter instance can provide next key hint, delegate the filtering
    // to filterKeyValue by stating that filterRowKey does not filter out the row
    //
    
    if (hasHinting) {
      return false;
    }    

    //
    // No data, so filter the row
    //
    
    if (null == buffer) {
      excludeRow = true;
      return true;
    }
    
    //
    // We first extract the slices, if there is not enough data to extract the
    // last slice, ignore the row
    //
    
    byte[] slices = getSlices(buffer, offset, length);
    
    if (null == slices) {
      excludeRow = true;
      return true;
    }
    
    //
    // Compare slice to first lower bound, if it's lower, filter the row immediately
    // 
    
    if (Bytes.compareTo(slices, 0, this.slicesLength, this.rangekeys, 0, this.slicesLength) < 0) {
      excludeRow = true;
      return true;
    }
    
    //
    // Compare slice to last upper bound if it's higher, filter row immediately
    //
    
    if (Bytes.compareTo(slice, 0, this.slicesLength, this.rangekeys, this.rangekeys.length - this.slicesLength, this.slicesLength) > 0) {
      //
      // If start of slice is at offset 0, then this also means we should filter out
      // all remaining rows.
      //
      
      if (0 == this.bounds[0]) {
        done = true;
      }
      
      excludeRow = true;
      return true;
    }
    
    //
    // Attempt to find the insertion point
    //
    
    int insertionPoint = findInsertionPoint(slices);
    
    int nranges = this.rangekeys.length / this.slicesLength;

    //
    // If the insertion point is >= 0 and < 'nranges' then we know the row is included as it
    // lies on a range key
    //
    
    if (insertionPoint >= 0 && insertionPoint < nranges) {
      return false;
    }
    
    // FIXME(hbs): I wonder if the next two tests are useful, they should be taken care of in the
    // initial rest against first/last bound.
    
    //
    // If slice should be inserted before the first range, then we know it's not
    // included in any range
    //
    
    if (-1 == insertionPoint) {
      excludeRow = true;
      return true;
    }
    
    //
    // If the insertion point is -nranges - 1 this means the slice lies after the last range
    // so filter out the row
    //
    
    if (-nranges - 1 == insertionPoint) {
      excludeRow = true;
      return true;
    }
    
    //
    // If the insertion point is even, this means the slice lies between valid ranges, so the row should be excluded
    //
    
    if (-(insertionPoint + 1) % 2 == 0) {
      excludeRow = true;
      return true;
    }

    //
    // Default behaviour is to include the row
    //
    
    return false;
  }
  
  public boolean filterRowKey(Cell cell) {
    byte[] row = cell.getRowArray();
    return filterRowKey(row, cell.getRowOffset(), cell.getRowLength());    
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    if (done) {
      return ReturnCode.NEXT_ROW;
    }
    
    //
    // If we don't have hinting ability we rely on filterRowKey and therefore we should include all columns here
    // Same thing if 'includeRow' is true, we've encountered a columns on this row and it was deemed valid, so we
    // include all other columns of the row without recomputing anything.
    //

    if (!hasHinting) {
      return excludeRow ? ReturnCode.NEXT_ROW : ReturnCode.INCLUDE;
    }
    
    //
    // If includeRow is 'true', it means we did not change row, so the current row belongs to the
    // same GTS, we then simply check if we've already read 'nvalues' or not
    //
    
    if (includeRow && this.nvalues > 0) {
      // Decrement number of remaining values to consider
      this.nvalues--;
      //includeCount++;
      return ReturnCode.INCLUDE;
    }

    //
    // Either row has changed or we've read enough values
    //
    
    //
    // Compute slices, accessing directly the underlying row array
    //
    
    byte[] subrow = getSlices(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    
    if (null == subrow) {
      return ReturnCode.NEXT_ROW;
    }
    
    //
    // Compare slice to first lower bound, if it's lower, filter the row immediately
    // 
    
    if (Bytes.compareTo(slice, 0, this.slicesLength, this.rangekeys, 0, this.slicesLength) < 0) {
      hintOffset = 0;
      minRange = 0;
      // Re-initialize number of values to consider
      this.nvalues = this.count;
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    
    //
    // Compare slice to last upper bound if it's higher, filter row immediately
    //
    
    if (Bytes.compareTo(slice, 0, this.slicesLength, this.rangekeys, this.rangekeys.length - this.slicesLength, this.slicesLength) > 0) {
      //
      // We only call filterKeyValue when the first range starts at offset 0 (hasHinting is true), so we
      // know we can filter all remaining rows now
      //
      
      done = true;      
      return ReturnCode.NEXT_ROW;
    }
    
    int insertionPoint = findInsertionPoint(subrow);

    int nranges = this.rangekeys.length / this.slicesLength;

    //
    // If the insertion point is >= 0 and < 'nranges' then we know the row is included as it
    // lies on a range key
    //
    
    if (insertionPoint >= 0 && insertionPoint < nranges) {
      // Reset nvalues if range is different from the previous one
      if (this.currentRange != insertionPoint / 2) {
        this.nvalues = this.count;
      }
      this.currentRange = insertionPoint / 2;
      if (this.nvalues > 0 && this.currentRange >= this.minRange) {
        includeRow = true;
        // Decrement number of remaining values to consider
        this.nvalues--;
        //includeCount++;
        return ReturnCode.INCLUDE;        
      } else {
        //
        // Skip to the start of the next range
        //
        
        hintOffset = this.slicesLength * ((0 == insertionPoint % 2) ? insertionPoint + 2 : insertionPoint + 1);
        minRange = (hintOffset / this.slicesLength) / 2;
        this.nvalues = this.count;
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }
    }
    
    //
    // If slice should be inserted before the first range, then we know it's not
    // included in any range
    //
    
    if (-1 == insertionPoint) {
      hintOffset = 0;
      minRange = 0;
      // Re-initialize number of values to consider
      this.nvalues = this.count;
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    
    //
    // If the insertion point is -nranges - 1 this means the slice lies after the last range
    //
    
    if (-nranges - 1 == insertionPoint) {
      done = true;
      return ReturnCode.NEXT_ROW;
    }
    
    //
    // If the insertion point is even, this means the slice lies between valid ranges, so the row should be excluded
    //
    
    if (-(insertionPoint + 1) % 2 == 0) {
      // hintOffset will be the start of the next range
      hintOffset = this.slicesLength * (-(insertionPoint + 1));
      minRange = (hintOffset / this.slicesLength) / 2;
      // Re-initialize number of values to consider
      this.nvalues = this.count;
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    if (this.currentRange != -(insertionPoint + 1) / 2) {
      this.nvalues = this.count;
    }
    
    this.currentRange = -(insertionPoint + 1) / 2;
    
    if (this.nvalues > 0 && this.currentRange >= this.minRange) {
      //
      // Set 'includeRow' to true so we can fast track columns inclusion
      //

      includeRow = true;
      // Decrement number of remaining values to consider
      this.nvalues--;
      //includeCount++;
      return ReturnCode.INCLUDE;          
    } else {
      //
      // Insertion point is odd, the row should have been included if there were still values to retrieve,
      // instead, since we've already return the expected number of values, skip to the next boundary
      //
      
      hintOffset = this.slicesLength * (-(insertionPoint + 1) + 1);
      minRange = (hintOffset / this.slicesLength) / 2;
      // Re-initialize number of values to consider
      this.nvalues = this.count;
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
  }
  
  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    //hintCount++;
    KeyValue hint = null;
        
    if (this.hintOffset >= 0 && this.hintOffset <= this.rangekeys.length - slicesLength) {
      hint = KeyValueUtil.createFirstOnRow(this.rangekeys, this.hintOffset, (short) (this.bounds[1] + 1));
      minRange = (hintOffset / this.slicesLength) / 2;
    } else {
      done = true;
    }

    /*
    byte[] row = currentKV.getRowArray();
    System.out.println("getNextKeyHint " + encodeHex(row, currentKV.getRowOffset(), currentKV.getRowLength()) + " nvalues = " + this.nvalues + " count = " + this.count + " hintOffset = " + hintOffset);
    if (null != hint) {
      row = hint.getRowArray();
      System.out.println("  hint = " + encodeHex(row, hint.getRowOffset(), hint.getRowLength())); 
    } else {
      System.out.println(" hint = null");
    }
    */
    
    return hint;
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
      
      System.arraycopy(row, offset + off, slice, sliceidx, len);
      sliceidx += len;
      boundsidx += 2;      
    }
    
    return slice;
  }
  
  private int findInsertionPoint(byte[] subrow) {    
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
            
      int res = Bytes.compareTo(subrow, 0, subrow.length, this.rangekeys, mid * this.slicesLength, this.slicesLength);

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
        // TODO(hbs): should we use right = mid;
      } else if (res > 0) {
        // If left==right this means the insertion point is after left
        if (right == left) {
          insertionPoint = -(right + 1) - 1;
          break;
        }
        left = mid + 1;
        // TODO(hbs): should we use left = mid;
      }
    }
    
    return insertionPoint;
  }
  
  @Override
  public boolean filterAllRemaining() {
    return done;
  }
  
  public byte[] getStartKey() {
    return Arrays.copyOfRange(this.rangekeys, 0, this.slicesLength);
  }
  
  public byte[] getStopKey() {
    return Arrays.copyOfRange(this.rangekeys, this.rangekeys.length - this.slicesLength, this.rangekeys.length);
  }
 
  /**
   * Serialize the filter
   */
  @Override
  public byte[] toByteArray() throws IOException {
    
    //
    // Allocate buffer for the following data:
    //
    // count: 8 bytes
    // slicesLength: 4 bytes
    // nbounds: 4 bytes (this.bounds.length)
    // bounds: 4 * this.bounds.length
    // Size of range keys: 4 bytes (this.rangekeys.length)
    // slices: this.rangekeys
    //
    
    ByteBuffer bb = ByteBuffer.wrap(new byte[8 + 4 + 4 * this.bounds.length + 4 + 4 + this.rangekeys.length]).order(ByteOrder.BIG_ENDIAN);
    
    bb.putLong(this.count);
    bb.putInt(this.slicesLength);
    bb.putInt(this.bounds.length);
    for (int i = 0; i < this.bounds.length; i++) {
      bb.putInt(this.bounds[i]);
    }
    bb.putInt(this.rangekeys.length);
    bb.put(this.rangekeys);

    return bb.array();
  }
  
  public static SlicedRowFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        
    //System.out.println("parseFrom " + encodeHex(pbBytes));
    
    ByteBuffer bb = ByteBuffer.wrap(pbBytes).order(ByteOrder.BIG_ENDIAN);
    
    SlicedRowFilter filter = new SlicedRowFilter();
    
    filter.count = bb.getLong();
    filter.slicesLength = bb.getInt();
    int nbounds = bb.getInt();
    filter.bounds = new int[nbounds];
    for (int i = 0; i < nbounds; i++) {
      filter.bounds[i] = bb.getInt();
    }
    
    //
    // If the first slice starts at offset 0 then we will be able to provide a key hint
    //
    
    if (0 == filter.bounds[0]) {
      filter.hasHinting = true;
    } else {
      filter.hasHinting = false;
    }

    filter.rangekeys = new byte[bb.getInt()];
    bb.get(filter.rangekeys);
    
    filter.slice = new byte[filter.slicesLength];
    
    return filter;
  }
    
  public String toString() {
    StringBuilder sb = new StringBuilder();
    
    sb.append("\n");
    
    for (int i = 0; i < this.bounds.length; i += 2) {
      sb.append("[");
      sb.append(this.bounds[i]);
      sb.append(",");
      sb.append(this.bounds[i + 1]);
      sb.append("]");
      sb.append("\n");
    }

    sb.append(encodeHex(this.rangekeys));
    /*
    for (Pair<byte[],byte[]> pair: this.ranges) {
      sb.append(" ");
      if (null == pair.getFirst()) {
        sb.append("null");
      } else {
        sb.append(Hex.encodeHexString(pair.getFirst()));
      }
      sb.append(" > ");
      if (null == pair.getSecond()) {
        sb.append("null");
      } else {
        sb.append(Hex.encodeHexString(pair.getSecond()));
      }
      sb.append("\n");
    }
    */
    return sb.toString();
  }
  
  private static final String HEXDIGITS = "0123456789ABCDEF";
  
  private static String encodeHex(byte[] buf) {
    return encodeHex(buf, 0, buf.length);
  }
  private static String encodeHex(byte[] buf, int offset, int len) {
    StringBuilder sb = new StringBuilder();
    
    for (int i = 0; i < len; i++) {
      sb.append(HEXDIGITS.charAt((buf[offset + i] & 0xF0) >>> 4));
      sb.append(HEXDIGITS.charAt(buf[offset + i] & 0xF));
    }
    
    return sb.toString();
  }
}
