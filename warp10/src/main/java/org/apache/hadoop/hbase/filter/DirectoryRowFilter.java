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

import io.warp10.continuum.store.Directory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Filter used by Directory to select rows
 * 
 * There are two levels of filtering, the first one is to select the rows which contain
 * the metadata which will be managed by a given Directory instance, the second one is
 * to select among those the rows retrieved by each thread.
 *
 * Both levels are specified as modulus / remainder
 *
 */
public class DirectoryRowFilter extends FilterBase {
  
  private static final sun.misc.Unsafe UNSAFE;

  /**
   * row key prefix for metadata
   */
  public static final byte[] HBASE_METADATA_KEY_PREFIX = "M".getBytes(StandardCharsets.UTF_8);
  
  static {
    sun.misc.Unsafe unsafe = null;
    try {
      java.lang.reflect.Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (sun.misc.Unsafe)field.get(null);
    } catch (Exception e) {
      e.printStackTrace();
    }
    UNSAFE = unsafe;
  }
  
  private static final long base = UNSAFE.arrayBaseOffset(new byte[0].getClass());
  
  private long instanceModulus;
  private long instanceRemainder;
  private long threadModulus;
  private long threadRemainder;
  
  private static final long k0 = 0xE0978629FE9E40FAL;
  private static final long k1 = 0xA1CBB3357588E9ADL;
  private static final long pad = 0x8FB1184805B2AEBCL;
  
  public DirectoryRowFilter() {   
  }
  
  public DirectoryRowFilter(long instanceModulus, long instanceRemainder, long threadModulus, long threadRemainder) {
    this.instanceModulus = instanceModulus;
    this.instanceRemainder = instanceRemainder;
    this.threadModulus = threadModulus;
    this.threadRemainder = threadRemainder;  
  }
     
  /**
   * @see HBASE-9717 for an API change suggestion that would speed up scanning.
   */
  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    
    //
    // Extract the 'long' which we will use to determine if we filter out the row or not,
    // we use the SipHash of the classid+labels id part of the row key so we distribute the values to mitigate hot
    // classes or label sets
    //
    
    long hash = computeHash(buffer, offset + HBASE_METADATA_KEY_PREFIX.length, 8);

    // If the row should not be included for the calling instance, filter it now
    
    if (hash % instanceModulus != instanceRemainder) {
      return true;
    }
    
    // XOR the hash with the pad constant so we select the row for the thread based
    // on another hash value
    
    hash = hash ^ pad;
    
    // If the row should not be included for the calling thread, filter it
    
    if (hash % threadModulus != threadRemainder) {
      return true;
    }
    
    //
    // Default behaviour is to include the row
    //
    
    return false;
  }
  
  @Override
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    return ReturnCode.INCLUDE;
  }
  
  /**
   * Serialize the filter
   */
  @Override
  public byte[] toByteArray() throws IOException {
    
    ByteBuffer bb = ByteBuffer.wrap(new byte[4 * 8]).order(ByteOrder.BIG_ENDIAN);
    
    bb.putLong(instanceModulus);
    bb.putLong(instanceRemainder);
    bb.putLong(threadModulus);
    bb.putLong(threadRemainder);
    
    return bb.array();
  }
  
  public static DirectoryRowFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        
    ByteBuffer bb = ByteBuffer.wrap(pbBytes).order(ByteOrder.BIG_ENDIAN);
    
    DirectoryRowFilter filter = new DirectoryRowFilter();

    filter.instanceModulus = bb.getLong();
    filter.instanceRemainder = bb.getLong();
    filter.threadModulus = bb.getLong();
    filter.threadRemainder = bb.getLong();
    
    return filter;
  }
  
  public static final long computeHash(byte[] buffer, int offset, int len) {
    return hash24(k0, k1, buffer, offset, len, false);
  }
  
  private static long hash24(long k0, long k1, byte[] data, int offset, int len, boolean reversed) {
    long v0 = 0x736f6d6570736575L ^ k0;
    long v1 = 0x646f72616e646f6dL ^ k1;
    long v2 = 0x6c7967656e657261L ^ k0;
    long v3 = 0x7465646279746573L ^ k1;
    long m;
    int last = len / 8 * 8;
    
    int i = 0;

    // processing 8 bytes blocks in data
    while (i < last) {
      // pack a block to long, as LE 8 bytes
      /*
      m = (data[!reversed ? offset + i++ : offset + (len - 1) - i++] & 0xffL)
          | (data[!reversed ? offset + i++ : offset + (len - 1) - i++] & 0xffL) << 8
          | (data[!reversed ? offset + i++ : offset + (len - 1) - i++] & 0xffL) << 16
          | (data[!reversed ? offset + i++ : offset + (len - 1) - i++] & 0xffL) << 24
          | (data[!reversed ? offset + i++ : offset + (len - 1) - i++] & 0xffL) << 32
          | (data[!reversed ? offset + i++ : offset + (len - 1) - i++] & 0xffL) << 40
          | (data[!reversed ? offset + i++ : offset + (len - 1) - i++] & 0xffL) << 48
          | (data[!reversed ? offset + i++ : offset + (len - 1) - i++] & 0xffL) << 56;
      */
      
      if (!reversed) {
        m = UNSAFE.getLong(data, base + offset + i);
        //m = Long.reverseBytes(m);
      } else {
        m = UNSAFE.getLong(data, base + offset + len - 1 - (i + 7));
        m = Long.reverseBytes(m);
      }
      
      i += 8;
      
      // MSGROUND {
      v3 ^= m;

      /*
       * SIPROUND wih hand reordering
       * 
       * SIPROUND in siphash24.c:
       * A: v0 += v1;
       * B: v1=ROTL(v1,13);
       * C: v1 ^= v0;
       * D: v0=ROTL(v0,32);
       * E: v2 += v3;
       * F: v3=ROTL(v3,16);
       * G: v3 ^= v2;
       * H: v0 += v3;
       * I: v3=ROTL(v3,21);
       * J: v3 ^= v0;
       * K: v2 += v1;
       * L: v1=ROTL(v1,17);
       * M: v1 ^= v2;
       * N: v2=ROTL(v2,32);
       * 
       * Each dependency:
       * B -> A
       * C -> A, B
       * D -> C
       * F -> E
       * G -> E, F
       * H -> D, G
       * I -> H
       * J -> H, I
       * K -> C, G
       * L -> K
       * M -> K, L
       * N -> M
       * 
       * Dependency graph:
       * D -> C -> B -> A
       * G -> F -> E
       * J -> I -> H -> D, G
       * N -> M -> L -> K -> C, G
       * 
       * Resulting parallel friendly execution order:
       * -> ABCDHIJ
       * -> EFGKLMN
       */

      // SIPROUND {
      v0 += v1;
      v2 += v3;
      v1 = (v1 << 13) | v1 >>> 51;
      v3 = (v3 << 16) | v3 >>> 48;
      v1 ^= v0;
      v3 ^= v2;
      v0 = (v0 << 32) | v0 >>> 32;
      v2 += v1;
      v0 += v3;
      v1 = (v1 << 17) | v1 >>> 47;
      v3 = (v3 << 21) | v3 >>> 43;
      v1 ^= v2;
      v3 ^= v0;
      v2 = (v2 << 32) | v2 >>> 32;
      // }
      // SIPROUND {
      v0 += v1;
      v2 += v3;
      v1 = (v1 << 13) | v1 >>> 51;
      v3 = (v3 << 16) | v3 >>> 48;
      v1 ^= v0;
      v3 ^= v2;
      v0 = (v0 << 32) | v0 >>> 32;
      v2 += v1;
      v0 += v3;
      v1 = (v1 << 17) | v1 >>> 47;
      v3 = (v3 << 21) | v3 >>> 43;
      v1 ^= v2;
      v3 ^= v0;
      v2 = (v2 << 32) | v2 >>> 32;
      // }
      v0 ^= m;
      // }
    }

    // packing the last block to long, as LE 0-7 bytes + the length in the top
    // byte
    m = 0;
    for (i = len - 1; i >= last; --i) {
      m <<= 8;
      m |= data[!reversed ? offset + i : offset + (len - 1) - i] & 0xffL;
    }
    m |= (long) len << 56;
    
    // MSGROUND {
    v3 ^= m;
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    v0 ^= m;
    // }

    // finishing...
    v2 ^= 0xff;
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    // SIPROUND {
    v0 += v1;
    v2 += v3;
    v1 = (v1 << 13) | v1 >>> 51;
    v3 = (v3 << 16) | v3 >>> 48;
    v1 ^= v0;
    v3 ^= v2;
    v0 = (v0 << 32) | v0 >>> 32;
    v2 += v1;
    v0 += v3;
    v1 = (v1 << 17) | v1 >>> 47;
    v3 = (v3 << 21) | v3 >>> 43;
    v1 ^= v2;
    v3 ^= v0;
    v2 = (v2 << 32) | v2 >>> 32;
    // }
    return v0 ^ v1 ^ v2 ^ v3;
  }

}
