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

package io.warp10.crypto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * SipHash implementation with hand inlining the SIPROUND.
 * <p>
 * To know details about SipHash, see;
 * "a fast short-input PRF" https://www.131002.net/siphash/
 * <p>
 * SIPROUND is defined in siphash24.c that can be downloaded from the above
 * site. Following license notice is subject to change based on the licensing
 * policy of siphash24.c.
 *
 * @see <a href="https://github.com/nahi/siphash-java-inline">https://github.com/nahi/siphash-java-inline</a>
 */
public class SipHashInline {

  private static final sun.misc.Unsafe UNSAFE;

  static {
    sun.misc.Unsafe unsafe = null;
    try {
      java.lang.reflect.Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (sun.misc.Unsafe) field.get(null);
    } catch (Exception e) {
      e.printStackTrace();
    }
    UNSAFE = unsafe;
  }

  private static final long base = UNSAFE.arrayBaseOffset(new byte[0].getClass());

  /**
   * Hash 24 long.
   *
   * @param key    the key
   * @param data   the data
   * @param offset the offset
   * @param len    the len
   * @return the long
   */
  public static long hash24(byte[] key, byte[] data, int offset, int len) {
    return hash24(key, data, offset, len, false);
  }

  /**
   * Hash 24 long.
   *
   * @param key      the key
   * @param data     the data
   * @param offset   the offset
   * @param len      the len
   * @param reversed the reversed
   * @return the long
   */
  public static long hash24(byte[] key, byte[] data, int offset, int len, boolean reversed) {
    ByteBuffer bb = ByteBuffer.wrap(key);
    bb.order(ByteOrder.BIG_ENDIAN);
    long k0 = bb.getLong();
    long k1 = bb.getLong();

    return hash24(k0, k1, data, offset, len, reversed);
  }

  /**
   * Hash 24 long.
   *
   * @param key  the key
   * @param data the data
   * @return the long
   */
  public static long hash24(byte[] key, byte[] data) {
    return hash24(key, data, 0, data.length);
  }

  /**
   * Hash 24 long.
   *
   * @param k0     the k 0
   * @param k1     the k 1
   * @param data   the data
   * @param offset the offset
   * @param len    the len
   * @return the long
   */
  public static long hash24(long k0, long k1, byte[] data, int offset, int len) {
    return hash24(k0, k1, data, offset, len, false);
  }

  /**
   * Hash 24 long.
   *
   * @param k0       the k 0
   * @param k1       the k 1
   * @param data     the data
   * @param offset   the offset
   * @param len      the len
   * @param reversed the reversed
   * @return the long
   */
  public static long hash24(long k0, long k1, byte[] data, int offset, int len, boolean reversed) {
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

  /**
   * Version of SipHash which computes the hash of the input concatenated with the
   * reversed input in order to minimize the risk of collisions (for a collision to be
   * an actual value, it must have the same structure as the original input, i.e. be a palindrome).
   *
   * @param k0     the k 0
   * @param k1     the k 1
   * @param data   the data
   * @param offset the offset
   * @param len    the len
   * @return long
   */
  public static long hash24_palindromic(long k0, long k1, byte[] data, int offset, int len) {
    long v0 = 0x736f6d6570736575L ^ k0;
    long v1 = 0x646f72616e646f6dL ^ k1;
    long v2 = 0x6c7967656e657261L ^ k0;
    long v3 = 0x7465646279746573L ^ k1;
    long m;
    int last = (2 * len) / 8 * 8;

    // Current position in the buffer
    int idx = 0;

    int offset2 = offset + len + len - 1;

    // processing 8 bytes blocks in data

    int len8 = len - (len % 8);

    if (last > 0) {
      while (idx < len8) {
        /*
        m = (data[offset + idx] & 0xffL);
        idx++;
        m |= (data[offset + idx] & 0xffL) << 8;
        idx++;
        m |= (data[offset + idx] & 0xffL) << 16;
        idx++;
        m |= (data[offset + idx] & 0xffL) << 24;
        idx++;
        m |= (data[offset + idx] & 0xffL) << 32;
        idx++;
        m |= (data[offset + idx] & 0xffL) << 40;
        idx++;
        m |= (data[offset + idx] & 0xffL) << 48;
        idx++;
        m |= (data[offset + idx] & 0xffL) << 56;
        idx++;        
        */

        m = UNSAFE.getLong(data, base + offset + idx);
        idx += 8;

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

      {
        // Use conditional only when around the end of the buffer, otherwise this is too slow

        m = (data[offset + (idx >= len ? (len - 1 - (idx % len)) : idx)] & 0xffL);
        idx++;
        m |= (data[offset + (idx >= len ? (len - 1 - (idx % len)) : idx)] & 0xffL) << 8;
        idx++;
        m |= (data[offset + (idx >= len ? (len - 1 - (idx % len)) : idx)] & 0xffL) << 16;
        idx++;
        m |= (data[offset + (idx >= len ? (len - 1 - (idx % len)) : idx)] & 0xffL) << 24;
        idx++;
        m |= (data[offset + (idx >= len ? (len - 1 - (idx % len)) : idx)] & 0xffL) << 32;
        idx++;
        m |= (data[offset + (idx >= len ? (len - 1 - (idx % len)) : idx)] & 0xffL) << 40;
        idx++;
        m |= (data[offset + (idx >= len ? (len - 1 - (idx % len)) : idx)] & 0xffL) << 48;
        idx++;
        m |= (data[offset + (idx >= len ? (len - 1 - (idx % len)) : idx)] & 0xffL) << 56;
        idx++;


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

      while (idx < last) {
        /*
        m = (data[offset2 - idx] & 0xffL);
        idx++;
        m |= (data[offset2 - idx] & 0xffL) << 8;
        idx++;
        m |= (data[offset2 - idx] & 0xffL) << 16;
        idx++;
        m |= (data[offset2 - idx] & 0xffL) << 24;
        idx++;
        m |= (data[offset2 - idx] & 0xffL) << 32;
        idx++;
        m |= (data[offset2 - idx] & 0xffL) << 40;
        idx++;
        m |= (data[offset2 - idx] & 0xffL) << 48;
        idx++;
        m |= (data[offset2 - idx] & 0xffL) << 56;
        idx++;        
        */

        m = Long.reverseBytes(UNSAFE.getLong(data, base + offset2 - (idx + 7)));
        idx += 8;

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
    }

    // packing the last block to long, as LE 0-7 bytes + the length in the top
    // byte
    m = 0;

    if (0 != idx) {
      // idx != 0 means we have already consumed part of the buffer going forward and backward, i.e. 2*len >= 8
      int i = 0;
      while (i < (len * 2 - idx)) {
        m <<= 8;
        m |= data[offset + i] & 0xffL;
        i++;
      }
    } else {
      // len*2 was < 8, so we need to go over the buffer backward then forward
      for (int i = 0; i < len * 2; i++) {
        m <<= 8;
        m |= data[offset + (i >= len ? (len - 1 - (i % len)) : i)] & 0xffL;
      }
    }
    m |= (long) (len * 2) << 56;

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

  /**
   * Hash 24 palindromic long.
   *
   * @param key    the key
   * @param data   the data
   * @param offset the offset
   * @param len    the len
   * @return the long
   */
  public static long hash24_palindromic(byte[] key, byte[] data, int offset, int len) {
    ByteBuffer bb = ByteBuffer.wrap(key);
    bb.order(ByteOrder.BIG_ENDIAN);
    long k0 = bb.getLong();
    long k1 = bb.getLong();

    return hash24_palindromic(k0, k1, data, offset, len);
  }

  /**
   * Hash 24 palindromic long.
   *
   * @param k0   the k 0
   * @param k1   the k 1
   * @param data the data
   * @return the long
   */
  public static long hash24_palindromic(long k0, long k1, byte[] data) {
    return hash24_palindromic(k0, k1, data, 0, data.length);
  }

  /**
   * Hash 24 palindromic long.
   *
   * @param key  the key
   * @param data the data
   * @return the long
   */
  public static long hash24_palindromic(byte[] key, byte[] data) {
    return hash24_palindromic(key, data, 0, data.length);
  }

  /**
   * Get key long [ ].
   *
   * @param key the key
   * @return the long [ ]
   */
  public static long[] getKey(byte[] key) {

    if (null == key) {
      return null;
    }

    ByteBuffer bb = ByteBuffer.wrap(key);
    bb.order(ByteOrder.BIG_ENDIAN);
    long[] sipkey = new long[2];

    sipkey[0] = bb.getLong();
    sipkey[1] = bb.getLong();

    return sipkey;
  }
}
