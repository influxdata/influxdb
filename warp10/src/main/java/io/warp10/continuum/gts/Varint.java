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

import io.warp10.script.WarpScriptException;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * Utility class for ZigZag encoded varints
 * 
 * @see{https://developers.google.com/protocol-buffers/docs/encoding}
 *
 */
public final class Varint {
  
  /**
   * Encode an unsigned long using varint encoding.
   * @param value Value to encode
   * @return A buffer containing the encoded value.
   */
  public static byte[] encodeUnsignedLongBRANCHMISPREDICT(long value) {
    //
    // Maximum size is 10 bytes
    //

    byte[] buf = null;
    
    /*
    int n = 10;
    
    n -= (0 == (value & 0x8000000000000000L)) ? 1 : 0;
    n -= (0 == (value & 0xff00000000000000L)) ? 1 : 0;
    n -= (0 == (value & 0xfffe000000000000L)) ? 1 : 0;
    n -= (0 == (value & 0xfffffc0000000000L)) ? 1 : 0;
    n -= (0 == (value & 0xfffffff800000000L)) ? 1 : 0;
    n -= (0 == (value & 0xfffffffff0000000L)) ? 1 : 0;
    n -= (0 == (value & 0xffffffffffe00000L)) ? 1 : 0;
    n -= (0 == (value & 0xffffffffffffc000L)) ? 1 : 0;
    n -= (0 == (value & 0xffffffffffffff80L)) ? 1 : 0;
    */
    
    if (0 != (value & 0x8000000000000000L)) {
      buf = new byte[10];
    } else if (0 != (value & 0xff00000000000000L)) {
      buf = new byte[9];
    } else if (0 != (value & 0xfffe000000000000L)) {
      buf = new byte[8];
    } else if (0 != (value & 0xfffffc0000000000L)) {
      buf = new byte[7];
    } else if (0 != (value & 0xfffffff800000000L)) {
      buf = new byte[6];
    } else if (0 != (value & 0xfffffffff0000000L)) {
      buf = new byte[5];
    } else if (0 != (value & 0xffffffffffe00000L)) {
      buf = new byte[4];      
    } else if (0 != (value & 0xffffffffffffc000L)) {
      buf = new byte[3];
    } else if (0 != (value & 0xffffffffffffff80L)) {
      buf = new byte[2];
    } else {
      buf = new byte[1];
    }
    
    
    //buf = new byte[n];
    //byte[] buf = new byte[10];
    
    byte count = 0;
    
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      buf[count++] = (byte) (((int) value & 0x7F) | 0x80);
      value >>>= 7;
    }

    //buf[count++] = (byte) (value & 0x7f);
    buf[count] = (byte) (value & 0x7f);
    
    return buf;
    /*
    if (count < buf.length) {
      System.out.println(buf.length + " " + count + " " + ov + " " + Long.toHexString(ov));
      return Arrays.copyOf(buf, count);
    } else {
      return buf;
    }
    */
    
  }

  public static byte[] encodeUnsignedLong_ORIGINAL(long value) {
    byte[] buf = new byte[10];

    int count = 0;
    
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      buf[count++] = (byte) (((int) value & 0x7F) | 0x80); 
      value >>>= 7;
    }

    buf[count++] = (byte) (value & 0x7f);

    if (count < 10) {
      return Arrays.copyOf(buf, count);
    } else {
      return buf;
    }
  }
  
  public static byte[] encodeUnsignedLong(long value) { // UNROLLED
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[1];
      buf[0] = (byte) (value & 0x7f);
      return buf;
    }

    byte b0 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[2];
      buf[0] = b0;
      buf[1] = (byte) (value & 0x7f);
      return buf;
    }
    
    byte b1 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[3];
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = (byte) (value & 0x7f);
      return buf;
    }

    byte b2 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[4];
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = (byte) (value & 0x7f);
      return buf;
    }

    byte b3 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[5];
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = (byte) (value & 0x7f);
      return buf;
    }

    byte b4 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[6];
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = b4;
      buf[5] = (byte) (value & 0x7f);
      return buf;
    }

    byte b5 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[7];
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = b4;
      buf[5] = b5;
      buf[6] = (byte) (value & 0x7f);
      return buf;
    }

    byte b6 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[8];
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = b4;
      buf[5] = b5;
      buf[6] = b6;
      buf[7] = (byte) (value & 0x7f);
      return buf;
    }

    byte b7 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      byte[] buf = new byte[9];
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = b4;
      buf[5] = b5;
      buf[6] = b6;
      buf[7] = b7;
      buf[8] = (byte) (value & 0x7f);
      return buf;
    }

    byte b8 = (byte) (((int) value & 0x7F) | 0x80);
    value >>>= 7;
    
    byte[] buf = new byte[10];
    buf[0] = b0;
    buf[1] = b1;
    buf[2] = b2;
    buf[3] = b3;
    buf[4] = b4;
    buf[5] = b5;
    buf[6] = b6;
    buf[7] = b7;
    buf[8] = b8;
    buf[9] = (byte) (value & 0x7f);
    return buf;
  }

  /**
   * 
   * @param value Value to encode
   * @param buf buffer to encode in, MUST be large enough
   * @return the number of bytes used for encoding
   */
  public static int encodeUnsignedLongInBuf(long value, byte[] buf) { // UNROLLED 
    int idx = 0;
    while(0 != (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[idx++] = (byte) ((value & 0x7FL) | 0x80);
      value >>>= 7;
    }
    buf[idx++] = (byte) (value & 0x7FL);
    return idx;
    /*
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = (byte) (value & 0x7FL);
      return 1;
    }

    byte b0 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = b0;
      buf[1] = (byte) (value & 0x7FL);
      return 2;
    }
    
    byte b1 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = (byte) (value & 0x7FL);
      return 3;
    }

    byte b2 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = (byte) (value & 0x7FL);
      return 4;
    }

    byte b3 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = (byte) (value & 0x7FL);
      return 5;
    }

    byte b4 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = b4;
      buf[5] = (byte) (value & 0x7FL);
      return 6;
    }

    byte b5 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = b4;
      buf[5] = b5;
      buf[6] = (byte) (value & 0x7FL);
      return 7;
    }

    byte b6 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = b4;
      buf[5] = b5;
      buf[6] = b6;
      buf[7] = (byte) (value & 0x7FL);
      return 8;
    }

    byte b7 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    if (0 == (value & 0xFFFFFFFFFFFFFF80L)) {
      buf[0] = b0;
      buf[1] = b1;
      buf[2] = b2;
      buf[3] = b3;
      buf[4] = b4;
      buf[5] = b5;
      buf[6] = b6;
      buf[7] = b7;
      buf[8] = (byte) (value & 0x7FL);
      return 9;
    }

    byte b8 = (byte) ((value & 0x7FL) | 0x80L);
    value >>>= 7;
    
    buf[0] = b0;
    buf[1] = b1;
    buf[2] = b2;
    buf[3] = b3;
    buf[4] = b4;
    buf[5] = b5;
    buf[6] = b6;
    buf[7] = b7;
    buf[8] = b8;
    buf[9] = (byte) (value & 0x7FL);
    return 10;
    */

  }

  /**
   * Encode a signed long using zig zag varint encoding.
   * 
   * @param value Value to encode
   * @return A buffer containing the encoded value.
   */
  public static int encodeSignedLongInBuf(long value, byte[] buf) {
    //
    // For zig zag varint encoding, we need to modify the value
    // so we alternatively encode negative and positive values
    //
    
    value = (value << 1) ^ (value >> 63);
    
    return encodeUnsignedLongInBuf(value, buf);
  }

  /**
   * Encode a signed long using zig zag varint encoding.
   * 
   * @param value Value to encode
   * @return A buffer containing the encoded value.
   */
  public static byte[] encodeSignedLong(long value) {
    //
    // For zig zag varint encoding, we need to modify the value
    // so we alternatively encode negative and positive values
    //
    
    value = (value << 1) ^ (value >> 63);
    
    return encodeUnsignedLong(value);
  }
  
  /**
   * Decode a varint encoded unsigned long value.
   * 
   * @param buf Buffer containing the encoded value
   * @return The decoded value.
   */
  public static long decodeUnsignedLong(byte[] buf) {    
    long value = 0L;
    int i = 0;
    int index = 0;
    long bytevalue;
    
    while (((bytevalue = buf[index++]) & 0x80L) != 0) {
      value |= (bytevalue & 0x7f) << i;
      i += 7;
      Preconditions.checkArgument(i <= 63, "Variable length quantity is too long");
    }
    
    return value | (bytevalue << i);
  }


  /**
   * Decode a varint encoded signed long value
   * 
   * @param buf Buffer containing the encoded value
   * @return The decoded value
   */
  public static long decodeSignedLong(byte[] buf) {
    long unsigned = decodeUnsignedLong(buf);
    
    //
    // Undo the zig zag trick
    //
    
    long signed = (((unsigned << 63) >> 63) ^ unsigned) >> 1;
    
    //
    // Flip the top bit
    //
    
    return signed ^ (unsigned & (1L << 63));
  }
  
  /**
   * Decode a varint encoded unsigned long value stored in a ByteBuffer.
   * @param buffer The ByteBuffer from which to read the data.
   *               The buffer is assumed to be positioned where the data should be read.
   * @return
   */
  public static long decodeUnsignedLong(ByteBuffer buffer) {
    long value = 0L;
    int i = 0;
    long bytevalue;
    
    while (((bytevalue = buffer.get()) & 0x80L) != 0) {
      value |= (bytevalue & 0x7f) << i;
      i += 7;
      Preconditions.checkArgument(i <= 63, "Variable length quantity is too long");
    }
    
    return value | (bytevalue << i);    
  }
  
  /**
   * Decode a varint encoded signed long value
   * 
   * @param buffer The ByteBuffer from which to read the data.
   *               The buffer is assumed to be positioned where the data should be read.
   * @return
   */
  public static long decodeSignedLong(ByteBuffer buffer) {
    long unsigned = decodeUnsignedLong(buffer);

    //
    // Undo the zig zag trick
    //
    
    long signed = (((unsigned << 63) >> 63) ^ unsigned) >> 1;
    
    //
    // Flip the top bit
    //
    
    return signed ^ (unsigned & (1L << 63));
  }
  
  /**
   * Skip a Varint in a ByteBuffer
   * @return the number of bytes skipped
   */
  public static int skipVarint(ByteBuffer buffer) {
    int skipped = 1;
    long bytevalue;
    
    while(((bytevalue = buffer.get()) & 0x80L) != 0) {
      skipped++;
      Preconditions.checkArgument(skipped <= 9, "Variable length quantity is too long");
    }
        
    return skipped;
  }
  
  /**
   * Decode a varint encoded unsigned long value stored in a ByteBuffer.
   * @param buffer The ByteBuffer from which to read the data.
   *               The buffer is assumed to be positioned where the data should be read.
   * @return
   */
  public static long decodeUnsignedLong(CustomBuffer buffer) {
    long value = 0L;
    int i = 0;
    long bytevalue;
    
    while (((bytevalue = buffer.get()) & 0x80L) != 0) {
      value |= (bytevalue & 0x7f) << i;
      i += 7;
      Preconditions.checkArgument(i <= 63, "Variable length quantity is too long");
    }
    
    return value | (bytevalue << i);    
  }
  
  /**
   * Decode a varint encoded signed long value
   * 
   * @param buffer The ByteBuffer from which to read the data.
   *               The buffer is assumed to be positioned where the data should be read.
   * @return
   */
  public static long decodeSignedLong(CustomBuffer buffer) {
    long unsigned = decodeUnsignedLong(buffer);

    //
    // Undo the zig zag trick
    //
    
    long signed = (((unsigned << 63) >> 63) ^ unsigned) >> 1;
    
    //
    // Flip the top bit
    //
    
    return signed ^ (unsigned & (1L << 63));
  }

}
