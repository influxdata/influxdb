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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * This class implements a Base64 like encoding which preserves the lexicographic order of the
 * original byte arrays in the encoded ones.
 * <p>
 * This is needed so we can compare encoded byte arrays without having to first decode them
 */
public class OrderPreservingBase64 {

  /**
   * Padding character, MUST be lexicographically less than the first alphabet letter.
   */
  private static final String PADDING = "-";

  /**
   * Alphabet preserving the ASCII lexicographic ordering.
   */
  private static final byte[] ALPHABET = ".0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.US_ASCII);

  private static final byte[] TEBAHPLA = new byte[256];

  static {
    Arrays.fill(TEBAHPLA, (byte) -1);

    for (int i = 0; i < ALPHABET.length; i++) {
      TEBAHPLA[ALPHABET[i]] = (byte) i;
    }
  }

  /**
   * Encode byte [ ].
   *
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] encode(byte[] data) {
    return encode(data, 0, data.length);
  }

  /**
   * Encode byte [ ].
   *
   * @param data   the data
   * @param offset the offset
   * @param len    the len
   * @return the byte [ ]
   */
  public static byte[] encode(byte[] data, int offset, int len) {
    byte[] encoded = new byte[4 * (len / 3) + (len % 3 != 0 ? 1 + (len % 3) : 0)];

    int idx = 0;

    for (int i = 0; i < len; i++) {
      switch (i % 3) {
        case 0:
          encoded[idx++] = ALPHABET[(data[offset + i] >> 2) & 0x3f];
          break;
        case 1:
          encoded[idx++] = ALPHABET[((data[offset + i - 1] & 0x3) << 4) | ((data[offset + i] >> 4) & 0xf)];
          break;
        case 2:
          encoded[idx++] = ALPHABET[((data[offset + i - 1] & 0xf) << 2) | ((data[offset + i] >> 6) & 0x3)];
          encoded[idx++] = ALPHABET[data[offset + i] & 0x3f];
          break;
      }
    }

    if (idx < encoded.length) {
      switch (len % 3) {
        case 1:
          encoded[idx] = ALPHABET[(data[offset + len - 1] << 4) & 0x30];
          break;
        case 2:
          encoded[idx] = ALPHABET[(data[offset + len - 1] << 2) & 0x3c];
          break;
      }
    }
    return encoded;
  }

  /**
   * Encode to stream.
   *
   * @param data the data
   * @param out  the out
   * @throws IOException the io exception
   */
  public static void encodeToStream(byte[] data, OutputStream out) throws IOException {
    encodeToStream(out, data, 0, data.length);
  }

  /**
   * Encode to stream.
   *
   * @param out     the out
   * @param data    the data
   * @param offset  the offset
   * @param datalen the datalen
   * @throws IOException the io exception
   */
  public static void encodeToStream(OutputStream out, byte[] data, int offset, int datalen) throws IOException {
    int i = 0;

    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);

    int bufidx = 0;

    for (i = offset; i < offset + datalen; i++) {

      switch (bufidx % 3) {
        case 0:
          out.write(ALPHABET[(data[i] >> 2) & 0x3f]);
          break;
        case 1:
          out.write(ALPHABET[((data[i - 1] & 0x3) << 4) | ((data[i] >> 4) & 0xf)]);
          break;
        case 2:
          out.write(ALPHABET[((data[i - 1] & 0xf) << 2) | ((data[i] >> 6) & 0x3)]);
          out.write(ALPHABET[data[i] & 0x3f]);
          break;
      }
      bufidx++;
    }

    if (bufidx < len) {
      switch (datalen % 3) {
        case 1:
          out.write(ALPHABET[(data[offset + datalen - 1] << 4) & 0x30]);
          break;
        case 2:
          out.write(ALPHABET[(data[offset + datalen - 1] << 2) & 0x3c]);
          break;
      }
    }
  }

  /**
   * Encode to writer.
   *
   * @param data the data
   * @param out  the out
   * @throws IOException the io exception
   */
  public static void encodeToWriter(byte[] data, Writer out) throws IOException {
    encodeToWriter(out, data, 0, data.length);
  }

  /**
   * Encode to writer.
   *
   * @param out     the out
   * @param data    the data
   * @param offset  the offset
   * @param datalen the datalen
   * @throws IOException the io exception
   */
  public static void encodeToWriter(Writer out, byte[] data, int offset, int datalen) throws IOException {
    int i = 0;

    int len = 4 * (datalen / 3) + (datalen % 3 != 0 ? 1 + (datalen % 3) : 0);

    int bufidx = 0;

    for (i = offset; i < offset + datalen; i++) {

      switch (bufidx % 3) {
        case 0:
          out.write(ALPHABET[(data[i] >> 2) & 0x3f]);
          break;
        case 1:
          out.write(ALPHABET[((data[i - 1] & 0x3) << 4) | ((data[i] >> 4) & 0xf)]);
          break;
        case 2:
          out.write(ALPHABET[((data[i - 1] & 0xf) << 2) | ((data[i] >> 6) & 0x3)]);
          out.write(ALPHABET[data[i] & 0x3f]);
          break;
      }
      bufidx++;
    }

    if (bufidx < len) {
      switch (datalen % 3) {
        case 1:
          out.write(ALPHABET[(data[offset + datalen - 1] << 4) & 0x30]);
          break;
        case 2:
          out.write(ALPHABET[(data[offset + datalen - 1] << 2) & 0x3c]);
          break;
      }
    }
  }

  /**
   * Decode byte [ ].
   *
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] decode(byte[] data) {
    return decode(data, 0, data.length);
  }

  /**
   * Decode byte [ ].
   *
   * @param data   the data
   * @param offset the offset
   * @param len    the len
   * @return the byte [ ]
   */
  public static byte[] decode(byte[] data, int offset, int len) {
    byte[] decoded = new byte[3 * (len / 4) + (len % 4 != 0 ? (len % 4 - 1) : 0)];

    int idx = 0;
    byte value = 0;

    for (int i = 0; i < len; i++) {
      switch (i % 4) {
        case 0:
          value = (byte) (TEBAHPLA[data[offset + i]] << 2);
          break;
        case 1:
          value |= (byte) ((TEBAHPLA[data[offset + i]] >> 4) & 0x3);
          decoded[idx++] = value;
          value = (byte) ((TEBAHPLA[data[offset + i]] << 4) & 0xf0);
          break;
        case 2:
          value |= (byte) ((TEBAHPLA[data[offset + i]] >> 2) & 0xf);
          decoded[idx++] = value;
          value = (byte) ((TEBAHPLA[data[offset + i]] << 6) & 0xc0);
          break;
        case 3:
          value |= TEBAHPLA[data[offset + i]];
          decoded[idx++] = value;
          break;
      }
    }

    // FIXME(hbs)
    if (idx < decoded.length) {
      decoded[idx++] = value;
    }

    return decoded;
  }
}
