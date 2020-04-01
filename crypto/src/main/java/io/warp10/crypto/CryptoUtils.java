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

import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;

import com.google.common.primitives.Longs;

import java.util.Arrays;

/**
 * The type Crypto utils.
 */
public class CryptoUtils {

  /**
   * Wrap byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] wrap(byte[] key, byte[] data) {
    AESWrapEngine engine = new AESWrapEngine();
    KeyParameter params = new KeyParameter(key);
    engine.init(true, params);
    PKCS7Padding padding = new PKCS7Padding();
    byte[] unpadded = data;

    //
    // Add padding
    //

    byte[] padded = new byte[unpadded.length + (8 - unpadded.length % 8)];
    System.arraycopy(unpadded, 0, padded, 0, unpadded.length);
    padding.addPadding(padded, unpadded.length);

    //
    // Wrap
    //

    byte[] encrypted = engine.wrap(padded, 0, padded.length);

    return encrypted;
  }

  /**
   * Unwrap byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] unwrap(byte[] key, byte[] data) {
    //
    // Decrypt the encrypted data
    //

    AESWrapEngine engine = new AESWrapEngine();
    CipherParameters params = new KeyParameter(key);
    engine.init(false, params);

    try {
      byte[] decrypted = engine.unwrap(data, 0, data.length);
      //
      // Unpad the decrypted data
      //

      PKCS7Padding padding = new PKCS7Padding();
      int padcount = padding.padCount(decrypted);

      //
      // Remove padding
      //

      decrypted = Arrays.copyOfRange(decrypted, 0, decrypted.length - padcount);

      return decrypted;
    } catch (InvalidCipherTextException icte) {
      return null;
    }
  }

  /**
   * Add mac byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] addMAC(long[] key, byte[] data) {
    long hash = SipHashInline.hash24_palindromic(key[0], key[1], data);

    byte[] authenticated = Arrays.copyOf(data, data.length + 8);
    System.arraycopy(Longs.toByteArray(hash), 0, authenticated, data.length, 8);

    return authenticated;
  }

  /**
   * Add mac byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] addMAC(byte[] key, byte[] data) {
    long hash = SipHashInline.hash24_palindromic(key, data);

    byte[] authenticated = Arrays.copyOf(data, data.length + 8);
    System.arraycopy(Longs.toByteArray(hash), 0, authenticated, data.length, 8);

    return authenticated;
  }

  /**
   * Check the MAC and return the verified content or null if the verification failed
   *
   * @param key  the key
   * @param data the data
   * @return byte [ ]
   */
  public static byte[] removeMAC(byte[] key, byte[] data) {
    long hash = SipHashInline.hash24_palindromic(key, data, 0, data.length - 8);
    long mac = Longs.fromByteArray(Arrays.copyOfRange(data, data.length - 8, data.length));

    if (mac == hash) {
      return Arrays.copyOf(data, data.length - 8);
    } else {
      return null;
    }
  }

  /**
   * Remove mac byte [ ].
   *
   * @param key  the key
   * @param data the data
   * @return the byte [ ]
   */
  public static byte[] removeMAC(long[] key, byte[] data) {
    long hash = SipHashInline.hash24_palindromic(key[0], key[1], data, 0, data.length - 8);
    long mac = Longs.fromByteArray(Arrays.copyOfRange(data, data.length - 8, data.length));

    if (mac == hash) {
      return Arrays.copyOf(data, data.length - 8);
    } else {
      return null;
    }
  }

  /**
   * Compute a safe SipHash of a given input by computing the
   * forward hash, the hash of the reversed input and finally the
   * hash of the two concatenated hashes.
   * <p>
   * This should prevent having meaningful collisions. If two contents have colliding hashes,
   * this means that the concatenation of their forward and reverse hashes are collisions for SipHah, quite unlikely.
   *
   * @param k0  the k 0
   * @param k1  the k 1
   * @param msg the msg
   * @return long
   */
  public static long safeSipHash(long k0, long k1, byte[] msg) {
    byte[] a = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(a).order(ByteOrder.BIG_ENDIAN);

    bb.putLong(SipHashInline.hash24(k0, k1, msg, 0, msg.length));
    bb.putLong(SipHashInline.hash24(k0, k1, msg, 0, msg.length, true));

    return SipHashInline.hash24(k0, k1, a, 0, a.length, false);
  }

  /**
   * Invert byte [ ].
   *
   * @param key the key
   * @return the byte [ ]
   */
  public static byte[] invert(byte[] key) {
    byte[] inverted = Arrays.copyOf(key, key.length);
    for (int i = 0; i < inverted.length; i++) {
      inverted[i] = (byte) (inverted[i] & 0xFF);
    }
    return inverted;
  }
}
