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

import java.util.HashMap;
import java.util.Map;

import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;

/**
 * Class used as a repository of keys in memory.
 * <p>
 * Keys can be set after being retrieved from an outside source (i.e. OSS).
 */
public class DummyKeyStore implements KeyStore {

  private final Map<String, byte[]> keys = new HashMap<String, byte[]>();

  /**
   * Instantiates a new Dummy key store.
   */
  public DummyKeyStore() {
    //
    // Initialize key store
    //

    setKey(KeyStore.AES_HBASE_METADATA, new byte[32]);
    setKey(KeyStore.AES_TOKEN, new byte[32]);
    setKey(KeyStore.SIPHASH_CLASS, new byte[16]);
    setKey(KeyStore.SIPHASH_LABELS, new byte[16]);
    //setKey(KeyStore.AES_KAFKA_DATA, new byte[32]);
    //setKey(KeyStore.AES_KAFKA_METADATA, new byte[32]);
    setKey(KeyStore.SIPHASH_KAFKA_DATA, new byte[16]);
    setKey(KeyStore.SIPHASH_TOKEN, new byte[16]);
    setKey(KeyStore.SIPHASH_APPID, new byte[16]);
    //setKey(KeyStore.SIPHASH_KAFKA_METADATA, new byte[16]);
  }

  /**
   * Retrieve a key given its name.
   *
   * @param name Name of key to retrieve.
   * @return The byte array corresponding to this key or null if key is unknown.
   */
  @Override
  public byte[] getKey(String name) {
    return keys.get(name);
  }

  /**
   * Store a key under a name.
   *
   * @param name Name of key.
   * @param bits Byte array containing the key bits.
   */
  @Override
  public void setKey(String name, byte[] bits) {
    keys.put(name, bits);
  }

  @Override
  public byte[] decodeKey(String encoded) {
    if (null == encoded) {
      return null;
    }
    if (encoded.startsWith("hex:")) {
      return Hex.decode(encoded.substring(4));
    } else if (encoded.startsWith("base64:")) {
      return Base64.decode(encoded.substring(7));
    } else {
      return null;
    }
  }

  @Override
  public KeyStore clone() {
    return this;
  }

  @Override
  public void forget() {
  }
}
