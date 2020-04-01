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
import java.util.Map.Entry;

import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;

import java.util.Arrays;

/**
 * Class used as a repository of keys in memory.
 * <p>
 * Keys can be set after being retrieved from an outside source.
 * <p>
 * This KeyStore is dubbed 'Unsecure' because it does not rely on OSS for
 * retrieving a master secret with which all other secrets are encrypted.
 */
public class UnsecureKeyStore implements KeyStore {

  private final Map<String, byte[]> keys = new HashMap<String, byte[]>();

  /**
   * Instantiates a new Unsecure key store.
   */
  public UnsecureKeyStore() {
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
    UnsecureKeyStore uks = new UnsecureKeyStore();

    for (Entry<String, byte[]> entry : keys.entrySet()) {
      uks.setKey(entry.getKey().intern(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
    }

    return uks;
  }

  @Override
  public void forget() {
  }
}
