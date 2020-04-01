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

import com.geoxp.oss.CryptoHelper;
import com.geoxp.oss.OSSException;
import com.geoxp.oss.client.OSSClient;

import java.util.Arrays;

/**
 * KeyStore implementation which uses a master key stored in OSS.
 *
 * @see <a href="http://github.com/hbs/oss">http://github.com/hbs/oss</a>
 */
public class OSSKeyStore implements KeyStore {

  private byte[] masterKey;

  private final Map<String, byte[]> keys;

  private OSSKeyStore() {
    this.keys = new HashMap<String, byte[]>();
  }

  /**
   * Instantiates a new Oss key store.
   *
   * @param masterKeySpec the master key spec
   * @throws OSSException the oss exception
   */
  public OSSKeyStore(String masterKeySpec) throws OSSException {
    this.keys = new HashMap<String, byte[]>();

    //
    // Determine if masterKey is a name or a valid key
    //

    this.masterKey = null;

    if (null != masterKeySpec) {
      try {
        this.masterKey = decodeKey(masterKeySpec);
      } catch (Exception e) {
        // empty
      }

      if (null == this.masterKey) {
        this.masterKey = OSSClient.getSecret(System.getProperty("oss.url"), masterKeySpec, System.getProperty("oss.sshkey"));
      }
    }
  }

  @Override
  public void setKey(String name, byte[] key) {
    if (null != key) {
      this.keys.put(name, Arrays.copyOf(key, key.length));
    }
  }

  @Override
  public byte[] getKey(String name) {
    // FIXME(hbs): for security in untrusted environments we should really
    // return a copy of the key space, but we'll assume we're in a trusted env.
    return this.keys.get(name);
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
    } else if (encoded.startsWith("wrapped:")) {
      if (null == this.masterKey) {
        throw new RuntimeException("Master Key not retrieved from OSS, aborting.");
      }
      return CryptoHelper.unwrapBlob(this.masterKey, decodeKey(encoded.substring(8)));
    } else {
      return null;
    }
  }

  @Override
  public KeyStore clone() {
    OSSKeyStore keystore = new OSSKeyStore();

    if (null != this.masterKey) {
      keystore.masterKey = Arrays.copyOf(this.masterKey, this.masterKey.length);
    }

    for (Entry<String, byte[]> entry : this.keys.entrySet()) {
      keystore.setKey(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
    }

    return keystore;
  }

  /**
   * Forget the master key
   */
  @Override
  public void forget() {
    Arrays.fill(this.masterKey, (byte) 0x00);
  }
}
