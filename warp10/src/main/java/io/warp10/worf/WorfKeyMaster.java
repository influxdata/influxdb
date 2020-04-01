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

package io.warp10.worf;


import com.geoxp.oss.OSSException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.warp10.continuum.Configuration;
import io.warp10.crypto.*;
import io.warp10.quasar.encoder.QuasarTokenDecoder;
import io.warp10.quasar.encoder.QuasarTokenEncoder;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.TokenType;
import io.warp10.quasar.token.thrift.data.WriteToken;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class WorfKeyMaster {
  Properties config;
  KeyStore keyStore = null;
  QuasarTokenEncoder encoder = new QuasarTokenEncoder();
  QuasarTokenDecoder decoder = null;

  public WorfKeyMaster(Properties config) {
    this.config = config;

  }

  public boolean loadKeyStore() {
    try {
      keyStore = readKeyStore(config);

      byte[] aesTokenkey = keyStore.getKey(KeyStore.AES_TOKEN);
      ByteBuffer bb = ByteBuffer.wrap(keyStore.getKey(KeyStore.SIPHASH_TOKEN));
      bb.order(ByteOrder.BIG_ENDIAN);
      decoder = new QuasarTokenDecoder(bb.getLong(), bb.getLong(), aesTokenkey);
    } catch (OSSException e) {
      e.printStackTrace();
    }

    return (keyStore != null);
  }

  public String deliverReadToken(String application, List<String> applications, String producer, List<String> owners, long ttl) throws WorfException {
    return deliverReadToken(application, applications, producer, null, owners, null, ttl);
  }

  public String deliverReadToken(String application, List<String> applications, String producer, List<String> owners, Map<String,String> labels, long ttl) throws WorfException {
    return deliverReadToken(application, applications, producer, null, owners, labels, ttl);
  }

  public String deliverReadToken(String application, List<String> applications, String producer, List<String> producers, List<String> owners, Map<String,String> labels, long ttl) throws WorfException {
    try {
      return encoder.deliverReadToken(application, producer, owners, producers, applications, labels, null, ttl, keyStore);
    } catch (TException e) {
     throw new WorfException("Unable to deliver read token cause=" + e.getMessage());
    }
  }

  public String deliverWriteToken(String application, String producer, String owner, long ttl) throws WorfException {
    return deliverWriteToken(application, producer, owner, null, ttl);
  }

  public String deliverWriteToken(String application, String producer, String owner, Map<String,String> labels, long ttl) throws WorfException {
    try {
      // add label only if the map contains values
      Map<String,String> fixedLabels = null;

      if (null != labels && labels.size() > 0) {
        fixedLabels = labels;
      }
      return encoder.deliverWriteToken(application, producer, owner, fixedLabels, ttl, keyStore);
    } catch (TException e) {
      throw new WorfException("Unable to deliver write token cause=" + e.getMessage());
    }
  }

  public TBase<?,?> decodeToken(String token) {
    byte[] rawToken = OrderPreservingBase64.decode(token.getBytes());

    try {
      return decoder.decodeReadToken(rawToken);
    } catch (QuasarTokenException e) {
      if (WorfCLI.verbose) {
        e.printStackTrace();
      }
    }
    try {
      return decoder.decodeWriteToken(rawToken);
    } catch (QuasarTokenException e) {
      if (WorfCLI.verbose) {
        e.printStackTrace();
      }
    }
    return null;
  }

  public String convertToken(WriteToken writeToken) throws WorfException {
    try {
      // Generate the READ Tokens
      ReadToken token = new ReadToken();
      token.setAppName(writeToken.getAppName());
      token.setIssuanceTimestamp(writeToken.getIssuanceTimestamp());
      token.setExpiryTimestamp(writeToken.getExpiryTimestamp());
      token.setTokenType(TokenType.READ);

      token.setApps(Arrays.asList(writeToken.getAppName()));

      token.addToOwners(ByteBuffer.wrap(writeToken.getOwnerId()));
      token.setProducers(new ArrayList<ByteBuffer>());

      // Billing
      token.setBilledId(writeToken.getProducerId());
      return encoder.cypherToken(token, keyStore);
    } catch (TException e) {
      throw new WorfException("Unable to convert this write token", e);
    }
  }

  public String getTokenIdent(String token) {
    return encoder.getTokenIdent(token, keyStore);
  }

  private KeyStore readKeyStore(Properties properties) throws OSSException {

    KeyStore keystore;

    if (config.containsKey(Configuration.OSS_MASTER_KEY)) {
      keystore = new OSSKeyStore(properties.getProperty(Configuration.OSS_MASTER_KEY));
    } else {
      keystore = new UnsecureKeyStore();
    }

    keystore = loadAESKey(KeyStore.AES_TOKEN, keystore, config.getProperty(Configuration.WARP_AES_TOKEN));
    keystore = loadKey(KeyStore.SIPHASH_TOKEN, keystore, config.getProperty(Configuration.WARP_HASH_TOKEN));
    keystore = loadKey(KeyStore.SIPHASH_APPID, keystore, config.getProperty(Configuration.WARP_HASH_APP));

    return keystore;
  }

  private KeyStore loadKey(String keyname, KeyStore keystore, String hashClass) {
    if (Strings.isNullOrEmpty(hashClass)) {
      return null;
    }
    keystore.setKey(keyname, keystore.decodeKey(hashClass));
    Preconditions.checkArgument(16 == keystore.getKey(keyname).length, keyname + " MUST be 128 bits long.");
    return keystore;
  }

  private KeyStore loadAESKey(String keyname, KeyStore keystore, String hashClass) {
    if (Strings.isNullOrEmpty(hashClass)) {
      return null;
    }
    keystore.setKey(keyname, keystore.decodeKey(hashClass));
    int keyLength = keystore.getKey(keyname).length;
    Preconditions.checkArgument((16 == keyLength) || (24 == keyLength) || (32 == keyLength), keyname + " MUST be 128, 192 or 256 bits long.");
    return keystore;
  }
}
