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

package io.warp10.quasar.filter;

import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.encoder.QuasarTokenDecoder;
import io.warp10.quasar.filter.exception.QuasarNoToken;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.filter.exception.QuasarTokenExpired;
import io.warp10.quasar.filter.sensision.QuasarTokenFilterSensisionConstants;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.quasar.trl.QuasarTokenRevocationListLoader;
import io.warp10.sensision.Sensision;

import com.google.common.base.Strings;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class QuasarTokenFilter {

  private final QuasarTokenDecoder quasarTokenDecoder;
  private final QuasarTokensRevoked quasarTokenRevoked;
  private Properties properties;

  private long tokenSipHashKeyK0;
  private long tokenSipHashKeyK1;

  // custom key constructor
  private QuasarTokenFilter(Properties props, KeyStore keystore, String tokenAesKeyName) {
    this.properties = (Properties) props.clone();
    if( keystore == null ) {
      throw new RuntimeException("keystore is null");
    }

    ByteBuffer bb = ByteBuffer.wrap(getKey(keystore, KeyStore.SIPHASH_TOKEN));
    bb.order(ByteOrder.BIG_ENDIAN);
    tokenSipHashKeyK0 = bb.getLong();
    tokenSipHashKeyK1 = bb.getLong();

    byte[] appSipHashKey = getKey(keystore,KeyStore.SIPHASH_APPID);
    byte[] tokenAESKey = getKey(keystore, tokenAesKeyName);

    this.quasarTokenDecoder = new QuasarTokenDecoder(tokenSipHashKeyK0, tokenSipHashKeyK1, tokenAESKey);
    this.quasarTokenRevoked = new QuasarTokensRevoked(properties, appSipHashKey);
  }

  // default contructor
  public QuasarTokenFilter(Properties props, KeyStore keystore) {
    this(props, keystore, KeyStore.AES_TOKEN);
  }

  public ReadToken getReadToken(String cryptedToken) throws QuasarTokenException {      // Decode the token hex string to byte array
    long now = System.nanoTime();
    Map<String,String> labels = new HashMap<>();
    try {
      labels.put("type", "READ");
      // Check if the filter is correctly initialized
      quasarTokenRevoked.available();

      if (Strings.isNullOrEmpty(cryptedToken)) {
        throw new QuasarNoToken("Read token missing.");
      }

      byte[] tokenB64Data = cryptedToken.getBytes();

      // check if the token is revoked by the owner
      quasarTokenRevoked.isTokenRevoked(getTokenSipHash(tokenB64Data));

      // Decode the token hex string to byte array
      byte[] tokenHexData = OrderPreservingBase64.decode(tokenB64Data);

      // decode the read token
      ReadToken token = quasarTokenDecoder.decodeReadToken(tokenHexData);

      // compute the app id
      long appId = QuasarTokenRevocationListLoader.getApplicationHash(token.getAppName());

      // check the token expiration
      checkTokenExpired(token.getIssuanceTimestamp(), token.getExpiryTimestamp(), appId);

      // check the registered application status
      quasarTokenRevoked.isRegisteredAppAuthorized(appId);

      return token;
    } catch(QuasarTokenException qexp) {
      labels.put("error", qexp.label);
      throw  qexp;
    } catch(Exception exp) {
      throw new QuasarTokenException("Read token unexpected error.", exp);
    } finally {
      long elapsedTimeUs= (System.nanoTime()-now)/1000;
      Sensision.update(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TOKEN_COUNT, labels, 1);
      Sensision.update(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TOKEN_TIME_US, labels, elapsedTimeUs);
    }
  }

  public boolean available() {
    return quasarTokenRevoked.loaded();
  }

  public WriteToken getWriteToken(String cryptedToken) throws QuasarTokenException {
    long now = System.nanoTime();
    Map<String,String> labels = new HashMap<>();
    try {
      labels.put("type", "WRITE");

      // Check if the filter is correctly initialized
      quasarTokenRevoked.available();

      if( Strings.isNullOrEmpty(cryptedToken) ) {
        throw new QuasarNoToken("Write token missing.");
      }

      byte[] tokenB64Data = cryptedToken.getBytes();

      // check if the token is revoked by the owner
      quasarTokenRevoked.isTokenRevoked(getTokenSipHash(tokenB64Data));

      // Decode the token hex string to byte array
      byte[] tokenHexData = OrderPreservingBase64.decode(tokenB64Data);

      // decode the write token
      WriteToken token = quasarTokenDecoder.decodeWriteToken(tokenHexData);

      // compute the app id based on the app name
      long appId = QuasarTokenRevocationListLoader.getApplicationHash(token.getAppName());

      // check the token expiration
      checkTokenExpired(token.getIssuanceTimestamp(), token.getExpiryTimestamp(), appId);

      // check the registered application status
      quasarTokenRevoked.isRegisteredAppAuthorized(appId);

      return token;
    } catch(QuasarTokenException qexp) {
      labels.put("error", qexp.label);
      throw  qexp;
    } catch(Exception exp) {
      throw new QuasarTokenException("Write token unexpected error.", exp);
    } finally {
      long elapsedTimeUs= (System.nanoTime()-now)/1000;
      Sensision.update(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TOKEN_COUNT, labels, 1);
      Sensision.update(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TOKEN_TIME_US, labels, elapsedTimeUs);
    }
  }

  /**
   * Extract a key of the keystore,
   * @param keystore the Keystore
   * @param name Name of the key
   * @return byte array
   */
  private  byte[] getKey(KeyStore keystore, String name) {
    byte[] key = keystore.getKey(name);

    if( key == null ) {
      throw new RuntimeException("key not found: " + name);
    }

    return key;
  }

  private long getTokenSipHash(byte[] nolookupToken) {
    return SipHashInline.hash24_palindromic(tokenSipHashKeyK0, tokenSipHashKeyK1, nolookupToken, 0, nolookupToken.length);
  }

  private void checkTokenExpired(long issuanceTimestamp, long expiryTimestamp, long clientId) throws QuasarTokenExpired {
    // check the token expiration
    if (isExpired(issuanceTimestamp, expiryTimestamp, clientId)) {
      throw new QuasarTokenExpired("Token Expired.");
    }
  }

  private boolean isExpired(long issuanceTimestamp, long expiryTimestamp, long clientId ) {
    // check the token expiration
    if (expiryTimestamp < System.currentTimeMillis()) {
      return true;
    }

    // test if this token must be refreshed
    Long endOfValidity = quasarTokenRevoked.getClientIdRefreshTimeStamp(clientId);

    if (endOfValidity!=null && issuanceTimestamp < endOfValidity) {
      return true;
    }

    // the token is valid
    return false;
  }
}
