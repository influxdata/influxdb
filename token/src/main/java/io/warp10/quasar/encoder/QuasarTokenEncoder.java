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

package io.warp10.quasar.encoder;

import org.apache.commons.codec.binary.Hex;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.TokenType;
import io.warp10.quasar.token.thrift.data.WriteToken;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class QuasarTokenEncoder {

  private final TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

  public String deliverReadToken(String appName, String producerUID, String ownerUID, java.util.List<java.lang.String> apps, long ttl, KeyStore keystore) throws TException {
    ReadToken token = getReadToken(appName, producerUID, Arrays.asList(ownerUID), null, apps, null, null, ttl);
    return cypherToken(token, keystore);
  }

  public String deliverReadToken(String appName, String producerUID, String ownerUID, java.util.List<java.lang.String> apps,  Map<String, String> labels, long ttl, KeyStore keystore) throws TException {
    ReadToken token = getReadToken(appName, producerUID, Arrays.asList(ownerUID), null, apps, labels, null, ttl);
    return cypherToken(token, keystore);
  }

  public String deliverReadToken(String appName, String producerUID, java.util.List<java.lang.String> owners, java.util.List<java.lang.String> apps, java.util.Map<java.lang.String, java.lang.String> hooks, long ttl, KeyStore keystore) throws TException {
    ReadToken token = getReadToken(appName, producerUID, owners, null, apps, null, hooks, ttl);
    return cypherToken(token, keystore);
  }

  public String deliverReadToken(String appName, String producerUID, java.util.List<java.lang.String> owners, java.util.List<java.lang.String> apps,  Map<String, String> labels, java.util.Map<java.lang.String, java.lang.String> hooks, long ttl, KeyStore keystore) throws TException {
    ReadToken token = getReadToken(appName, producerUID, owners, null, apps, labels, hooks, ttl);
    return cypherToken(token, keystore);
  }

  public String deliverReadToken(String appName, String producerUID, java.util.List<java.lang.String> owners, java.util.List<java.lang.String> producers, java.util.List<java.lang.String> apps,  Map<String, String> labels, java.util.Map<java.lang.String, java.lang.String> hooks, long ttl, KeyStore keystore) throws TException {
    ReadToken token = getReadToken(appName, producerUID, owners, producers, apps, labels, hooks, ttl);
    return cypherToken(token, keystore);
  }

  /**
   * @param appName     this token belongs to this application
   * @param producerUID this token belongs to this producer
   * @param owners      this token can access time series owned to these owners
   * @param producers   this token can access time series pushed by these producers
   * @param apps        this token can access time series stored in these applications
   * @param labels      this token can access time series with the given labels
   * @param hooks       tokens Warpscript hooks
   * @param ttl         Time to live (ms)
   * @return ReadToken thrift structure
   * @throws TException
   */
  public ReadToken getReadToken(String appName, String producerUID, java.util.List<java.lang.String> owners, java.util.List<java.lang.String> producers, java.util.List<java.lang.String> apps,  Map<String, String> labels, java.util.Map<java.lang.String, java.lang.String> hooks, long ttl) throws TException {
    long currentTime = System.currentTimeMillis();

    // Generate the READ Tokens
    ReadToken token = new ReadToken();
    token.setAppName(appName);
    token.setIssuanceTimestamp(currentTime);
    token.setExpiryTimestamp(currentTime + ttl);
    token.setTokenType(TokenType.READ);

    // applications
    token.setApps(apps);

    // owners
    if (null != owners && owners.size() > 0) {
      for (String owner : owners) {
        token.addToOwners(toByteBuffer(owner));
      }
    } else {
      token.setOwners(new ArrayList<ByteBuffer>());
    }

    // producers
    if (null != producers && producers.size() > 0) {
      for (String producer : producers) {
        token.addToProducers(toByteBuffer(producer));
      }
    } else {
      token.setProducers(new ArrayList<ByteBuffer>());
    }


    // hooks
    if (null != labels && labels.size() > 0) {
      token.setLabels(labels);
    }

    // hooks
    if (null != hooks && hooks.size() > 0) {
      token.setHooks(hooks);
    }

    // Billing
    token.setBilledId(toByteBuffer(producerUID));
    return token;
  }


  /**
   * Classic Write token owner = producer
   *
   * @param appName  Warp10 application's name
   * @param uuid     producer and owner uuid
   * @param ttl      token time to live (ms)
   * @param keystore
   * @return Warp10 writes token
   * @throws TException
   */
  public String deliverWriteToken(String appName, String uuid, long ttl, KeyStore keystore) throws TException {
    WriteToken token = getWriteToken(appName, uuid, uuid, null, null, ttl);
    return cypherToken(token, keystore);
  }

  public String deliverWriteToken(String appName, String producerUID, String ownerUID, long ttl, KeyStore keystore) throws TException {
    WriteToken token = getWriteToken(appName, producerUID, ownerUID, null, null, ttl);
    return cypherToken(token, keystore);
  }

  public String deliverWriteToken(String appName, String producerUID, String ownerUID, Map<String, String> labels, long ttl, KeyStore keystore) throws TException {
    WriteToken token = getWriteToken(appName, producerUID, ownerUID, labels, null, ttl);
    return cypherToken(token, keystore);
  }

  public WriteToken getWriteToken(String appName, String producerUID, String ownerUID, Map<String, String> labels, List<Long> indices, long ttl) throws TException {
    long currentTime = System.currentTimeMillis();

    WriteToken token = new WriteToken();
    token.setAppName(appName);
    token.setIssuanceTimestamp(currentTime);
    token.setExpiryTimestamp(currentTime + ttl);
    token.setTokenType(TokenType.WRITE);

    if (null != labels && labels.size() > 0) {
      token.setLabels(labels);
    }

    if (null != indices && indices.size() > 0) {
      token.setIndices(indices);
    }

    token.setProducerId(toByteBuffer(producerUID));
    token.setOwnerId(toByteBuffer(ownerUID));
    return token;
  }

  public String cypherToken(TBase<?, ?> token, KeyStore keyStore) throws TException {
    return encryptToken(token, keyStore.getKey(KeyStore.AES_TOKEN), keyStore.getKey(KeyStore.SIPHASH_TOKEN));
  }
  
  public String encryptToken(TBase<?, ?> token, byte[] tokenAESKey, byte[] tokenSipHashKey) throws TException {
    // Serialize the  thrift token into byte array
    byte[] serialized = serializer.serialize(token);

    // Calculate the SIP
    long sip = SipHashInline.hash24_palindromic(tokenSipHashKey, serialized);

    //Create the token byte buffer
    ByteBuffer buffer = ByteBuffer.allocate(8 + serialized.length);
    buffer.order(ByteOrder.BIG_ENDIAN);
    // adds the sip
    buffer.putLong(sip);
    // adds the thrift token
    buffer.put(serialized);

    // Wrap the TOKEN
    byte[] wrappedData = CryptoUtils.wrap(tokenAESKey, buffer.array());

    String accessToken = new String(OrderPreservingBase64.encode(wrappedData));

    return accessToken;
  }

  public String getTokenIdent(String token, KeyStore keystore) {
    byte[] tokenSipHashkey = keystore.getKey(KeyStore.SIPHASH_TOKEN);
    return getTokenIdent(token, tokenSipHashkey);
  }

  public String getTokenIdent(String token, byte[] tokenSipHashkey) {
    long ident = SipHashInline.hash24_palindromic(tokenSipHashkey, token.getBytes());

    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(ident);
    return Hex.encodeHexString(buffer.array());
  }

  public ByteBuffer toByteBuffer(String strUUID) {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.order(ByteOrder.BIG_ENDIAN);

    UUID uuid = UUID.fromString(strUUID);

    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());

    buffer.position(0);
    return buffer;
  }
}
