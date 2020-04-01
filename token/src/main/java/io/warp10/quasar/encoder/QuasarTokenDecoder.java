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

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;

import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.filter.exception.QuasarTokenInvalid;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.TokenType;
import io.warp10.quasar.token.thrift.data.WriteToken;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *
 * Decode a read /write token
 * The token have the given structure
 *
 * AES_WRAPPING( SIPHASH | SERIALIZED_TOKEN)
 * SIPHASH = hash24_palindromic( SERIALIZED_TOKEN )
 * SERIALIZED_TOKEN = Serialization of a Thrift ReadToken or Write token with TCompactProtocol
 *
 */
public class QuasarTokenDecoder {

  private long tokenSipHashKeyK0;
  private long tokenSipHashKeyK1;
  private byte[] tokenAESKey;

  public QuasarTokenDecoder( long tokenSipHashKeyK0, long tokenSipHashKeyK1, byte[] tokenAESKey) {
    this.tokenSipHashKeyK0 =  tokenSipHashKeyK0;
    this.tokenSipHashKeyK1 = tokenSipHashKeyK1;
    this.tokenAESKey = tokenAESKey;
  }

  public ReadToken decodeReadToken(byte[] nolookupToken) throws QuasarTokenException {
    ReadToken token = (ReadToken) decodeToken(nolookupToken, new ReadToken());

    if (!TokenType.READ.equals(token.getTokenType())) {
      throw new QuasarTokenInvalid("Token type invalid (READ expected).");
    }

    return token;
  }

  public WriteToken decodeWriteToken(byte[] nolookupToken) throws QuasarTokenException {
    WriteToken token = (WriteToken) decodeToken(nolookupToken, new WriteToken());

    if (!TokenType.WRITE.equals(token.getTokenType())) {
      throw new QuasarTokenInvalid("Token type invalid (WRITE expected).");
    }

    return token;
  }


  /**
   * Decode an encrypted toke into an instance of ReadToken or WriteToken
   * TBase are used to avoid explicit cast
   *
   * @param nolookupToken the given encrypted token
   * @param thriftToken Empty instance of the token to decode ( ReadToken or WriteToken )
   * @return the ReadToken or WriteToken decoded
   * @throws QuasarTokenException
   */
  protected TBase<?, ?> decodeToken(byte[] nolookupToken, TBase<?, ?> thriftToken) throws QuasarTokenException {
    try {
      //  Uncrypt the token
      byte[] uncrypted = CryptoUtils.unwrap(tokenAESKey, nolookupToken);

      if (uncrypted == null) {
        // the token is corrupted
        throw new QuasarTokenInvalid("AES_INVALID", "Invalid token.");
      }

      // Extract the SIP Hash of the token
      // The 8 first bytes of the token represents the sip ordered in Big endian
      long tokenSIPHash = ByteBuffer.wrap(uncrypted, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();

      // check the token integrity
      checkTokenIntegrity(tokenSIPHash, uncrypted);

      // Deserialize the token into a Thrift Object
      deserializeThriftToken(thriftToken, uncrypted);

      return thriftToken;
    } catch (QuasarTokenException quasarExp) {
      throw quasarExp;
    } catch (Exception exp) {
      //Hum something bad appends
      throw new QuasarTokenInvalid("Invalid token.");
    }
  }

  private void checkTokenIntegrity(long sipHash, byte[] thriftToken) throws QuasarTokenInvalid {
    int tokenLength = thriftToken.length - 8;

    // Compute the sip hash
    long tokenSipHash = SipHashInline.hash24_palindromic(tokenSipHashKeyK0, tokenSipHashKeyK1, thriftToken, 8, tokenLength);

    // check the hash integrity
    if (tokenSipHash != sipHash) {
      throw new QuasarTokenInvalid("SIP_INVALID", "Invalid token.");
    }
  }

  /**
   * Deserialize the given byte array into any type of Thrift tokens
   * This method avoid an explicit cast on the deserialized token
   * @param base The Thrift instance
   * @param bytes the serialized thrift token
   */
  private void deserializeThriftToken(TBase<?, ?> base, byte[] bytes) throws TException {
    // Thrift deserialization
    TMemoryInputTransport trans_ = new TMemoryInputTransport();
    TProtocol protocol_ = new TCompactProtocol.Factory().getProtocol(trans_);
    try {
      trans_.reset(bytes);
      // TRASH THE 8 fist bytes (SIP HASH)
      trans_.consumeBuffer(8);
      base.read(protocol_);
    } finally {
      trans_.clear();
      protocol_.reset();
    }
  }
}
