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
package io.warp10.continuum.egress;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;

import java.nio.charset.StandardCharsets;

public class UnpackingGTSDecoderIterator extends GTSDecoderIterator {
  
  private final GTSDecoderIterator iter;
  private final String suffix;
  
  private GTSDecoder currentDecoder = null;
  
  public UnpackingGTSDecoderIterator(GTSDecoderIterator iter, String suffix) {
    this.iter = iter;
    this.suffix = suffix;
  }
  
  @Override
  public void close() throws Exception {
    iter.close();
  }
  
  @Override
  public boolean hasNext() {
    if (null != currentDecoder) {
      return true;
    }
    return iter.hasNext();
  }
  
  @Override
  public GTSDecoder next() {
    if (null == currentDecoder) {
      currentDecoder = iter.next();
    }
    
    if (!currentDecoder.getName().endsWith(suffix)) {
      throw new RuntimeException("Class does not end in '" + suffix + "'");
    }
    
    //
    // Read the next value from 'currentDecoder'
    //
    
    if (currentDecoder.next()) {
      Object value = currentDecoder.getBinaryValue();
      
      if (!(value instanceof String) && !(value instanceof byte[])) {
        throw new RuntimeException("Invalid value, expected String or byte array.");
      }
      
      //
      // Unwrap the value
      //
      
      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
      
      GTSWrapper wrapper = new GTSWrapper();
      try {
        if (value instanceof byte[]) {
          deser.deserialize(wrapper, (byte[]) value);
        } else {
          deser.deserialize(wrapper, OrderPreservingBase64.decode(value.toString().getBytes(StandardCharsets.US_ASCII)));
        }
      } catch (TException te) {
        throw new RuntimeException(te);
      }
      
      GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
      decoder.setMetadata(currentDecoder.getMetadata());
      
      if (decoder.getMetadata().getName().endsWith(suffix)) {
        decoder.setName(decoder.getName().substring(0, decoder.getName().length() - suffix.length()));
      }
      
      if (0 == currentDecoder.getRemainingSize()) {
        currentDecoder = null;
      }
      return decoder;
    } else {
      currentDecoder = null;
      return next();
    }
  }
}
