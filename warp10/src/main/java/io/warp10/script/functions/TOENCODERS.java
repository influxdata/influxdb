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

package io.warp10.script.functions;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Converts an encoder into a map of encoders, one per type
 */
public class TOENCODERS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOENCODERS(String name) {
    super(name);
  }  

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof String) && !(top instanceof byte[]) && !(top instanceof GTSEncoder)) {
      throw new WarpScriptException(getName() + " operates on a string, byte array or encoder.");
    }
    
    Map<String,GTSEncoder> encoders = new HashMap<String,GTSEncoder>();
    
    GTSDecoder decoder;
    
    if (top instanceof GTSEncoder) {
      decoder = ((GTSEncoder) top).getUnsafeDecoder(false);
    } else {
      try {
        byte[] bytes = top instanceof String ? OrderPreservingBase64.decode(top.toString().getBytes(StandardCharsets.US_ASCII)) : (byte[]) top;
        
        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
        
        GTSWrapper wrapper = new GTSWrapper();
        
        deser.deserialize(wrapper, bytes);

        decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);        
      } catch (TException te) {
        throw new WarpScriptException(getName() + " failed to unwrap encoder.", te);
      }            
    }

    GTSEncoder enc;
    
    try {
      while(decoder.next()) {
        Object value = decoder.getBinaryValue();
        
        String type = "DOUBLE";
        
        if (value instanceof String) {
          type = "STRING";
        } else if (value instanceof Boolean) {
          type = "BOOLEAN";
        } else if (value instanceof Long) {
          type = "LONG";
        } else if (value instanceof Double || value instanceof BigDecimal) { 
          type = "DOUBLE";
        } else if (value instanceof byte[]) {
          type = "BINARY";
        }
        
        enc = encoders.get(type);
        
        if (null == enc) {
          enc = new GTSEncoder(0L);
          enc.setMetadata(decoder.getMetadata());
          encoders.put(type, enc);
        }

        enc.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), value);
      }      
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " encountered an exception during conversion.");
    }
        
    stack.push(encoders);

    return stack;
  }  
}
