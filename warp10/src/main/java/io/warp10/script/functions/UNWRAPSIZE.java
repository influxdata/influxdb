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

import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * Extract the size of a GTS from GTSWrapper
 */
public class UNWRAPSIZE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public UNWRAPSIZE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof String) && !(top instanceof byte[]) && !(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a string or byte array or a list thereof.");
    }
    
    List<Object> inputs = new ArrayList<Object>();
    
    if (top instanceof String || top instanceof byte[]) {
      inputs.add(top);
    } else {
      for (Object o: (List) top) {
        if (!(o instanceof String) && !(o instanceof byte[])) {
          throw new WarpScriptException(getName() + " operates on a string or byte array or a list thereof.");
        }
        inputs.add(o);
      }
    }
    
    List<Object> outputs = new ArrayList<Object>();
    
    for (Object s: inputs) {
      byte[] bytes = s instanceof String ? OrderPreservingBase64.decode(s.toString().getBytes(StandardCharsets.US_ASCII)) : (byte[]) s;
      
      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
      
      try {
        GTSWrapper wrapper = new GTSWrapper();
        
        deser.deserialize(wrapper, bytes);

        outputs.add(wrapper.getCount());
      } catch (TException te) {
        throw new WarpScriptException(getName() + " failed to unwrap GTS.", te);
      }      
    }
    
    if (!(top instanceof List)) {
      stack.push(outputs.get(0));      
    } else {
      stack.push(outputs);
    }
    
    return stack;
  }  
}
