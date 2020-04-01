//
//   Copyright 2018-2019  SenX S.A.S.
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Unwraps a GTSWrapper into an encoder
 */
public class UNWRAPENCODER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public UNWRAPENCODER(String name) {
    super(name);
  }  

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof String) && !(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a string or byte array.");
    }
    
    byte[] bytes = top instanceof String ? OrderPreservingBase64.decode(top.toString().getBytes(StandardCharsets.US_ASCII)) : (byte[]) top;
    
    TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
    
    try {
      GTSWrapper wrapper = new GTSWrapper();      
      deser.deserialize(wrapper, bytes);

      stack.push(GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper));      
    } catch (TException te) {
      throw new WarpScriptException(getName() + " failed to unwrap encoder.", te);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " failed to unwrap encoder.", ioe);
    }      

    return stack;
  }  
}
