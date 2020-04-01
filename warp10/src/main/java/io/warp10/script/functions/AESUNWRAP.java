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

import io.warp10.crypto.CryptoUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Arrays;

/**
 * Unwraps a byte array which was AESWRAPed
 */
public class AESUNWRAP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public AESUNWRAP(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof byte[])) {
      throw new WarpScriptException(getName() + " expects a byte array containing a 128, 192 or 256 bits AES key on the stack.");      
    }
    
    byte[] key = (byte[]) o;
    
    if (16 != key.length && 24 != key.length && 32 != key.length) {
      throw new WarpScriptException(getName() + " expects a byte array containing a 128, 192 or 256 bits AES key on the stack.");      
    }
    
    o = stack.pop();
    
    if (!(o instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }
    
    byte[] data = (byte[]) o;
    
    byte[] unwrapped = CryptoUtils.unwrap(key, data);
    
    if (null == unwrapped) {
      throw new WarpScriptException(getName() + " encountered undecipherable data.");
    }
    
    // Remove the nonce
    stack.push(Arrays.copyOfRange(unwrapped, AESWRAP.NONCE_LEN, unwrapped.length));

    return stack;
  }
}
