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

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * AES wraps a byte array
 */
public class AESWRAP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public static final int NONCE_LEN = 8;
  
  private static final SecureRandom sr = new SecureRandom();
  
  public AESWRAP(String name) {
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
    
    if (!(o instanceof byte[]) && !(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a byte array or a String.");
    }
    
    if (o instanceof String) {
      o = o.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    byte[] data = new byte[NONCE_LEN];
    
    // Generate a nonce
    
    sr.nextBytes(data);
    
    data = Arrays.copyOf(data, data.length + ((byte[]) o).length);
    
    System.arraycopy((byte[]) o, 0, data, NONCE_LEN, data.length - NONCE_LEN);
    
    byte[] wrapped = CryptoUtils.wrap(key, data);
    
    stack.push(wrapped);

    return stack;
  }
}
