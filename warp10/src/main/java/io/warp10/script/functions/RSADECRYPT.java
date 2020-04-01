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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.security.Key;
import java.util.Arrays;

import com.geoxp.oss.CryptoHelper;

/**
 * Decrypt content encoded data using RSA
 */
public class RSADECRYPT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public RSADECRYPT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Key)) {
      throw new WarpScriptException(getName() + " expects a key on top of the stack.");
    }

    Key key = (Key) top;
    
    top = stack.pop();
    
    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }
    
    byte[] data = (byte[]) top;

    byte[] nonced = CryptoHelper.decryptRSA(key, data);

    //
    // Remove the nonce
    //
    
    data = Arrays.copyOfRange(nonced, 8, nonced.length);
    
    stack.push(data);
    
    return stack;
  }
}
