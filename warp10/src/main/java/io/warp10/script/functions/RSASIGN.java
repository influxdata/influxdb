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

import java.security.PrivateKey;

import com.geoxp.oss.CryptoHelper;

/**
 * Sign data using RSA and a hash algorithm
 */
public class RSASIGN extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public RSASIGN(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof PrivateKey)) {
      throw new WarpScriptException(getName() + " expects a private key on top of the stack.");
    }

    PrivateKey key = (PrivateKey) top;
    
    top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects an algorithm name below the key.");
    }
    
    //
    // Algorithms are among those supported by BouncyCastle
    // cf http://stackoverflow.com/questions/8778531/bouncycastle-does-not-find-algorithms-that-it-provides
    //
    
    String alg = top.toString();
        
    top = stack.pop();
    
    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }
    
    byte[] data = (byte[]) top;

    //
    // Sign
    //
    
    byte[] signature = CryptoHelper.sign(alg, key, data);
    
    if (null == signature) {
      throw new WarpScriptException(getName() + " invalid algorithm.");
    }
   
    stack.push(signature);
    
    return stack;
  }
}
