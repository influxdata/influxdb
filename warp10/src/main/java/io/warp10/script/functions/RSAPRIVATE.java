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

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.math.BigInteger;
import java.security.interfaces.RSAPrivateKey;
import java.util.Map;

/**
 * Produce an RSA private key from a parameter map
 */
public class RSAPRIVATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public RSAPRIVATE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map on top of the stack.");
    }

    Map<String,String> params = (Map<String,String>) top;
    
    if (!"RSA".equals(params.get(Constants.KEY_ALGORITHM))) {
      throw new WarpScriptException(getName() + " invalid value for key '" + Constants.KEY_ALGORITHM + "', expected value 'RSA'.");
    }
    
    final BigInteger modulus = new BigInteger(params.get(Constants.KEY_MODULUS));
    final BigInteger exponent = new BigInteger(params.get(Constants.KEY_EXPONENT));
    
    RSAPrivateKey pub = new RSAPrivateKey() {
      public BigInteger getModulus() { return modulus; }
      public String getFormat() { return "PKCS#8"; }
      public byte[] getEncoded() { return null; }
      public String getAlgorithm() { return "RSA"; }
      public BigInteger getPrivateExponent() { return exponent; }
    };
    
    stack.push(pub);

    return stack;
  }
}
