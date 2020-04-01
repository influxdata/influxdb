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
import java.util.HashMap;
import java.util.Map;

import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.generators.RSAKeyPairGenerator;
import org.bouncycastle.crypto.params.RSAKeyGenerationParameters;
import org.bouncycastle.crypto.params.RSAKeyParameters;

import com.geoxp.oss.CryptoHelper;

/**
 * Generate an RSA key pair
 */
public class RSAGEN extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public RSAGEN(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a key length on top of the stack.");
    }

    int strength = ((Number) top).intValue();
    
    top = stack.pop();

    BigInteger exponent = new BigInteger(top.toString());
            
    RSAKeyPairGenerator gen = new RSAKeyPairGenerator();
    
    // For explanation of 'certainty', refer to http://bouncy-castle.1462172.n4.nabble.com/Questions-about-RSAKeyGenerationParameters-td1463186.html
    RSAKeyGenerationParameters params = new RSAKeyGenerationParameters(exponent, CryptoHelper.getSecureRandom(), strength, 64);
    
    gen.init(params);
    final AsymmetricCipherKeyPair keypair = gen.generateKeyPair();
        
    Map<String,String> keyparams = new HashMap<String,String>();
    
    keyparams.put(Constants.KEY_MODULUS, ((RSAKeyParameters) keypair.getPrivate()).getModulus().toString());
    keyparams.put(Constants.KEY_ALGORITHM, "RSA");
    keyparams.put(Constants.KEY_EXPONENT, ((RSAKeyParameters) keypair.getPrivate()).getExponent().toString());
    
    stack.push(keyparams);
    
    keyparams = new HashMap<String,String>();

    keyparams.put(Constants.KEY_MODULUS, ((RSAKeyParameters) keypair.getPublic()).getModulus().toString());
    keyparams.put(Constants.KEY_ALGORITHM, "RSA");
    keyparams.put(Constants.KEY_EXPONENT, ((RSAKeyParameters) keypair.getPublic()).getExponent().toString());

    stack.push(keyparams);

    return stack;
  }
}
