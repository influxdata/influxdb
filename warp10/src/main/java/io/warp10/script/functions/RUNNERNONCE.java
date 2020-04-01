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

import io.warp10.WarpDist;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import com.geoxp.oss.CryptoHelper;
import com.google.common.primitives.Longs;

import java.nio.charset.StandardCharsets;

/**
 * Extract the content of a Runner Nonce
 */
public class RUNNERNONCE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private byte[] runnerPSK;
  
  public RUNNERNONCE(String name) {
    super(name);    
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a String.");      
    }
    
    synchronized(RUNNERNONCE.class) {
      if (null == runnerPSK) {
        try {
          runnerPSK = WarpDist.getKeyStore().getKey(KeyStore.AES_RUNNER_PSK);
        } catch (Throwable t) {
          // Catch NoClassDefFoundError
        }
      }
    }

    if (null != runnerPSK) {
      // Unwrap the blob
      byte[] wrapped = OrderPreservingBase64.decode(o.toString().getBytes(StandardCharsets.US_ASCII));
      byte[] raw = CryptoHelper.unwrapBlob(runnerPSK, wrapped);
      
      if (null == raw) {
        throw new WarpScriptException(getName() + " invalid runner nonce.");
      }
      stack.push(Longs.fromByteArray(raw));
    } else {
      stack.push(null);
    }

    return stack;
  }
}
