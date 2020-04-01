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
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.GeneralDigest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;

public class HMAC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private Class digestAlgo;

  public HMAC(String name, Class<? extends GeneralDigest> digestAlgo) {
    super(name);
    this.digestAlgo = digestAlgo;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " expects a byte array (key) on top of the stack.");
    }

    byte[] key = (byte[]) top;

    top = stack.pop();

    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }

    byte[] bytes = (byte[]) top;

    try {
      HMac m = new HMac((Digest) digestAlgo.newInstance());

      m.init(new KeyParameter(key));

      m.update(bytes,0,bytes.length);

      byte[] mac=new byte[m.getMacSize()];

      m.doFinal(mac,0);

      stack.push(mac);

      return stack;
    } catch (Exception exp) {
      throw new WarpScriptException(getName() + " unable to instantiate message digest", exp);
    }
  }
}
