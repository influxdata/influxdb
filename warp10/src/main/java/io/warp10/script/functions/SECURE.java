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
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.thrift.data.SecureScript;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * Builds a SecureScript from the String on the stack and pushes it on the stack
 */
public class SECURE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static byte[] aesKey = null;
  
  public SECURE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();

    if (null == stack.getAttribute(WarpScriptStack.ATTRIBUTE_SECURE_KEY)) {
      throw new WarpScriptException("You need to set the secure key first.");
    }
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a string.");
    }
        
    stack.push(secure(stack.getAttribute(WarpScriptStack.ATTRIBUTE_SECURE_KEY).toString(), o.toString()));
    
    return stack;
  }
  
  public static final String secure(String key, String script) throws WarpScriptException {
    SecureScript sscript = new SecureScript();
    sscript.setTimestamp(System.currentTimeMillis());
    sscript.setKey(key);

    byte[] scriptBytes = script.getBytes(StandardCharsets.UTF_8);
    
    // Check if we should compress the script or not
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
    boolean compress = false;
    
    try {
      GZIPOutputStream gzos = new GZIPOutputStream(baos);
      gzos.write(scriptBytes);
      gzos.close();
      byte[] gzipped = baos.toByteArray();
      if (gzipped.length < scriptBytes.length) {
        compress = true;
        scriptBytes = gzipped;
      }
    } catch (IOException ioe) {              
    }
    
    sscript.setCompressed(compress);
    sscript.setScript(scriptBytes);
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    try {
      byte[] serialized = serializer.serialize(sscript);
      // TODO(hbs): encrypt script
      
      synchronized(SECURE.class) {
        if (null == aesKey) {
          try {
            aesKey = WarpDist.getKeyStore().getKey(KeyStore.AES_SECURESCRIPTS);
          } catch (Throwable t) {
            // Catch NoClassDefFoundError
          }
        }
      }
      
      if (null == aesKey) {
        throw new WarpScriptException("Missing secure script encryption key.");
      }
      
      byte[] wrapped = CryptoUtils.wrap(aesKey, serialized);
      
      String encoded = new String(OrderPreservingBase64.encode(wrapped), StandardCharsets.US_ASCII);
      
      return encoded;
    } catch (TException te) {
      throw new WarpScriptException("Unable to secure script.", te);
    }

  }
}
