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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * Unwraps a secure script
 */
public class UNSECURE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Flag indicating whether or not we should check secure key when unwrapping the script
   */
  private final boolean checkkey;
  
  private static byte[] aesKey = null;
  
  public UNSECURE(String name, boolean checkkey) {
    super(name);
    this.checkkey = checkkey;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();

    if (this.checkkey && null == stack.getAttribute(WarpScriptStack.ATTRIBUTE_SECURE_KEY)) {
      throw new WarpScriptException("You need to set the secure key first.");
    }
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a string.");
    }
    
    // Retrieve raw bytes
    byte[] raw = OrderPreservingBase64.decode(o.toString().getBytes(StandardCharsets.US_ASCII));
    
    // Unwrap
    
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
    
    byte[] unwrapped = CryptoUtils.unwrap(aesKey, raw);

    // Deserialize
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    SecureScript sscript = new SecureScript();
    
    try {
      deserializer.deserialize(sscript, unwrapped);
    } catch (TException te) {
      throw new WarpScriptException("Unable to unsecure script.", te);
    }
    
    if (this.checkkey) {
      if (!stack.getAttribute(WarpScriptStack.ATTRIBUTE_SECURE_KEY).toString().equals(sscript.getKey())) {
        throw new WarpScriptException("Invalid secure key.");
      }
    }
    
    // Decompress script content if needed
    
    if (sscript.isCompressed()) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ByteArrayInputStream bais = new ByteArrayInputStream(sscript.getScript());
      
      try {
        GZIPInputStream gzipin = new GZIPInputStream(bais);
        
        byte[] buf = new byte[128];
        
        while(true) {
          int len = gzipin.read(buf);
          if (len < 0) {
            break;
          }
          baos.write(buf, 0, len);
        }
        
        sscript.setCompressed(false);
        sscript.setScript(baos.toByteArray());        
      } catch (IOException ioe) {
        throw new WarpScriptException("Unable to unsecure script.", ioe);
      }
    }
    
    // Convert bytes to String
    String script = new String(sscript.getScript(), StandardCharsets.UTF_8);

    stack.push(script);
    
    return stack;
  }
}
