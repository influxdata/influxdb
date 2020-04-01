//
//   Copyright 2019  SenX S.A.S.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

/**
 * Decompresses a compressed byte array using ZLIB.
 */
public class INFLATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public INFLATE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof byte[])) {
      throw new WarpScriptException(getName() + " operates on a byte array.");
    }
    
    byte[] data = (byte[]) o;
    
    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    ByteArrayOutputStream decompressed = new ByteArrayOutputStream(data.length);
    
    byte[] buf = new byte[1024];
    
    try {
      InflaterInputStream in = new InflaterInputStream(bin);

      while(true) {
        int len = in.read(buf);
        
        if (len < 0) {
          break;
        }
        
        decompressed.write(buf, 0, len);
      }
      
      in.close();
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " encountered an error while decompressing.", ioe);
    }
    
    stack.push(decompressed.toByteArray());
    
    return stack;
  }
}
