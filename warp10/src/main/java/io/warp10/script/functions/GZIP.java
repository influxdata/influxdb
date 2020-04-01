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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

/**
 * Compresses a byte array or String
 */
public class GZIP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GZIP(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof byte[]) && !(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a byte array or a String.");
    }
    
    if (o instanceof String) {
      o = o.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    byte[] data = (byte[]) o;
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
    try {
      GZIPOutputStream out = new GZIPOutputStream(baos);
    
      out.write(data);
    
      out.close();
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " encountered an error while compressing.", ioe);
    }
    
    stack.push(baos.toByteArray());
    
    return stack;
  }
}
