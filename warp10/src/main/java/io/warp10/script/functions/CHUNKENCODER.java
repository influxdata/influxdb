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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Chunk a GTSEncoder instance into multiple encoders.
 * @deprecated Use CHUNK instead
 */
public class CHUNKENCODER extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean withOverlap;
  
  public CHUNKENCODER(String name, boolean withOverlap) {
    super(name);
    this.withOverlap = withOverlap;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects on top of the stack a boolean indicating whether or not to keep empty chunks.");
    }

    boolean keepempty = (boolean) top;
    
    top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the name of the 'chunk' label below the top of the stack.");
    }
    
    String chunklabel = (String) top;
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of chunks under the 'chunk' label.");
    }
    
    long chunkcount = (long) top;
    
    long overlap = 0L;
    
    if (this.withOverlap) {
      top = stack.pop();
      if (!(top instanceof Long)) {
        throw new WarpScriptException(getName() + " expects an overlap below the number of chunks.");
      }
      overlap = (long) top;
      
      if (overlap < 0) {
        overlap = 0;
      }      
    }
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a chunk width on top of the end timestamp of the most recent chunk.");
    }
    
    long chunkwidth = (long) top;

    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects the end timestamp of the most recent chunk under the chunk width.");
    }
    
    long lastchunk = (long) top;

    top = stack.pop();
    
    if (!(top instanceof GTSEncoder)) {
      throw new WarpScriptException(getName() + " operates on an encoder instance.");
    }
    
    stack.push(GTSHelper.chunk((GTSEncoder) top, lastchunk, chunkwidth, chunkcount, chunklabel, keepempty, overlap));
    
    return stack;
  }
}
