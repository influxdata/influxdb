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

package io.warp10.script.processing.image;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.processing.ProcessingUtil;

import java.util.List;

import processing.core.PGraphics;

/**
 * Call get
 */
public class Pget extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pget(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 0, 2, 4);
        
    PGraphics pg = (PGraphics) params.get(0);
    
    if (1 == params.size()) {
      stack.push(pg.parent.get());
    } else if (3 == params.size()) {
      stack.push((long) pg.parent.get(
        ((Number) params.get(1)).intValue(),
        ((Number) params.get(2)).intValue()
      ));
    } else if (5 == params.size()) {
      stack.push(pg.parent.get(
        ((Number) params.get(1)).intValue(),
        ((Number) params.get(2)).intValue(),
        ((Number) params.get(3)).intValue(),
        ((Number) params.get(4)).intValue()
      ));
      
    }
    
    stack.push(pg);
    stack.swap();
    
    return stack;
  }
}
