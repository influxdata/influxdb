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

package io.warp10.script.processing.shape;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.processing.ProcessingUtil;

import java.util.List;

import processing.core.PGraphics;
import processing.core.PShape;

/**
 * Draw a shape in a PGraphics instance
 */
public class Pshape extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pshape(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 1, 3, 5);
        
    PGraphics pg = (PGraphics) params.get(0);
    PShape shape = (PShape) params.get(1);
    
    if (2 == params.size()) {
      pg.parent.shape(shape);
    } else if (4 == params.size()) {
      pg.parent.shape(shape,
          ((Number) params.get(2)).floatValue(),
          ((Number) params.get(3)).floatValue()
      );
    } else if (6 == params.size()) {
      pg.parent.shape(shape,
          ((Number) params.get(2)).floatValue(),
          ((Number) params.get(3)).floatValue(),
          ((Number) params.get(4)).floatValue(),
          ((Number) params.get(5)).floatValue()
      );      
    }
    
    stack.push(pg);
        
    return stack;
  }
}
