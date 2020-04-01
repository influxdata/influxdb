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

/**
 * Draw a curved line in a PGraphics instance
 */
public class Pcurve extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pcurve(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 8, 12);
        
    PGraphics pg = (PGraphics) params.get(0);
    
    if (9 == params.size()) {
      pg.parent.curve(
          ((Number) params.get(1)).floatValue(),
          ((Number) params.get(2)).floatValue(),
          ((Number) params.get(3)).floatValue(),
          ((Number) params.get(4)).floatValue(),
          ((Number) params.get(5)).floatValue(),
          ((Number) params.get(6)).floatValue(),
          ((Number) params.get(7)).floatValue(),
          ((Number) params.get(8)).floatValue()
      );      
    } else if (pg.is3D()) {
      pg.parent.curve(
          ((Number) params.get(1)).floatValue(),
          ((Number) params.get(2)).floatValue(),
          ((Number) params.get(3)).floatValue(),
          ((Number) params.get(4)).floatValue(),
          ((Number) params.get(5)).floatValue(),
          ((Number) params.get(6)).floatValue(),
          ((Number) params.get(7)).floatValue(),
          ((Number) params.get(8)).floatValue(),
          ((Number) params.get(9)).floatValue(),
          ((Number) params.get(10)).floatValue(),
          ((Number) params.get(11)).floatValue(),
          ((Number) params.get(12)).floatValue()
      );      
    } else {
      throw new WarpScriptException(getName() + " need a 3D container to draw a 3D Bezier curve.");
    }
    
    stack.push(pg);
        
    return stack;
  }
}
