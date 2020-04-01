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
import processing.core.PImage;

/**
 * Call blend
 */
public class Pblend extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pblend(String name) {
    super(name);
  }

  private int getBlendMode(String mode) throws WarpScriptException {
    if ("BLEND".equals(mode)) {
      return(PGraphics.BLEND);
    } else if ("ADD".equals(mode)) {
      return(PGraphics.ADD);
    } else if ("SUBTRACT".equals(mode)) {
      return(PGraphics.SUBTRACT);
    } else if ("DARKEST".equals(mode)) {
      return(PGraphics.DARKEST);
    } else if ("LIGHTEST".equals(mode)) {
      return(PGraphics.LIGHTEST);
    } else if ("DIFFERENCE".equals(mode)) {
      return(PGraphics.DIFFERENCE);
    } else if ("EXCLUSION".equals(mode)) {
      return(PGraphics.EXCLUSION);
    } else if ("MULTIPLY".equals(mode)) {
      return(PGraphics.MULTIPLY);
    } else if ("SCREEN".equals(mode)) {
      return(PGraphics.SCREEN);
    } else if ("OVERLAY".equals(mode)) {
      return(PGraphics.OVERLAY);
    } else if ("HARD_LIGHT".equals(mode)) {
      return(PGraphics.HARD_LIGHT);
    } else if ("SOFT_LIGHT".equals(mode)) {
      return(PGraphics.SOFT_LIGHT);
    } else if ("DODGE".equals(mode)) {
      return(PGraphics.DODGE);
    } else if ("BURN".equals(mode)) {
      return(PGraphics.BURN);
    } else {
      throw new WarpScriptException(getName() + ": invalid mode, should be 'BLEND', 'ADD', 'SUBTRACT', 'DARKEST', 'LIGHTEST', 'DIFFERENCE', 'EXCLUSION', 'MULTIPLY', 'SCREEN', 'OVERLAY', 'HARD_LIGHT', 'SOFT_LIGHT', 'DODGE' or 'BURN'.");
    }
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 9, 10);
        
    PGraphics pg = (PGraphics) params.get(0);
    
    if (10 == params.size()) {
      pg.parent.blend(
        ((Number) params.get(1)).intValue(),
        ((Number) params.get(2)).intValue(),
        ((Number) params.get(3)).intValue(),
        ((Number) params.get(4)).intValue(),
        ((Number) params.get(5)).intValue(),
        ((Number) params.get(6)).intValue(),
        ((Number) params.get(7)).intValue(),
        ((Number) params.get(8)).intValue(),
        getBlendMode((String) params.get(9))
      );
    } else if (11 == params.size()) {
      pg.parent.blend(        
        (PImage) params.get(1),
        ((Number) params.get(2)).intValue(),
        ((Number) params.get(3)).intValue(),
        ((Number) params.get(4)).intValue(),
        ((Number) params.get(5)).intValue(),
        ((Number) params.get(6)).intValue(),
        ((Number) params.get(7)).intValue(),
        ((Number) params.get(8)).intValue(),
        ((Number) params.get(9)).intValue(),
        getBlendMode((String) params.get(10))
      );
    }
    
    stack.push(pg);
        
    return stack;
  }
}
