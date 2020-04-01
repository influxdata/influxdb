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

package io.warp10.script.processing.rendering;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.processing.ProcessingUtil;

import java.util.List;

import processing.core.PGraphics;

/**
 * Call blendMode
 */ 
public class PblendMode extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PblendMode(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 1);
        
    PGraphics pg = (PGraphics) params.get(0);
    
    String mode = params.get(1).toString();
    
    if ("BLEND".equals(mode)) {
      pg.parent.blendMode(PGraphics.BLEND);      
    } else if ("ADD".equals(mode)) {
      pg.parent.blendMode(PGraphics.ADD);
    } else if ("SUBTRACT".equals(mode)) {
      pg.parent.blendMode(PGraphics.SUBTRACT);
    } else if ("DARKEST".equals(mode)) {
      pg.parent.blendMode(PGraphics.DARKEST);
    } else if ("LIGHTEST".equals(mode)) {
      pg.parent.blendMode(PGraphics.LIGHTEST);
    } else if ("DIFFERENCE".equals(mode)) {
      pg.parent.blendMode(PGraphics.DIFFERENCE);
    } else if ("EXCLUSION".equals(mode)) {
      pg.parent.blendMode(PGraphics.EXCLUSION);
    } else if ("MULTIPLY".equals(mode)) {
      pg.parent.blendMode(PGraphics.MULTIPLY);
    } else if ("SCREEN".equals(mode)) {
      pg.parent.blendMode(PGraphics.SCREEN);
    } else if ("REPLACE".equals(mode)) {
      pg.parent.blendMode(PGraphics.REPLACE);
    } else {
      throw new WarpScriptException(getName() + ": invalid mode, should be 'BLEND', 'ADD', 'SUBTRACT', 'DARKEST', 'LIGHTEST', 'DIFFERENCE', 'EXCLUSION', 'MULTIPLY', 'SCREEN', or 'REPLACE'.");
    }
    
    stack.push(pg);
        
    return stack;
  }
}
