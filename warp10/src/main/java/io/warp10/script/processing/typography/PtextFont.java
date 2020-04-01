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

package io.warp10.script.processing.typography;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.processing.ProcessingUtil;

import java.util.List;

import processing.core.PFont;
import processing.core.PGraphics;

/**
 * Call textFont
 */
public class PtextFont extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PtextFont(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 1, 2);
        
    PGraphics pg = (PGraphics) params.get(0);

    if (2 == params.size()) {
      pg.parent.textFont((PFont) params.get(1));
    } else if (3 == params.size()) {
      pg.parent.textFont((PFont) params.get(1),
          ((Number) params.get(2)).floatValue());
    }

    stack.push(pg);
    
    return stack;
  }
}
