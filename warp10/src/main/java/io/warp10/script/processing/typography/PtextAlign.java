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

import processing.core.PGraphics;

/**
 * Call textAlign
 */
public class PtextAlign extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PtextAlign(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 1, 2);
        
    PGraphics pg = (PGraphics) params.get(0);

    String alignXstr = params.get(1).toString();
    int alignX = 0;
    
    if ("LEFT".equals(alignXstr)) {
      alignX = PGraphics.LEFT;
    } else if ("RIGHT".equals(alignXstr)) {
      alignX = PGraphics.RIGHT;
    } else if ("CENTER".equals(alignXstr)) {
      alignX = PGraphics.CENTER;
    } else {
      throw new WarpScriptException(getName() + " invalid alignment for X, should be 'LEFT', 'RIGHT' or 'CENTER'.");
    }
    
    if (2 == params.size()) {
      pg.parent.textAlign(alignX);
    } else if (3 == params.size()) {
      String alignYstr = params.get(2).toString();
      int alignY = 0;
      
      if ("TOP".equals(alignYstr)) {
        alignY = PGraphics.TOP;
      } else if ("BOTTOM".equals(alignYstr)) {
        alignY = PGraphics.BOTTOM;
      } else if ("CENTER".equals(alignYstr)) {
        alignY = PGraphics.CENTER;
      } else if ("BASELINE".equals(alignYstr)) {
        alignY = PGraphics.BASELINE;
      } else {
        throw new WarpScriptException(getName() + " invalid alignment for Y, should be 'TOP', 'BOTTOM', 'CENTER' or 'BASELINE'.");
      }
      
      pg.parent.textAlign(alignX, alignY);
    }

    stack.push(pg);
    
    return stack;
  }
}
