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
 * Call beginShape
 */ 
public class PbeginShape extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PbeginShape(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 0, 1);
        
    PGraphics pg = (PGraphics) params.get(0);

    if (1 == params.size()) {
      pg.parent.beginShape();
    } else if (2 == params.size()) {
      String mode = params.get(1).toString();
      if ("POLYGON".equals(mode)) {
        pg.parent.beginShape(PGraphics.POLYGON);
      } else if ("POINTS".equals(mode)) {
        pg.parent.beginShape(PGraphics.POINTS);
      } else if ("LINES".equals(mode)) {
        pg.parent.beginShape(PGraphics.LINES);
      } else if ("TRIANGLES".equals(mode)) {
        pg.parent.beginShape(PGraphics.TRIANGLES);
      } else if ("TRIANGLE_STRIP".equals(mode)) {
        pg.parent.beginShape(PGraphics.TRIANGLE_STRIP);
      } else if ("TRIANGLE_FAN".equals(mode)) {
        pg.parent.beginShape(PGraphics.TRIANGLE_FAN);
      } else if ("QUADS".equals(mode)) {
        pg.parent.beginShape(PGraphics.QUADS);
      } else if ("QUAD_STRIP".equals(mode)) {
        pg.parent.beginShape(PGraphics.QUAD_STRIP);
      } else {
        throw new WarpScriptException(getName() + ": invalid mode, should be 'POLYGON', 'POINTS', 'LINES', 'TRIANGLES', 'TRIANGLE_STRIP', 'TRIANGLE_FAN', 'QUADS' or 'QUAD_STRIP'.");
      }
    }
    stack.push(pg);
        
    return stack;
  }
}
