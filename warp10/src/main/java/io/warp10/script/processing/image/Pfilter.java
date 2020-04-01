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
 * Call filter
 */
public class Pfilter extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pfilter(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 1, 2);
        
    PGraphics pg = (PGraphics) params.get(0);
    
    // We do not support shaders (yet?)
    
    String kindstr = params.get(1).toString();
    
    int kind = 0;

    if ("THRESHOLD".equals(kindstr)) {
      kind = PGraphics.THRESHOLD;
    } else if ("GRAY".equals(kindstr)) {
      kind = PGraphics.GRAY;
    } else if ("OPAQUE".equals(kindstr)) {
      kind = PGraphics.OPAQUE;
    } else if ("INVERT".equals(kindstr)) {
      kind = PGraphics.INVERT;
    } else if ("POSTERIZE".equals(kindstr)) {
      kind = PGraphics.POSTERIZE;
    } else if ("BLUR".equals(kindstr)) {
      kind = PGraphics.BLUR;
    } else if ("ERODE".equals(kindstr)) {
      kind = PGraphics.ERODE;
    } else if ("DILATE".equals(kindstr)) {
      kind = PGraphics.DILATE;
    } else {
      throw new WarpScriptException(getName() + " invalid mode.");
    }
    
    if (2 == params.size()) {
      pg.parent.filter(kind);
    } else if (3 == params.size()) {
      pg.parent.filter(
        kind,
        ((Number) params.get(2)).floatValue()
      );
    }
    
    stack.push(pg);
        
    return stack;
  }
}
