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

import java.util.ArrayList;
import java.util.List;

import processing.core.PGraphics;

/**
 * Call update the pixel array then call loadPixels
 */
public class PupdatePixels extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PupdatePixels(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 1);
        
    PGraphics pg = (PGraphics) params.get(0);

    if (!(params.get(1) instanceof List)) {
      throw new WarpScriptException(getName() + " expects an array of pixels on top of the stack.");
    }
    
    List<Object> pixels = (List<Object>) params.get(1);
    
    pg.parent.loadPixels();

    if (pixels.size() != pg.pixels.length) {
      throw new WarpScriptException(getName() + " expected an array of " + pg.pixels.length + " pixels, found " + pixels.size());
    }
    
    for (int i = 0; i < pg.pixels.length; i++) {
      if (!(pixels.get(i) instanceof Long)) {
        throw new WarpScriptException(getName() + " expected an array of LONG.");
      }
      
      pg.pixels[i] = ((Long) pixels.get(i)).intValue();
    }

    pg.parent.updatePixels();
    
    stack.push(pg);
    
    return stack;
  }
}
