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

import java.awt.image.BufferedImage;

import processing.awt.PGraphicsJava2D;
import processing.core.PApplet;
import processing.opengl.PGraphics3D;

/**
 * Push onto the stack a newly create PGraphics instance
 */
public class PGraphics extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PGraphics(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    processing.core.PGraphics pg = null;

    if (String.valueOf(top).startsWith("2D")) {
      pg = new PGraphicsJava2D();
      if (!"2D".equals(top)) {
        pg.smooth(Integer.parseInt(String.valueOf(top).substring(2)));
      }
    } else if (String.valueOf(top).startsWith("3D")) {
      try {
        pg = new PGraphics3D();
        if (!"3D".equals(top)) {
          pg.smooth(Integer.parseInt(String.valueOf(top).substring(2)));
        }
      } catch (Throwable t) {
        throw new WarpScriptException(getName() + " unable to create 3D container.", t);
      }
    } else {
      throw new WarpScriptException(getName() + " expects a type ('2D' or '3D') on top of the stack.");
    }
    
    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a height in pixels below the type.");
    }

    int height = ((Number) top).intValue();

    top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a width in pixels below the height.");
    }

    int width = ((Number) top).intValue();

    long PIXEL_LIMIT = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MAX_PIXELS);
    
    if (width * height > PIXEL_LIMIT) {
      throw new WarpScriptException(getName() + " only allows graphics with a total number of pixels less than " + PIXEL_LIMIT);
    }

    // Disable async saving of frame in case we want to save a frame to a file
    pg.hint(processing.core.PGraphics.DISABLE_ASYNC_SAVEFRAME);
    
    // Declare a PApplet as parent so we can create fonts or use a recorder for example
    PApplet pa = new PApplet();
    pa.g = pg;
    pg.parent = pa;
    
    // Set display size
    pg.setSize(width, height);
    pg.setPrimary(false); // Set primary to false so we get transparency

    // Initialize the image, otherwise the headless mode won't work
    pg.image = new BufferedImage(pg.pixelWidth, pg.pixelHeight, BufferedImage.TYPE_INT_ARGB);
    
    // Begin the drawing mode
    pg.beginDraw();
 
    pg.parent.loadPixels();
    pg.parent.colorMode(pg.RGB, 255, 255, 255, 255);

    stack.push(pg);
    
    return stack;
  }
}
