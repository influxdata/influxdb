//
//   Copyright 2019  SenX S.A.S.
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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import processing.core.PImage;

import java.awt.image.BufferedImage;

public class PtoImage extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public PtoImage(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof processing.core.PGraphics)) {
      throw new WarpScriptException(getName() + " operates on a PGraphics instance.");
    }

    processing.core.PGraphics pg = (processing.core.PGraphics) top;

    pg.endDraw();

    BufferedImage bimage = new BufferedImage(pg.pixelWidth, pg.pixelHeight, BufferedImage.TYPE_INT_ARGB);
    // This method copy pixel values so the initial PGraphics can be modified without altering the image
    bimage.setRGB(0, 0, pg.pixelWidth, pg.pixelHeight, pg.pixels, 0, pg.pixelWidth);

    stack.push(new PImage(bimage));

    //
    // Re-issue a 'beginDraw' so we can continue using the PGraphics instance
    //

    pg.beginDraw();

    return stack;
  }
}
