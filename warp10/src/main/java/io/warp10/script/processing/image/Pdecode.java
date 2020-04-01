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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.awt.Image;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import javax.swing.ImageIcon;

import org.apache.commons.codec.binary.Base64;
import processing.core.PImage;

/**
 * Decode a base64 encoded image content into a PImage instance
 */
public class Pdecode extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pdecode(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    byte[] bytes;
    if ((top instanceof String) && ((String) top).startsWith("data:image/")) {
      bytes = Base64.decodeBase64(((String) top).substring(((String) top).indexOf(",") + 1));
    } else if (top instanceof byte[]) {
      bytes = (byte[]) top;
    } else {
      throw new WarpScriptException(getName() + " expects a base64 data URI or a byte array on top of the stack.");
    }
    
    Image awtImage = new ImageIcon(bytes).getImage();
    
    if (awtImage instanceof BufferedImage) {
      BufferedImage buffImage = (BufferedImage) awtImage;
      int space = buffImage.getColorModel().getColorSpace().getType();
      if (space == ColorSpace.TYPE_CMYK) {
        throw new WarpScriptException(getName() + " only supports RGB images.");
      }
    }
    
    PImage image = new PImage(awtImage);
    
    //The transparency test in processing lib is not working correctly: set format to ARGB is mandatory to keep transparency.
    // setting it on Jpeg or non transparent image has no impact, so it is better to set it by default, as every PGraphics objects.
    image.format = PImage.ARGB;
    
    stack.push(image);
    
    return stack;
  }
}
