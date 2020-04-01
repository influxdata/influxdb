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

package io.warp10.script.processing;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.metadata.IIOMetadata;

import org.apache.commons.codec.binary.Base64;

/**
 * Encode the PGraphics on the stack into a base64 string suitable for a data URL.
 */
public class Pencode extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public Pencode(String name) {
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
    bimage.setRGB(0, 0, pg.pixelWidth, pg.pixelHeight, pg.pixels, 0, pg.pixelWidth);
    Iterator<ImageWriter> iter = ImageIO.getImageWritersByFormatName("png");
    ImageWriter writer = null;
    if (iter.hasNext()) {
      writer = iter.next();
    }
    ImageWriteParam param = writer.getDefaultWriteParam();
    IIOMetadata metadata = null;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BufferedOutputStream output = new BufferedOutputStream(baos);
    
    try {
      writer.setOutput(ImageIO.createImageOutputStream(output));
      writer.write(metadata, new IIOImage(bimage, null, metadata), param);      
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " error while encoding PGraphics.", ioe);
    }
    
    writer.dispose();

    StringBuilder sb = new StringBuilder("data:image/png;base64,");
    sb.append(Base64.encodeBase64String(baos.toByteArray()));
    
    stack.push(sb.toString());

    //
    // Re-issue a 'beginDraw' so we can continue using the PGraphics instance
    //
    
    pg.beginDraw();

    return stack;
  }
}
