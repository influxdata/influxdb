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

import java.io.Reader;
import java.io.StringReader;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import processing.awt.PShapeJava2D;
import processing.data.XML;

/**
 * 'Loads' an SVG shape from a String
 */
public class PloadShape extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PloadShape(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    String xml = String.valueOf(stack.pop());
    
    Reader reader = new StringReader(xml);
    
    try {
      PShapeJava2D shape = new PShapeJava2D(new XML(reader, null));
    
      stack.push(shape);
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " caught an exception while loading SVG.", e);
    }
        
    return stack;
  }
}
