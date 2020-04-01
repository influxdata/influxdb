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

package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.linear.RealVector;

/**
 * Converts a Vector into a list
 */
public class VECTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public VECTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof RealVector)) {
      throw new WarpScriptException(getName() + " expects a vector on top of the stack.");
    }
    
    RealVector vector = (RealVector) o;
        
    List<Object> elts = new ArrayList<Object>(vector.getDimension());
    
    for (int i = 0; i < vector.getDimension(); i++) {
      elts.add(vector.getEntry(i));
    }

    stack.push(elts);
    
    return stack;
  }
}
