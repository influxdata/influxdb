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

import java.util.List;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

/**
 * Converts a list of numbers into a vector
 */
public class TOVEC extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOVEC(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (o instanceof RealMatrix) {
      RealMatrix matrix = (RealMatrix) o;
      
      if (1 != matrix.getColumnDimension()) {
        throw new WarpScriptException(getName() + " expects a matrix with a single column on top of the stack.");
      }
      
      RealVector vector = matrix.getColumnVector(0);
      stack.push(vector);
      return stack;
    }
    
    if (!(o instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list onto the stack.");
    }
    
    double[] data = new double[((List) o).size()];
    
    for (int i = 0; i < data.length; i++) {
      Object oo = ((List) o).get(i);
      if (!(oo instanceof Number)) {
        throw new WarpScriptException(getName() + " expects a list of numbers onto the stack.");
      }
      data[i] = ((Number) oo).doubleValue();
    }
    
    stack.push(MatrixUtils.createRealVector(data));
    
    return stack;
  }
}
