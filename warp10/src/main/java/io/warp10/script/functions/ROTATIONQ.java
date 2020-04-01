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
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Create a quaternion from an axis and rotation angle (in degrees)
 */
public class ROTATIONQ extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ROTATIONQ(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Double)) {
      throw new WarpScriptException(getName() + " expects a vector coordinates and an angle in degrees on the stack.");
    }
    
    double angle = ((Number) top).doubleValue();
    
    top = stack.pop();
    
    if (!(top instanceof Double)) {
      throw new WarpScriptException(getName() + " expects a vector coordinates and an angle in degrees on the stack.");
    }
    
    double z = ((Number) top).doubleValue();

    top = stack.pop();
    
    if (!(top instanceof Double)) {
      throw new WarpScriptException(getName() + " expects a vector coordinates and an angle in degrees on the stack.");
    }
    
    double y = ((Number) top).doubleValue();

    top = stack.pop();
    
    if (!(top instanceof Double)) {
      throw new WarpScriptException(getName() + " expects a vector coordinates and an angle in degrees on the stack.");
    }
    
    double x = ((Number) top).doubleValue();

    //
    // Normalize vector
    //
    
    double norm = Math.sqrt(x * x + y * y + z * z);
    
    if (0.0D != norm) {
      x = x / norm;
      y = y / norm;
      z = z / norm;
    }
    
    double cos = Math.cos(Math.toRadians(angle / 2.0D));
    double sin = Math.sin(Math.toRadians(angle / 2.0D));
    
    long q = TOQUATERNION.toQuaternion(cos, x * sin, y * sin, z * sin);

    stack.push(q);
    
    return stack;
  }
}
