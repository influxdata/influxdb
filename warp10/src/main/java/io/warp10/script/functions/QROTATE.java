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
 * Rotate a vector by a quaternion
 */
public class QROTATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public QROTATE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a quaternion on top of the stack.");
    }
    
    Object zo = stack.pop();
    Object yo = stack.pop();
    Object xo = stack.pop();
    
    if (!(zo instanceof Double) || !(yo instanceof Double) || !(xo instanceof Double)) {
      throw new WarpScriptException(getName() + " operates on a vector whose x,y,z coordinates are doubles below the quaternion.");
    }
    
    double x = ((Number) xo).doubleValue();
    double y = ((Number) yo).doubleValue();
    double z = ((Number) zo).doubleValue();
        
    double[] q = QUATERNIONTO.fromQuaternion(((Number) top).longValue());
    
    double v0 = (1.0D - 2.0D * q[2] * q[2] - 2.0D * q[3] * q[3]) * x + 2.0D * (q[1] * q[2] + q[0] * q[3]) * y + 2.0D * (q[1] * q[3] - q[0] * q[2]) * z;
    double v1 = 2.0D * (q[1] * q[2] - q[0] * q[3]) * x + (1.0D - 2.0D * q[1] * q[1] - 2.0D * q[3] * q[3]) * y + 2.0D * (q[2] * q[3] + q[0] * q[1]) * z;
    double v2 = 2.0D * (q[1] * q[3] + q[0] * q[2]) * x + 2.0D * (q[2] * q[3] - q[0] * q[1]) * y + (1.0D - 2.0D * q[1] * q[1] - 2.0D * q[2] * q[2]) * z;
    
    stack.push(v0);
    stack.push(v1);
    stack.push(v2);

    return stack;
  }
}
