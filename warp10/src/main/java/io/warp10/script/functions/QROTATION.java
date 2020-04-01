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
 * Extract the axis and angle of the rotation represented by the quaternion on the stack
 */
public class QROTATION extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public QROTATION(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " can only operate on a quaternion.");
    }
    
    double[] q = QUATERNIONTO.fromQuaternion(((Number) top).longValue());
    
    //
    // Extract angle
    //
    
    double angle = Math.acos(q[0]);
    
    double sin = Math.sin(angle);
    
    if (0.0D != sin) {
      q[1] = q[1] / sin;
      q[2] = q[2] / sin;
      q[3] = q[3] / sin;
    }
    
    stack.push(q[1]);
    stack.push(q[2]);
    stack.push(q[3]);

    stack.push(Math.toDegrees(angle * 2.0D));

    return stack;
  }
}
