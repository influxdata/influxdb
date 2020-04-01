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
 * Multiply a quaternion q by a quaternion r
 * 
 * @see http://fr.mathworks.com/help/aeroblks/quaternionmultiplication.html
 */
public class QMULTIPLY extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public QMULTIPLY(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object ro = stack.pop();

    if (!(ro instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a quaternion on top of the stack.");
    }
    
    double[] r = QUATERNIONTO.fromQuaternion(((Number) ro).longValue());

    Object qo = stack.pop();
    
    if (!(qo instanceof Long)) {
      throw new WarpScriptException(getName() + " expects another quaternion below the quaternion on top of the stack.");
    }
    
    double[] q = QUATERNIONTO.fromQuaternion(((Number) qo).longValue());
    
    double w = r[0] * q[0] - r[1] * q[1] - r[2] * q[2] - r[3] * q[3];
    double x = r[0] * q[1] + r[1] * q[0] - r[2] * q[3] + r[3] * q[2];
    double y = r[0] * q[2] + r[1] * q[3] + r[2] * q[0] - r[3] * q[1];
    double z = r[0] * q[3] - r[1] * q[2] + r[2] * q[1] + r[3] * q[0];
    
    stack.push(TOQUATERNION.toQuaternion(w, x, y, z));
    
    return stack;
  }
}
