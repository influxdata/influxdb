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
 * Converts a quaternion on a double to 4 elements w x y z
 */
public class QUATERNIONTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Precision used to determine heuristically if a quaternion is a unit one or not
   */
  private static final double PRECISION = 4.0D / 65535.0D;
  
  public QUATERNIONTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object qo = stack.pop();
    
    if (!(qo instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a quaternion as a LONG on top of the stack.");
    }

    double[] elements = fromQuaternion(((Number) qo).longValue());
    
    stack.push(elements[0]);
    stack.push(elements[1]);
    stack.push(elements[2]);
    stack.push(elements[3]);
    
    return stack;
  }
  
  public static double[] fromQuaternion(long q) throws WarpScriptException {
    int iz = (int) (q & 0xffffL);
    q >>>= 16;
    int iy = (int) (q & 0xffffL);
    q >>>= 16;
    int ix = (int) (q & 0xffffL);
    q >>>= 16;
    int iw = (int) (q & 0xffffL);
    
    double[] elements = new double[4];
    
    elements[0] = 2.0D * (iw / 65535.0D) - 1.0D;
    elements[1] = 2.0D * (ix / 65535.0D) - 1.0D;
    elements[2] = 2.0D * (iy / 65535.0D) - 1.0D;
    elements[3] = 2.0D * (iz / 65535.0D) - 1.0D;

    //
    // Compute norm, if it's not within 2**-16 of 1.0, assume input was not a quaternion
    //
    
    double norm = elements[0] * elements[0] + elements[1] * elements[1] + elements[2] * elements[2] + elements[3] * elements[3];
    
    if (Math.abs(1.0D - norm) > PRECISION) {
      throw new WarpScriptException("Input does not appear to be a unit quaternion, norm was " + norm + ".");
    }
    
    return elements;
  }
}
