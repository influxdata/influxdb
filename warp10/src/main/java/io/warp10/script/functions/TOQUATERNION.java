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

import io.warp10.DoubleUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Converts 4 double to a unit quaternion
 * The stack is expected to contain w x y z (z being on top)
 */
public class TOQUATERNION extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOQUATERNION(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object zo = stack.pop();
    
    if (!(zo instanceof Double)) {
      throw new WarpScriptException(getName() + " expects the quaternion components w x y z as doubles on the stack (z on top).");
    }
    
    Object yo = stack.pop();
    
    if (!(yo instanceof Double)) {
      throw new WarpScriptException(getName() + " expects the quaternion components w x y z as doubles on the stack (z on top).");
    }
    
    Object xo = stack.pop();
    
    if (!(xo instanceof Double)) {
      throw new WarpScriptException(getName() + " expects the quaternion components w x y z as doubles on the stack (z on top).");
    }
    
    Object wo = stack.pop();
    
    if (!(wo instanceof Double)) {
      throw new WarpScriptException(getName() + " expects the quaternion components w x y z as doubles on the stack (z on top).");
    }
    
    double w = ((Number) wo).doubleValue();
    double x = ((Number) xo).doubleValue();
    double y = ((Number) yo).doubleValue();
    double z = ((Number) zo).doubleValue();

    if (!DoubleUtils.isFinite(w) || !DoubleUtils.isFinite(x) || !DoubleUtils.isFinite(y) || !DoubleUtils.isFinite(z)) {
      throw new WarpScriptException(getName() + " expects finite arguments.");
    }
    
    long q = toQuaternion(w,x,y,z);
    
    stack.push(q);
    
    return stack;
  }
  
  public static long toQuaternion(double w, double x, double y, double z) {
    //
    // Compute norm
    //
    
    double norm = Math.sqrt(w * w + x * x + y * y + z * z);
    
    //
    // Make sure it is a unit quaternion
    //
    
    if (0.0D != norm) {
      w = w / norm;
      x = x / norm;
      y = y / norm;
      z = z / norm;
    }
    
    //
    // Quantize components by multiplying them by 1.0 and multiplying them by (2**16-1)/2.0D
    //
    
    int iw = (int) Math.floor(((w + 1.0D) / 2.0D) * 65535);
    int ix = (int) Math.floor(((x + 1.0D) / 2.0D) * 65535);
    int iy = (int) Math.floor(((y + 1.0D) / 2.0D) * 65535);
    int iz = (int) Math.floor(((z + 1.0D) / 2.0D) * 65535);
    
    //
    // Make a LONG
    //
    
    long q = (iw & 0xffff);
    q <<= 16;
    q |= (ix & 0xffff);
    q <<= 16;
    q |= (iy & 0xffff);
    q <<= 16;
    q |= (iz & 0xffff);
    
    return q;
  }
}
