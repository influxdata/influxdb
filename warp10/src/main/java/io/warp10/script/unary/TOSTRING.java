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

package io.warp10.script.unary;

import java.io.PrintWriter;
import java.io.StringWriter;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Converts the operand on top of the stack to a String
 */
public class TOSTRING extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOSTRING(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op = stack.pop();
    
    if (op instanceof Number || op instanceof Boolean || op instanceof String) {
      stack.push(op.toString());
    } else if (op instanceof GeoTimeSerie) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      
      ((GeoTimeSerie) op).dump(pw);
      
      pw.close();
      
      stack.push(sw.toString());
    } else {
      throw new WarpScriptException(getName() + " can only operate on numeric, boolean or string values.");
    }
    
    return stack;
  }
}
