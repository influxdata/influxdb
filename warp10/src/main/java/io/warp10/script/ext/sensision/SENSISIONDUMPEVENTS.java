//
//   Copyright 2019  SenX S.A.S.
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

package io.warp10.script.ext.sensision;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.sensision.Sensision;

/**
 * Retrieve a string representation of all current Sensision metrics
 */
public class SENSISIONDUMPEVENTS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SENSISIONDUMPEVENTS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    StringBuilder sb = new StringBuilder();
    
    for (String event: Sensision.getEvents()) {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      sb.append(event);
    }
    
    stack.push(sb.toString());
    
    return stack;
  }
  
}
