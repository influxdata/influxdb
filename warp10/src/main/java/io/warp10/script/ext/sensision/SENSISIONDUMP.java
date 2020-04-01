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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.sensision.Sensision;

/**
 * Retrieve a string representation of all current Sensision metrics
 */
public class SENSISIONDUMP extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SENSISIONDUMP(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a flag (BOOLEAN) on top of the stack indicating whether to use the metrics update timestamps or the current timestamp when generating Geo Time Series™.");
    }
    
    boolean useMetricsTimestamps = Boolean.TRUE.equals(top);
    
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    
    try {
      Sensision.dump(pw, useMetricsTimestamps);
      pw.flush();
      
      stack.push(sw.toString());
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " error while fetching metrics.", ioe);
    }
        
    return stack;
  }
  
}
