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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Push onto the stack the elapsed times if available.
 */
public class ELAPSED extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ELAPSED(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    List<Long> elapsed = (List<Long>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_ELAPSED);
    
    if (null == elapsed) {
      stack.push(new ArrayList<Long>());
    } else {
      stack.push(Collections.unmodifiableList(elapsed));
    }
    
    return stack;
  }
}
