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
 * Replace a regular expression by a replacement string
 */
public class REPLACE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean all;
  
  public REPLACE(String name, boolean all) {
    super(name);
    this.all = all;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    String replacement = stack.pop().toString();
    String regexp = stack.pop().toString();
    String in = stack.pop().toString();
    
    if (this.all) {
      stack.push(in.replaceAll(regexp, replacement));
    } else {
      stack.push(in.replaceFirst(regexp, replacement));
    }
    
    return stack;
  }
}
