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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Applies a regexp to a String
 */
public class MATCH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public MATCH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object regexp = stack.pop();
    
    if (!(regexp instanceof String) && !(regexp instanceof Pattern) && !(regexp instanceof Matcher)) {
      throw new WarpScriptException(getName() + " regular expression MUST be a string, a compiled pattern or a matcher.");
    }
    
    Matcher m = null;
    Pattern pattern = null;
    
    if (regexp instanceof Matcher) {
      m = (Matcher) regexp;
    } else if (regexp instanceof Pattern) {
      pattern = (Pattern) regexp;
      m = pattern.matcher("");
    } else {
      pattern = Pattern.compile(regexp.toString());
      m = pattern.matcher("");
    }
    
    Object string = stack.pop();
    
    if (!(string instanceof String)) {
      throw new WarpScriptException(getName() + " can only be applied to a string.");
    }
    
    m.reset(string.toString());

    List<String> groups = new ArrayList<String>();
    
    if (m.matches()) {
      groups.add(m.group(0));
      for (int i = 1; i <= m.groupCount(); i++) {
        groups.add(m.group(i));
      }
    }

    stack.push(groups);
    
    return stack;
  }
}
