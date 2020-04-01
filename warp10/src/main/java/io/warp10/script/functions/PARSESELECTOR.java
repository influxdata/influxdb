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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.warp10.WarpURLDecoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Parse a GTS Selector in a String and push the class selector and labels selector onto the stack
 */
public class PARSESELECTOR extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final Pattern EXPR_RE = Pattern.compile("^([^{]+)\\{(.*)\\}$");

  public PARSESELECTOR(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object sel = stack.pop();
    
    if (!(sel instanceof String)) {
      throw new WarpScriptException(getName() + " expects a String on top of the stack.");
    }
    
    try {
      Object[] result = parse(sel.toString());
      
      //
      // Push result onto the stack
      //
      
      stack.push(result[0]);
      stack.push(result[1]);      
    } catch (WarpScriptException ee) {
      throw new WarpScriptException(ee);
    }
        
    return stack;
  }
  
  public static Object[] parse(String sel) throws WarpScriptException {
    Object[] result = new Object[2];
    
    Matcher m = EXPR_RE.matcher(sel.toString());
    
    if (!m.matches()) {
      throw new WarpScriptException(" invalid syntax for selector.");
    }
    
    String classSelector = null;
    
    try {
      classSelector = WarpURLDecoder.decode(m.group(1), StandardCharsets.UTF_8);
    } catch (UnsupportedEncodingException uee) {
      // Can't happen, we're using UTF-8
    }
    
    String labelsSelection = m.group(2);
    
    Map<String,String> labelsSelectors;

    try {
      labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
    } catch (ParseException pe) {
      throw new WarpScriptException(pe);
    }

    result[0] = classSelector;
    result[1] = labelsSelectors;
    
    return result;
  }
}
