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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class ERROR extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ERROR(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Throwable t = (Throwable) stack.getAttribute(WarpScriptStack.ATTRIBUTE_LAST_ERROR);
    
    // List of errors (with cause for each one)
    List<Object> errlist = new ArrayList<Object>();
    
    // Set to make sure we don't have cycles
    Set<Throwable> throwables = new HashSet<Throwable>();
    
    while(null != t && !throwables.contains(t)) {
      Map<String,Object> err = new HashMap<String,Object>();
      
      List<Object> trace = new ArrayList<Object>();
      
      StackTraceElement[] st = t.getStackTrace();

      if (null != st) {
        for (StackTraceElement elt: st) {
          String className = elt.getClassName();
          int lineNumber = elt.getLineNumber();
          String methodName = elt.getMethodName();
          String fileName = elt.getFileName();

          List<Object> elts = new ArrayList<Object>();

          elts.add(fileName);
          elts.add(lineNumber);
          elts.add(className);
          elts.add(methodName);

          trace.add(elts);
        }
      }

      err.put("type", t.getClass().toString());
      err.put("message", t.getMessage());
      err.put("stacktrace", trace);

      throwables.add(t);
      
      t = t.getCause();
      
      errlist.add(err);
    }
    
    stack.push(errlist);
    
    return stack;
  }
}
