//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10.script.ext.stackps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStackRegistry;

public class WSINFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public static final String[] EXPOSED_ATTRIBUTES = new String[] {
      WarpScriptStack.ATTRIBUTE_CREATION_TIME,
      WarpScriptStack.ATTRIBUTE_FETCH_COUNT,
      WarpScriptStack.ATTRIBUTE_GTS_COUNT,
      WarpScriptStack.ATTRIBUTE_MACRO_NAME,
      WarpScriptStack.ATTRIBUTE_NAME,
      WarpScriptStack.ATTRIBUTE_SECTION_NAME,
      StackPSWarpScriptExtension.ATTRIBUTE_SESSION,
  };
  
  public WSINFO(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a session id.");
    }
    
    List<Object> infos = new ArrayList<Object>();

    String session = top.toString();
    
    for (WarpScriptStack stck: WarpScriptStackRegistry.stacks()) {
      if (session.equals(stck.getAttribute(StackPSWarpScriptExtension.ATTRIBUTE_SESSION))) {
        infos.add(getInfos(stck));
      }
    }
    
    stack.push(infos);
    
    return stack;
  }
  
  public static Map<Object,Object> getInfos(WarpScriptStack stck) {
    Map<Object,Object> infos = new HashMap<Object,Object>();

    infos.put("uuid", stck.getUUID());
    
    Map<String,Object> attributes = new HashMap<String,Object>();
    infos.put("attributes", attributes);
    
    for (String attr: EXPOSED_ATTRIBUTES) {
      attributes.put(attr, stck.getAttribute(attr));          
    }  
    
    return infos;
  }
}
