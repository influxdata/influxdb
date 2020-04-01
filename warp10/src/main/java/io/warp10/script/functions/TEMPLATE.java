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

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.github.mustachejava.MustacheResolver;

/**
 * Fill a Mustache template from a map
 */
public class TEMPLATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  /**
   * Factory for creating Mustache templates. Use a 'null' resolver.
   */
  private static final MustacheFactory mf = new DefaultMustacheFactory(new MustacheResolver() {
    @Override
    public Reader getReader(String resourceName) {
      throw new RuntimeException("Template inclusion is disabled.");
    }    
  });
  
  public TEMPLATE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map on top of the stack.");
    }
    
    Map<String,Object> scope = (Map) o;
    
    o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a string.");
    }
    
    //
    // Check that 'scope' only contains strings, numbers, booleans or lists thereof
    //
    
    List<Object> elts = new ArrayList<Object>();
    elts.add(scope);
    
    while(!elts.isEmpty()) {         
      Object res = checkTypes(elts.get(0));
      elts.remove(0);
      
      if (null != res) {
        elts.addAll((Collection) res);
      }
    }
        
    try {
      Mustache mustache = mf.compile(new StringReader(o.toString()), "");
      
      StringWriter writer = new StringWriter();
      
      mustache.execute(writer, scope);
      
      stack.push(writer.toString());

      return stack;      
    } catch (Exception e) {
      throw new WarpScriptException(e);
    }
  }
  
  private Object checkTypes(Object o) throws WarpScriptException {
    if (o instanceof String || o instanceof Number || o instanceof Boolean) {
      return null;
    }
    
    if (o instanceof List) {
      return o;
    }
        
    if (o instanceof Map) {
      ArrayList<Object> l = new ArrayList<Object>();
      l.addAll(((Map) o).keySet());
      l.addAll(((Map) o).values());
      
      return l;
    }
    
    throw new WarpScriptException("Invalid type for template substitution.");
  }
}
