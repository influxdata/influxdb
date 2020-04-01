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

package io.warp10.script.functions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Extract all used variables in a macro. If a STORE/CSTORE or LOAD operation is
 * found with a parameter which is not a string, then an error is raised.
 */
public class VARS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public VARS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a macro.");
    }
    
    //
    // Now loop over the macro statement, extracting variable names
    //
    
    Set<Object> symbols = new LinkedHashSet<Object>();
    
    final Map<Object,AtomicInteger> occurrences = new HashMap<Object,AtomicInteger>();
    
    List<Macro> allmacros = new ArrayList<Macro>();
    allmacros.add((Macro) top);
    
    boolean abort = false;

    while(!abort && !allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);
      
      List<Object> statements = new ArrayList<Object>(m.statements());
                
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i) instanceof Macro) {
          allmacros.add((Macro) statements.get(i));
          continue;
        } else if (statements.get(i) instanceof POPR) {
          symbols.add((long) ((POPR) statements.get(i)).getRegister());
          AtomicInteger occ = occurrences.get((long) ((POPR) statements.get(i)).getRegister());
          if (null == occ) {
            occ = new AtomicInteger();
            occurrences.put((long) ((POPR) statements.get(i)).getRegister(), occ);
          }
          occ.incrementAndGet();
        } else if (statements.get(i) instanceof PUSHR) {
          symbols.add((long) ((PUSHR) statements.get(i)).getRegister());
          AtomicInteger occ = occurrences.get((long) ((PUSHR) statements.get(i)).getRegister());
          if (null == occ) {
            occ = new AtomicInteger();
            occurrences.put((long) ((PUSHR) statements.get(i)).getRegister(), occ);
          }
          occ.incrementAndGet();                  
        } else if (statements.get(i) instanceof LOAD || statements.get(i) instanceof CSTORE) {
          Object symbol = statements.get(i - 1);
          // If the parameter to LOAD/STORE/CSTORE is not a string, then we cannot extract
          // the variables in a safe way as some may be unknown to us (as their name may be the result
          // of a computation), so in this case we abort the process
          if (symbol instanceof String || symbol instanceof Long) {
            symbols.add(symbol);
            AtomicInteger occ = occurrences.get(symbol);
            if (null == occ) {
              occ = new AtomicInteger();
              occurrences.put(symbol, occ);
            }
            occ.incrementAndGet();
          } else {
            abort = true;
            break;
          }
        } else if (statements.get(i) instanceof STORE) {
          if (0 == i) {
            abort = true;
            break;
          }
          Object symbol = statements.get(i-1);
          if (symbol instanceof List) {
            // We inspect the list, looking for registers
            for (Object elt: (List) symbol) {
              if (elt instanceof String || elt instanceof Long) {
                symbols.add(elt);
                AtomicInteger occ = occurrences.get(elt);
                if (null == occ) {
                  occ = new AtomicInteger();
                  occurrences.put(elt, occ);
                }
                occ.incrementAndGet();
              } else if (null != elt) {
                abort = true;
                break;
              }
            }
          } else if (symbol instanceof ENDLIST) {
            // We go backwards in statements until we find a MARK, inspecting elements
            // If we encounter something else than String/Long/NULL, we abort as we cannot
            // determine if a register is used or not
            int idx = i - 2;
            while (idx >= 0 && !(statements.get(idx) instanceof MARK)) {
              Object stmt = statements.get(idx--);
              if (stmt instanceof String || stmt instanceof Long) {
                symbols.add(stmt);
                AtomicInteger occ = occurrences.get(stmt);
                if (null == occ) {
                  occ = new AtomicInteger();
                  occurrences.put(stmt, occ);
                }
                occ.incrementAndGet();                
              } else if (null != stmt && !(stmt instanceof NULL)) {
                abort = true;
                break;
              }
            }            
          } else if (!(symbol instanceof String) && !(symbol instanceof Long)) {
            // We encountered a STORE with something that is neither a register, a string or
            // a list, so we cannot determine if a register is involved or not, so we abort
            abort = true;
          }
        }
      }            
    }
    
    if (abort) {
      throw new WarpScriptException(getName() + " encountered a LOAD/STORE or CSTORE operation with a non explicit symbol name.");
    }
    
    List<Object> vars = new ArrayList<Object>(symbols);
    
    // Now sort according to the number of occurrences (decreasing)
    
    vars.sort(new Comparator<Object>() {
      @Override
      public int compare(Object s1, Object s2) {
        AtomicInteger occ1 = occurrences.get(s1);
        AtomicInteger occ2 = occurrences.get(s2);
        
        if (occ1.get() < occ2.get()) {
          return 1;
        } else if (occ1.get() > occ2.get()) {
          return -1;
        } else {
          return s1.toString().compareTo(s2.toString());
        }
      }
    });
        
    stack.push(vars);
    
    return stack;
  }
}
