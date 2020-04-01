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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

/**
 * Modifies a Macro so the LOAD/STORE operations for the given variables are
 * replaced by use of registers
 */
public class ASREGS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final NOOP NOOP = new NOOP(WarpScriptLib.NOOP);
  
  public ASREGS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of variable names on top of the stack.");
    }

    List vars = (List) top;
    
    top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a macro.");
    }

    Object[] regs = stack.getRegisters();
    int nregs = regs.length;

    // Bitset to determine the used registers
    BitSet inuse = new BitSet(nregs);
    
    //
    // Inspect the macro to determine the registers which are already used
    //
    List<Macro> allmacros = new ArrayList<Macro>();
    allmacros.add((Macro) top);
    
    boolean abort = false;
    
    while(!abort && !allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);
      
      List<Object> statements = new ArrayList<Object>(m.statements());
                
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i) instanceof PUSHR) {
          inuse.set(((PUSHR) statements.get(i)).getRegister());
        } else if (statements.get(i) instanceof POPR) {
          inuse.set(((POPR) statements.get(i)).getRegister());
        } else if (statements.get(i) instanceof LOAD || statements.get(i) instanceof CSTORE) {
          // If the statement is the first, we cannot determine if what
          // we load or update is a register or a variable, so abort.
          if (0 == i) {
            abort = true;
            break;
          }
          // Fetch what precedes the LOAD/CSTORE
          Object symbol = statements.get(i - 1);
          if (symbol instanceof Long) {
            inuse.set(((Long) symbol).intValue());
          } else if (!(symbol instanceof String)) {
            // We encountered a LOAD/CSTORE with a non string and non long param,
            // we cannot determine what is being loaded or updated, so no replacement
            // can occur
            abort = true;
          }
        } else if (statements.get(i) instanceof STORE) {
          // If the statement is the first, we cannot determine if what
          // we load is a register or a variable, so abort.
          if (0 == i) {
            abort = true;
            break;
          }
          // Fetch what precedes the STORE
          Object symbol = statements.get(i - 1);
          if (symbol instanceof Long) {
            inuse.set(((Long) symbol).intValue());
          } else if (symbol instanceof List) {
            // We inspect the list, looking for registers
            for (Object elt: (List) symbol) {
              if (elt instanceof Long) {
                inuse.set(((Long) elt).intValue());
              }
            }
          } else if (symbol instanceof ENDLIST) {
            // We go backwards in statements until we find a MARK, inspecting elements
            // If we encounter something else than String/Long/NULL, we abort as we cannot
            // determine if a register is used or not
            int idx = i - 2;
            while (idx >= 0 && !(statements.get(idx) instanceof MARK)) {
              Object stmt = statements.get(idx--);
              if (stmt instanceof Long) {
                inuse.set(((Long) stmt).intValue());
              } else if (null != stmt && !(stmt instanceof String) && !(stmt instanceof NULL)) {
                abort = true;
                break;
              }
            }            
          } else if (!(symbol instanceof String)) {
            // We encountered a STORE with something that is neither a register, a string or
            // a list, so we cannot determine if a register is involved or not, so we abort
            abort = true;
          }
        } else if (statements.get(i) instanceof Macro) {
          allmacros.add((Macro) statements.get(i));
        }
      }
    }
    
    if (abort) {
      throw new WarpScriptException(getName() + " was unable to convert variables to registers.");
    }

    Map<String,Integer> varregs = new HashMap<String,Integer>();
    
    int regidx = 0;
    int numregs = 0;
    
    for (Object v: vars) {
      
      if (v instanceof Long) {
        continue;
      }
      
      // Stop processing variables if we already assigned all the registers
      if (inuse.cardinality() == nregs) {
        break;
      }
      
      if (!(v instanceof String)) {
        throw new WarpScriptException(getName() + " expects a list of variable names on top of the stack.");        
      }
      
      if (null == varregs.get(v.toString())) {
        regidx = inuse.nextClearBit(0);
        inuse.set(regidx);
        varregs.put(v.toString(), regidx);
      }
    }    

    numregs = regidx + 1;

    WarpScriptStackFunction[] regfuncs = new WarpScriptStackFunction[numregs * 3];
    
    for (int i = 0; i < numregs; i++) {
      regfuncs[i] = new PUSHR(WarpScriptLib.PUSHR + i, i); // LOAD
      regfuncs[numregs + i] = new POPR(WarpScriptLib.POPR + i, i); // STORE
      regfuncs[numregs + numregs + i] = new POPR(WarpScriptLib.POPR + i, i, true); // CSTORE
    }
        
    //
    // Now loop over the macro statement, replacing occurrences of X LOAD and X STORE by the use
    // of the assigned register
    //
    
    allmacros.clear();
    allmacros.add((Macro) top);
    
    while(!abort && !allmacros.isEmpty()) {
      Macro m = allmacros.remove(0);
      
      List<Object> statements = new ArrayList<Object>(m.statements());
                
      for (int i = 0; i < statements.size(); i++) {
        if (statements.get(i) instanceof Macro) {
          allmacros.add((Macro) statements.get(i));
          continue;
        } else if (i > 0 && statements.get(i) instanceof LOAD) {
          Object symbol = statements.get(i - 1);
          
          if (symbol instanceof String) {
            Integer regno = varregs.get(symbol.toString());
            if (null != regno) {
              statements.set(i - 1, NOOP);
              statements.set(i, regfuncs[regno]);
            }            
          } else if (!(symbol instanceof Long)) {
            abort = true;
            break;
          }
        } else if (i > 0 && statements.get(i) instanceof STORE) {
          Object symbol = statements.get(i - 1);
          if (symbol instanceof String) {
            Integer regno = varregs.get(symbol.toString());
            if (null != regno) {
              statements.set(i - 1, NOOP);
              statements.set(i, regfuncs[regno + numregs]);
            }                    
          } else if (symbol instanceof List) {
            for (int k = 0; k < ((List) symbol).size(); k++) {
              if (((List) symbol).get(k) instanceof String) {
                Integer regno = varregs.get(((List) symbol).get(k).toString());
                if (null != regno) {
                  ((List) symbol).set(k, (long) regno);
                }
              }
            }
          } else if (symbol instanceof ENDLIST) {
            int idx = i - 2;
            while (idx >= 0 && !(statements.get(idx) instanceof MARK)) {
              Object stmt = statements.get(idx);
              if (stmt instanceof String) {
                Integer regno = varregs.get(statements.get(idx));
                if (null != regno) {
                  statements.set(idx, (long) regno);
                }
              }
              idx--;
            }            
          } else if (!(symbol instanceof Long)) {
            abort = true;
            break;
          }
        } else if (i > 0 && statements.get(i) instanceof CSTORE) {
          Object symbol = statements.get(i - 1);
          if (symbol instanceof String) {
            Integer regno = varregs.get(symbol.toString());
            if (null != regno) {
              statements.set(i - 1, NOOP);
              statements.set(i, regfuncs[regno + numregs + numregs]);
            }
          } else if (!(symbol instanceof Long)) {
            abort = true;
            break;
          }
        }
      }      
      
      if (!abort) {
        List<Object> macstmt = m.statements();
        // Ignore the NOOPs
        int noops = 0;
        for (int i = 0; i < statements.size(); i++) {
          if (statements.get(i) instanceof NOOP) {
            noops++;
            continue;
          }
          macstmt.set(i - noops, statements.get(i));
        }
        m.setSize(statements.size() - noops);
      }
    }

    if (abort) {
      throw new WarpScriptException(getName() + " was unable to convert variables to registers.");
    }
    stack.push(top);
    
    return stack;
  }
}
