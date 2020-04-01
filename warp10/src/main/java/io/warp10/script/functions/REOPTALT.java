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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.warp10.continuum.TimeSource;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Creates an optimized regular expression alternative from a set of strings
 * so as to minimize backtracking.
 */
public class REOPTALT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public REOPTALT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of strings on top of the stack.");
    }
    
    List<String> alternatives = new ArrayList<String>();
    
    for (Object o: (List) top) {
      alternatives.add(String.valueOf(o));
    }
    
    stack.push(buildOptAlt(alternatives));
    return stack;
  }
  
  private static final String buildOptAlt(List<String> alternatives) {
    
    if (alternatives.isEmpty()) {
      return "";
    } else if (alternatives.size() == 1) {
      return alternatives.get(0);
    }
    
    //
    // Make sure we do not have duplicates
    //
    
    Set<String> uniques = new HashSet<String>(alternatives);
    
    boolean hasEmpty = uniques.remove("");
    
    if (uniques.isEmpty()) {
      return "";
    }
    
    alternatives = new ArrayList<String>(uniques);
    
    //
    // Sort the strings
    //
    
    Collections.sort(alternatives);
    
    //
    // Now extract the maximum prefix length, i.e. the one that produces prefixes with the same cardinality
    // as the previous length (i.e. -1)
    //
    
    int len = 1;
    
    int prevcard = Integer.MAX_VALUE;
    int cardinality = 0;
    
    int minlen = alternatives.get(0).length();
    
    while(cardinality <= prevcard && len <= minlen) {      
      cardinality = 1;
      int idx = 1;
      
      while(idx < alternatives.size() && cardinality <= prevcard) {
        String previous = alternatives.get(idx - 1);
        String current = alternatives.get(idx);
    
        if (1 == len) {
          int mlen = current.length();
          if (mlen < minlen) {
            minlen = mlen;
          }
        }
        
        for (int i = 0; i < len; i++) {
          // If one character is different
          if (previous.charAt(i) != current.charAt(i)) {
            cardinality++;
            break;
          }
        }
        idx++;
      }
      
      if (cardinality <= prevcard) {
        prevcard = cardinality;
        len++;
      } else {
        len--;
        break;
      }
    }
    
    if (len > minlen) {
      len = minlen;
    }

    // len now contains the prefix len which minimizes the cardinality
    
    StringBuilder regexp = new StringBuilder("(");
    
    if (hasEmpty) {
      regexp.append("|");          
    }
    
    String prefix = alternatives.get(0).substring(0, len);
    
    int idx = 0;
    int nprefixes = 0;
    
    Set<String> suffixes = new HashSet<String>();
    
    while(idx < alternatives.size()) {
      String alternative = alternatives.get(idx);
      
      if (alternative.startsWith(prefix)) {
        suffixes.add(alternative.substring(len));
      } else {
        // Update the regexp with the current prefix
        if (nprefixes > 0) {
          regexp.append("|");
        }
        regexp.append(escape(prefix));
        // add the subregexp
        List<String> subalts = new ArrayList<String>(suffixes);
        
        //
        // Check if all suffixes are of the same size and share a common end
        //
        
        int length = -1;
        
        if (subalts.size() > 1) {
          for (String suffix: subalts) {
            if (-1 == length) {
              length = suffix.length();
            }
            if (suffix.length() != length) {
              length = -1;
              break;
            }
          }      
        }
        
        String end = "";

        int endlen = 0;

        if (-1 != length) {
          endlen = length - 1;
          
          boolean done = false;
          
          while (!done && endlen > 0) {
            end = subalts.get(0).substring(length - endlen);
            
            done = true;
            
            for (String suffix: subalts) {
              if (!suffix.endsWith(end)) {
                done = false;
                break;
              }
            }
            
            if (!done) {
              endlen--;
            }
          }
          
          //
          // Shorten all alternatives by the length of their common suffix
          //
          
          if (endlen > 0) {
            for (int i = 0; i < subalts.size(); i++) {
              String subalt = subalts.get(i);
              subalts.set(i, subalt.substring(0, subalt.length() - endlen));
            }        
          }
        }
        
        regexp.append(buildOptAlt(subalts));
        
        if (endlen > 0) {
          regexp.append(end);
        }
        
        suffixes.clear();
        suffixes.add(alternative.substring(len));
        // Change the prefix
        prefix = alternative.substring(0, len);
        nprefixes++;
      }
      idx++;
    }

    if (nprefixes > 0) {
      regexp.append("|");
    }
    regexp.append(escape(prefix));
    // add the subregexp
    List<String> subalts = new ArrayList<String>(suffixes);
    
    //
    // Check if all suffixes are of the same size and share a common end
    //
    
    int length = -1;
    
    if (subalts.size() > 1) {
      for (String suffix: subalts) {
        if (-1 == length) {
          length = suffix.length();
        }
        if (suffix.length() != length) {
          length = -1;
          break;
        }
      }      
    }
    
    String end = "";

    int endlen = 0;

    if (-1 != length) {
      endlen = length - 1;
      
      boolean done = false;
      
      while (!done && endlen > 0) {
        end = subalts.get(0).substring(length - endlen);
        
        done = true;
        
        for (String suffix: subalts) {
          if (!suffix.endsWith(end)) {
            done = false;
            break;
          }
        }
        
        if (!done) {
          endlen--;
        }
      }
      
      //
      // Shorten all alternatives by the length of their common suffix
      //
      
      if (endlen > 0) {
        for (int i = 0; i < subalts.size(); i++) {
          String subalt = subalts.get(i);
          subalts.set(i, subalt.substring(0, subalt.length() - endlen));
        }        
      }
    }
    
    regexp.append(buildOptAlt(subalts));
    if (endlen > 0) {
      regexp.append(end);
    }
    regexp.append(")");
      

    //
    // Check if the regexp if of the form (x|y|z|...) where each alternative is a single
    // char, if it is the case, replace this by [xyz...]
    //
    
    // Count the number of '|'
    int count = 0;
    for (int i = 0; i < regexp.length(); i++) {
      if ('|' ==  regexp.charAt(i)) {
        count++;
      }
    }
    
    // If the regexp is of the right shape AND does not contain '-' as an alternative,
    // rewrite it
    if (regexp.length() - 2 == count + count + 1 && -1 == regexp.indexOf("-")) {
      regexp.deleteCharAt(0);
      regexp.deleteCharAt(regexp.length() - 1);
      while(regexp.indexOf("|") >= 0) {
        regexp.deleteCharAt(regexp.indexOf("|"));
      }
      regexp.insert(0, "[");
      regexp.append("]");
    }
    
    return regexp.toString();
  }
  
  private static final String escape(String str) {
    str = str.replaceAll("\\\\", "\\\\");
    str = str.replaceAll("\\[", "\\[");
    str = str.replaceAll("\\]", "\\]");
    str = str.replaceAll("\\{", "\\{");
    str = str.replaceAll("\\}", "\\}");
    str = str.replaceAll("\\(", "\\(");
    str = str.replaceAll("\\)", "\\)");
    str = str.replaceAll("\\|", "\\|");
    str = str.replaceAll("\\.", "\\.");
    str = str.replaceAll("\\?", "\\?");
    str = str.replaceAll("\\+", "\\+");
    str = str.replaceAll("\\*", "\\*");
    str = str.replaceAll("\\]", "\\]");
    return str;
  }
}
