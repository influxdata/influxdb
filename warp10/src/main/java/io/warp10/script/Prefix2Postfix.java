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

package io.warp10.script;

import io.warp10.continuum.gts.UnsafeString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Convert prefix notation to postfix one
 */
public class Prefix2Postfix {
  
  /**
   * Converts prefix to postfix, considering the arguments of the prefix
   * notation are in the order they will be popped out of the stack
   * 
   * @param prefix
   * @return
   */
  public static String convert(String prefix) {
    StringBuilder sb = new StringBuilder();
    
    //
    // Replace '\n' with ';'
    //
    
    prefix = prefix.replace('\n', ';');
    
    //
    // Split by ';'
    //
    
    String[] lines = prefix.split("[;]");
    
    for (String line: lines) {
      //
      // Identify tokens separated by parens or commas
      //
      
      List<String> tokens = new ArrayList<String>();
      
      char[] c = UnsafeString.getChars(line);
      
      int lastidx = 0;
      int idx = 0;
      
      while (idx < c.length) {
        while(c[idx] != '(' && c[idx] != ')' && c[idx] != ',') {
          idx++;
        }
        
        if (idx != lastidx) {
          tokens.add(new String(c, lastidx, idx - lastidx));
        }
        
        idx++;
        lastidx = idx;
      }
      
      //
      // Reverse tokens
      //

      Collections.reverse(tokens);
      
      for (String token: tokens) {
        sb.append(token);
        sb.append("\n");
      }
      
    }

    return sb.toString();
  }
  
  public static String convertReversedArguments(String prefix) {
    StringBuilder sb = new StringBuilder();
    
    //
    // Replace '\n' with ';'
    //
    
    prefix = prefix.replace("\n", ";");
    
    //
    // Split by ';'
    //
    
    String[] lines = prefix.split("[;\n]");
    
    for (String line: lines) {
      //
      // Identify tokens separated by parens or commas
      //
      
      List<String> tokens = new ArrayList<String>();
      
      char[] c = UnsafeString.getChars(line);
      
      int lastidx = 0;
      int idx = 0;
      
      while (idx < c.length) {
        while(c[idx] != '(' && c[idx] != ')' && c[idx] != ',') {
          idx++;
        }
             
        if (idx != lastidx) {
          tokens.add(new String(c, lastidx, idx - lastidx));
        }
     
        if (',' != c[idx]) {
          tokens.add(new String(c, idx, 1));
        }

        idx++;
        lastidx = idx;
      }
      
      //
      // Remove empty tokens
      //
      
      Iterator<String> iter = tokens.iterator();
      
      while(iter.hasNext()) {
        String token = iter.next().trim();
        
        if ("".equals(token)) {
          iter.remove();
        }        
      }
      
      //
      // Reverse the order of the tokens
      //
      
      Collections.reverse(tokens);
      
      //
      // Now work from inner groups to outer groups, replacing the prefix
      // notation with postfix one, reversing the order of arguments for each group
      //
      
      List<Object> expressions = new ArrayList<Object>();

      expressions.addAll(tokens);
      
      //
      // Loop until we only have one expression
      //
      
      while(expressions.size() > 1) {
        
        idx = 0;
        int endidx = 0;
        int startidx;
        
        //
        // Find the next opening paren and the matching closing one
        //
        
        while(idx < expressions.size() && !"(".equals(expressions.get(idx))) {
          if (")".equals(expressions.get(idx))) {
            endidx = idx;
          }
          idx++;
        }

        if (idx >= expressions.size()) {
          //throw new RuntimeException("Invalid syntax");
          break;
        }
        
        //
        // Extract parameters
        //
        
        List<Object> args = expressions.subList(endidx + 1, idx);
        
        // Extract 'function'
        
        List<Object> group = new ArrayList<Object>(args.size() + 1);

        if (idx < expressions.size() - 1) {
          Object func = expressions.get(idx + 1);
        
          group.add(func);
        }

        group.addAll(args);

        // Replace the group we just converted

        for (int i = 0; i < group.size() + 2; i++) {
          expressions.remove(endidx);
        }
        
        // Insert the group
        
        expressions.add(endidx, group);        
      }
      
      //
      // Now flatten 'expressions'
      //
      
      boolean hassubgroups = true;
      
      
      while (hassubgroups) {
        
        hassubgroups = false;
        
        List<Object> flattened = new ArrayList<Object>();
        
        for (Object group: expressions) {
          if (group instanceof List) {
            for (Object subgroup: (List) group) {
              if (subgroup instanceof List) {
                hassubgroups = true;
              }
              flattened.add(subgroup);
            }
          } else {
            flattened.add(group);
          }
        }
        
        expressions = flattened;
      }
      
      //
      // Flush 'expressions'
      //
      
      for (idx = expressions.size() - 1; idx >= 0; idx--) {
        sb.append(expressions.get(idx).toString().trim());
        sb.append("\n");
      }
    }    
    
    return sb.toString();
  }
}
