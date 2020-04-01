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

package io.warp10.script.processing;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import processing.core.PGraphics;

public class ProcessingUtil {
  
  /**
   * Retrieve parameters from the stack until a PGraphics instance is found.
   * 
   * @return a list of parameters, the PGraphics instance being the first one.
   */
  public static List<Object> parseParams(WarpScriptStack stack, int... numparams) throws WarpScriptException {
    List<Object> params = new ArrayList<Object>();
        
    int count = 0;
    
    Arrays.sort(numparams);
    int maxparams = numparams[numparams.length - 1];
    
    while(0 != stack.depth() && count <= maxparams) {
      Object top = stack.pop();
      
      if (top instanceof PGraphics) {
        params.add(top);
        Collections.reverse(params);
        break;
      }
      
      params.add(top);
      count++;
    }
    
    if (Arrays.binarySearch(numparams, count) < 0) {
      throw new WarpScriptException("Invalid number of parameters, expected one of " + Arrays.toString(numparams) + " but found " + count);
    }
    
    return params;
  }

}
