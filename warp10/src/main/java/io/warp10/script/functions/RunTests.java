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

import io.warp10.script.WarpScriptStack;
import io.warp10.script.MemoryWarpScriptStack;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class RunTests {

  public static boolean run(String arg) {
    
    try {
      WarpScriptStack stack = new MemoryWarpScriptStack(null,  null,  new Properties());
      
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.MAX_VALUE);
      
      BufferedReader br = new BufferedReader(new FileReader(arg));
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        stack.exec(line);
      }
      
      br.close();          
    } catch (Throwable t) {
      t.printStackTrace();
      return false;
    }
    
    return true;
  }
  
  public static void main(String[] args) {
    
    boolean failures = false;
    
    for (String arg: args) {
      System.out.print(arg);
      System.out.print("...");
      if (run(arg)) {
        System.out.println("SUCCESS");
      } else {
        System.out.println("FAILURE");
        failures = true;
      }
    }
    
    System.exit(failures ? -1 : 0);
  }
}
