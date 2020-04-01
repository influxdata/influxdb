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

package io.warp10.warp.sdk;

import java.util.List;

public interface WarpScriptJavaFunction {
    
  /**
   * @return true if the UDF can only be called from within a secure macro
   */
  public boolean isProtected();
  
  /**
   * Return the number of stack levels to pass to the function when calling it
   */
  public int argDepth();
  
  /**
   * Apply the function to the given arguments. The return value
   * is a list of elements to be put onto the stack.
   * @param args
   * @return
   */
  public List<Object> apply(List<Object> args) throws WarpScriptJavaFunctionException;
}
