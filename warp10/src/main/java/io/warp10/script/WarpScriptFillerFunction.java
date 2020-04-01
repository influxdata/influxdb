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


public interface WarpScriptFillerFunction {
  /**
   * This method will determine the tick,loc,elev,value to fill
   * with.
   * 
   * It takes as parameters an array containing multiple arrays.
   * 
   * An array of class, labels, class labels of the GTS being filled and of the 'other'
   * GTS.
   * 
   * Arrays of tick,loc,elev,value for the 'pre' window, for the current tick of the 'other'
   * GTS and for the 'post' window.
   * 
   * It is expected to return an array of tick,loc,elev,value which will be used to fill the
   * current GTS.
   * 
   * @param args
   * @return
   * @throws WarpScriptException
   */
  public Object[] apply(Object[] args) throws WarpScriptException;
  
  /**
   * Returns the size of the pre-window (in number of ticks)
   * @return
   */
  public int getPreWindow();
  
  /**
   * Returns the size of the post-window (in ticks)
   * @return
   */
  public int getPostWindow();
}

