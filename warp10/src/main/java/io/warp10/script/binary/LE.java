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

package io.warp10.script.binary;

/**
 * Checks the two operands on top of the stack for less or equal
 */
public class LE extends ComparisonOperation {
  
  public LE(String name) {
    super(name, false, true); // NaN NaN <= must return true in WarpScript
  }
  
  @Override
  public boolean operator(int op1, int op2) {
    return op1 <= op2;
  }
  
}
