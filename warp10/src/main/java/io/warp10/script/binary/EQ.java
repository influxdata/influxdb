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

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Checks the two operands on top of the stack for equality
 */
public class EQ extends ComparisonOperation {
  
  public EQ(String name) {
    super(name, false, true); // NaN NaN == must return true in WarpScript
  }
  
  @Override
  public boolean operator(int op1, int op2) {
    return op1 == op2;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op1 instanceof Boolean || op1 instanceof String
        || op1 instanceof RealVector || op1 instanceof RealMatrix
        || op1 instanceof List || op1 instanceof Map || op1 instanceof Set) {
      stack.push(op1.equals(op2));
    } else {
      // gts and numbers
      comparison(stack, op1, op2);
    }
    return stack;
  }
  
  public static int compare(Number a, Number b) {
    if (a.equals(b)) {
      return 0;
    }
    
    if (a instanceof Double && b instanceof Double) {
      return ((Double) a).compareTo((Double) b);
    } else if (((a instanceof Long || a instanceof Integer || a instanceof Short || a instanceof Byte || a instanceof AtomicLong)
        && (b instanceof Long || b instanceof Integer || b instanceof Short || b instanceof Byte || b instanceof AtomicLong))) {
      return Long.compare(a.longValue(), b.longValue());
    } else {
      // If the equals function fails and the types do not permit direct number comparison,
      // we test again with BigDecimal comparison for type abstraction
      // We want '10 10.0 ==' or '10 10.0 >=' to be true
      BigDecimal bda;
      BigDecimal bdb;
      
      if (a instanceof Double || a instanceof Float) {
        bda = new BigDecimal(a.doubleValue());
      } else if (a instanceof Long || a instanceof Integer || a instanceof Short || a instanceof Byte) {
        bda = new BigDecimal(a.longValue());
      } else {
        bda = new BigDecimal(a.toString());
      }
      
      if (b instanceof Double || a instanceof Float) {
        bdb = new BigDecimal(b.doubleValue());
      } else if (b instanceof Long || b instanceof Integer || b instanceof Short || b instanceof Byte) {
        bdb = new BigDecimal(b.longValue());
      } else {
        bdb = new BigDecimal(b.toString());
      }
      
      return bda.compareTo(bdb);
    }
  }
}
