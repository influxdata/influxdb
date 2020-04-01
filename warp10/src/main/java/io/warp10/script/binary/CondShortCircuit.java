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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSOpsHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack.Macro;

import java.util.List;

/**
 * Conditional boolean operation taking either:
 * - two operands on top of the stack.
 * - a list of booleans or boolean-returning-macros.
 * This class implements short-circuit evaluation (https://en.wikipedia.org/wiki/Short-circuit_evaluation).
 */
public abstract class CondShortCircuit extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  protected final boolean triggerValue;

  private final GTSOpsHelper.GTSBinaryOp op = new GTSOpsHelper.GTSBinaryOp() {
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      return operator(((boolean) GTSHelper.valueAtIndex(gtsa, idxa)) , ((boolean) GTSHelper.valueAtIndex(gtsb, idxb)));
    }
  };

  public abstract boolean operator(boolean bool1, boolean bool2);

  public CondShortCircuit(String name, boolean triggerValue) {
    super(name);
    this.triggerValue = triggerValue;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    String exceptionMessage = getName() + " can only operate on two boolean values, or two boolean GTS, or a list of booleans or macros, each macro putting a single boolean on top of the stack.";

    Object top = stack.pop();

    if (top instanceof List) {
      // List version: check each element, one after another, exiting when triggerValue() is found. In case of early
      // exiting, triggerValue() is putted on top of the stack.
      for (Object operand : (List) top) {
        // If a macro is found, execute it and use the result as the operand.
        if (operand instanceof Macro) {
          stack.exec((Macro) operand);
          operand = stack.pop();
        }

        if (operand instanceof Boolean) {
          // Short-circuit evaluation: found an early exit case.
          if (triggerValue == (boolean) operand) {
            stack.push(triggerValue);
            return stack;
          }
        } else {
          throw new WarpScriptException(exceptionMessage);
        }
      }

      // No short-circuit found
      stack.push(!triggerValue);
      return stack;
    } else {
      // two operands
      Object op2 = top;
      Object op1 = stack.pop();
      if (op2 instanceof Boolean && op1 instanceof Boolean) {
        // Simple case: both operands are booleans, do a || and push to the stack.
        stack.push(operator((boolean) op1, (boolean) op2));
        return stack;
      } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
        // logical operator between boolean GTS values
        GeoTimeSerie gts1 = (GeoTimeSerie) op1;
        GeoTimeSerie gts2 = (GeoTimeSerie) op2;
        if (GeoTimeSerie.TYPE.BOOLEAN == gts1.getType() && GeoTimeSerie.TYPE.BOOLEAN == gts2.getType()) {
          GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
          result.setType(GeoTimeSerie.TYPE.BOOLEAN);
          GTSOpsHelper.applyBinaryOp(result, gts1, gts2, this.op);
          // If result is empty, set type and sizehint to default.
          if (0 == result.size()) {
            result = result.cloneEmpty();
          }
          stack.push(result);
          return stack;
        } else if ((GeoTimeSerie.TYPE.UNDEFINED == gts1.getType() || GeoTimeSerie.TYPE.BOOLEAN == gts1.getType())
                && (GeoTimeSerie.TYPE.UNDEFINED == gts2.getType() || GeoTimeSerie.TYPE.BOOLEAN == gts2.getType())) {
          // gts1 or gts2 empty, return an empty gts
          GeoTimeSerie result = new GeoTimeSerie();
          // Make sure the bucketization logic is still applied to the result, even if empty.
          GTSOpsHelper.handleBucketization(result, gts1, gts2);
          stack.push(result);
          return stack;
        } else {
          throw new WarpScriptException(getName() + " can only operate on boolean values or boolean GTS.");
        }
      } else {
        throw new WarpScriptException(exceptionMessage);
      }
    }
  }
}
