//
//   Copyright 2020  SenX S.A.S.
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

import org.apache.hadoop.hbase.util.Bytes;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSOpsHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public abstract class ComparisonOperation extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public abstract boolean operator(int op1, int op2);
  
  //default behavior for > < operators. For >= <= == or !=, you need to set these.
  private final boolean ifOneNaNOperand;
  private final boolean ifTwoNaNOperands;
  
  /**
   * Default constructor with default behavior for > < operators. For >= <= == or !=, you need to set ifOneNaNOperand and ifTwoNaNOperands.
   *
   * @param name WarpScript function name
   */
  public ComparisonOperation(String name) {
    super(name);
    ifOneNaNOperand = false;
    ifTwoNaNOperands = false;
  }
  
  /**
   * @param name                 WarpsScript function name
   * @param trueIfOneNaNOperand  set it for != comparison
   * @param trueIfTwoNaNOperands set it for == or <= or >= comparison
   */
  public ComparisonOperation(String name, boolean trueIfOneNaNOperand, boolean trueIfTwoNaNOperands) {
    super(name);
    ifOneNaNOperand = trueIfOneNaNOperand;
    ifTwoNaNOperands = trueIfTwoNaNOperands;
  }
  
  private final GTSOpsHelper.GTSBinaryOp stringOp = new GTSOpsHelper.GTSBinaryOp() {
    //could be optimized for string inequality operation
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      return operator((GTSHelper.valueAtIndex(gtsa, idxa)).toString().compareTo((GTSHelper.valueAtIndex(gtsb, idxb)).toString()), 0) ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
    }
  };
  
  // building four different Op for each combination avoid unnecessary type tests in each Operation :
  //  double / double : need to test every NaN use cases
  //  long / long : fastest
  //  double / long : need to test if gtsa value is NaN
  //  long / double : need to test if gtsb value is NaN
  
  private final GTSOpsHelper.GTSBinaryOp longOp = new GTSOpsHelper.GTSBinaryOp() {
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      return operator(EQ.compare((Number) (GTSHelper.valueAtIndex(gtsa, idxa)), (Number) (GTSHelper.valueAtIndex(gtsb, idxb))), 0) ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
    }
  };
  
  private final GTSOpsHelper.GTSBinaryOp doublesOp = new GTSOpsHelper.GTSBinaryOp() {
    // both input GTS are doubles, we need to deal with NaN special cases.
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      if (Double.isNaN((Double) GTSHelper.valueAtIndex(gtsa, idxa)) && Double.isNaN((Double) GTSHelper.valueAtIndex(gtsb, idxb))) {
        return ifTwoNaNOperands ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
      } else if ((Double.isNaN((Double) GTSHelper.valueAtIndex(gtsa, idxa)) || Double.isNaN((Double) GTSHelper.valueAtIndex(gtsb, idxb)))) {
        return ifOneNaNOperand ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
      } else {
        return operator(EQ.compare((Number) (GTSHelper.valueAtIndex(gtsa, idxa)), (Number) (GTSHelper.valueAtIndex(gtsb, idxb))), 0) ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
      }
    }
  };
  
  private final GTSOpsHelper.GTSBinaryOp gtsaIsDoubleOp = new GTSOpsHelper.GTSBinaryOp() {
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      if (Double.isNaN((Double) GTSHelper.valueAtIndex(gtsa, idxa))) {
        return ifOneNaNOperand ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
      } else {
        return operator(EQ.compare((Number) (GTSHelper.valueAtIndex(gtsa, idxa)), (Number) (GTSHelper.valueAtIndex(gtsb, idxb))), 0) ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
      }
    }
  };
  
  private final GTSOpsHelper.GTSBinaryOp gtsbIsDoubleOp = new GTSOpsHelper.GTSBinaryOp() {
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      if (Double.isNaN((Double) GTSHelper.valueAtIndex(gtsb, idxb))) {
        return ifOneNaNOperand ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
      } else {
        return operator(EQ.compare((Number) (GTSHelper.valueAtIndex(gtsa, idxa)), (Number) (GTSHelper.valueAtIndex(gtsb, idxb))), 0) ? GTSHelper.valueAtIndex(gtsa, idxa) : null;
      }
    }
  };
  
  // Default apply function for > < >= =<
  // It only supports number and strings.
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    return comparison(stack, op1, op2);
  }
  
  public Object comparison(WarpScriptStack stack, Object op1, Object op2) throws WarpScriptException {
    
    if (op1 instanceof Double && op2 instanceof Double && Double.isNaN((Double) op1) && Double.isNaN((Double) op2)) {
      //two NaN
      stack.push(ifTwoNaNOperands);
    } else if (op1 instanceof Double && Double.isNaN((Double) op1) && !(op2 instanceof GeoTimeSerie)) { // Do we have only one NaN ?
      stack.push(ifOneNaNOperand);
    } else if (op2 instanceof Double && Double.isNaN((Double) op2) && !(op1 instanceof GeoTimeSerie)) { // Do we have only one NaN ?
      stack.push(ifOneNaNOperand);
    } else if (op2 instanceof Number && op1 instanceof Number) {
      stack.push(operator(EQ.compare((Number) op1, (Number) op2), 0));
    } else if (op2 instanceof String && op1 instanceof String) {
      stack.push(operator(op1.toString().compareTo(op2.toString()), 0));
    } else if (op1 instanceof byte[] && op2 instanceof byte[]) {
      stack.push(operator(Bytes.compareTo((byte[]) op1, (byte[]) op2), 0));
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      // compare two GTS
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;
      if (GeoTimeSerie.TYPE.UNDEFINED == gts1.getType() || GeoTimeSerie.TYPE.UNDEFINED == gts2.getType()) {
        // gts1 or gts2 empty, return an empty gts
        GeoTimeSerie result = new GeoTimeSerie();
        // Make sure the bucketization logic is still applied to the result, even if empty.
        GTSOpsHelper.handleBucketization(result, gts1, gts2);
        stack.push(result);
      } else if (GeoTimeSerie.TYPE.STRING == gts1.getType() && GeoTimeSerie.TYPE.STRING == gts2.getType()) {
        // both strings, compare lexicographically
        GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
        result.setType(GeoTimeSerie.TYPE.STRING);
        GTSOpsHelper.applyBinaryOp(result, gts1, gts2, stringOp, true);
        // If result is empty, set type and sizehint to default.
        if (0 == result.size()) {
          result = result.cloneEmpty();
        }
        stack.push(result);
      } else if ((GeoTimeSerie.TYPE.LONG == gts1.getType() || GeoTimeSerie.TYPE.DOUBLE == gts1.getType())
          && (GeoTimeSerie.TYPE.LONG == gts2.getType() || GeoTimeSerie.TYPE.DOUBLE == gts2.getType())) {
        // both are numbers
        GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
        if (GeoTimeSerie.TYPE.DOUBLE == gts1.getType() && GeoTimeSerie.TYPE.DOUBLE == gts2.getType()) {
          //both input gts are double
          result.setType(GeoTimeSerie.TYPE.DOUBLE);
          GTSOpsHelper.applyBinaryOp(result, gts1, gts2, doublesOp, true);
        } else if (GeoTimeSerie.TYPE.DOUBLE == gts1.getType() && GeoTimeSerie.TYPE.LONG == gts2.getType()) {
          //gts1 is double
          result.setType(GeoTimeSerie.TYPE.DOUBLE);
          GTSOpsHelper.applyBinaryOp(result, gts1, gts2, gtsaIsDoubleOp, true);
        } else if (GeoTimeSerie.TYPE.LONG == gts1.getType() && GeoTimeSerie.TYPE.DOUBLE == gts2.getType()) {
          //gts2 is double
          result.setType(GeoTimeSerie.TYPE.DOUBLE);
          GTSOpsHelper.applyBinaryOp(result, gts1, gts2, gtsbIsDoubleOp, true);
        } else {
          result.setType(GeoTimeSerie.TYPE.LONG);
          GTSOpsHelper.applyBinaryOp(result, gts1, gts2, longOp, true);
        }
        // If result is empty, set type and sizehint to default.
        if (0 == result.size()) {
          result = result.cloneEmpty();
        }
        stack.push(result);
      } else {
        throw new WarpScriptException(getName() + "can only operate on two GTS with NUMBER or STRING values.");
      }
    } else if (op1 instanceof GeoTimeSerie && GeoTimeSerie.TYPE.UNDEFINED == ((GeoTimeSerie) op1).getType() && (op2 instanceof String || op2 instanceof Number)) {
      // empty gts compared to a string or a number
      stack.push(((GeoTimeSerie) op1).cloneEmpty());
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof String && GeoTimeSerie.TYPE.STRING == ((GeoTimeSerie) op1).getType()) {
      // one string gts compared to a string
      GeoTimeSerie gts = (GeoTimeSerie) op1;
      GeoTimeSerie result = gts.cloneEmpty();
      result.setType(GeoTimeSerie.TYPE.STRING);
      for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
        GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
            operator((GTSHelper.valueAtIndex(gts, i)).toString().compareTo(op2.toString()), 0) ? GTSHelper.valueAtIndex(gts, i) : null, false);
      }
      // If result is empty, set type and sizehint to default.
      if (0 == result.size()) {
        result = result.cloneEmpty();
      }
      stack.push(result);
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof Number && GeoTimeSerie.TYPE.DOUBLE == ((GeoTimeSerie) op1).getType()) {
      // one double gts compared to number
      GeoTimeSerie gts = (GeoTimeSerie) op1;
      GeoTimeSerie result = gts.cloneEmpty();
      result.setType(GeoTimeSerie.TYPE.DOUBLE);
      if (op2 instanceof Double && Double.isNaN((Double) op2)) {
        // op2 is NaN, must test if both are NaN
        for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
          if ((Double.isNaN((Double) GTSHelper.valueAtIndex(gts, i)) && ifTwoNaNOperands) || (!Double.isNaN((Double) GTSHelper.valueAtIndex(gts, i)) && ifOneNaNOperand)) {
            GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
                GTSHelper.valueAtIndex(gts, i), false);
          }
        }
      } else {
        // op2 is not NaN, must only test if gts content for NaN
        for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
          if (Double.isNaN((Double) GTSHelper.valueAtIndex(gts, i))) {
            GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
                ifOneNaNOperand ? GTSHelper.valueAtIndex(gts, i) : null, false);
          } else {
            GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
                operator(EQ.compare((Number) GTSHelper.valueAtIndex(gts, i), (Number) op2), 0) ? GTSHelper.valueAtIndex(gts, i) : null, false);
          }
        }
      }
      // If result is empty, set type and sizehint to default.
      if (0 == result.size()) {
        result = result.cloneEmpty();
      }
      stack.push(result);
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof Number && GeoTimeSerie.TYPE.LONG == ((GeoTimeSerie) op1).getType()) {
      // one long gts compared to number
      GeoTimeSerie gts = (GeoTimeSerie) op1;
      GeoTimeSerie result = gts.cloneEmpty();
      result.setType(GeoTimeSerie.TYPE.LONG);
      if (op2 instanceof Double && Double.isNaN((Double) op2)) {
        // if the comparison with a NaN is always true, returns a clone of the input GTS ( $longGts NaN != )
        stack.push(ifOneNaNOperand ? gts.clone() : result);
      } else {
        for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
          GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
              operator(EQ.compare((Number) GTSHelper.valueAtIndex(gts, i), (Number) op2), 0) ? GTSHelper.valueAtIndex(gts, i) : null, false);
        }
        // If result is empty, set type and sizehint to default.
        if (0 == result.size()) {
          result = result.cloneEmpty();
        }
        stack.push(result);
      }
    } else {
      throw new WarpScriptException(getName() + " can only operate when GTS values and the top stack operand have the same type. Booleans are not supported.");
    }
    
    return stack;
  }
}
