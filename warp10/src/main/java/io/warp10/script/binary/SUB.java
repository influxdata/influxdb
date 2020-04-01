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

import io.warp10.continuum.gts.GTSOpsHelper;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Subtracts the two operands on top of the stack
 */
public class SUB extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final String typeCheckErrorMsg;

  public SUB(String name) {
    super(name);
    typeCheckErrorMsg = getName() + " can only operate on numeric values, vectors, matrices and numeric Geo Time Series.";
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op2 instanceof Number && op1 instanceof Number) {
      if (op1 instanceof Double || op2 instanceof Double) {
        stack.push(((Number) op1).doubleValue() - ((Number) op2).doubleValue());
      } else {
        stack.push(((Number) op1).longValue() - ((Number) op2).longValue());        
      }
    } else if (op1 instanceof RealMatrix && op2 instanceof RealMatrix) {
      stack.push(((RealMatrix) op1).subtract((RealMatrix) op2));
    } else if (op1 instanceof RealVector && op2 instanceof RealVector) {
      stack.push(((RealVector) op1).subtract((RealVector) op2));
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;

      // Returns immediately a new gts if both inputs are empty
      if (0 == GTSHelper.nvalues(gts1) || 0 == GTSHelper.nvalues(gts2)) {
        GeoTimeSerie result = new GeoTimeSerie();
        // Make sure the bucketization logic is still applied to the result, even if empty.
        GTSOpsHelper.handleBucketization(result, gts1, gts2);
        stack.push(result);
        return stack;
      }

      if (!(gts1.getType() == TYPE.DOUBLE || gts1.getType() == TYPE.LONG) || !(gts2.getType() == TYPE.DOUBLE || gts2.getType() == TYPE.LONG)) {
        throw new WarpScriptException(typeCheckErrorMsg);
      }

      // The result type is LONG if both inputs are LONG.
      GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
      result.setType((gts1.getType() == TYPE.LONG && gts2.getType() == TYPE.LONG) ? TYPE.LONG : TYPE.DOUBLE);

      // Determine if result should be bucketized or not
      GTSOpsHelper.handleBucketization(result, gts1, gts2);
      
      // Sort GTS
      GTSHelper.sort(gts1);
      GTSHelper.sort(gts2);
      
      // Sweeping line over the timestamps
      int idxa = 0;
      int idxb = 0;
               
      int na = GTSHelper.nvalues(gts1);
      int nb = GTSHelper.nvalues(gts2);
      
      Long tsa = null;
      Long tsb = null;

      if (idxa < na) {
        tsa = GTSHelper.tickAtIndex(gts1, idxa);
      }
      if (idxb < na) {
        tsb = GTSHelper.tickAtIndex(gts2, idxb);
      }

      while(idxa < na || idxb < nb) {
        if (idxa >= na) {
          tsa = null;
        }
        if (idxb >= nb) {
          tsb = null;
        }
        if (null != tsa && null != tsb) {
          // We have values at the current index for both GTS
          if (0 == tsa.compareTo(tsb)) {
            // Both indices indicate the same timestamp
            GTSHelper.setValue(result, tsa, ((Number) GTSHelper.valueAtIndex(gts1, idxa)).doubleValue() - ((Number) GTSHelper.valueAtIndex(gts2, idxb)).doubleValue());
            // Advance both indices
            idxa++;
            idxb++;
          } else if (tsa < tsb) {
            // Timestamp at index A is lower than timestamp at index B
            //GTSHelper.setValue(result, tsa, ((Number) GTSHelper.valueAtIndex(gts1, idxa)).doubleValue());
            // Advance index for GTS A
            idxa++;
          } else {
            // Timestamp at index B is >= timestamp at index B
            //GTSHelper.setValue(result, tsb, ((Number) GTSHelper.valueAtIndex(gts2, idxb)).doubleValue());
            // Advance index for GTS B
            idxb++;
          }
        } else if (null == tsa && null != tsb) {
          // Index A has reached the end of GTS A, GTS B still has values to scan
          //GTSHelper.setValue(result, tsb, ((Number) GTSHelper.valueAtIndex(gts2, idxb)).doubleValue());          
          idxb++;
        } else if (null == tsb && null != tsa) {
          // Index B has reached the end of GTS B, GTS A still has values to scan
          //GTSHelper.setValue(result, tsa, ((Number) GTSHelper.valueAtIndex(gts1, idxa)).doubleValue());          
          idxa++;
        }
        if (idxa < na) {
          tsa = GTSHelper.tickAtIndex(gts1, idxa);
        }
        if (idxb < nb) {
          tsb = GTSHelper.tickAtIndex(gts2, idxb);
        }
      }
      // Returns an non typed GTS if the result is empty
      if (0 == result.size()) {
        result = new GeoTimeSerie();
      }
      stack.push(result);
    } else if ((op1 instanceof GeoTimeSerie && op2 instanceof Number) || (op1 instanceof Number && op2 instanceof GeoTimeSerie)) {
      boolean op1gts = op1 instanceof GeoTimeSerie;
      
      int n = op1gts ? GTSHelper.nvalues((GeoTimeSerie) op1) : GTSHelper.nvalues((GeoTimeSerie) op2);
      
      GeoTimeSerie result = op1gts ? ((GeoTimeSerie) op1).cloneEmpty(n) : ((GeoTimeSerie) op2).cloneEmpty();
      GeoTimeSerie gts = op1gts ? (GeoTimeSerie) op1 : (GeoTimeSerie) op2;

      // Returns immediately a new clone if gts is empty.
      if (0 == n) {
        stack.push(result);
        return stack;
      }

      if (!(gts.getType() == TYPE.LONG || gts.getType() == TYPE.DOUBLE)) {
        throw new WarpScriptException(typeCheckErrorMsg);
      }


      Number op = op1gts ? (Number) op2 : (Number) op1;

      if (op instanceof Double || gts.getType() == TYPE.DOUBLE) {
        double opDouble = op.doubleValue();
        for (int i = 0; i < n; i++) {
          double value;
          if (op1gts) {
            value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() - opDouble;
          } else {
            value = opDouble - ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue();
          }
          GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
        }
      } else {
        long opLong = op.longValue();
        for (int i = 0; i < n; i++) {
          long value;
          if (op1gts) {
            value = ((Number) GTSHelper.valueAtIndex(gts, i)).longValue() - opLong;
          } else {
            value = opLong - ((Number) GTSHelper.valueAtIndex(gts, i)).longValue();
          }
          GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
        }
      }
      stack.push(result);      
    } else {
      throw new WarpScriptException(typeCheckErrorMsg);
    }
    
    return stack;
  }
}
