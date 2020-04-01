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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSOpsHelper;
import io.warp10.continuum.gts.GTSOpsHelper.GTSBinaryOp;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Adds the two operands on top of the stack
 */
public class ADD extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public ADD(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op2 instanceof Number && op1 instanceof Number) {
      if (op1 instanceof Double || op2 instanceof Double) {
        stack.push(((Number) op1).doubleValue() + ((Number) op2).doubleValue());        
      } else {
        stack.push(((Number) op1).longValue() + ((Number) op2).longValue());
      }
    } else if (op2 instanceof String && op1 instanceof String) {
      stack.push(op1.toString() + op2.toString());
    } else if (op1 instanceof List) {
      List<Object> l = new ArrayList<Object>();
      l.addAll((List) op1);
      l.add(op2);
      stack.push(l);
    } else if (op1 instanceof Set) {
      Set<Object> s = new HashSet<Object>();
      s.addAll((Set) op1);
      s.add(op2);
      stack.push(s);
    } else if (op1 instanceof Macro && op2 instanceof Macro) {
      Macro macro = new Macro();
      macro.addAll((Macro) op1);
      macro.addAll((Macro) op2);
      stack.push(macro);
    } else if (op1 instanceof RealMatrix && op2 instanceof RealMatrix) {
      stack.push(((RealMatrix) op1).add((RealMatrix) op2));
    } else if (op1 instanceof RealMatrix && op2 instanceof Number) {
      stack.push(((RealMatrix) op1).scalarAdd(((Number) op2).doubleValue()));
    } else if (op2 instanceof RealMatrix && op1 instanceof Number) {
      stack.push(((RealMatrix) op2).scalarAdd(((Number) op1).doubleValue()));
    } else if (op1 instanceof RealVector && op2 instanceof RealVector) {
      stack.push(((RealVector) op1).add((RealVector) op2));
    } else if (op1 instanceof RealVector && op2 instanceof Number) {
      stack.push(((RealVector) op1).mapAdd(((Number) op2).doubleValue()));
    } else if (op2 instanceof RealVector && op1 instanceof Number) {
      stack.push(((RealVector) op2).mapAdd(((Number) op1).doubleValue()));
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;

      //
      // Determine the type of the result GTS
      //
      
      TYPE type = TYPE.UNDEFINED;
      
      if (TYPE.BOOLEAN == gts1.getType() || TYPE.BOOLEAN == gts2.getType()) {
        throw new WarpScriptException(getName() + " cannot operate on BOOLEAN Geo Time Series™.");
      } else if (TYPE.STRING == gts1.getType() || TYPE.STRING == gts2.getType()) {
        type = TYPE.STRING;
      } else if (TYPE.DOUBLE == gts1.getType() || TYPE.DOUBLE == gts2.getType()) {
        type = TYPE.DOUBLE;
      } else if (TYPE.LONG == gts1.getType() || TYPE.LONG == gts2.getType()) {
        type = TYPE.LONG;
      }
      
      GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
      
      result.setType(type);

      GTSBinaryOp op = null;
      
      switch (type) {
        case STRING:
          op = new GTSBinaryOp() {
            @Override
            public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
              return GTSHelper.valueAtIndex(gtsa, idxa).toString() + GTSHelper.valueAtIndex(gtsb, idxb).toString();
            }
          };
          break;
        case LONG:
          op = new GTSBinaryOp() {
            @Override
            public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
              return ((Number) GTSHelper.valueAtIndex(gtsa, idxa)).longValue() + ((Number) GTSHelper.valueAtIndex(gtsb, idxb)).longValue();
            }
          };
          break;
        case DOUBLE:
          op = new GTSBinaryOp() {
            @Override
            public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
              return ((Number) GTSHelper.valueAtIndex(gtsa, idxa)).doubleValue() + ((Number) GTSHelper.valueAtIndex(gtsb, idxb)).doubleValue();
            }
          };
          break;
        default:
          // Leave op to null.
          // Both GTSs are empty, thus applyBinaryOp will only apply its bucketization logic to the result.
      }

      GTSOpsHelper.applyBinaryOp(result, gts1, gts2, op);
      
      // If result is empty, set type and sizehint to default.
      if (0 == result.size()) {
        result = result.cloneEmpty();
      }
      stack.push(result);
    } else if (op1 instanceof GeoTimeSerie || op2 instanceof GeoTimeSerie) {
      TYPE type = TYPE.UNDEFINED;
      
      boolean op1gts = op1 instanceof GeoTimeSerie;
      
      int n = op1gts ? GTSHelper.nvalues((GeoTimeSerie) op1) : GTSHelper.nvalues((GeoTimeSerie) op2);
      
      GeoTimeSerie result = op1gts ? ((GeoTimeSerie) op1).cloneEmpty(n) : ((GeoTimeSerie) op2).cloneEmpty(n);
      GeoTimeSerie gts = op1gts ? (GeoTimeSerie) op1 : (GeoTimeSerie) op2;
      
      // Determine type of result
      
      Object op = op1gts ? op2 : op1;
      
      if (op instanceof String) {
        type = TYPE.STRING;
      } else if (op instanceof Double) {
        if (TYPE.DOUBLE == gts.getType() || TYPE.LONG == gts.getType()) {
          type = TYPE.DOUBLE;
        } else if (TYPE.BOOLEAN == gts.getType()) {
          throw new WarpScriptException(getName() + " cannot operate on BOOLEAN Geo Time Series™.");
        } else {
          type = TYPE.STRING;
        }
      } else if (op instanceof Long) {
        if (TYPE.DOUBLE == gts.getType()) {
          type = TYPE.DOUBLE;
        } else if (TYPE.LONG == gts.getType()) {
          type = TYPE.LONG;
        } else if (TYPE.BOOLEAN == gts.getType()) {
          throw new WarpScriptException(getName() + " cannot operate on BOOLEAN Geo Time Series™.");
        } else {
          type = TYPE.STRING;
        }
      }
      
      for (int i = 0; i < n; i++) {
        Object value;
        if (op1gts) {
          switch (type) {
            case STRING:
              value = GTSHelper.valueAtIndex(gts, i).toString() + op.toString();
              break;
            case DOUBLE:
              value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() + ((Number) op).doubleValue();
              break;
            case LONG:
              value = ((Number) GTSHelper.valueAtIndex(gts, i)).longValue() + ((Number) op).longValue();
              break;
            default:
              throw new WarpScriptException(getName() + " Invalid Geo Time Series™ type.");
          }          
        } else {
          switch (type) {
            case STRING:
              value = op.toString() + GTSHelper.valueAtIndex(gts, i).toString();
              break;
            case DOUBLE:
              value = ((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue() + ((Number) op).doubleValue();
              break;
            case LONG:
              value = ((Number) GTSHelper.valueAtIndex(gts, i)).longValue() + ((Number) op).longValue();
              break;
            default:
              throw new WarpScriptException(getName() + " Invalid Geo Time Series™ type.");
          }                    
        }
        GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value, false);
      }      

      stack.push(result);          
    } else if (op1 instanceof byte[] && op2 instanceof byte[]) {
      stack.push(ArrayUtils.addAll((byte[])op1,(byte[])op2));
    } else {
      throw new WarpScriptException(getName() + " can only operate on numeric, string, lists, matrices, vectors, Geo Time Series, byte array and macro values.");
    }
    
    return stack;
  }
}
