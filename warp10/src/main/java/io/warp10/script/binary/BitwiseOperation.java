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
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public abstract class BitwiseOperation extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final GTSOpsHelper.GTSBinaryOp op = new GTSOpsHelper.GTSBinaryOp() {
    @Override
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) {
      return operator(((Number) GTSHelper.valueAtIndex(gtsa, idxa)).longValue(), ((Number) GTSHelper.valueAtIndex(gtsb, idxb)).longValue());
    }
  };
  
  public abstract long operator(long op1, long op2);
  
  public BitwiseOperation(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    String exceptionMessage = getName() + " can only operate on two long, or two long GTS, or one long GTS and a long.";
    
    Object op2 = stack.pop();
    Object op1 = stack.pop();
    
    if (op2 instanceof Long && op1 instanceof Long) {
      stack.push(operator(((Long) op1).longValue(), ((Long) op2).longValue()));
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof GeoTimeSerie) {
      GeoTimeSerie gts1 = (GeoTimeSerie) op1;
      GeoTimeSerie gts2 = (GeoTimeSerie) op2;
      if (GeoTimeSerie.TYPE.LONG == gts1.getType() && GeoTimeSerie.TYPE.LONG == gts2.getType()) {
        GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
        result.setType(GeoTimeSerie.TYPE.LONG);
        GTSOpsHelper.applyBinaryOp(result, gts1, gts2, op);
        stack.push(result);
      } else if ((GeoTimeSerie.TYPE.UNDEFINED == gts1.getType() && GeoTimeSerie.TYPE.LONG == gts2.getType())
          || (GeoTimeSerie.TYPE.UNDEFINED == gts2.getType() && GeoTimeSerie.TYPE.LONG == gts1.getType())) {
        // gts1 or gts2 empty, return an empty gts
        stack.push(new GeoTimeSerie());
      } else {
        throw new WarpScriptException(exceptionMessage);
      }
    } else if (op1 instanceof GeoTimeSerie && op2 instanceof Long) {
      GeoTimeSerie gts = (GeoTimeSerie) op1;
      long mask = ((Number) op2).longValue();
      if (GeoTimeSerie.TYPE.LONG == gts.getType()) {
        GeoTimeSerie result = gts.cloneEmpty();
        result.setType(GeoTimeSerie.TYPE.LONG);
        for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
          GTSHelper.setValue(result, GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i),
              operator(((Number) GTSHelper.valueAtIndex(gts, i)).longValue(), mask), false);
        }
        stack.push(result);
      } else if (GeoTimeSerie.TYPE.UNDEFINED == gts.getType()) {
        // gts is empty return gts
        stack.push(gts.cloneEmpty());
      } else {
        throw new WarpScriptException(exceptionMessage);
      }
    } else {
      throw new WarpScriptException(exceptionMessage);
    }
    
    return stack;
  }
}
