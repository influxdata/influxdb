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

package io.warp10.script.unary;

import java.io.IOException;

import com.sun.org.apache.bcel.internal.generic.Type;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Converts the double on top of the stack into its bit representation
 */
public class TOBITS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean asFloat;
  
  public TOBITS(String name, boolean asFloat) {
    super(name);
    this.asFloat = asFloat;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op = stack.pop();
    
    if (!(op instanceof Number) && !(op instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " operates on a DOUBLE, LONG or Geo Time Series thereof.");
    }
    
    if (op instanceof Number) {
      if (this.asFloat) {
        int bits = Float.floatToRawIntBits(((Number) op).floatValue());
        stack.push(((long) bits) & 0xFFFFFFFFL);
      } else {
        stack.push(Double.doubleToRawLongBits(((Number) op).doubleValue()));      
      }      
    } else {
      GeoTimeSerie gts = (GeoTimeSerie) op;
      if (TYPE.LONG != gts.getType() && TYPE.DOUBLE != gts.getType()) {
        throw new WarpScriptException(getName() + " operates on a DOUBLE or LONG Geo Time Series.");
      }
      GTSEncoder encoder = new GTSEncoder(0L);
      encoder.setMetadata(new Metadata(gts.getMetadata()));
      int n = gts.size();
      try {
        for (int i = 0; i < n; i++) {
          Object value;
          if (this.asFloat) {
            int bits = Float.floatToRawIntBits(((Number) GTSHelper.valueAtIndex(gts, i)).floatValue());
            value = ((long) bits) & 0xFFFFFFFFL;          
          } else {
            value = Double.doubleToRawLongBits(((Number) GTSHelper.valueAtIndex(gts, i)).doubleValue());
          }
          encoder.addValue(GTSHelper.tickAtIndex(gts, i), GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), value);
        }        
      } catch (IOException ioe) {
        throw new WarpScriptException(ioe);
      }
      
      stack.push(encoder.getDecoder(true).decode());
    }
        
    return stack;
  }
}
