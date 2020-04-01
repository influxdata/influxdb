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
 * Converts the long on top of the stack into a double by considering it a raw bit representation
 */
public class FROMBITS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean asFloat;
  
  public FROMBITS(String name, boolean asFloat) {
    super(name);
    this.asFloat = asFloat;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object op = stack.pop();
    
    if (!(op instanceof Long) && !(op instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " operates on a LONG or a Geo Time Series thereof.");
    }
    
    if ((op instanceof GeoTimeSerie) && TYPE.LONG != ((GeoTimeSerie) op).getType()) {
      throw new WarpScriptException(getName() + " operates on a LONG Geo Time Series.");
    }
    
    if (op instanceof Long) {
      if (this.asFloat) {
        stack.push((double) Float.intBitsToFloat((int) (((long) op) & 0xFFFFFFFFL)));
      } else {
        stack.push(Double.longBitsToDouble((long) op));
      }      
    } else {
      GeoTimeSerie gts = (GeoTimeSerie) op;
      GTSEncoder encoder = new GTSEncoder(0L);
      encoder.setMetadata(new Metadata(gts.getMetadata()));
      int n = gts.size();
      try {
        for (int i = 0; i < n; i++) {
          double value;
          if (this.asFloat) {
            value = (double) Float.intBitsToFloat((int) ((((Number) GTSHelper.valueAtIndex(gts, i)).longValue()) & 0xFFFFFFFFL));
          } else {
            value = (double) Double.longBitsToDouble(((Number) GTSHelper.valueAtIndex(gts, i)).longValue());
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
