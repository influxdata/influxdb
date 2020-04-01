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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import com.geoxp.GeoXPLib;
import com.geoxp.oss.jarjar.org.apache.commons.codec.binary.Hex;
import com.google.common.primitives.Longs;

/**
 * Convert a lat/lon pair to an HHCode
 */
public class TOHHCODE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean tostring;
  private final boolean useGtsConvention;

  public TOHHCODE(String name, boolean tostring) {
    this(name, tostring, false);
  }

  public TOHHCODE(String name, boolean tostring, boolean useGtsConvention) {
    super(name);
    this.tostring = tostring;
    this.useGtsConvention = useGtsConvention;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object lon = stack.pop();
    Object lat = stack.pop();
    
    if (!(lon instanceof Number) || !(lat instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a latitude and a longitude on the stack.");
    }

    double latDouble = ((Number) lat).doubleValue();
    double lonDouble = ((Number) lon).doubleValue();

    long geoxppoint;

    if (useGtsConvention) {
      if (Double.isNaN(latDouble) != Double.isNaN(lonDouble)) {
        throw new WarpScriptException(getName() + " expects latitude and longitude to both be NaN or both be not NaN");
      } else if (!Double.isNaN(latDouble)) { // also !Double.isNaN(lonDouble)
        geoxppoint = GeoXPLib.toGeoXPPoint(latDouble, lonDouble);
      } else {
        geoxppoint = GeoTimeSerie.NO_LOCATION;
      }
    } else {
      geoxppoint = GeoXPLib.toGeoXPPoint(latDouble, lonDouble);
    }

    if (this.tostring) {
      String hhcode = Hex.encodeHexString(Longs.toByteArray(geoxppoint));
    
      stack.push(hhcode);
    } else {
      stack.push(geoxppoint);
    }

    return stack;
  }
}
