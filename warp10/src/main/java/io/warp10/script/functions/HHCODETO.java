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

import com.geoxp.GeoXPLib;
import com.google.common.primitives.Longs;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Convert a GeoHash to lat/lon
 */
public class HHCODETO extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final boolean useGtsConvention;

  public HHCODETO(String name) {
    this(name, false);
  }

  public HHCODETO(String name, boolean useGtsConvention) {
    super(name);
    this.useGtsConvention = useGtsConvention;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object hhcode = stack.pop();

    long hh;

    if (hhcode instanceof Long) {
      hh = (long) hhcode;
    } else if (hhcode instanceof String) {
      String hhstr = hhcode.toString();
      if (hhstr.length() > 16) {
        throw new WarpScriptException(getName() + " expects an hexadecimal HHCode string of length <= 16");
      } else if (hhstr.length() < 16) {
        hhcode = new StringBuilder(hhstr).append("0000000000000000");
        ((StringBuilder) hhcode).setLength(16);
      }
      hh = Long.parseUnsignedLong(hhcode.toString(), 16);
    } else if (hhcode instanceof byte[]) {
      hh = Longs.fromByteArray((byte[]) hhcode);
    } else {
      throw new WarpScriptException(getName() + " expects a long, a string or a byte array.");
    }

    if (useGtsConvention && GeoTimeSerie.NO_LOCATION == hh) {
      stack.push(Double.NaN);
      stack.push(Double.NaN);
    } else {
      double[] latlon = GeoXPLib.fromGeoXPPoint(hh);
      stack.push(latlon[0]);
      stack.push(latlon[1]);
    }

    return stack;
  }
}
