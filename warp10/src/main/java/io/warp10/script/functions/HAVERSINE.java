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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Compute distance between two lat/lon using the Haversine formula
 */
public class HAVERSINE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  // Mean radius in meters see https://en.wikipedia.org/wiki/Earth_radius#Global_average_radii
  private static final double EARTH_RADIUS = 6371000.0D;

  public HAVERSINE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object lon2 = stack.pop();
    Object lat2 = stack.pop();
    Object lon1 = stack.pop();
    Object lat1 = stack.pop();

    if (!(lat1 instanceof Number) || !(lat2 instanceof Number) || !(lon1 instanceof Number) || !(lon2 instanceof Number)) {
      throw new WarpScriptException(getName() + " expects two sets of lat/lon coordinates.");
    }

    double rlat1 = Math.toRadians(((Number) lat1).doubleValue());
    double rlon1 = Math.toRadians(((Number) lon1).doubleValue());
    double rlat2 = Math.toRadians(((Number) lat2).doubleValue());
    double rlon2 = Math.toRadians(((Number) lon2).doubleValue());

    double sinphi2 = Math.sin((rlat2 - rlat1) / 2.0D);
    sinphi2 *= sinphi2;

    double sinlambda2 = Math.sin((rlon2 - rlon1) / 2.0D);
    sinlambda2 *= sinlambda2;

    double d = 2.0D * EARTH_RADIUS * Math.asin(Math.sqrt(sinphi2 + Math.cos(rlat1) * Math.cos(rlat2) * sinlambda2));

    stack.push(d);

    return stack;
  }
}
