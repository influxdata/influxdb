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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;

/**
 * Compute Kurtosis of a numerical GTS
 */
public class KURTOSIS extends GTSStackFunction {

  private static final String APPLYBESSEL = "applyBessel";

  public KURTOSIS(String name) {
    super(name);
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    int n = GTSHelper.nvalues(gts);

    if (0 == n) {
      throw new WarpScriptException(getName() + " can only compute kurtosis for non empty series.");
    }

    if (TYPE.DOUBLE != gts.getType() && TYPE.LONG != gts.getType()) {
      throw new WarpScriptException(getName() + " can only compute kurtosis for numerical series.");
    }

    return GTSHelper.kurtosis(gts, (Boolean) params.get(APPLYBESSEL));
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Map<String, Object> params = new HashMap<String, Object>();

    Object o = stack.pop();

    if (!(o instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects a boolean on top of the stack to determine if Bessel's correction should be applied or not.");
    }

    boolean applyBessel = Boolean.TRUE.equals(o);

    params.put(APPLYBESSEL, applyBessel);

    return params;
  }
}
