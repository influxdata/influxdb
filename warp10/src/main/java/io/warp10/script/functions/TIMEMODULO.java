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
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Split a GTS into N GTS whose timestamps are the original timestamps MODULO XXX
 * The output GTS have a 'quotient' label which holds the original timestamp DIV MODULO
 * 
 * 3: GTS
 * 2: MODULO
 * 1: QUOTIENT_LABEL_NAME
 */
public class TIMEMODULO extends GTSStackFunction {

  private static String MODULO = "modulo";
  private static String LABEL = "label";

  public TIMEMODULO(String name) {
    super(name);
  }

  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    Object quotientLabel = stack.pop();

    if (!(quotientLabel instanceof String)) {
      throw new WarpScriptException(getName() + " expects a quotient label name on top of the stack.");
    }

    Map<String,Object> params = new HashMap<String, Object>();

    params.put(LABEL, (String) quotientLabel);

    Object modulo = stack.pop();

    if (!(modulo instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a LONG modulo under the quotient label name.");
    }

    params.put(MODULO, (long) modulo);

    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    long modulo = (long) params.get(MODULO);
    String label = (String) params.get(LABEL);

    Map<Long,GeoTimeSerie> output = new TreeMap<Long,GeoTimeSerie>();

    int n = GTSHelper.nvalues(gts);

    for (int i = 0; i < n; i++) {
      long tick = GTSHelper.tickAtIndex(gts, i);
      long quotient = tick / modulo;
      long remainder = tick % modulo;

      GeoTimeSerie qgts = output.get(quotient);

      if (null == qgts) {
        qgts = new GeoTimeSerie();
        qgts.setName(gts.getName());
        qgts.setMetadata(gts.getMetadata());
        qgts.getMetadata().putToLabels(label, Long.toString(quotient));
        output.put(quotient, qgts);
      }

      GTSHelper.setValue(qgts, remainder, GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), GTSHelper.valueAtIndex(gts, i), false);
    }

    List<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();

    result.addAll(output.values());

    return result;
  }


}
