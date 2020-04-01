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

package io.warp10.script.functions;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.geoxp.geo.Coverage;
import com.geoxp.geo.CoverageHelper;
import com.geoxp.geo.HHCodeHelper;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TOGEOJSON extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOGEOJSON(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    boolean allCells = false;

    Object top = stack.pop();

    if (top instanceof Boolean) {
      allCells = (Boolean) top;
      top = stack.pop();
    }

    if (!(top instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " operates on a GEOSHAPE.");
    }

    GeoXPShape shape = (GeoXPShape) top;

    if (allCells) {
      long[] cells = GeoXPLib.getCells(shape);

      StringBuilder sb = new StringBuilder();
      sb.append("{\"type\":\"MultiPolygon\",\"coordinates\":[");
      String prefix = "";

      for (long cell: cells) {
        int cellRes = ((int) (cell >>> 60)) << 1;
        long hh = cell << 4;
        double[] bbox = HHCodeHelper.getHHCodeBBox(hh, cellRes);
        sb.append(prefix);
        prefix = ",";
        // Counterclockwise from sw, lon/lat
        sb.append("[[[").append(bbox[1]).append(",").append(bbox[0]).append("],");// SW
        sb.append("[").append(bbox[3]).append(",").append(bbox[0]).append("],"); // SE
        sb.append("[").append(bbox[3]).append(",").append(bbox[2]).append("],"); // NE
        sb.append("[").append(bbox[1]).append(",").append(bbox[2]).append("],"); // NW
        sb.append("[").append(bbox[1]).append(",").append(bbox[0]).append("]]]"); // SW
      }

      sb.append("]}");

      stack.push(sb.toString());
    } else {
      long[] cells = GeoXPLib.getCells(shape);
      Coverage coverage = new Coverage(cells);
      stack.push(CoverageHelper.toGeoJSON(coverage));
    }

    return stack;
  }
}
