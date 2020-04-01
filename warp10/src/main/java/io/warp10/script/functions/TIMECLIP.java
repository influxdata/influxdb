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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Only retain ticks within the given timerange
 */
public class TIMECLIP extends ElementOrListStackFunction {

  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  public TIMECLIP(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    long start;

    boolean iso8601 = false;

    if (top instanceof String) {
      iso8601 = true;
      start = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(top.toString());
    } else if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects either an ISO8601 timestamp as the origin timestamp or a duration.");
    } else {
      start = (long) top;
    }

    final long end;

    top = stack.pop();

    if (top instanceof String) {
      end = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(top.toString());
    } else if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects either an ISO8601 timestamp or a delta since Unix Epoch as 'now' parameter.");
    } else {
      end = (long) top;
    }

    if (!iso8601) {
      start = end - start + 1;
    }

    final long finalStart = start;

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof GeoTimeSerie) {
          return GTSHelper.timeclip((GeoTimeSerie) element, finalStart, end);
        } else if (element instanceof GTSEncoder) {
          return GTSHelper.timeclip((GTSEncoder) element, finalStart, end);
        } else {
          throw new WarpScriptException(getName()+" expects a Geo Time Series, a GTSEncoder or a list thereof under the timeframe definition.");
        }
      }
    };
  }

}
