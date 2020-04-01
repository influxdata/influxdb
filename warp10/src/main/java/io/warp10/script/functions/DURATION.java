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

import io.warp10.continuum.gts.UnsafeString;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.Locale;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutablePeriod;
import org.joda.time.Period;
import org.joda.time.ReadWritablePeriod;
import org.joda.time.format.ISOPeriodFormat;

/**
 * Pushes onto the stack the number of time units described by the ISO8601 duration
 * on top of the stack (String).
 *
 * @see http://en.wikipedia.org/wiki/ISO_8601#Durations
 */
public class DURATION extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  final private static Double STU = new Double(Constants.TIME_UNITS_PER_S);
  
  public DURATION(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects an ISO8601 duration (a string) on top of the stack. See http://en.wikipedia.org/wiki/ISO_8601#Durations");
    }

    // Separate seconds from digits below second precision
    String duration_string = o.toString();
    String[] tokens = UnsafeString.split(duration_string, '.');

    long offset = 0;
    if (tokens.length > 2) {
      throw new WarpScriptException(getName() + "received an invalid ISO8601 duration.");
    }

    if (2 == tokens.length) {
      duration_string = tokens[0].concat("S");
      String tmp = tokens[1].substring(0, tokens[1].length() - 1);
      Double d_offset = Double.valueOf("0." + tmp) * STU;
      offset = d_offset.longValue();
    }
    
    ReadWritablePeriod period = new MutablePeriod();
    
    ISOPeriodFormat.standard().getParser().parseInto(period, duration_string, 0, Locale.US);

    Period p = period.toPeriod();
    
    if (p.getMonths() != 0 || p.getYears() != 0) {
      throw new WarpScriptException(getName() + " doesn't support ambiguous durations containing years or months, please convert those to days.");
    }

    Duration duration = p.toDurationFrom(new Instant());

    // check if offset should be positive of negative
    if (p.getSeconds() < 0) {
      offset = -offset;
    }

    stack.push(duration.getMillis() * Constants.TIME_UNITS_PER_MS + offset);

    return stack;
  }
}
