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

import io.warp10.continuum.gts.UnsafeString;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.MutablePeriod;
import org.joda.time.ReadWritablePeriod;
import org.joda.time.format.ISOPeriodFormat;

import java.util.List;
import java.util.Locale;

public class ADDDURATION extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  final private static WarpScriptStackFunction TSELEMENTS = new TSELEMENTS(WarpScriptLib.TSELEMENTS);
  final private static WarpScriptStackFunction FROMTSELEMENTS = new FROMTSELEMENTS(WarpScriptLib.TSELEMENTSTO);

  public ADDDURATION(String name) {
    super(name);
  }

  @Override
  public WarpScriptStack apply(WarpScriptStack stack) throws WarpScriptException {

    //
    // Retrieve arguments
    //

    Object top = stack.pop();

    if (!(top instanceof String || top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects an ISO8601 duration (a string) on top of the stack (see http://en.wikipedia.org/wiki/ISO_8601#Durations), or a number of durations (a Long).");
    }

    String duration;
    long N = 1;
    if (top instanceof String) {
      duration = top.toString();
    } else {
      N = (Long) top;
      top = stack.pop();
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects an ISO8601 duration (a string) in the second level of the stack (see http://en.wikipedia.org/wiki/ISO_8601#Durations).");
      }
      duration = top.toString();
    }

    String tz = null;
    if (stack.peek() instanceof String) {
      tz = stack.pop().toString();
      if (!(stack.peek() instanceof Long)) {
        throw new WarpScriptException(getName() + " operates on a tselements list, timestamp, or timestamp and timezone.");
      }
    } else if (!(stack.peek() instanceof List || stack.peek() instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a tselements list, timestamp, or timestamp and timezone.");
    }

    //
    // Handle time zone
    //

    DateTimeZone dtz = DateTimeZone.UTC;
    if (null != tz) {
      dtz = DateTimeZone.forID(tz);
    }

    //
    // Handle duration
    //

    ReadWritablePeriodWithSubSecondOffset period;
    try {
      period = durationToPeriod(duration);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " encountered an exception.", wse);
    }

    //
    // Do the computation
    //

    boolean tselements = false;
    if (stack.peek() instanceof List) {
      FROMTSELEMENTS.apply(stack);
      tselements = true;
    }

    long instant = ((Number) stack.pop()).longValue();
    stack.push(addPeriod(instant, period, dtz, N));

    if (tselements) {
      TSELEMENTS.apply(stack);
    }

    return stack;
  }

  /**
   * A joda time period with sub second precision (the long offset).
   */
  public static class ReadWritablePeriodWithSubSecondOffset {
    private final ReadWritablePeriod period;
    private final long offset;

    public ReadWritablePeriodWithSubSecondOffset(ReadWritablePeriod period, long offset) {
      this.period = period;
      this.offset = offset;
    }

    public ReadWritablePeriod getPeriod() {
      return period;
    }

    public long getOffset() {
      return offset;
    }
  }

  /**
   * Convert an ISO8601 duration to a Period.
   * @param duration
   * @return
   * @throws WarpScriptException
   */
  public static ReadWritablePeriodWithSubSecondOffset durationToPeriod(String duration) throws WarpScriptException {
    // Separate seconds from  digits below second precision
    String[] tokens = UnsafeString.split(duration, '.');

    long offset = 0;
    if (tokens.length > 2) {
      throw new WarpScriptException("Invalid ISO8601 duration");
    }

    if (2 == tokens.length) {
      duration = tokens[0].concat("S");
      String tmp = tokens[1].substring(0, tokens[1].length() - 1);

      try {
        offset = ((Double) (Double.parseDouble("0." + tmp) * Constants.TIME_UNITS_PER_S)).longValue();
      } catch (NumberFormatException e) {
        throw new WarpScriptException("Parsing of sub second precision part of duration has failed. tried to parse: " + tmp);
      }
    }

    ReadWritablePeriod period = new MutablePeriod();
    if (ISOPeriodFormat.standard().getParser().parseInto(period, duration, 0, Locale.US) < 0) {
      throw new WarpScriptException("Parsing of duration without sub second precision has failed. Tried to parse: " + duration);
    }

    return new ReadWritablePeriodWithSubSecondOffset(period, offset);
  }

  public static long addPeriod(long instant, ReadWritablePeriod period, DateTimeZone dtz) {
    return addPeriod(instant, period, dtz, 1);
  }

  public static long addPeriod(long instant, ReadWritablePeriod period, DateTimeZone dtz, long N) {
    return addPeriod(instant, new ReadWritablePeriodWithSubSecondOffset(period, 0), dtz, N);
  }

  public static long addPeriod(long instant, ReadWritablePeriodWithSubSecondOffset periodAndOffset, DateTimeZone dtz) {
    return addPeriod(instant, periodAndOffset, dtz, 1);
  }

  /**
   * Add a duration in ISO8601 duration format to a timestamp
   * @param instant a timestamp since Unix Epoch
   * @param periodAndOffset a period (with subsecond precision) to add
   * @param dtz timezone
   * @param N number of times the period is added
   * @return resulting timestamp
   */
  public static long addPeriod(long instant, ReadWritablePeriodWithSubSecondOffset periodAndOffset, DateTimeZone dtz, long N) {

    ReadWritablePeriod period = periodAndOffset.getPeriod();
    long offset = periodAndOffset.getOffset();

    //
    // Do the computation
    //

    DateTime dt = new DateTime(instant / Constants.TIME_UNITS_PER_MS, dtz);

    //
    // Add the duration
    // Note that for performance reasons we add N times ISO8601 duration, then N times the sub seconds offset.
    // This calculation is not exact in some rare edge cases  e.g. in the last second of the 28th february on a year before a leap year if we add 'P1YT0.999999S'.
    //

    long M = N;
    while (M > Integer.MAX_VALUE) {
      dt = dt.withPeriodAdded(period, Integer.MAX_VALUE);
      M = M - Integer.MAX_VALUE;
    }
    while (M < Integer.MIN_VALUE) {
      dt = dt.withPeriodAdded(period, Integer.MIN_VALUE);
      M = M - Integer.MIN_VALUE;
    }
    dt = dt.withPeriodAdded(period, Math.toIntExact(M));

    // check if offset should be positive of negative
    if (period.toPeriod().getSeconds() < 0) {
      offset = -offset;
    }

    long ts = dt.getMillis() * Constants.TIME_UNITS_PER_MS;
    ts += instant % Constants.TIME_UNITS_PER_MS;
    ts += offset * N;

    return ts;
  }
}
