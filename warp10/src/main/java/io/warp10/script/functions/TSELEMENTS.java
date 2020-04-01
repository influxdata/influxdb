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

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Pushes on the stack the various elements of a timestamp for a given timezone
 */
public class TSELEMENTS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final int MAX_TZ_CACHE_ENTRIES = 64;

  private final Map<String, DateTimeZone> tzones = new LinkedHashMap<String, DateTimeZone>() {
    protected boolean removeEldestEntry(java.util.Map.Entry<String, DateTimeZone> eldest) {
      return size() > MAX_TZ_CACHE_ENTRIES;
    }
  };

  public TSELEMENTS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object obj = stack.peek();

    String tz = null;

    if (obj instanceof String) {
      tz = (String) obj;
      stack.pop();
    } else if (!(obj instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a timestamp or a timestamp + timezone.");
    }

    DateTimeZone dtz = this.tzones.get(tz);

    if (null == dtz) {
      dtz = DateTimeZone.forID(null == tz ? "UTC" : tz);
      this.tzones.put(tz, dtz);
    }

    obj = stack.pop();

    if (!(obj instanceof Long)) {
      throw new WarpScriptException(getName() + " operates on a timestamp or a timestamp + timezone.");
    }

    long ts = (long) obj;

    // Convert ts to milliseconds

    long tsms = ts / Constants.TIME_UNITS_PER_MS;

    // We want the floor, not truncate: update millis if needed.
    if (0 > ts && 0 != ts % Constants.TIME_UNITS_PER_MS) {
      tsms--;
    }

    DateTime dt = new DateTime(tsms, dtz);

    // Extract components into an array

    List<Long> elements = new ArrayList<Long>();

    elements.add((long) dt.getYear());
    elements.add((long) dt.getMonthOfYear());
    elements.add((long) dt.getDayOfMonth());
    elements.add((long) dt.getHourOfDay());
    elements.add((long) dt.getMinuteOfHour());
    elements.add((long) dt.getSecondOfMinute());
    elements.add((long) dt.getMillisOfSecond()* Constants.TIME_UNITS_PER_MS + Math.abs(ts - tsms * Constants.TIME_UNITS_PER_MS));
    elements.add((long) dt.getDayOfYear());
    elements.add((long) dt.getDayOfWeek());
    elements.add((long) dt.getWeekOfWeekyear());

    stack.push(elements);

    return stack;
  }
}
