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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptBucketizerFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Bucketizes some GTS instances using a bucketduration rather than a bucketspan.
 * The tick of the resulting buckets will be their index (number of bucketduration since Unix Epoch).
 * The goal is to make possible to apply functions for bucketized series on a series with bucket of irregular calendar bucket span.
 * To restore timestamp for ticks, there is UNBUCKETIZE.CALENDAR.
 */
public class BUCKETIZECALENDAR extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final String DURATION_ATTRIBUTE_KEY = ".bucketduration";
  public static final String OFFSET_ATTRIBUTE_KEY = ".bucketoffset";
  public static final String TIMEZONE_ATTRIBUTE_KEY = ".buckettimezone";
  public static final Instant EPOCH = new Instant(0);

  public BUCKETIZECALENDAR(String name) {
    super(name);
  }

  public BUCKETIZECALENDAR() {
    super(getDefaultName());
  }

  public static String getDefaultName() {
    return WarpScriptLib.BUCKETIZE_CALENDAR;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    //
    // Handle parameters
    //

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list as input.");
    }

    List<Object> params = (List<Object>) top;

    if (5 > params.size()) {
      throw new WarpScriptException(getName() + " needs a list of at least 5 parameters as input.");
    }

    DateTimeZone dtz = DateTimeZone.UTC;
    if (params.get(params.size() - 1) instanceof String) {
      String tz = (String) params.remove(params.size() - 1);
      dtz = DateTimeZone.forID(tz);
    }

    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();

    for (int i = 0; i < params.size() - 4; i++) {
      if (params.get(i) instanceof GeoTimeSerie) {
        series.add((GeoTimeSerie) params.get(i));
      } else if (params.get(i) instanceof List) {
        for (Object o: (List) params.get(i)) {
          if (!(o instanceof GeoTimeSerie)) {
            throw new WarpScriptException(getName() + " expects a list of Geo Time Series as first parameter.");
          }
          series.add((GeoTimeSerie) o);
        }
      } else {
        throw new WarpScriptException(getName() + " expects a Geo Time Series or a list of Geo Time Series as first parameter.");
      }
    }

    if (!(params.get(params.size() - 4) instanceof WarpScriptBucketizerFunction) && !(params.get(params.size() - 4) instanceof Macro) && null != params.get(params.size() - 4)) {
      throw new WarpScriptException(getName() + " expects a bucketizer function or a macro as fourth to last parameter.");
    }

    if (!(params.get(params.size() - 3) instanceof Long) || !(params.get(params.size() - 2) instanceof String) || !(params.get(params.size() - 1) instanceof Long)) {
      throw new WarpScriptException(getName() + " expects lastbucket, bucketduration, bucketcount (and optionally timezone) as last parameters.");
    }

    Object bucketizer = params.get(params.size() - 4);
    long lastbucket = (long) params.get(params.size() - 3);
    String bucketduration = (String) params.get(params.size() - 2);
    long bucketcount = (long) params.get(params.size() - 1);

    //
    // Check that lastbucket is not 0
    //

    if (0 == lastbucket) {
      throw new WarpScriptException(getName() + " does not allow lastbucket to be 0. It must be specified.");
    }

    //
    // Check that bucketcount is not negative or null and not over maxbuckets
    //

    if (bucketcount < 0) {
      throw new WarpScriptException(getName() + " expects a positive bucketcount.");
    }

    long maxbuckets = (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MAX_BUCKETS);
    if (bucketcount > maxbuckets) {
      throw new WarpScriptException(getName() + " error: bucket count (" + bucketcount + ") would exceed maximum value of " + maxbuckets);
    }

    //
    // Check that input gts are not already duration-bucketized
    //

    for (GeoTimeSerie gts: series) {
      Map<String, String> attributes = gts.getMetadata().getAttributes();
      if (attributes.get(DURATION_ATTRIBUTE_KEY) != null || attributes.get(OFFSET_ATTRIBUTE_KEY) != null || attributes.get(TIMEZONE_ATTRIBUTE_KEY) != null) {
        throw new WarpScriptException(getName() + " expects GTS for which the attributes " + DURATION_ATTRIBUTE_KEY + ", " + OFFSET_ATTRIBUTE_KEY + " and " + TIMEZONE_ATTRIBUTE_KEY + " are not set. If an input GTS is supposed to be already duration-bucketized, duration-unbucketize it first before applying a new duration-bucketization.");
      }
    }

    //
    // Check nullity of bucketizer
    //

    if (null == bucketizer) {
      throw new WarpScriptException(getName() + " expects a non null bucketizer.");
    }

    //
    // Convert duration to joda.time.Period
    //

    if ('P' != bucketduration.charAt(0)) {
      throw new WarpScriptException(getName() + " expects that the bucketduration is in ISO8601 duration format.");
    }

    ADDDURATION.ReadWritablePeriodWithSubSecondOffset bucketperiod;
    try {
      bucketperiod = ADDDURATION.durationToPeriod(bucketduration);
    } catch (WarpScriptException wse) {
      throw new WarpScriptException(getName() + " encountered an exception.", wse);
    }

    //
    // Check that the bucketduration is positive
    //

    long averageSpan = bucketperiod.getPeriod().toPeriod().toDurationFrom(EPOCH).getMillis() * Constants.TIME_UNITS_PER_MS + bucketperiod.getOffset();
    if (averageSpan < 0) {
      throw new WarpScriptException(getName() + " expects the bucketduration parameter to be a positive ISO8601 duration.");
    }

    //
    // Compute bucketindex of lastbucket and compute bucketoffset
    //

    long flag = 0; // always equal to epoch modulo period
    long bucketoffset;
    long lastbucketIndex;

    //
    // Starting from Epoch, we make a hint and land the flag close to lastbucket
    //

    long lastbucketIndexHint = lastbucket / averageSpan;
    if (lastbucket > 0) {
      flag = addNonNegativePeriod(flag, bucketperiod, dtz, lastbucketIndexHint + 1);
      lastbucketIndex = lastbucketIndexHint;

    } else {
      flag = addNonNegativePeriod(flag, bucketperiod, dtz, lastbucketIndexHint);
      lastbucketIndex = lastbucketIndexHint - 1;
    }

    //
    // We move the flag left and right on the time axis to make sure lastbucket is its leftmost bucketend
    //

    while (flag > lastbucket) {
      long N = - ((flag - lastbucket) / averageSpan - 1);
      if (N < -1) {
        flag = addNonNegativePeriod(flag, bucketperiod, dtz, N);
        lastbucketIndex = lastbucketIndex + N;
      } else {
        break;
      }
    }

    while (flag <= lastbucket) {
      long N = (lastbucket - flag) / averageSpan - 1;
      if (N > 1) {
        flag = addNonNegativePeriod(flag, bucketperiod, dtz, N);
        lastbucketIndex = lastbucketIndex + N;
      } else {
        break;
      }
    }

    while (flag > lastbucket) {
      flag = addNonNegativePeriod(flag, bucketperiod, dtz, -1);
      lastbucketIndex--;
    }

    while (flag <= lastbucket) {
      flag = addNonNegativePeriod(flag, bucketperiod, dtz, 1);
      lastbucketIndex++;
    }

    bucketoffset = flag - (lastbucket + 1);

    //
    // Duration-Bucketize
    //

    List<GeoTimeSerie> bucketized = new ArrayList<GeoTimeSerie>(series.size());
    for (GeoTimeSerie gts: series) {

      GeoTimeSerie b;
      try {
        b = durationBucketize(gts, bucketperiod, dtz, bucketcount, lastbucket, lastbucketIndex, bucketizer, maxbuckets, bucketizer instanceof Macro ? stack : null);
      } catch (WarpScriptException wse) {
        throw new WarpScriptException(getName() + " encountered an exception.", wse);
      }

      b.getMetadata().putToAttributes(DURATION_ATTRIBUTE_KEY, bucketduration);
      b.getMetadata().getAttributes().put(OFFSET_ATTRIBUTE_KEY, String.valueOf(bucketoffset));
      b.getMetadata().getAttributes().put(TIMEZONE_ATTRIBUTE_KEY, dtz.getID());

      bucketized.add(b);
    }

    stack.push(bucketized);
    return stack;
  }

  public static long addNonNegativePeriod(long origin, ADDDURATION.ReadWritablePeriodWithSubSecondOffset bucketperiod, DateTimeZone dtz, long N) throws WarpScriptException {
    long result = ADDDURATION.addPeriod(origin, bucketperiod, dtz, N);

    if (N == 0) {
      return origin;
    }

    //
    // We make sure the period from origin translates to a positive duration
    //

    if (result > origin ^ N > 0) {
      throw new WarpScriptException("Period is negative from timestamp " + origin + ". Can not add a negative period. Period is " + N + " times " + bucketperiod.getPeriod().toString() + " plus " + bucketperiod.getOffset() + " time unit(s).");
    }

    return result;
  }

  private static void aggregateAndSet(Object aggregator, GeoTimeSerie subgts, GeoTimeSerie bucketized, long bucketindex, WarpScriptStack stack) throws WarpScriptException {
    Object[] aggregated;
    if (null != stack) {
      stack.push(subgts);
      Object res = stack.peek();

      if (res instanceof List) {
        aggregated = MACROMAPPER.listToObjects((List<Object>) stack.pop());
      } else {
        aggregated = MACROMAPPER.stackToObjects(stack);
      }

    } else {

      Object[] parms = new Object[8];

      parms[0] = bucketindex;
      parms[1] = new String[] {subgts.getName()};
      parms[2] = new Map[] {subgts.getLabels()};
      parms[3] = GTSHelper.getTicks(subgts);
      if (subgts.hasLocations()) {
        parms[4] = GTSHelper.getLocations(subgts);
      } else {
        parms[4] = new long[subgts.size()];
        Arrays.fill((long[]) parms[4], GeoTimeSerie.NO_LOCATION);
      }
      if (subgts.hasElevations()) {
        parms[5] = GTSHelper.getElevations(subgts);
      } else {
        parms[5] = new long[subgts.size()];
        Arrays.fill((long[]) parms[5], GeoTimeSerie.NO_ELEVATION);
      }
      parms[6] = new Object[subgts.size()];
      parms[7] = new long[] {0, -1, bucketindex, bucketindex};

      for (int j = 0; j < subgts.size(); j++) {
        ((Object[]) parms[6])[j] = GTSHelper.valueAtIndex(subgts, j);
      }

      aggregated = (Object[]) ((WarpScriptBucketizerFunction) aggregator).apply(parms);
    }

    //
    // Only set value if it is non null
    //

    if (null != aggregated[3]) {
      GTSHelper.setValue(bucketized, bucketindex, (long) aggregated[1], (long) aggregated[2], aggregated[3], false);
    }
  }


  public static GeoTimeSerie durationBucketize(GeoTimeSerie gts, ADDDURATION.ReadWritablePeriodWithSubSecondOffset bucketperiod, DateTimeZone dtz, long bucketcount, long lastbucket, long lastbucketIndex, Object aggregator, long maxbuckets, WarpScriptStack stack) throws WarpScriptException {

    long lastTick = GTSHelper.lasttick(gts);
    long firstTick = GTSHelper.firsttick(gts);
    int hint = Math.min(gts.size(), (int) (1.05 * (lastTick - firstTick) / addNonNegativePeriod(0, bucketperiod, dtz, 1)));

    GeoTimeSerie durationBucketized = gts.cloneEmpty(hint);

    //
    // We loop through the input GTS values in reverse order
    // We feed a buffer of values while traversing
    //

    GTSHelper.sort(gts);
    GeoTimeSerie subgts = gts.cloneEmpty();

    if (null != stack) {
      if (!(aggregator instanceof Macro)) {
        throw new WarpScriptException("Expected a macro as bucketizer.");
      }
    } else {
      if (!(aggregator instanceof WarpScriptBucketizerFunction)) {
        throw new WarpScriptException("Invalid bucketizer function.");
      }
    }

    // initialize bucketstart (start boundary), and bucketindex of current tick
    long bucketstart = addNonNegativePeriod(lastbucket, bucketperiod, dtz, -1) + 1;
    long bucketindex = lastbucketIndex;

    for (int i = gts.size() - 1; i >= 0; i--) {
      long tick = GTSHelper.tickAtIndex(gts, i);

      if (tick < bucketstart) {

        //
        // Call the aggregation function on the last batch
        //

        if (subgts.size() > 0) {
          aggregateAndSet(aggregator, subgts, durationBucketized, bucketindex, stack);

          //
          // Reset buffer
          //

          subgts = GTSHelper.shrinkTo(subgts, 0);
        }
      }

      // update bucketstart and bucketindex
      while (tick < bucketstart) {
        bucketstart = addNonNegativePeriod(bucketstart, bucketperiod, dtz, -1);
        bucketindex--;
      }

      //
      // Break off the loop if bucketcount is exceeded (except if it is equal to 0)
      //

      if (bucketcount != 0 && lastbucketIndex - bucketindex + 1 >= bucketcount) {
        break;
      }

      if (lastbucketIndex - bucketindex + 2 > maxbuckets) {
        throw new WarpScriptException("Bucket count (" + (lastbucketIndex - bucketindex + 2) + ") is exceeding maximum value of " + maxbuckets);
      }

      //
      //  Save value in subgts (if tick is not more recent than lastbucket)
      //

      if (tick <= lastbucket) {
        GTSHelper.setValue(subgts, tick, GTSHelper.locationAtIndex(gts, i), GTSHelper.elevationAtIndex(gts, i), GTSHelper.valueAtIndex(gts, i), false);
      }
    }

    //
    // Aggregate on the last batch
    //

    if (subgts.size() > 0) {
      aggregateAndSet(aggregator, subgts, durationBucketized, bucketindex, stack);
    }

    //
    // Set bucket parameters
    //

    GTSHelper.setLastBucket(durationBucketized, lastbucketIndex);
    GTSHelper.setBucketSpan(durationBucketized, 1);
    GTSHelper.setBucketCount(durationBucketized, bucketcount == 0 ? durationBucketized.size() : Math.toIntExact(bucketcount));

    //
    // Reverse the order
    //

    GTSHelper.sort(durationBucketized);

    return durationBucketized;
  }

  public static boolean isDurationBucketized(GeoTimeSerie gts) {
    Map<String, String> attributes = gts.getMetadata().getAttributes();

    return attributes.get(DURATION_ATTRIBUTE_KEY) != null && attributes.get(OFFSET_ATTRIBUTE_KEY) != null && attributes.get(TIMEZONE_ATTRIBUTE_KEY) != null;
  }
}
