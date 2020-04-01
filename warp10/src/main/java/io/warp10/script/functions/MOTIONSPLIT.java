//
//   Copyright 2019  SenX S.A.S.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Split a GTS according to motion
 */
public class MOTIONSPLIT extends ElementOrListStackFunction {

  public static final String PARAM_TIMESPLIT = "timesplit";
  public static final String PARAM_SPLIT_NUMBER_LABEL = "label.split.number";
  public static final String PARAM_PROXIMITY_IN_ZONE_TIME_LABEL = "label.stopped.time";
  public static final String PARAM_SPLIT_TYPE_LABEL = "label.split.type";
  public static final String PARAM_DISTANCETHRESHOLD = "distance.split";
  public static final String PARAM_PROXIMITY_ZONE_TIME = "stopped.min.time";
  public static final String PARAM_PROXIMITY_ZONE_SPEED = "stopped.max.speed";
  public static final String PARAM_PROXIMITY_ZONE_RADIUS = "stopped.max.radius";
  public static final String PARAM_PROXIMITY_IN_ZONE_MAX_SPEED = "stopped.max.mean.speed";
  public static final String PARAM_PROXIMITY_SPLIT_STOPPED = "stopped.split";

  public static final String SPLIT_TYPE_TIME = "timesplit";
  public static final String SPLIT_TYPE_DISTANCE = "distancesplit";
  public static final String SPLIT_TYPE_END = "end";
  public static final String SPLIT_TYPE_STOPPED = "stopped";
  public static final String SPLIT_TYPE_MOVING = "moving";

  public MOTIONSPLIT(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {

    Map<String, Object> params = null;
    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a parameter MAP on top of the stack.");
    }

    params = (Map) top;

    long timeThreshold = Long.MAX_VALUE;
    if (params.containsKey(PARAM_TIMESPLIT)) {
      Object o = params.get((PARAM_TIMESPLIT));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_TIMESPLIT + " must be a number.");
      }
      timeThreshold = ((Number) o).longValue();
    }

    String splitNumberLabel = null;
    if (params.containsKey(PARAM_SPLIT_NUMBER_LABEL)) {
      Object o = params.get((PARAM_SPLIT_NUMBER_LABEL));
      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " " + PARAM_SPLIT_NUMBER_LABEL + " must be a string.");
      }
      splitNumberLabel = o.toString();
    }

    String splitTypeLabel = null;
    if (params.containsKey(PARAM_SPLIT_TYPE_LABEL)) {
      Object o = params.get((PARAM_SPLIT_TYPE_LABEL));
      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " " + PARAM_SPLIT_TYPE_LABEL + " must be a string.");
      }
      splitTypeLabel = o.toString();
    }

    String stoppedTimeLabel = null;
    if (params.containsKey(PARAM_PROXIMITY_IN_ZONE_TIME_LABEL)) {
      Object o = params.get((PARAM_PROXIMITY_IN_ZONE_TIME_LABEL));
      if (!(o instanceof String)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_IN_ZONE_TIME_LABEL + " must be a string.");
      }
      stoppedTimeLabel = o.toString();
    }

    double distanceThreshold = Double.MAX_VALUE;
    if (params.containsKey(PARAM_DISTANCETHRESHOLD)) {
      Object o = params.get((PARAM_DISTANCETHRESHOLD));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_DISTANCETHRESHOLD + " must be a number.");
      }
      distanceThreshold = ((Number) o).doubleValue();
    }

    double proximityZoneMaxSpeed = Double.MAX_VALUE;
    if (params.containsKey(PARAM_PROXIMITY_ZONE_SPEED)) {
      Object o = params.get((PARAM_PROXIMITY_ZONE_SPEED));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_ZONE_SPEED + " must be a number: speed in m/s.");
      }
      proximityZoneMaxSpeed = ((Number) o).doubleValue();
    }

    double proximityInZoneMaxMeanSpeed = Double.MAX_VALUE;
    if (params.containsKey(PARAM_PROXIMITY_IN_ZONE_MAX_SPEED)) {
      Object o = params.get((PARAM_PROXIMITY_IN_ZONE_MAX_SPEED));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_IN_ZONE_MAX_SPEED + " must be a number: speed in m/s.");
      }
      proximityInZoneMaxMeanSpeed = ((Number) o).doubleValue();
    }

    long proximityZoneTime = Long.MAX_VALUE;
    if (params.containsKey(PARAM_PROXIMITY_ZONE_TIME)) {
      Object o = params.get((PARAM_PROXIMITY_ZONE_TIME));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_ZONE_TIME + " must be a number: time in platform time unit");
      }
      proximityZoneTime = ((Number) o).longValue();
    }

    double proximityZoneRadius = Double.MAX_VALUE;
    if (params.containsKey(PARAM_PROXIMITY_ZONE_RADIUS)) {
      Object o = params.get((PARAM_PROXIMITY_ZONE_RADIUS));
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_ZONE_RADIUS + " must be a number: radius in meters.");
      }
      proximityZoneRadius = ((Number) o).doubleValue();
    }

    boolean proximityZoneSplit = false;
    if (params.containsKey(PARAM_PROXIMITY_SPLIT_STOPPED)) {
      Object o = params.get((PARAM_PROXIMITY_SPLIT_STOPPED));
      if (!(o instanceof Boolean)) {
        throw new WarpScriptException(getName() + " " + PARAM_PROXIMITY_SPLIT_STOPPED + " must be a boolean.");
      }
      proximityZoneSplit = ((Boolean) o).booleanValue();
    }

    final String fsplitNumberLabel = splitNumberLabel;
    final long ftimeThreshold = timeThreshold;
    final double fdistanceThreshold = distanceThreshold;
    final double fproximityZoneRadius = proximityZoneRadius;
    final double fproximityZoneMaxSpeed = proximityZoneMaxSpeed;
    final long fproximityZoneTime = proximityZoneTime;
    final double fproximityInZoneMaxMeanSpeed = proximityInZoneMaxMeanSpeed;
    final String fsplitTypeLabel = splitTypeLabel;
    final String fstoppedTimeLabel = stoppedTimeLabel;
    final boolean fproximityZoneSplit = proximityZoneSplit;

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (!(element instanceof GeoTimeSerie)) {
          throw new WarpScriptException(getName() + " can only be applied on Geo Time Series.");
        }

        GeoTimeSerie gts = (GeoTimeSerie) element;

        return motionSplit(gts, fsplitNumberLabel, ftimeThreshold, fdistanceThreshold, fproximityZoneRadius, fproximityZoneMaxSpeed, fproximityZoneTime, fproximityInZoneMaxMeanSpeed, fsplitTypeLabel, fstoppedTimeLabel, fproximityZoneSplit);
      }
    };
  }

  /**
   * Split a Geo Time Series according to motion.
   * <p>
   * The criteria for splitting are:
   * <p>
   * - time between datapoints, above a certain threshold a split will happen
   * - distance between datapoints, above a certain threshold a split will happen
   * - time not moving, if datapoints stay within a certain area and no distance is traveled for a certain time, a split will happen
   *
   * @param gts                         GTS to split
   * @param splitNumberLabel            If set, add a label which will contain the start of the split
   * @param timeThreshold               If the delay between two ticks goes beyond this value, force a split
   * @param distanceThreshold           If we traveled more than distanceThreshold between two ticks, force a split
   * @param proximityZoneRadius         Radius of the proximity zone in meters.
   * @param proximityZoneTime           If we spent more than that much time in the proximity zone without moving and traveled slower than proximityInZoneMaxMeanSpeed while in the proximityZone force a split
   * @param proximityZoneMaxSpeed       Maximum instant speed to consider you are in the same proximity zone
   * @param proximityInZoneMaxMeanSpeed Minimum mean speed in the proximityZone to prevent a split, in m/s
   * @param splitTypeLabel              If set, add a label with the reason of the split
   * @param stoppedTimeLabel            If set, add a label with the time in the stop state
   * @return
   * @throws WarpScriptException
   */
  private static List<GeoTimeSerie> motionSplit(GeoTimeSerie gts, String splitNumberLabel, long timeThreshold, double distanceThreshold, double proximityZoneRadius, double proximityZoneMaxSpeed,
                                                long proximityZoneTime, double proximityInZoneMaxMeanSpeed, String splitTypeLabel, String stoppedTimeLabel, boolean proximityZoneSplit) throws WarpScriptException {
    //
    // Sort GTS according to timestamps
    //

    GTSHelper.sort(gts, false);

    GeoTimeSerie split = null;

    List<GeoTimeSerie> splits = new ArrayList<GeoTimeSerie>();

    int idx = 0;
    int gtsid = 1;
    int n = GTSHelper.nvalues(gts);

    boolean mustSplit = true;

    long refLocation = GeoTimeSerie.NO_LOCATION;
    long refTimestamp = Long.MIN_VALUE;

    long previousTimestamp = Long.MIN_VALUE;
    long previousLocation = GeoTimeSerie.NO_LOCATION;
    double proximityZoneTraveledDistance = 0.0D;
    String splitReason = SPLIT_TYPE_END;

    //
    // empty gts, or gts with only one value, returns a clone + extra labels when set
    //
    if (n <= 1) {
      GeoTimeSerie result = gts.clone();
      if (null != splitTypeLabel) {
        result.getMetadata().putToLabels(splitTypeLabel, SPLIT_TYPE_END);
      }
      if (null != splitNumberLabel) {
        result.getMetadata().putToLabels(splitNumberLabel, "1");
      }
      splits.add(result);
      return splits;
    }

    while (idx < n) {
      long timestamp = GTSHelper.tickAtIndex(gts, idx);
      long location = GTSHelper.locationAtIndex(gts, idx);
      long elevation = GTSHelper.elevationAtIndex(gts, idx);
      Object value = GTSHelper.valueAtIndex(gts, idx);

      //
      // Initialise refLocation as soon as there is a valid position
      //
      if (GeoTimeSerie.NO_LOCATION == refLocation && GeoTimeSerie.NO_LOCATION != location) {
        refLocation = location;
        refTimestamp = timestamp;
      }

      // highest priority: if proximity detection is activated, test proximity first.
      // If the current point is farther away from the reference point than 'proximityZoneRadius', or the instant speed is greater than proximityZoneSpeed
      // change the reference point and reset the traveled distance.
      // If the distance traveled in the proximity zone was traveled at less than proximityInZoneMaxMeanSpeed and the time spent in the zone is above proximityZoneTime, force a split
      //
      long timeStopped = previousTimestamp - refTimestamp;
      if (proximityZoneRadius < Double.MAX_VALUE && proximityZoneTime < Double.MAX_VALUE && GeoTimeSerie.NO_LOCATION != refLocation && GeoTimeSerie.NO_LOCATION != location) {

        double currentSpeed = 0.0D;
        if (GeoTimeSerie.NO_LOCATION != previousLocation && timestamp != previousTimestamp) {
          currentSpeed = GeoXPLib.orthodromicDistance(previousLocation, location) / ((double) (timestamp - previousTimestamp) / Constants.TIME_UNITS_PER_S);
        }

        // quit the radius, or speed greater than max stopped speed.
        if (GeoXPLib.orthodromicDistance(refLocation, location) > proximityZoneRadius || currentSpeed > proximityZoneMaxSpeed) {
          double zoneMeanSpeed = 0.0D;
          if (previousTimestamp != refTimestamp) {
            zoneMeanSpeed = proximityZoneTraveledDistance / ((double) (previousTimestamp - refTimestamp) / Constants.TIME_UNITS_PER_S);
          }
          if (timeStopped > proximityZoneTime && zoneMeanSpeed < proximityInZoneMaxMeanSpeed) {
            splitReason = SPLIT_TYPE_STOPPED;
            mustSplit = true;
            if (null != stoppedTimeLabel && null != split) {
              split.getMetadata().putToLabels(stoppedTimeLabel, Long.toString(timeStopped));
            }
          }
          refLocation = location;
          refTimestamp = timestamp;
          proximityZoneTraveledDistance = 0.0D;
        } else { // inside the radius
          if (GeoTimeSerie.NO_LOCATION != previousLocation) {
            proximityZoneTraveledDistance += GeoXPLib.orthodromicDistance(previousLocation, location);
          }
        }
      }

      if (!mustSplit) {
        //
        // priority 2: if no stop detection, test for distance
        // If the distance to the previous location is above distanceThreshold, split
        //
        if (GeoTimeSerie.NO_LOCATION != previousLocation && GeoTimeSerie.NO_LOCATION != location && GeoXPLib.orthodromicDistance(location, previousLocation) > distanceThreshold) {
          splitReason = SPLIT_TYPE_DISTANCE;
          mustSplit = true;
        } else if (timestamp - previousTimestamp > timeThreshold) {
          //
          // priority 3: if no stop detection and no distance detection, test time split.
          // If the previous tick was more than 'timeThreshold' ago, split now
          //
          splitReason = SPLIT_TYPE_TIME;
          mustSplit = true;
        }
      }

      //
      // mustSplit is also true on the first iteration
      //
      if (mustSplit) {
        //
        // End the previous split (could be null on the first iteration), add optional labels.
        //
        if (null != splitTypeLabel && null != split) {
          split.getMetadata().putToLabels(splitTypeLabel, splitReason);
        }
        //
        // Split again the current split if proximityZoneSplit is set
        //
        if (proximityZoneSplit && null != split && SPLIT_TYPE_STOPPED.equals(splitReason)) {
          long stopTimestamp = GTSHelper.lasttick(split) - timeStopped;
          if (stopTimestamp != GTSHelper.firsttick(split)) {
            GeoTimeSerie moving = GTSHelper.timeclip(split, Long.MIN_VALUE, stopTimestamp);
            GeoTimeSerie stopped = GTSHelper.timeclip(split, stopTimestamp + 1, Long.MAX_VALUE);
            if (null != splitTypeLabel) {
              moving.getMetadata().putToLabels(splitTypeLabel, SPLIT_TYPE_MOVING);
            }
            if (null != splitNumberLabel) {
              stopped.getMetadata().putToLabels(splitNumberLabel, Long.toString(gtsid));
              gtsid++;
            }
            splits.remove(splits.size() - 1);
            splits.add(moving);
            splits.add(stopped);
          }
        }

        split = gts.cloneEmpty();
        //
        // Start of the split, add optional labels.
        //
        if (null != splitNumberLabel) {
          split.getMetadata().putToLabels(splitNumberLabel, Long.toString(gtsid));
          gtsid++;
        }
        splits.add(split);
        mustSplit = false;

        refLocation = location;
        refTimestamp = timestamp;
        proximityZoneTraveledDistance = 0.0D;
      }

      GTSHelper.setValue(split, timestamp, location, elevation, value, false);

      //
      // On the last iteration, also manage the split type label (end or stopped), and split again if needed
      //
      if (idx == n - 1) {
        if ((previousTimestamp - refTimestamp) > proximityZoneTime) {
          splitReason = SPLIT_TYPE_STOPPED;
          if (null != stoppedTimeLabel) {
            split.getMetadata().putToLabels(stoppedTimeLabel, Long.toString(timeStopped));
          }
          if (null != splitTypeLabel) {
            split.getMetadata().putToLabels(splitTypeLabel, splitReason);
          }
          //
          // Split again the current split if proximityZoneSplit is set
          //
          if (proximityZoneSplit) {
            long stopTimestamp = GTSHelper.lasttick(split) - timeStopped;
            GeoTimeSerie moving = GTSHelper.timeclip(split, Long.MIN_VALUE, stopTimestamp);
            GeoTimeSerie stopped = GTSHelper.timeclip(split, stopTimestamp + 1, Long.MAX_VALUE);
            if (null != splitTypeLabel) {
              moving.getMetadata().putToLabels(splitTypeLabel, SPLIT_TYPE_MOVING);
            }
            if (null != splitNumberLabel) {
              stopped.getMetadata().putToLabels(splitNumberLabel, Long.toString(gtsid));
            }
            splits.remove(splits.size() - 1);
            splits.add(moving);
            splits.add(stopped);
          }
        } else {
          if (null != splitTypeLabel) {
            if (proximityZoneSplit) {
              split.getMetadata().putToLabels(splitTypeLabel, SPLIT_TYPE_MOVING);
            } else {
              split.getMetadata().putToLabels(splitTypeLabel, SPLIT_TYPE_END);
            }
          }
        }
      }

      previousTimestamp = timestamp;
      previousLocation = location;

      idx++;
    }

    return splits;
  }
}
