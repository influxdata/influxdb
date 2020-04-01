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

package io.warp10.continuum.gts;

import com.geoxp.GeoXPLib;
import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pickler for Geo Time Series
 */
public class GTSPickler implements IObjectPickler {

  private static String CLASSNAME_KEY = "classname";
  private static String LABELS_KEY = "labels";
  private static String ATTRIBUTES_KEY = "attributes";
  private static String TIMESTAMPS_KEY = "timestamps";
  private static String VALUES_KEY = "values";
  private static String LATITUDE_KEY = "latitudes";
  private static String LONGITUDE_KEY = "longitudes";
  private static String ELEVATION_KEY = "elevations";

  public void pickle(Object o, OutputStream out, Pickler currentPickler) throws PickleException, IOException {
    if (!(o instanceof GeoTimeSerie)) {
      throw new IllegalArgumentException("Invalid type for first argument. It must be a GeoTimeSerie™.");
    }

    GeoTimeSerie gts = (GeoTimeSerie) o;

    Map<String, Object> gtsAsMap = new HashMap<String, Object>();
    gtsAsMap.put(CLASSNAME_KEY, gts.getName());
    gtsAsMap.put(LABELS_KEY, gts.getMetadata().getLabels());
    gtsAsMap.put(ATTRIBUTES_KEY, gts.getMetadata().getAttributes());

    List<Long> ticks = new ArrayList<Long>(gts.values);
    for (int i = 0; i < gts.values; i++) {
      ticks.add(gts.ticks[i]);
    }
    gtsAsMap.put(TIMESTAMPS_KEY, ticks);

    if (0 == gts.values) {
      gtsAsMap.put(VALUES_KEY, new long[0]);
    } else {
      List<Object> values = new ArrayList<Object>(gts.values);

      for (int i = 0; i < gts.values; i++) {
        values.add(GTSHelper.valueAtIndex(gts, i));
      }
      gtsAsMap.put(VALUES_KEY, values);
    }

    if (gts.hasLocations()) {
      long[] locations = gts.locations;
      List<Float> lats = new ArrayList<Float>();
      List<Float> lons = new ArrayList<Float>();

      for (int i = 0; i < gts.values; i++) {

        if (GeoTimeSerie.NO_LOCATION == locations[i]) {
          lats.add(Float.NaN);
          lons.add(Float.NaN);

        } else {
          double[] latlon = GeoXPLib.fromGeoXPPoint(locations[i]);
          lats.add((float) latlon[0]);
          lons.add((float) latlon[1]);
        }
      }

      gtsAsMap.put(LATITUDE_KEY, lats);
      gtsAsMap.put(LONGITUDE_KEY, lons);
    }

    if (gts.hasElevations()) {
      List<Long> elevs = new ArrayList<Long>(gts.values);
      for (int i = 0; i < gts.values; i++) {
        elevs.add(gts.elevations[i]); // no elevation is Long.MIN_VALUE
      }
      gtsAsMap.put(ELEVATION_KEY, elevs);
    }

    currentPickler.save(gtsAsMap);
  }
}
