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

package io.warp10.continuum.gts;

import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.geoxp.GeoXPLib;

/**
 * Container for Geo Time Serie when manipulating them.
 */
public class GeoTimeSerie implements Cloneable {

  public static enum TYPE {
    UNDEFINED, LONG, DOUBLE, BOOLEAN, STRING    
  };
  
  /**
   * Type of the GTS instance, determined by the first value added
   */
  TYPE type = TYPE.UNDEFINED;
  
  /**
   * Factor by which we grow arrays when they filled up.
   */
  static final float ARRAY_GROWTH_FACTOR = 0.2F;
  
  /**
   * Mininum number of elements to add to arrays when growing them.
   * 64 elements consumes grossly 2kb when location/elevation are present
   */
  static final int MIN_ARRAY_GROWTH = 64;
  
  /**
   * Maximum number of elements to add to arrays when growing them.
   * 32768 represents ~1Mb when location/elevation are present
   */
  static final int MAX_ARRAY_GROWTH = 32768;
  
  /**
   * Value used for elevation when there is no elevation.
   */
  public static final long NO_ELEVATION = Long.MIN_VALUE;
  
  /**
   * Value used for location when there is no location.
   * This is a perfectly valid GeoXPPoint, but it lies at
   * 
   * -90.0 latitude
   * -141.88235295005143 longitude
   * 
   * which is one longitude for the south pole. One highly
   * unlikely to be chosen as a valid location....
   */
  public static final long NO_LOCATION = 0x0145014501450145L;
  
  /**
   * Width of each bucket in microseconds.
   * Value of 0 means Geo Time Serie is not bucketized
   */
  long bucketspan = 0L;
  
  /**
   * Number of buckets.
   * Value of 0 means GTS is not bucketized.
   */
  int bucketcount = 0;
  
  /**
   * Timestamp of end of last bucket.
   * Value of 0 means GTS is not bucketized.
   */
  long lastbucket = 0L;
  
  /**
   * Array of timestamps (bucket ends if GTS is bucketized) with
   * values.
   */
  
  long[] ticks = null;
  
  /**
   * Array of locations (GeoXPPoints)
   */
  long[] locations = null;
  
  /**
   * Array of elevations.
   */
  long[] elevations = null;
  
  /**
   * Array of values if Geo Time Serie is of type LONG
   */
  long[] longValues = null;
  
  /**
   * Array of values if Geo Time Serie is of type DOUBLE
   */
  double[] doubleValues = null;
  
  /**
   * Array of values if Geo Time Serie is of type STRING
   */
  String[] stringValues = null;
  
  /**
   * BitSet for values of BOOLEAN Geo Time Serie
   */
  BitSet booleanValues = null;
  
  /**
   * Number of values in the Geo Time Serie
   */
  int values = 0;

  /**
   * Hint about the initial size of the Geo Time Serie
   * This value will be used for the initial allocation
   */
  int sizehint = 0;
  
  /**
   * Flag indicating whether or not the GTS is sorted.
   * This flag is correctly maintained by GTSHelper, if
   * another class fiddles with internal arrays, then
   * it might not reflect reality.
   */
  boolean sorted = false;
  
  /**
   * Flag indicating whether or not the GTS is sorted
   * in reverse order.
   */
  boolean reversed = false;

  private Metadata metadata;
  
  /**
   * Flag indicating whether or not a GTS was renamed. This is used as a safety net for 'UPDATE'
   */
  private boolean renamed = false;
  
  public GeoTimeSerie() {
  }
  
  /**
   * Create a new GeoTimeSerie instance with the initial hint
   * about its size
   * 
   * @param sizehint
   */
  public GeoTimeSerie(int sizehint) {
    this.sizehint = sizehint;
  }  
  
  /**
   * Create a new bucketized GeoTimeSerie instance
   * 
   * @param lastbucket End timestamp of most recent bucket.
   * @param bucketcount Number of buckets in the geo time serie
   * @param bucketspan Time span of each bucket
   * @param sizehint Size hint
   */
  public GeoTimeSerie(long lastbucket, int bucketcount, long bucketspan, int sizehint) {
    this.lastbucket = lastbucket;
    this.bucketcount = bucketcount;
    this.bucketspan = bucketspan;
    this.sizehint = sizehint;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    GTSHelper.encodeName(sb, getName());
    sb.append("{");
    boolean haslabels = false;
    
    if (null != getLabels()) {
      for (Map.Entry<String, String> entry: getLabels().entrySet()) {
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        sb.append(",");
        haslabels = true;
      }  
    }

    if (haslabels) {
      sb.setLength(sb.length() - 1);
    }
    sb.append("}");
    sb.append("\n");
    for (int i = 0; i < this.values; i++) {
      sb.append("=");
      sb.append(ticks[i]);
      sb.append("/");
      if (null != locations && NO_LOCATION != locations[i]) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(locations[i]);
        sb.append(latlon[0]);
        sb.append(":");
        sb.append(latlon[1]);
      }
      sb.append("/");
      if (null != elevations && NO_ELEVATION != elevations[i]) {
        sb.append(elevations[i]);
      }
      sb.append(" ");
      if (TYPE.LONG == this.type) {
        sb.append(longValues[i]);
      } else if (TYPE.DOUBLE == this.type) {
        sb.append(doubleValues[i]);
      } else if (TYPE.STRING == this.type) {
        GTSHelper.encodeValue(sb, stringValues[i]);
      } else {
        if (booleanValues.get(i)) {
          sb.append("T");
        } else {
          sb.append("F");
        }
      }
      sb.append("\n");
    }
    return sb.toString();
  }
  
  public void dump(PrintWriter pw) {
    StringBuilder sb = new StringBuilder(" ");
    
    GTSHelper.encodeName(sb, this.getName());
    if (this.metadata.getLabelsSize() > 0) {
      sb.append("{");
      boolean first = true;
      for (Entry<String,String> entry: this.getLabels().entrySet()) {
        if (!first) {
          sb.append(",");
        }
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        first = false;
      }
      sb.append("}");      
    } else {
      sb.append("{}");
    }
    
    if (this.metadata.getAttributesSize() > 0) {
      sb.append("{");
      boolean first = true;    
      for (Entry<String,String> entry: this.metadata.getAttributes().entrySet()) {
        if (!first) {
          sb.append(",");
        }
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        first = false;
      }
      sb.append("}");
    } else {
      sb.append("{}");
    }
    
    sb.append(" ");
    
    String clslbs = sb.toString();
    
    for (int i = 0; i < this.values; i++) {
      if (i > 0) {
        pw.print("=");
      }
      pw.print(this.ticks[i]);
      pw.print("/");
      if (null != this.locations && GeoTimeSerie.NO_LOCATION != this.locations[i]) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(this.locations[i]);
        pw.print(latlon[0]);
        pw.print(":");
        pw.print(latlon[1]);
      }
      pw.print("/");
      if (null != this.elevations && GeoTimeSerie.NO_ELEVATION != this.elevations[i]) {
        pw.print(this.elevations[i]);
      }
      if (0 == i) {
        pw.print(clslbs);
      } else {
        pw.print(" ");
      }
      sb.setLength(0);
      GTSHelper.encodeValue(sb, GTSHelper.valueAtIndex(this,i));
      pw.print(sb.toString());
      pw.print("\r\n");
    }
  }
  
  /**
   * Return the number of measures in this Geo Time Series.
   * @return
   */
  public int size() {
    return this.values;
  }
  
  @Override
  public GeoTimeSerie clone() {
    GeoTimeSerie serie = cloneEmpty();
    
    serie.type = this.type;
    
    if (null != this.ticks) {
      serie.ticks = Arrays.copyOf(this.ticks, this.values);
    }
    if (null != this.locations) {
      serie.locations = Arrays.copyOf(this.locations, this.values);
    }
    if (null != this.elevations) {
      serie.elevations = Arrays.copyOf(this.elevations, this.values);
    }
    
    //
    // Only copy the relevant arrays.
    //
    
    if (TYPE.BOOLEAN == this.type && null != this.booleanValues) {
      serie.booleanValues = (BitSet) this.booleanValues.clone();
    }
    if (TYPE.DOUBLE == this.type && null != this.doubleValues) {
      serie.doubleValues = Arrays.copyOf(this.doubleValues, this.values);
    }
    if (TYPE.LONG == this.type && null != this.longValues) {
      serie.longValues = Arrays.copyOf(this.longValues, this.values);
    }
    if (TYPE.STRING == this.type && null != this.stringValues) {
      serie.stringValues = Arrays.copyOf(this.stringValues, this.values);
    }
    
    serie.values = this.values;
    serie.sorted = this.sorted;
    serie.reversed = this.reversed;
    
    return serie;
  }
  
  /**
   * Clone everything except the type/values/locations/elevations.
   * 
   * @return
   */
  public GeoTimeSerie cloneEmpty(int sizehint) {
    GeoTimeSerie serie = new GeoTimeSerie(this.lastbucket, this.bucketcount, this.bucketspan, sizehint);
    serie.setMetadata(this.getMetadata());
    //serie.type = this.type;

    return serie;
  }
  
  public GeoTimeSerie cloneEmpty() {
    return cloneEmpty(0);
  }
  
  /**
   * Return the type of this GTS instance.
   */
  public TYPE getType() {
    return this.type;
  }
  
  public long getClassId() {
    return this.getMetadata().getClassId();
  }

  public void setClassId(long classId) {
    this.getMetadata().setClassId(classId);
  }

  public long getLabelsId() {
    return this.getMetadata().getLabelsId();
  }

  public void setLabelsId(long labelsId) {
    this.getMetadata().setLabelsId(labelsId);
  }
  
  public String getName() {
    return this.getMetadata().getName();
  }

  public void setName(String name) {
    this.getMetadata().setName(name);
  }

  public Map<String, String> getLabels() {
    return Collections.unmodifiableMap(this.getMetadata().getLabels());
  }
  
  public boolean hasLabel(String name) {
    return this.getMetadata().getLabels().containsKey(name);
  }

  public String getLabel(String name) {
    return this.getMetadata().getLabels().get(name);
  }
  
  public void setLabels(Map<String, String> labels) {
    this.getMetadata().setLabels(new HashMap<String,String>(labels));
  }

  public void setLabel(String key, String value) {
    this.getMetadata().getLabels().put(key, value);
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = new Metadata(metadata);
  }

  public void safeSetMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public Metadata getMetadata() {
    if (null == this.metadata) {
      this.metadata = new Metadata();
    }
    
    if (null == this.metadata.getLabels()) {
      this.metadata.setLabels(new HashMap<String,String>());
    }
    
    if (null == this.metadata.getAttributes()) {
      this.metadata.setAttributes(new HashMap<String,String>());
    }
    
    return this.metadata;
  }
  
  public boolean hasLocations() {
    return null != this.locations;
  }
  
  public boolean hasElevations() {
    return null != this.elevations;
  }
  
  public void setRenamed(boolean renamed) {
    this.renamed = renamed;
  }
  
  public boolean isRenamed() {
    return this.renamed;
  }
  
  public void setSizeHint(int sizehint) {
    this.sizehint = sizehint;
  }
  
  public void reset(long[] ticks, long[] locations, long[] elevations, double[] values, int size) throws IOException {
    
    int n = ticks.length;
    
    if (n != values.length || (null != locations && n != locations.length) || (null != elevations && n != elevations.length)) {
      throw new IOException("Incoherent array lengths");
    }
    
    this.ticks = ticks;
    this.locations = locations;
    this.elevations = elevations;
    this.doubleValues = values;
    this.longValues = null;
    this.booleanValues = null;
    this.stringValues = null;
    this.sorted = false;
    this.values = size;
    this.sizehint = n;
    
    this.type = TYPE.DOUBLE;    
  }

  public void reset(long[] ticks, long[] locations, long[] elevations, long[] values, int size) throws IOException {
    
    int n = ticks.length;
    
    if (n != values.length || (null != locations && n != locations.length) || (null != elevations && n != elevations.length)) {
      throw new IOException("Incoherent array lengths");
    }
    
    this.ticks = ticks;
    this.locations = locations;
    this.elevations = elevations;
    this.doubleValues = null;
    this.longValues = values;
    this.booleanValues = null;
    this.stringValues = null;
    this.sorted = false;
    this.values = size;
    this.sizehint = n;
    
    this.type = TYPE.LONG;    
  }
  
  public void reset(long[] ticks, long[] locations, long[] elevations, String[] values, int size) throws IOException {
    
    int n = ticks.length;
    
    if (n != values.length || (null != locations && n != locations.length) || (null != elevations && n != elevations.length)) {
      throw new IOException("Incoherent array lengths");
    }
    
    this.ticks = ticks;
    this.locations = locations;
    this.elevations = elevations;
    this.doubleValues = null;
    this.longValues = null;
    this.booleanValues = null;
    this.stringValues = values;
    this.sorted = false;
    this.values = size;
    this.sizehint = n;
    
    this.type = TYPE.STRING;    
  }

  public void reset(long[] ticks, long[] locations, long[] elevations, BitSet values, int size) throws IOException {
    
    int n = ticks.length;
    
    if (n != values.size() || (null != locations && n != locations.length) || (null != elevations && n != elevations.length)) {
      throw new IOException("Incoherent array lengths");
    }
    
    this.ticks = ticks;
    this.locations = locations;
    this.elevations = elevations;
    this.doubleValues = null;
    this.longValues = null;
    this.booleanValues = values;
    this.stringValues = null;
    this.sorted = false;
    this.values = size;
    this.sizehint = n;
    
    this.type = TYPE.BOOLEAN;    
  }

  /**
   * Attempts to force the type of an empty GeoTimeSerie instance.
   * Will only succeed if the current type is UNDEFINED.
   * 
   * @param type
   */
  public void setType(TYPE type) {
    if (!TYPE.UNDEFINED.equals(this.type)) {
      return;
    }
    this.type = type;
    
    //
    // Allocate empty arrays so functions which expect arrays to be defined
    // do not break, even if the GTS has no values yet
    //
    
    this.ticks = new long[0];
    
    switch(type) {
      case LONG:
        this.longValues = new long[0];
        break;
      case DOUBLE:
        this.doubleValues = new double[0];
        break;
      case BOOLEAN:
        this.booleanValues = new BitSet(0);
        break;
      case STRING:
        this.stringValues = new String[0];
        break;
      default:
    }
  }
}
