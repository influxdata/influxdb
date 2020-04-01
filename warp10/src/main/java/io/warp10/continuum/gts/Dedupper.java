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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ignores duplicates in a stream of measurements.
 * Assumes all String are UTF-8
 * Only works if the measurements for a given class+labels are
 * sorted in chronological order.
 */
public class Dedupper {
  /**
   * Map of class+labels to last timestamp
   */
  private LinkedHashMap<String, Long> lastTimestamp = new LinkedHashMap<String, Long>();

  /**
   * Map of class+labels to last locations/elevation/value
   */
  private LinkedHashMap<String, String> lastLocationElevationValue = new LinkedHashMap<String, String>();
  
  /**
   * Last line discarded for each class (will be flushed when closing dedupper)
   */
  private LinkedHashMap<String, String> lastDiscardedLine = new LinkedHashMap<String, String>();
  
  private Pattern REGEXP = Pattern.compile("^([0-9]+)/([^/]*)/([0-9]*)\\s+([^\\s]+)\\s+(.*)$");
  
  private final long heartbeat;
  
  private final OutputStream os;
  
  public Dedupper(OutputStream os, long heartbeat) {
    this.heartbeat = heartbeat;
    this.os = os;
  }
  
  public void dedup(String line) throws IOException {
    //
    // Extract timestamp, location/elevation, class+labels, value
    //
    
    Matcher m = REGEXP.matcher(line);
    
    if (!m.matches()) {
      throw new IOException("Invalid syntax.");
    }
    
    long timestamp = Long.valueOf(m.group(1));
    String location = m.group(2);
    String elevation = m.group(3);
    String class_labels = m.group(4);
    String value = m.group(5);
    
    Long lastts = lastTimestamp.get(class_labels);
    
    StringBuilder sb = new StringBuilder();
    sb.append(location);
    sb.append("/");
    sb.append(elevation);
    sb.append(value);

    String locelevvalue = sb.toString();
    
    //
    // If the last value was more than 'heartbeat' us ago or
    // this is the first value for this class+labels,
    if (null == lastts || (timestamp - lastts) > heartbeat || !locelevvalue.equals(lastLocationElevationValue.get(class_labels))) {
      os.write(line.getBytes(StandardCharsets.UTF_8));
      os.write('\n');
      lastTimestamp.put(class_labels, timestamp);
      lastLocationElevationValue.put(class_labels, locelevvalue);
      lastDiscardedLine.remove(class_labels);
      return;
    }
   
    lastDiscardedLine.put(class_labels, line);
  }
  
  public void flush() throws IOException {
    for (String line: lastDiscardedLine.values()) {
      os.write(line.getBytes(StandardCharsets.UTF_8));
      os.write('\n');
    }
  }
  
  public void reset() {
    lastTimestamp.clear();
    lastLocationElevationValue.clear();
    lastDiscardedLine.clear();
  }
  
  public void close() {
    reset();
  }
}
