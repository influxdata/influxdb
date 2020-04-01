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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Comparator class used to sort GTS instances.
 * 
 * The sorting order is by name, then by labels, then by last timestamp and finally by most recent value.
 * 
 * The side effect of using this comparator is that if sorting needs to check timestamps or values,
 * the GTS instances might end up being sorted internally.
 * 
 */
public class GTSComparator implements Comparator<GeoTimeSerie> {
  @Override
  public int compare(GeoTimeSerie gts1, GeoTimeSerie gts2) {
    int nameCompare = gts1.getName().compareTo(gts2.getName());
    
    //
    // Names differ, return the result of their comparison
    //
    
    if (0 != nameCompare) {
      return nameCompare;
    }
    
    //
    // Compare labels
    //
    
    //
    // We need to compare last timestamp, first check if GTS instances are
    // bucketized, in which case we just need to compare the values of 'lastbucket'
    // Otherwise we need to sort the non bucketized instances and check their last
    // timestamp.
    // If one instance is not bucketized and has no values, its last timestamp is
    // assumed to be Long.MIN_VALUE
    //
 
    Map<String,String> labels1 = gts1.getLabels();
    Map<String,String> labels2 = gts2.getLabels();
    
    List<String> keys1 = new ArrayList<String>();
    keys1.addAll(labels1.keySet());
    Collections.sort(keys1);
    
    List<String> keys2 = new ArrayList<String>();
    keys2.addAll(labels2.keySet());
    Collections.sort(keys2);

    // Last label to check, passed this the label names differ
    // and the comparison will be that of the names
    
    int differingLabel = 0;
    
    while (differingLabel < keys1.size() && differingLabel < keys2.size() && 0 == keys1.get(differingLabel).compareTo(keys2.get(differingLabel))) {
      differingLabel++;
    }
    
    int labelsCompare = 0;

    for (int i = 0; i < differingLabel; i++) {
      String value1 = labels1.get(keys1.get(i));
      String value2 = labels2.get(keys1.get(i));
      
      labelsCompare = value1.compareTo(value2);
      
      if (0 != labelsCompare) {
        return labelsCompare;
      }
    }
    
    // All labels so far have the same values, check the differingLabel'th one
    
    // If differingLabel == keys1.size() and differingLabel == keys2.size() then
    // we will have to check the timestamps as all labels are equal, otherwise, check the
    // first differing label
    
    if (differingLabel != keys1.size() || differingLabel != keys2.size()) {
      // GTS1 has fewer labels than GTS2, it's therefore smaller
      if (differingLabel == keys1.size()) {
        return -1;
      }
      
      // GTS2 has fewer labels than GTS1, it's therefore smaller
      if (differingLabel == keys2.size()) {
        return 1;
      }
      
      // Compare label names
      return keys1.get(differingLabel).compareTo(keys2.get(differingLabel));
    }
    
    //
    // All labels are equal, compare the ticks
    //
    
    GTSHelper.sort(gts1);
    GTSHelper.sort(gts2);
    
    int idx = 0;
    
    while (idx < gts1.values && idx < gts2.values) {
      if (gts1.ticks[gts1.values - 1 - idx] < gts2.ticks[gts2.values - 1 - idx]) {
        return -1;
      } else if (gts1.ticks[gts1.values - 1 - idx] > gts2.ticks[gts2.values - 1 - idx]) {
        return 1;
      } else {
        // Ticks are equal, compare values
        Object value1 = GTSHelper.valueAtIndex(gts1, gts1.values - 1 - idx);
        Object value2 = GTSHelper.valueAtIndex(gts2, gts1.values - 1 - idx);
        
        if (value1.equals(value2)) {
          continue;
        }
        
        if (value1 instanceof String || value2 instanceof String) {
          int comp = value1.toString().compareTo(value2.toString());
          if (0 != comp) {
            return comp;
          }
        } else if (value1 instanceof Number && value2 instanceof Number) {
          BigDecimal bd1;
          BigDecimal bd2;
                   
          if (value1 instanceof Long) {
            if (value2 instanceof Long) {
              if (((Number) value1).longValue() < ((Number) value2).longValue()) {
                return -1;
              } else if (((Number) value1).longValue() > ((Number) value2).longValue()) {
                return 1;
              }
            } else {
              bd1 = new BigDecimal(((Number) value1).longValue());            
              bd2 = new BigDecimal(((Number) value2).doubleValue());
              
              int bdCompare = bd1.compareTo(bd2);
              
              if (0 != bdCompare) {
                return bdCompare;
              }
            }
          } else if (value1 instanceof Double) {
            if (value2 instanceof Double) {
              if (((Number) value1).doubleValue() < ((Number) value2).doubleValue()) {
                return -1;
              } else if (((Number) value1).doubleValue() > ((Number) value2).doubleValue()) {
                return 1;
              }
            } else {
              bd1 = new BigDecimal(((Number) value1).doubleValue());            
              bd2 = new BigDecimal(((Number) value2).longValue());
              
              int bdCompare = bd1.compareTo(bd2);
              
              if (0 != bdCompare) {
                return bdCompare;
              }
            }
          }
        } else if (value1 instanceof Boolean) {
          // False is less than anything else
          if (Boolean.FALSE.equals(value1)) {
            return -1;
          } else if (Boolean.TRUE.equals(value2)) {
            return 1;
          }
        } else if (value2 instanceof Boolean) {
          // True is more than anything else
          if (Boolean.FALSE.equals(value2)) {
            return 1;
          } else if (Boolean.TRUE.equals(value2)) {
            return -1;
          }
        }
      }
      idx++;
    }
    
    return 0;
  }
}
