//
//   Copyright 2019-2020  SenX S.A.S.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PIVOT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean synchronous;
  
  public PIVOT(String name, boolean sync) {
    super(name);
    this.synchronous = sync;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List) || (0 == ((List) top).size())) {
      throw new WarpScriptException(getName() + " expects a non empty list of labeling Geo Time Series.");
    }
    
    List<Object> paramLabeling = (List<Object>) top;
    List<Object> labeling = new ArrayList<Object>(paramLabeling.size());
    
    Set<String> classes = new HashSet<String>();
    for (Object o: paramLabeling) {
      if (o instanceof GTSEncoder) {
        GTSEncoder encoder = (GTSEncoder) o;
        GTSDecoder decoder = encoder.getDecoder(true);
        try {
          o = decoder.decode(null, true);
        } catch (Throwable t) {
          throw new WarpScriptException(getName() + " encountered an error while decoding encoder.", t);
        }
      }
      
      if (!(o instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " expects a list of labeling Geo Time Series.");        
      }

      labeling.add(o);
      
      if (0 == GTSHelper.nvalues((GeoTimeSerie) o)) {
        throw new WarpScriptException(getName() + " expects labeling Geo Time Series to be non empty.");
      }
      
      String cls = ((GeoTimeSerie) o).getName();
      
      if (!classes.add(cls)) {
        throw new WarpScriptException(getName() + " labeling Geo Time Series™ must all have different class names.");
      }
    }
    
    top = stack.pop();
    
    if (!(top instanceof List) || 0 == ((List) top).size()) {
      throw new WarpScriptException(getName() + " operates on a non empty list of Geo Time Series or GTS Encoders.");
    }

    List<Object> paramGts = (List<Object>) top;
    List<Object> gts = new ArrayList<Object>(paramGts.size());
    
    for (Object o: paramGts) {
      if (o instanceof GTSEncoder) {
        GTSEncoder encoder = (GTSEncoder) o;
        GTSDecoder decoder = encoder.getDecoder(true);
        try {
          o = decoder.decode(null, true);
        } catch (Throwable t) {
          throw new WarpScriptException(getName() + " encountered an error while decoding encoder.", t);
        }
      }
      
      if (!(o instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " operates on a list Geo Time Series or GTS Encoders.");        
      }

      gts.add(o);
      
      //
      // Check that none of the labeling GTS class names is already a label of one of the
      // GTS to label
      //
      
      GeoTimeSerie g = (GeoTimeSerie) o;

      if (0 == GTSHelper.nvalues(g)) {
        throw new WarpScriptException(getName() + " operates on non empty Geo Time Series or GTS Encoders.");
      }
      
      for (String key: g.getLabels().keySet()) {
        if (classes.contains(key)) {
          throw new WarpScriptException(getName() + " labeling class '" + key + "' is already a label of a Geo Time Series or GTS Encoder to label.");
        }
      }
    }
        
    //
    // Now sort all GTS
    //
    
    for (Object g: gts) {
      GTSHelper.sort((GeoTimeSerie) g);
    }
    
    for (Object g: labeling) {
      GTSHelper.sort((GeoTimeSerie) g);
    }
    
    int[] labelingIndices = new int[labeling.size()];
    int[] gtsIndices = new int[gts.size()];
    
    boolean done = false;
    
    Map<Map<String,String>, GeoTimeSerie> labeled = new HashMap<Map<String,String>, GeoTimeSerie>();
    
    //
    long commonts = Long.MAX_VALUE;
    
    if (synchronous) {
      commonts = GTSHelper.tickAtIndex((GeoTimeSerie) labeling.get(0), 0);
    } else {
      for (int i = 0; i < labelingIndices.length; i++) {
        if (GTSHelper.tickAtIndex((GeoTimeSerie) labeling.get(i), 0) < commonts) {
          commonts = GTSHelper.tickAtIndex((GeoTimeSerie) labeling.get(i), 0);
        }
      }
    }

    while (!done) {
      boolean skip = false;
      
      if (synchronous) {        
        // Advance labeling GTS indices so that all timestamps are equal
        for (int i = 0; i < labelingIndices.length; i++) {
          GeoTimeSerie g = (GeoTimeSerie) labeling.get(i);
          int n = GTSHelper.nvalues(g);
          while(labelingIndices[i] < n && GTSHelper.tickAtIndex(g, labelingIndices[i]) < commonts) {
            labelingIndices[i]++;
          }
          if (labelingIndices[i] >= n) {
            done = true;
            skip = true;
            break;
          }
          if (GTSHelper.tickAtIndex(g, labelingIndices[i]) > commonts) {
            commonts = GTSHelper.tickAtIndex(g, labelingIndices[i]);
            skip = true;
            break;
          }
        }
        
        if (skip) {
          continue;
        }            
      } else {
        // Nothing to do for the non synchronous case
      }
      
      
      //
      // All the labeling GTS are aligned (synchronous), we now scan the GTS to label
      //

      // Build the label map
      Map<String,String> labels = new HashMap<String,String>();
      
      for (int i = 0; i < labelingIndices.length; i++) {
        GeoTimeSerie g = (GeoTimeSerie) labeling.get(i);
        if (commonts == GTSHelper.tickAtIndex(g, labelingIndices[i])) {
          String key = g.getName();
          String value = String.valueOf(GTSHelper.valueAtIndex(g, labelingIndices[i]));
          labels.put(key, value);
        }
      }

      for (int i = 0; i < gtsIndices.length; i++) {
        GeoTimeSerie g = (GeoTimeSerie) gts.get(i);
        
        int n = GTSHelper.nvalues(g);


        if (synchronous) {          
          while(gtsIndices[i] < n && GTSHelper.tickAtIndex(g, gtsIndices[i]) < commonts) {
            gtsIndices[i]++;
          }
          if (gtsIndices[i] >= n || GTSHelper.tickAtIndex(g, gtsIndices[i]) > commonts) {
            continue;
          }
          
          // Create the labels for the GTS we are about to populate
          Map<String,String> gtsLabels = new HashMap<String,String>(g.getLabels());
          gtsLabels.putAll(labels);
          gtsLabels.put(null, g.getName());
          GeoTimeSerie target = labeled.get(gtsLabels);
        
          if (null == target) {
            target = new GeoTimeSerie();
            target.setName(g.getName());
            target.setLabels(gtsLabels);
            target.getMetadata().setAttributes(g.getMetadata().getAttributes());
            labeled.put(gtsLabels, target);
          }

          while(gtsIndices[i] < n && commonts == GTSHelper.tickAtIndex(g, gtsIndices[i])) {
            GTSHelper.setValue(target, commonts, GTSHelper.locationAtIndex(g, gtsIndices[i]), GTSHelper.elevationAtIndex(g, gtsIndices[i]), GTSHelper.valueAtIndex(g, gtsIndices[i]), false);          
            gtsIndices[i]++;
          }
        } else {
          // Advance index if the current tick is less than the commonts
          while(gtsIndices[i] < n && GTSHelper.tickAtIndex(g, gtsIndices[i]) < commonts) {
            // Create the labels for the GTS we are about to populate
            // As we are before commonts, only the labels from g are added
            Map<String,String> gtsLabels = new HashMap<String,String>(g.getLabels());
            gtsLabels.put(null, g.getName());
            GeoTimeSerie target = labeled.get(gtsLabels);
          
            if (null == target) {
              target = new GeoTimeSerie();
              target.setName(g.getName());
              target.setLabels(gtsLabels);
              target.getMetadata().setAttributes(g.getMetadata().getAttributes());
              labeled.put(gtsLabels, target);
            }
            GTSHelper.setValue(target, GTSHelper.tickAtIndex(g, gtsIndices[i]), GTSHelper.locationAtIndex(g, gtsIndices[i]), GTSHelper.elevationAtIndex(g, gtsIndices[i]), GTSHelper.valueAtIndex(g, gtsIndices[i]), false);          
            gtsIndices[i]++;
          }
          
          if (gtsIndices[i] >= n || GTSHelper.tickAtIndex(g, gtsIndices[i]) > commonts) {
            continue;
          }

          // Create the labels for the GTS we are about to populate
          Map<String,String> gtsLabels = new HashMap<String,String>(g.getLabels());
          gtsLabels.putAll(labels);
          gtsLabels.put(null, g.getName());
          GeoTimeSerie target = labeled.get(gtsLabels);
        
          if (null == target) {
            target = new GeoTimeSerie();
            target.setName(g.getName());
            target.setLabels(gtsLabels);
            target.getMetadata().setAttributes(g.getMetadata().getAttributes());
            labeled.put(gtsLabels, target);
          }

          while(gtsIndices[i] < n && commonts == GTSHelper.tickAtIndex(g, gtsIndices[i])) {
            GTSHelper.setValue(target, commonts, GTSHelper.locationAtIndex(g, gtsIndices[i]), GTSHelper.elevationAtIndex(g, gtsIndices[i]), GTSHelper.valueAtIndex(g, gtsIndices[i]), false);          
            gtsIndices[i]++;
          }
        }
      }
      
      if (synchronous) {
        // Increase the index of the first labeling GTS
        labelingIndices[0]++;

        if (labelingIndices[0] >= GTSHelper.nvalues((GeoTimeSerie) labeling.get(0))) {
          done = true;
        } else {
          // Update the next commonts
          commonts = GTSHelper.tickAtIndex((GeoTimeSerie) labeling.get(0), labelingIndices[0]);
        }
      } else {
        // Advance the indices for the labeling GTS which are at the lowest timestamp
        
        long newcommonts = Long.MAX_VALUE;

        done = true;
        
        for (int i = 0; i < labelingIndices.length; i++) {
          GeoTimeSerie g = (GeoTimeSerie) labeling.get(i);
          if (labelingIndices[i] < GTSHelper.nvalues(g)) {
            if (commonts == GTSHelper.tickAtIndex(g, labelingIndices[i])) {
              labelingIndices[i]++;
            }
            if (GTSHelper.tickAtIndex(g, labelingIndices[i]) < newcommonts) {
              newcommonts = GTSHelper.tickAtIndex(g, labelingIndices[i]);
            }
            done = false;
          }
        }
        
        if (done) {
          // We consider we are done because we consumed all the ticks from the labeling GTS
          // We will reconsider being done if there are still some ticks to process in some GTS of gts
          for (int i = 0; i < gtsIndices.length; i++) {
            if (gtsIndices[i] < GTSHelper.nvalues((GeoTimeSerie) gts.get(i))) {
              done = false;
              break;
            }
          }
        }
        commonts = newcommonts;
      }
    }

    List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>(labeled.values());
    
    // Remove the null key in the labels (we used it to store the class name)
    for (GeoTimeSerie g: results) {
      g.getMetadata().getLabels().remove(null);
    }
    
    stack.push(results);
    
    return stack;
  }
}
