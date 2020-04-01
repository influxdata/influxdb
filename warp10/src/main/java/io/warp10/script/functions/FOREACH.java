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

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLoopBreakException;
import io.warp10.script.WarpScriptLoopContinueException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.geoxp.GeoXPLib;

/**
 * Implements a 'foreach' loop on a list or map
 * 
 * 2: LIST or MAP
 * 1: RUN-macro
 * FOREACH
 * 
 */
public class FOREACH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public FOREACH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    boolean pushIndex = false;
    if (top instanceof Boolean) {
      pushIndex = (Boolean) top;
      top = stack.pop();
    }

    Object macro = top;// RUN-macro

    Object obj = stack.pop(); // LIST or MAP
    
    if (!(macro instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }
    
    if (!(obj instanceof List) && !(obj instanceof Map) && !(obj instanceof Iterator) && !(obj instanceof Iterable) && !(obj instanceof GeoTimeSerie) && !(obj instanceof GTSEncoder) && !(obj instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a list, map, Geo Time Series™, ENCODER, STRING, iterator or iterable.");
    }

    long index = 0;
    
    if (obj instanceof String) {
      final String s = (String) obj;
      
      obj = new Iterator<String>() {        
        int idx = 0;
        
        @Override
        public boolean hasNext() {
          return idx < s.length();
        }
        @Override
        public String next() {
          return Character.toString(s.charAt(idx++));
        }
      };
    }
   
    if (obj instanceof List) {
      for (Object o: ((List<Object>) obj)) {
        stack.push(o);
        if (pushIndex) {
          stack.push(index++);
        }
        //
        // Execute RUN-macro
        //        
        try {
          stack.exec((Macro) macro);
        } catch (WarpScriptLoopBreakException elbe) {
          break;
        } catch (WarpScriptLoopContinueException elbe) {
          // Do nothing!
        }
      }
    } else if (obj instanceof Map) {
      for (Entry<Object,Object> entry: ((Map<Object,Object>) obj).entrySet()) {
        stack.push(entry.getKey());
        stack.push(entry.getValue());
        if (pushIndex) {
          stack.push(index++);
        }
        try {
          stack.exec((Macro) macro);
        } catch (WarpScriptLoopBreakException elbe) {
          break;
        } catch (WarpScriptLoopContinueException elbe) {
          // Do nothing!
        }
      }
    } else if (obj instanceof Iterator || obj instanceof Iterable) {
      Iterator<Object> iter = obj instanceof Iterator ? (Iterator<Object>) obj : ((Iterable<Object>) obj).iterator();
      while(iter.hasNext()) {
        Object o = iter.next();
        stack.push(o);
        if (pushIndex) {
          stack.push(index++);
        }
        try {
          stack.exec((Macro) macro);
        } catch (WarpScriptLoopBreakException elbe) {
          break;
        } catch (WarpScriptLoopContinueException elbe) {
          // Do nothing!
        }
      }
    } else if (obj instanceof GeoTimeSerie) {
      GeoTimeSerie gts = (GeoTimeSerie) obj;
      for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
        List<Object> elt = new ArrayList<Object>(5);
        elt.add(GTSHelper.tickAtIndex(gts, i));
        long location = GTSHelper.locationAtIndex(gts, i);
        if (GeoTimeSerie.NO_LOCATION == location) {
          elt.add(Double.NaN);
          elt.add(Double.NaN);
        } else {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          elt.add(latlon[0]);
          elt.add(latlon[1]);
        }
        long elevation = GTSHelper.elevationAtIndex(gts, i);
        if (GeoTimeSerie.NO_ELEVATION == elevation) {
          elt.add(Double.NaN);
        } else {
          elt.add(elevation);
        }
        elt.add(GTSHelper.valueAtIndex(gts, i));
        stack.push(elt);
        if (pushIndex) {
          stack.push(index++);
        }
        try {
          stack.exec((Macro) macro);
        } catch (WarpScriptLoopBreakException elbe) {
          break;
        } catch (WarpScriptLoopContinueException elbe) {
          // Do nothing!
        }        
      }
    } else if (obj instanceof GTSEncoder) {
      GTSDecoder decoder = ((GTSEncoder) obj).getDecoder();
      while(decoder.next()) {
        List<Object> elt = new ArrayList<Object>(5);
        elt.add(decoder.getTimestamp());
        long location = decoder.getLocation();
        if (GeoTimeSerie.NO_LOCATION == location) {
          elt.add(Double.NaN);
          elt.add(Double.NaN);
        } else {
          double[] latlon = GeoXPLib.fromGeoXPPoint(location);
          elt.add(latlon[0]);
          elt.add(latlon[1]);
        }
        long elevation = decoder.getElevation();
        if (GeoTimeSerie.NO_ELEVATION == elevation) {
          elt.add(Double.NaN);
        } else {
          elt.add(elevation);
        }
        elt.add(decoder.getBinaryValue());
        stack.push(elt);
        if (pushIndex) {
          stack.push(index++);
        }
        try {
          stack.exec((Macro) macro);
        } catch (WarpScriptLoopBreakException elbe) {
          break;
        } catch (WarpScriptLoopContinueException elbe) {
          // Do nothing!
        }        
      }      
    }

    return stack;
  }
}
