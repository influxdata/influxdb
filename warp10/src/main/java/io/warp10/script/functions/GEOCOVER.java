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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.geoxp.geo.HHCodeHelper;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class GEOCOVER extends ElementOrListStackFunction {
  
  private final boolean rhumblines;
  
  public GEOCOVER(String name, boolean rhumblines) {
    super(name);
    this.rhumblines = rhumblines;
  }
  
  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a resolution (even number from 2 to 30).");
    }
    
    final long resolution = ((Long) top).longValue();
    
    if (resolution < 2 || resolution > 30 || 1 == resolution % 2) {
      throw new WarpScriptException(getName() + " expects a resolution (even number from 2 to 30).");
    }
    
    final long mask = (0xFFFFFFFFFFFFFFFFL >>> (64 - (resolution * 2))) << (64 - (resolution * 2));
    final long prefix = (resolution >>> 1) << 60;
    
    return new ElementStackFunction() {      
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        
        Set<Long> cells = new HashSet<Long>();

        // If computing coverage for the rhumbline
        if (rhumblines && element instanceof GTSEncoder) {
          element = ((GTSEncoder) element).getDecoder().decode();
        }
        
        if (element instanceof GeoTimeSerie) {
          GeoTimeSerie gts = (GeoTimeSerie) element;
          
          if (rhumblines) {
            GTSHelper.sort(gts);
            List<Long> vertices = new ArrayList<Long>();
            for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
              long location = GTSHelper.locationAtIndex(gts, i);
              if (GeoTimeSerie.NO_LOCATION != location) {
                vertices.add(location);
              }
            }            

            for (long cell: HHCodeHelper.coverPolyline(vertices, (int) resolution, false).toGeoCells((int) resolution)) {
              cells.add(cell);
            }
          } else {
            for (int i = 0; i < GTSHelper.nvalues(gts); i++) {
              long location = GTSHelper.locationAtIndex(gts, i);
              if (GeoTimeSerie.NO_LOCATION != location) {
                long cell = ((location & mask) >>> 4) | prefix;
                cells.add(cell);
              }
            }            
          }
        } else if (element instanceof GTSEncoder) {          
          if (rhumblines) {
            // Can't happen
          } else {
            GTSDecoder decoder = ((GTSEncoder) element).getDecoder();

            while (decoder.next()) {
              long location = decoder.getLocation();
              if (GeoTimeSerie.NO_LOCATION != location) {
                long cell = ((location & mask) >>> 4) | prefix;
                cells.add(cell);
              }
            }            
          }
        } else {
          throw new WarpScriptException(getName() + " can only operate on Geo Time Series™ or GTS Encoders.");
        }
        
        GeoXPShape shape = GeoXPLib.fromCells(cells);
        
        return shape;
      }
    };   
  }
}
