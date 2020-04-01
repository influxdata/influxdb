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

import java.util.ArrayList;
import java.util.List;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
import com.geoxp.geo.Coverage;
import com.geoxp.geo.HHCodeHelper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class GEOSHIFT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GEOSHIFT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a resolution, even number between 0 and 30.");
    }
    
    int resolution = ((Long) top).intValue();
    
    if (resolution < 0 || resolution > 30 || 0 != resolution % 2) {
      throw new WarpScriptException(getName() + " expects a resolution, even number between 0 and 30.");      
    }
    
    resolution = resolution >>> 1;
    
    top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a longitude delta between -360.0 and 360.0.");
    }
    
    double deltalon = ((Number) top).doubleValue();
    
    if (deltalon < -360.0D || deltalon > 360.0D) {
      throw new WarpScriptException(getName() + " expects a longitude delta between -360.0 and 360.0.");      
    }

    top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a latitude delta between -180.0 and 180.0.");
    }
    
    double deltalat = ((Number) top).doubleValue();
    
    if (deltalat < -180.0D || deltalat > 180.0D) {
      throw new WarpScriptException(getName() + " expects a latitude delta between -180.0 and 180.0.");      
    }
    
    top = stack.pop();
    
    double meridian = Double.NaN;
    
    if (top instanceof Number) {
      //
      // This is the meridian along which the latitude shift will be performed.
      // Cells on the 'other side of the world' will be shifted in the opposite direction
      //
      
      meridian = (((((Number) top).doubleValue() + 180.0D) % 360.0D) + 360.0D) % 360.0D;
      top = stack.pop();
    }
        
    if (!(top instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " operates on a GEOSHAPE.");
    }
    
    GeoXPShape shape = (GeoXPShape) top;
    
    //
    // Compute the resolution to use if resolution is 0
    // then check that it is compatible with the shape
    //
    
    long[] cells = GeoXPLib.getCells(shape);

    int shaperes = 0;
    for (long cell: cells) {
      if (cell >>> 60 > shaperes) {
        shaperes = (int) (cell >>> 60);
      }
    }

    if (0 == resolution) {
      resolution = shaperes;
    } else {
      // check that the requested resolution is not finer than the finest present in
      // the shape
      if (resolution > shaperes) {
        throw new WarpScriptException(getName() + " cannot use resolution finer than " + (shaperes << 1));
      }
    }

    //
    // Compute the size of a quarter equator
    //
    
    long quarterequator = (1 << (resolution << 1) >> 2) << (32 - (resolution << 1));
    
    //
    // Compute the longitude of 'meridian' at 'resolution'
    //
    
    long longmeridian = Double.isFinite(meridian) ? Math.round(meridian / (360.0D / (1 << (resolution << 1)))) * (1 << (32 - (resolution << 1))) : 0L;
    
    //
    // Now compute the delta in HHCode units
    //
    
    long longdeltalat = Math.round(deltalat / (180.0D / (1 << (resolution << 1)))) * (1 << (32 - (resolution << 1)));
    long longdeltalon = Math.round(deltalon / (360.0D / (1 << (resolution << 1)))) * (1 << (32 - (resolution << 1)));
    
    //
    // Now iterate over the cells, if a cell is of a finer resolution than 'resolution',
    // simply add the deltas and rebuild the HHCode. If a cell is of a coarser resolution,
    // first split the cell down to 'resolution' and iterate over the subcells
    //
    
    Coverage coverage = new Coverage();
    // Enable auto-optimize so not too many cells are created in the Coverage
    coverage.setAutoOptimize(true);

    long latlon[] = new long[2];
    
    // Iterate over the cells, shifting those which are at a resolution of 'resolution' or finer
    // and adding the others to a list we will process differently
    
    List<Long> geocells = new ArrayList<Long>();
    
    for (long cell: cells) {
      int res = (int) (cell >>> 60);
      if (res >= resolution) {
        int reso = res << 1;
        long hhcode = cell << 4;
    
        HHCodeHelper.stableSplitHHCode(hhcode, reso, latlon);
        
        long factor = !Double.isNaN(meridian) && Math.abs(latlon[1] - longmeridian) > quarterequator ? -1L : 1L;
        latlon[0] += factor * longdeltalat;
        latlon[1] += longdeltalon;
        hhcode = HHCodeHelper.buildHHCode(latlon[0], latlon[1], HHCodeHelper.MAX_RESOLUTION);
        coverage.addCell(reso, hhcode);
      } else {
        //
        // The resolution is coarser
        //
        geocells.add(cell);        
      }
    }

    while(!geocells.isEmpty()) {
      long geocell = geocells.remove(geocells.size() - 1);
      
      int res = (int) (geocell >>> 60);
      int reso = res << 1;
      int reso2 = reso << 1;
      long hhcode = geocell << 4;
      
      if (res >= resolution) {    
        HHCodeHelper.stableSplitHHCode(hhcode, reso, latlon);
        long factor = !Double.isNaN(meridian) && Math.abs(latlon[1] - longmeridian) > quarterequator ? -1L : 1L;
        latlon[0] += factor * longdeltalat;
        latlon[1] += longdeltalon;
        hhcode = HHCodeHelper.buildHHCode(latlon[0], latlon[1], HHCodeHelper.MAX_RESOLUTION);
        coverage.addCell(reso, hhcode);        
      } else {
       // Add all 16 subcells
       for (long offset = 0L; offset < 16L; offset++) {
         long cell = ((res + 1L) << 60) | ((hhcode | (offset << (60 - reso2))) >>> 4);
         geocells.add(cell);
       }
      }      
    }

    stack.push(GeoXPLib.fromCells(coverage.toGeoCells(HHCodeHelper.MAX_RESOLUTION), false));
    
    return stack;
  }
}
