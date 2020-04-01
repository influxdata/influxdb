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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.List;

import com.geoxp.GeoXPLib;
import com.geoxp.geo.GeoHashHelper;
import com.geoxp.oss.jarjar.org.apache.commons.codec.binary.Hex;
import com.google.common.primitives.Longs;

/**
 * Convert a GeoHash to lat/lon
 */
public class GEOHASHTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public GEOHASHTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (top instanceof List) {
      List<Object> l = (List<Object>) top;
      
      List<String> geohashes = new ArrayList<String>(l.size());
      
      for (Object geohash: l) {
        if (!(geohash instanceof String)) {
          throw new WarpScriptException(getName() + " expects a GeoHash to be a string.");          
        }
        
        geohashes.add(geohash.toString().toLowerCase());
      }

      long[] geocells = GeoHashHelper.toGeoCells(geohashes);
      
      stack.push(GeoXPLib.fromCells(geocells, false));
    } else {      
      Boolean toHHCodeString = null;
      
      if (top instanceof Boolean) {
        toHHCodeString = Boolean.TRUE.equals(top);
        top = stack.pop();
      }
      
      if (!(top instanceof String)) {
        throw new WarpScriptException(getName() + " expects a GeoHash to be a string.");
      }
      
      String geohash = top.toString().toLowerCase();
      
      long geoxppoint = GeoHashHelper.toHHCode(geohash);
      
      if (null != toHHCodeString) {
        if (Boolean.TRUE.equals(toHHCodeString)) {
          stack.push(Hex.encodeHexString(Longs.toByteArray(geoxppoint)));
        } else {
          stack.push(geoxppoint);
        }
      } else {
        double[] latlon = GeoXPLib.fromGeoXPPoint(geoxppoint);
      
        stack.push(latlon[0]);
        stack.push(latlon[1]);
      }
    }
    
    return stack;
  }
}
