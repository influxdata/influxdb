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

import java.io.IOException;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import com.geoxp.GeoXPLib;

/**
 * Add a value to a GTS. Expects 6 parameters on the stack
 * 
 * GTS
 * tick
 * latitude (or NaN)
 * longitude (or NaN)
 * elevation (or NaN)
 * TOP: value
 * 
 */
public class ADDVALUE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean overwrite;
  
  public ADDVALUE(String name, boolean overwrite) {
    super(name);
    this.overwrite = overwrite;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Object value = null;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    long location = GeoTimeSerie.NO_LOCATION;
    long timestamp = 0L;

    Object o = stack.pop();
    
    if (o instanceof List) {
      Object[] array = MACROMAPPER.listToObjects((List) o);
      
      timestamp = (long) array[0];
      location = (long) array[1];
      elevation = (long) array[2];
      value = array[3];
    } else {
      if (!(o instanceof Number)
          && !(o instanceof String)
          && !(o instanceof Boolean)
          && !(o instanceof byte[])
          && !(o instanceof GeoTimeSerie)
          && !(o instanceof GTSEncoder)) {
        throw new WarpScriptException(getName() + " expects a LONG, DOUBLE, STRING, byte array or BOOLEAN value or a Geo Time Series™ or ENCODER.");
      }
      
      value = o;
      
      o = stack.pop();
      
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " expects the elevation to be numeric or NaN.");
      }
           
      if (!(o instanceof Double && Double.isNaN((double) o))) {
        elevation = ((Number) o).longValue();
      }

      o = stack.pop();
      
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " expects the longitude to be numeric or NaN.");
      }
      
      double longitude = o instanceof Double ? (double) o : ((Number) o).doubleValue();

      o = stack.pop();
      
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " expects the latitude to be numeric or NaN.");
      }
      
      double latitude = o instanceof Double ? (double) o : ((Number) o).doubleValue();
      
      if (!Double.isNaN(latitude) && !Double.isNaN(longitude)) {
        location = GeoXPLib.toGeoXPPoint(latitude, longitude);
      }
      
      o = stack.pop();
      
      if (!(o instanceof Number)) {
        throw new WarpScriptException(getName() + " expects the tick to be numeric.");
      }
      
      timestamp = ((Number) o).longValue();      
    }
    
    o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie) && !(o instanceof GTSEncoder)) {
      throw new WarpScriptException(getName() + " operates on a single Geo Time Series or GTS Encoder.");
    }
    
    //
    // Convert GTS and GTSEncoder values to Wrappers
    // 
    
    if (value instanceof GeoTimeSerie) {
      TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
      try {
        value = ser.serialize(GTSWrapperHelper.fromGTSToGTSWrapper((GeoTimeSerie) value, true, GTSWrapperHelper.DEFAULT_COMP_RATIO_THRESHOLD, Integer.MAX_VALUE, false, false));
      } catch (TException te) {
        throw new WarpScriptException(getName() + " encountered an error while serializing the Geo Time Series™.");
      }
    } else if (value instanceof GTSEncoder) {
      TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
      try {
        value = ser.serialize(GTSWrapperHelper.fromGTSEncoderToGTSWrapper((GTSEncoder) value, true, GTSWrapperHelper.DEFAULT_COMP_RATIO_THRESHOLD, Integer.MAX_VALUE, false));
      } catch (TException te) {
        throw new WarpScriptException(getName() + " encountered an error while serializing the GTS Encoder.");
      }
    }
    
    if (o instanceof GeoTimeSerie) {
      GeoTimeSerie gts = (GeoTimeSerie) o;
    
      GTSHelper.setValue(gts, timestamp, location, elevation, value, this.overwrite);
    
      stack.push(gts);
    } else {
      GTSEncoder encoder = (GTSEncoder) o;
      
      try {
        encoder.addValue(timestamp, location, elevation, value);
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " error adding datapoint to encoder.", ioe);
      }
      
      stack.push(encoder);
    }
    
    return stack;
  }
}
