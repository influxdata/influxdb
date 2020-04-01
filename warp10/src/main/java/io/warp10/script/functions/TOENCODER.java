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
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * Builds an encoder from a list of tick,lat,lon,elev,value or GTSs.
 */
public class TOENCODER extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOENCODER(String name) {
    super(name);
  }  

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list on top of the stack.");
    }
    
    List<Object> elements = (List<Object>) top;
    
    GTSEncoder encoder = new GTSEncoder(0L);
    
    for (Object element: elements) {
      // If we encounter a GTS, a wrap or raw wrap, add all of it to the encoder

      // If wrap, convert to raw wrap
      if (element instanceof String) {
        element = OrderPreservingBase64.decode(element.toString().getBytes(StandardCharsets.US_ASCII));
      }

      // If raw wrap, convert to GTS
      if (element instanceof byte[]) {
        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());

        try {
          GTSWrapper wrapper = new GTSWrapper();

          deser.deserialize(wrapper, (byte[]) element);

          GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
          decoder.next();
          encoder.merge(decoder.getEncoder(true));
        } catch (TException te) {
          throw new WarpScriptException(getName() + " failed to unwrap encoder.", te);
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " cannot get encoder from wrapper.", ioe);
        }

        // Wrap all added to encoder, next.
        continue;
      }

      // If GTS, add it all to the encoder
      if (element instanceof GeoTimeSerie) {
        try {
          encoder.encode((GeoTimeSerie) element);
          // GTS metadata is lost in the process
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " was unable to add Geo Time Series™", ioe);
        }

        // Geo Time Series all added to encoder, next.
        continue;
      }

      // If not a GTS, it should be a list of tick,lat,lon,elev,value.
      if (!(element instanceof List)) {
        throw new WarpScriptException(getName() + " encountered an invalid element.");
      }
      
      List<Object> elt = (List<Object>) element;
      
      if (elt.size() < 2 || elt.size() > 5) {
        throw new WarpScriptException(getName() + " encountered an invalid element.");
      }
      
      Object tick = elt.get(0);
      
      if (!(tick instanceof Long)) {
        throw new WarpScriptException(getName() + " encountered an invalid timestamp.");
      }
      
      Object value = null;
      long location = GeoTimeSerie.NO_LOCATION;
      long elevation = GeoTimeSerie.NO_ELEVATION;
      
      if (2 == elt.size()) { // tick,value
        value = elt.get(1);
      } else if (3 == elt.size()) { // tick,elevation,value
        Object elev = elt.get(1);
        if (elev instanceof Number) {
          if (!Double.isNaN(((Number) elev).doubleValue())) {
            elevation = ((Number) elev).longValue();
          }
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid element.");
        }
        value = elt.get(2);
      } else if (4 == elt.size()) { // tick,lat,lon,value
        Object lat = elt.get(1);
        Object lon = elt.get(2);
        if (lat instanceof Number && lon instanceof Number) {
          if (!Double.isNaN(((Number) lat).doubleValue()) && !Double.isNaN(((Number) lon).doubleValue())) {
            location = GeoXPLib.toGeoXPPoint(((Number) lat).doubleValue(), ((Number) lon).doubleValue());
          }
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid element.");
        }
        value = elt.get(3);
      } else {
        Object lat = elt.get(1);
        Object lon = elt.get(2);
        if (lat instanceof Number && lon instanceof Number) {
          if (!Double.isNaN(((Number) lat).doubleValue()) && !Double.isNaN(((Number) lon).doubleValue())) {
            location = GeoXPLib.toGeoXPPoint(((Number) lat).doubleValue(), ((Number) lon).doubleValue());
          }
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid element.");
        }
        Object elev = elt.get(3);
        if (elev instanceof Number) {
          if (!Double.isNaN(((Number) elev).doubleValue())) {
            elevation = ((Number) elev).longValue();
          }
        } else {
          throw new WarpScriptException(getName() + " encountered an invalid element.");
        }
        value = elt.get(4);
      }
      
      try {
        encoder.addValue((long) tick, location, elevation, value);
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " was unable to add value.", ioe);
      }
    }
    
    stack.push(encoder);

    return stack;
  }  
}
