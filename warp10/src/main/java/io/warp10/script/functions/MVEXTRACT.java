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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.ElementOrListStackFunction.ElementStackFunction;

public class MVEXTRACT extends ElementOrListStackFunction implements ElementStackFunction {
  
  public static enum ELEMENT {
    TICK,
    LOCATION,
    LATLON,
    ELEVATION,
    VALUE 
  }
  
  private final ELEMENT elementType;
  
  public MVEXTRACT(String name, ELEMENT element) {
    super(name);
    this.elementType = element;
  }
  
  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return this;
  }
  
  @Override
  public Object applyOnElement(Object element) throws WarpScriptException {
    if (!(element instanceof GTSEncoder) && !(element instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " can only be applied on Geo Time Series™ or GTS Encoders.");
    }
    
    Object o = mvextract(element);

    return o;
  }
  
  private List<Object> mvextract(Object element) throws WarpScriptException {
    List<Object> values = new ArrayList<Object>();

    GTSDecoder decoder = null;
    GeoTimeSerie gts = null;
    int nvalues = 0;
    
    if (element instanceof GTSEncoder) {
      decoder = ((GTSEncoder) element).getDecoder();
    } else {
      gts = (GeoTimeSerie) element;
      nvalues = GTSHelper.nvalues(gts);
    }
    
    int idx = -1;
        
    TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
    GTSWrapper wrapper = new GTSWrapper();

    while(true) {
      
      boolean done = !(null == decoder ? ++idx < nvalues : decoder.next());

      if (done) {
        break;
      }
      
      Object value = null;
      
      if (null != decoder) {
        value = decoder.getBinaryValue();
      } else {
        value = GTSHelper.valueAtIndex(gts, idx);
      }
      
      if (value instanceof Number || value instanceof Boolean) {
        values.add(elt(decoder, gts, idx, value));
      } else if (value instanceof byte[]) {
        try {
          wrapper.clear();
          deser.deserialize(wrapper, (byte[]) value);
          if (ELEMENT.VALUE == this.elementType) {
            values.add(mvextract(GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper)));
          } else {
            List<Object> elt = new ArrayList<Object>();
            elt.add(elt(decoder, gts, idx, value));
            elt.add(mvextract(GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper)));
            values.add(elt);
          }
        } catch (IOException e) {
          throw new WarpScriptException("Error decoding.");
        } catch (TException te) {
          values.add(elt(decoder, gts, idx, value));
        }
      } else if (value instanceof String) {
        if (null != decoder) {
          // We are getting values from a decoder, so a STRING is not a binary value
          values.add(elt(decoder, gts, idx, value));
        } else {
          // Attempt to decode a Wrapper
          try {
            byte[] bytes = value.toString().getBytes(StandardCharsets.ISO_8859_1);
            wrapper.clear();
            deser.deserialize(wrapper, bytes);
            if (ELEMENT.VALUE == this.elementType) {
              values.add(mvextract(GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper)));
            } else {
              List<Object> elt = new ArrayList<Object>();
              elt.add(elt(decoder, gts, idx, value));
              elt.add(mvextract(GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper)));
              values.add(elt);
            }
          } catch (IOException e) {
            throw new WarpScriptException("Error decoding.");
          } catch (TException te) {
            values.add(elt(decoder, gts, idx, value));
          }
        }
      }
    }
    
    return values;
  }
  
  private Object elt(GTSDecoder decoder, GeoTimeSerie gts, int idx, Object value) {
    switch (this.elementType) {
      case VALUE:
        if (value instanceof BigDecimal) {
          value = ((BigDecimal) value).doubleValue();
        }
        return value;
      case TICK:
        return null != decoder ? decoder.getTimestamp() : GTSHelper.tickAtIndex(gts, idx);
      case LATLON:
        long location = null != decoder ? decoder.getLocation() : GTSHelper.locationAtIndex(gts, idx);
        double[] latlon;
        
        if (GeoTimeSerie.NO_LOCATION == location) {
          latlon = new double[2];
          latlon[0] = Double.NaN;
          latlon[1] = Double.NaN;
        } else {
          latlon = GeoXPLib.fromGeoXPPoint(location);
        }        
        return Arrays.asList(latlon[0], latlon[1]);
      case ELEVATION:
        return null != decoder ? decoder.getElevation() : GTSHelper.elevationAtIndex(gts, idx);
      case LOCATION:
        return null != decoder ? decoder.getLocation() : GTSHelper.locationAtIndex(gts, idx);
      default:
        throw new RuntimeException("Invalid element type.");
    }    
  }
}
