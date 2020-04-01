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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

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

/**
 * Decodes an encoder into an array of [ ts lat lon elev value ] arrays.
 */
public class ENCODERTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ENCODERTO(String name) {
    super(name);
  }  

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof String) && !(top instanceof byte[]) && !(top instanceof GTSEncoder)) {
      throw new WarpScriptException(getName() + " operates on a string, byte array or encoder.");
    }
    
    List<Object> elements = new ArrayList<Object>();
    
    GTSDecoder decoder;
    
    if (top instanceof GTSEncoder) {
      decoder = ((GTSEncoder) top).getDecoder(true);
    } else {
      try {
        byte[] bytes = top instanceof String ? OrderPreservingBase64.decode(top.toString().getBytes(StandardCharsets.US_ASCII)) : (byte[]) top;
        
        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
        
        GTSWrapper wrapper = new GTSWrapper();
        
        deser.deserialize(wrapper, bytes);

        decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
        
      } catch (TException te) {
        throw new WarpScriptException(getName() + " failed to unwrap encoder.", te);
      }            
    }

    while(decoder.next()) {
      List<Object> element = new ArrayList<Object>(5);
      element.add(decoder.getTimestamp());
      long location = decoder.getLocation();
      if (GeoTimeSerie.NO_LOCATION == location) {
        element.add(Double.NaN);
        element.add(Double.NaN);
      } else {
        double[] latlon = GeoXPLib.fromGeoXPPoint(location);
        element.add(latlon[0]);
        element.add(latlon[1]);
      }
      long elevation = decoder.getElevation();
      if (GeoTimeSerie.NO_ELEVATION == elevation) {
        element.add(Double.NaN);
      } else {
        element.add(elevation);
      }
      element.add(decoder.getBinaryValue());
      elements.add(element);
    }
    
    stack.push(decoder.getName());
    stack.push(decoder.getLabels());
    stack.push(decoder.getMetadata().getAttributes());
    stack.push(elements);

    return stack;
  }  
}
