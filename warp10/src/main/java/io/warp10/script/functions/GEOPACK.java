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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;
/**
 * Pack a GeoXPShape
 * 
 * We relay on GTSWrappers for this, this is kinda weird but hey, it works!
 * 
 */
public class GEOPACK extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GEOPACK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof GeoXPShape)) {
      throw new WarpScriptException(getName() + " expects a shape on top of the stack.");
    }
    
    GeoXPShape shape = (GeoXPShape) o;
    
    stack.push(pack(shape));
    
    return stack;
  }
  
  public static String pack(GeoXPShape shape) throws WarpScriptException {
    long[] cells = GeoXPLib.getCells(shape);
    
    GTSEncoder encoder = new GTSEncoder();
    
    try {
      for (long cell: cells) {
        encoder.addValue(cell, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, true);
      }      
    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    }
    
    GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true);
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    try {
      byte[] serialized = serializer.serialize(wrapper);
      
      return new String(OrderPreservingBase64.encode(serialized, 0, serialized.length), StandardCharsets.US_ASCII);
    } catch (TException te) {
      throw new WarpScriptException(te);
    }
  }
}
