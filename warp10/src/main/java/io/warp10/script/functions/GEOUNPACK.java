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
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.geoxp.GeoXPLib;
import com.geoxp.GeoXPLib.GeoXPShape;

import java.nio.charset.StandardCharsets;

/**
 * Unpack a GeoXPShape
 * 
 * We relay on GTSWrappers for this, this is kinda weird but hey, it works!
 * 
 */
public class GEOUNPACK extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public GEOUNPACK(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    byte[] serialized;
    
    if (o instanceof String) {
      serialized = OrderPreservingBase64.decode(o.toString().getBytes(StandardCharsets.US_ASCII));
    } else if (o instanceof byte[]) {
      serialized = (byte[]) o;
    } else {
      throw new WarpScriptException(getName() + " expects a packed shape on top of the stack.");      
    }
    
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    GTSWrapper wrapper = new GTSWrapper();
    
    try {
      deserializer.deserialize(wrapper, serialized);
    } catch (TException te) {
      throw new WarpScriptException(te);
    }
    
    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
    
    long[] cells = new long[(int) wrapper.getCount()];
    
    int idx = 0;
    
    while(idx < cells.length && decoder.next()) {
      long cell = decoder.getTimestamp();
      // We do not call getBinaryValue because we expect booleans anyway
      Object value = decoder.getValue();
      
      if (!Boolean.TRUE.equals(value)) {
        throw new WarpScriptException(getName() + " invalid GeoXPShape.");        
      }
      
      cells[idx++] = cell;
    }
    
    if (idx != cells.length) {
      throw new WarpScriptException(getName() + " invalid GeoXPShape.");
    }
    
    GeoXPShape shape = GeoXPLib.fromCells(cells, false);
    
    stack.push(shape);
    
    return stack;
  }
}
