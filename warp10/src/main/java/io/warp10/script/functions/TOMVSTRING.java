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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.geoxp.GeoXPLib;
import io.warp10.WarpURLEncoder;
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

public class TOMVSTRING extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public TOMVSTRING(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof String) && !(top instanceof byte[]) && !(top instanceof GTSEncoder) && !(top instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " operates on a Geo Time Series™, ENCODER, STRING or byte array.");
    }
    
    if (top instanceof GTSEncoder) {
      GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper((GTSEncoder) top, false);
      stack.push(wrapperToString(null, wrapper).toString());
    } else if (top instanceof GeoTimeSerie) {
      GTSWrapper wrapper = GTSWrapperHelper.fromGTSToGTSWrapper((GeoTimeSerie) top, false);
      stack.push(wrapperToString(null, wrapper).toString());
    } else {
      byte[] bytes = (top instanceof byte[]) ? (byte[]) top: OrderPreservingBase64.decode(top.toString().getBytes(StandardCharsets.US_ASCII));

      StringBuilder sb = bytesToString(null, bytes);
      
      stack.push(sb.toString());
    }    
    
    return stack;
  }

  private static StringBuilder wrapperToString(StringBuilder sb, GTSWrapper wrapper) {
    if (null == sb) {
      sb = new StringBuilder();
    }

    if (wrapper.isCompressed()) {
      sb.append("[ ");
    } else {
      sb.append("[! ");
    }
    
    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
    
    while(decoder.next()) {
      long ts = decoder.getTimestamp();
      long location = decoder.getLocation();
      long elevation = decoder.getElevation();
      Object value = decoder.getBinaryValue();

      double[] latlon = null;
      
      if (GeoTimeSerie.NO_LOCATION != location) {
        latlon = GeoXPLib.fromGeoXPPoint(location);
      }

      // If the timestamp is not 0 or there is a location or an elevation, we need to
      // emit the timestamp
      if (ts != 0 || null != latlon || GeoTimeSerie.NO_ELEVATION != elevation) {
        sb.append(ts);
        sb.append("/");
      }
      
      if (null != latlon) {
        sb.append(latlon[0]);
        sb.append(":");
        sb.append(latlon[1]);
        sb.append("/");
      } else if (GeoTimeSerie.NO_ELEVATION != elevation) {
        sb.append("/");          
      }

      if (GeoTimeSerie.NO_ELEVATION != elevation) {
        sb.append(elevation);
        sb.append("/");
      }
      
      if (value instanceof byte[]) {
        bytesToString(sb, (byte[]) value);
      } else if (value instanceof String) {
        sb.append("'");
        try {
          sb.append(WarpURLEncoder.encode(value.toString(), StandardCharsets.UTF_8));
        } catch (UnsupportedEncodingException uee) {        
        }
        sb.append("'");
      } else if (value instanceof Boolean) {
        if (Boolean.TRUE.equals(value)) {
          sb.append("T");
        } else {
          sb.append("F");
        }
      } else { // Numbers
        sb.append(value.toString());
      }
      sb.append(" ");
    }

    sb.append("]");

    return sb;
  }
  
  private static StringBuilder bytesToString(StringBuilder sb, byte[] bytes) {
    TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
    GTSWrapper wrapper = new GTSWrapper();

    try {
      deser.deserialize(wrapper, bytes);

      return wrapperToString(sb, wrapper);
    } catch (TException te) {
      if (null == sb) {
        sb = new StringBuilder();
      }
      sb.append("b64:");
      sb.append(Base64.encodeBase64URLSafeString(bytes));
      return sb;
    }    
  }
}
