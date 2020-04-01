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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.nio.charset.StandardCharsets;

/**
 * Wrap GTS or Encoders into GTSWrappers
 */
public class WRAP extends ElementOrListStackFunction {

  private final boolean opt;
  private final boolean compress;
  private final boolean raw;
  private final boolean mv;
  
  private final ElementStackFunction function;

  public WRAP(String name) {
    this(name, false, true, false, false);
  }

  public WRAP(String name, boolean opt) {
    this(name, opt, true, false, false);
  }

  public WRAP(String name, boolean opt, boolean compress) {
    this(name, opt, compress, false, false);
  }
  
  public WRAP(String name, boolean opt, boolean compress, boolean raw) {
    this(name, opt, compress, raw, false);
  }

  public WRAP(String name, boolean opt, boolean compress, boolean raw, boolean mv) {
    super(name);
    this.opt = opt;
    this.compress = compress;
    this.raw = raw;
    this.mv = mv;
    
    if (this.opt && !this.compress) {
      throw new RuntimeException("Invalid combination of opt and compress.");
    }

    function = generateFunctionOnce();    
  }

  private ElementStackFunction generateFunctionOnce() {
    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        GTSWrapper wrapper;
        if (element instanceof GeoTimeSerie) {
          if (opt) {
            wrapper = GTSWrapperHelper.fromGTSToGTSWrapper((GeoTimeSerie) element, compress, 1.0, true);
          } else {
            wrapper = GTSWrapperHelper.fromGTSToGTSWrapper((GeoTimeSerie) element, compress);
          }
        } else if (element instanceof GTSEncoder) {
          if (opt) {
            wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper((GTSEncoder) element, compress, 1.0);
          } else {
            wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper((GTSEncoder) element, compress);
          }
        } else {
          throw new WarpScriptException(getName() + " expects a Geo Time Series™ of a GTSEncoder or a list on top of the stack");
        }

        if (mv) {
          // Remove metadata and count
          wrapper.unsetMetadata();
          wrapper.unsetCount();
        }
        
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

        try {
          byte[] bytes = serializer.serialize(wrapper);

          if (raw) {
            return bytes;
          } else {
            return new String(OrderPreservingBase64.encode(bytes), StandardCharsets.US_ASCII);
          }
        } catch (TException te) {
          throw new WarpScriptException(getName() + " failed to wrap GTS.", te);
        }
      }
    };
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return function;
  }

}
