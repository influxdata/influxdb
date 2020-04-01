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
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Create a GTSEncoder for each instance of GTS, wrapped GTS or raw wrapped GTS.
 */
public class ASENCODERS extends ElementOrListStackFunction {

  private final ElementStackFunction gtstoFunction = new ElementStackFunction() {
    @Override
    public Object applyOnElement(Object element) throws WarpScriptException {
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

          return GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper);
        } catch (TException te) {
          throw new WarpScriptException(getName() + " failed to unwrap encoder.", te);
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " failed to convert wrapper to encoder.", ioe);
        }
      }

      // If GTS, convert to an encoder
      if (element instanceof GeoTimeSerie) {
        try {
          GeoTimeSerie gts = (GeoTimeSerie) element;

          GTSEncoder encoder = new GTSEncoder();
          encoder.encodeOptimized(gts);
          encoder.setMetadata(gts.getMetadata());

          return encoder;
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " cannot convert Encoder to Geo Time Series™.", ioe);
        }
      } else if (element instanceof GTSEncoder) {
        // Nothing to do, return instance.
        return element;
      } else {
        throw new WarpScriptException(getName() + " expects a Geo Time Series™ or a list thereof on top on the stack.");
      }
    }
  };

  public ASENCODERS(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    return gtstoFunction;
  }
}
