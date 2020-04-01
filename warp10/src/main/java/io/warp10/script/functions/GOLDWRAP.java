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
import java.util.Map;
import java.util.TreeMap;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

public class GOLDWRAP extends ElementOrListStackFunction {
  
  private final ElementStackFunction function;
  
  
  public GOLDWRAP(String name) {
    super(name);
    function = generateFunctionOnce();
  }

  private ElementStackFunction generateFunctionOnce() {
    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        GTSEncoder encoder = null;
        
        boolean sortedEncoder = false;
        
        try {
          if (element instanceof GeoTimeSerie) {
            encoder = new GTSEncoder(0L);
            encoder.setMetadata(((GeoTimeSerie) element).getMetadata());
            GTSHelper.fullsort((GeoTimeSerie) element, false);
            sortedEncoder = true;
            encoder.encodeOptimized((GeoTimeSerie) element);
          } else if (element instanceof GTSEncoder) {
            encoder = (GTSEncoder) element;
          } else if (element instanceof String || element instanceof byte[]) {
            TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
            byte[] bytes;
            
            if (element instanceof String) {
              bytes = OrderPreservingBase64.decode(element.toString().getBytes(StandardCharsets.US_ASCII));
            } else {
              bytes = (byte[]) element;
            }
            GTSWrapper wrapper = new GTSWrapper();
            deser.deserialize(wrapper, bytes);
            encoder = GTSWrapperHelper.fromGTSWrapperToGTSEncoder(wrapper);      
          } else {
            throw new WarpScriptException(getName() + " can only be applied to Geo Time Series™, GTS Encoders or wrapped instances of those types.");
          } 
          
          GTSEncoder enc = null;
          
          if (sortedEncoder) {
            // We had a single GeoTimeSerie instance that we sorted and encoded
            // as an optimized encoder, so take a fastpath!
            enc = encoder;
          } else {
            enc = GTSHelper.fullsort(encoder, false, 0L);
          }
          
          // Save the current metadata
          
          Metadata metadata = enc.getRawMetadata();
          
          // Create an empty Metadata instance which will be populated
          // using class, labels and attributes only
          
          enc.setMetadata(new Metadata());
                    
          try {
            //
            // We need to ensure the metadata (labels/attributes) are in a deterministic order
            //
                      
            if (metadata.getLabelsSize() > 0) {
              Map<String,String> smap = new TreeMap<String,String>();
              smap.putAll(metadata.getLabels());
              enc.getRawMetadata().setLabels(smap);
            }
            
            if (metadata.getAttributesSize() > 0) {
              Map<String,String> smap = new TreeMap<String,String>();
              smap.putAll(metadata.getAttributes());
              enc.getRawMetadata().setAttributes(smap);            
            }

            // Copy the elements which we need
            enc.getRawMetadata().setName(metadata.getName());

            GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(enc, true, 1.0D, Integer.MAX_VALUE);
            TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
            byte[] bytes = ser.serialize(wrapper);
            
            return bytes;            
          } finally {
            // Restore the original metadata
            enc.safeSetMetadata(metadata);
          }                    
        } catch (TException te) {
          throw new WarpScriptException(getName() + " encountered an error while manipulating GTS Wrapper.", te);
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " encountered an error while manipulating Encoder or GTS Wrapper.", ioe);
        }
      }
    };
  }
  
  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {        
    return function;
  }
}
