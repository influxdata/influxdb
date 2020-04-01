//
//   Copyright 2020  SenX S.A.S.
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
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.MetadataSelectorMatcher;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Converts an encoder into a map of gts, one per type,
 * or converts list of encoders in list of GTS.
 */
public class TOGTS extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String TYPE_LABEL_NAME_PARAMETER = "label.type";
  private static final String DEFAULT_TYPE_LABEL_NAME = ".type";

  public TOGTS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    Map<String, ArrayList<MetadataSelectorMatcher>> typeMap = null;
    Object top = stack.pop();
    // this is the default label name.
    String extraLabel = DEFAULT_TYPE_LABEL_NAME;

    if (top instanceof Map) {
      typeMap = new LinkedHashMap<String, ArrayList<MetadataSelectorMatcher>>();
      // this is a map to specify type by selector. MetadataSelectorMatcher are build once here.
      for (Map.Entry<Object, Object> entry: ((Map<Object, Object>) top).entrySet()) {
        if (entry.getKey() instanceof String) {
          String t = (String) entry.getKey();
          if ("LONG".equals(t) || "DOUBLE".equals(t) || "BOOLEAN".equals(t) || "STRING".equals(t) || "BINARY".equals(t)) {
            if (entry.getValue() instanceof String) {
              // MAP with type->selector
              ArrayList<MetadataSelectorMatcher> l = new ArrayList<MetadataSelectorMatcher>();
              l.add(new MetadataSelectorMatcher((String) entry.getValue()));
              typeMap.put((String) entry.getKey(), l);
            } else if (entry.getValue() instanceof List) {
              // MAP with type->listOfSelectors
              ArrayList<MetadataSelectorMatcher> l = new ArrayList<MetadataSelectorMatcher>();
              for (Object sel: (List) entry.getValue()) {
                if (sel instanceof String) {
                  l.add(new MetadataSelectorMatcher((String) sel));
                } else {
                  throw new WarpScriptException(getName() + " type MAP input must contains selector or list of thereof for each type.");
                }
              }
              typeMap.put((String) entry.getKey(), l);
            } else {
              throw new WarpScriptException(getName() + " type MAP input must contains selector or list of thereof for each type.");
            }
          } else if (TYPE_LABEL_NAME_PARAMETER.equals(t)) {
            if (null == entry.getValue()) {
              extraLabel = null;
            } else if (entry.getValue() instanceof String) {
              extraLabel = (String) entry.getValue();
            } else {
              throw new WarpScriptException(getName() + " extra 'label' input in the MAP must be either null or a string.");
            }
          } else {
            throw new WarpScriptException(getName() + " type MAP input must contains valid types as key (LONG, DOUBLE, BOOLEAN, STRING or BINARY) or '" + TYPE_LABEL_NAME_PARAMETER + "' to override '" + DEFAULT_TYPE_LABEL_NAME + "' label.");
          }
        } else {
          throw new WarpScriptException(getName() + " type MAP input must contains valid types as key (LONG, DOUBLE, BOOLEAN, STRING or BINARY) or '" + TYPE_LABEL_NAME_PARAMETER + "' to override '" + DEFAULT_TYPE_LABEL_NAME + "' label.");
        }
      }
      top = stack.pop();
    }

    List<GTSDecoder> decodersInput = new ArrayList<GTSDecoder>();

    boolean listInput = false;

    if (top instanceof List) {
      for (Object o: (List) top) {
        if (!(o instanceof String) && !(o instanceof byte[]) && !(o instanceof GTSEncoder)) {
          throw new WarpScriptException(getName() + " operates on a string, a byte array, an encoder, or list thereof.");
        }
        decodersInput.add(getDecoderFromObject(o));
      }
      listInput = true;
    } else if (top instanceof String || top instanceof byte[] || top instanceof GTSEncoder) {
      decodersInput.add(getDecoderFromObject(top));
    } else {
      throw new WarpScriptException(getName() + " operates on a string, a byte array, an encoder, or list thereof.");
    }

    // if there is no type map, the output is a map of GTS (v2.3.0 signature), or a map of lists of GTS
    if (null == typeMap) {
      Map<String, ArrayList<GeoTimeSerie>> result = new HashMap<String, ArrayList<GeoTimeSerie>>();
      for (GTSDecoder decoder: decodersInput) {
        Map<String, GeoTimeSerie> series = new HashMap<String, GeoTimeSerie>();
        GeoTimeSerie gts;
        while (decoder.next()) {
          Object value = decoder.getBinaryValue();

          String type = "DOUBLE";

          if (value instanceof String) {
            type = "STRING";
          } else if (value instanceof Boolean) {
            type = "BOOLEAN";
          } else if (value instanceof Long) {
            type = "LONG";
          } else if (value instanceof Double || value instanceof BigDecimal) {
            type = "DOUBLE";
          } else if (value instanceof byte[]) {
            type = "BINARY";
          }

          gts = series.get(type);

          if (null == gts) {
            gts = new GeoTimeSerie();
            gts.setMetadata(decoder.getMetadata());
            series.put(type, gts);
          }

          GTSHelper.setValue(gts, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), value, false);
        }
        // exit here if input is not a list.
        if (!listInput) {
          stack.push(series);
          return stack;
        }
        // merge the series Map into the big one
        for (Entry<String, GeoTimeSerie> entry: series.entrySet()) {
          if (!result.containsKey(entry.getKey())) {
            result.put(entry.getKey(), new ArrayList<GeoTimeSerie>());
          }
          result.get(entry.getKey()).add(entry.getValue());
        }
      }
      stack.push(result);
      return stack;

    } else {
      // if there is a type map on the stack:
      //  - map contains types as key, and selectors or list of selectors as value for each type
      //  - if the encoder doesn't fit to any selector, the gts will have the type of the first encountered element in the encoder.
      // GTSHelper.setValue will try to convert values whenever possible, and
      // a byte array will be serialized as an ISO-8859-1 string by GTSHelper.setValue
      ArrayList<GeoTimeSerie> result = new ArrayList<GeoTimeSerie>();

      for (GTSDecoder decoder: decodersInput) {
        GeoTimeSerie gts = new GeoTimeSerie((int) Math.min(Integer.MAX_VALUE, decoder.getCount()));
        gts.setMetadata(decoder.getMetadata());
        String enforcedType = null;
        boolean mustGuessTypeFromFirstValue = true;
        for (Entry<String, ArrayList<MetadataSelectorMatcher>> entry: typeMap.entrySet()) {
          for (MetadataSelectorMatcher m: entry.getValue()) {
            if (m.matches(decoder.getMetadata())) {
              enforcedType = entry.getKey();
              mustGuessTypeFromFirstValue = false;
            }
            if (null != enforcedType) {
              break;
            }
          }
          if (null != enforcedType) {
            break;
          }
        }
        if (!mustGuessTypeFromFirstValue) {
          if ("DOUBLE".equals(enforcedType)) {
            gts.setType(GeoTimeSerie.TYPE.DOUBLE);
          } else if ("LONG".equals(enforcedType)) {
            gts.setType(GeoTimeSerie.TYPE.LONG);
          } else if ("STRING".equals(enforcedType) || "BINARY".equals(enforcedType)) {
            gts.setType(GeoTimeSerie.TYPE.STRING);
          } else if ("BOOLEAN".equals(enforcedType)) {
            gts.setType(GeoTimeSerie.TYPE.BOOLEAN);
          }
        }
        while (decoder.next()) {
          Object value = decoder.getBinaryValue();
          GTSHelper.setValue(gts, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), value, false);
          // Test here the value type, because GTS do not handle BINARY type.
          if (mustGuessTypeFromFirstValue) {
            if (value instanceof String) {
              enforcedType = "STRING";
            } else if (value instanceof Boolean) {
              enforcedType = "BOOLEAN";
            } else if (value instanceof Long) {
              enforcedType = "LONG";
            } else if (value instanceof Double || value instanceof BigDecimal) {
              enforcedType = "DOUBLE";
            } else if (value instanceof byte[]) {
              enforcedType = "BINARY";
            }
            mustGuessTypeFromFirstValue = false;
          }
        }
        // also set an extra label with the enforced type, or UNDEFINED if there is no type enforcement match AND the encoder was empty.
        if (null != extraLabel) {
          if (null != gts.getLabel(extraLabel)) {
            // the label is already present in the input, throw an error
            throw new WarpScriptException(getName() + " the input already has label " + extraLabel);
          }
          if (null == enforcedType) {
            gts.setLabel(extraLabel, "UNDEFINED");
          } else {
            gts.setLabel(extraLabel, enforcedType);
          }
        }
        // exit here if input is not a list.
        if (!listInput) {
          stack.push(gts);
          return stack;
        }
        result.add(gts);
      }
      stack.push(result);
    }
    return stack;
  }

  /**
   * try to decode an encoder from its opb64 string representation or its byte array representation.
   *
   * @param o string, encoder, or byte array
   * @return a GTSDecoder object
   * @throws WarpScriptException
   */
  private GTSDecoder getDecoderFromObject(Object o) throws WarpScriptException {
    GTSDecoder decoder;
    if (o instanceof GTSEncoder) {
      decoder = ((GTSEncoder) o).getUnsafeDecoder(false);
    } else {
      try {
        byte[] bytes = o instanceof String ? OrderPreservingBase64.decode(o.toString().getBytes(StandardCharsets.US_ASCII)) : (byte[]) o;
        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
        GTSWrapper wrapper = new GTSWrapper();
        deser.deserialize(wrapper, bytes);
        decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
      } catch (TException te) {
        throw new WarpScriptException(getName() + " failed to unwrap encoder.", te);
      }
    }
    return decoder;
  }

}
