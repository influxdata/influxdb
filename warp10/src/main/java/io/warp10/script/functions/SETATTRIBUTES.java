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
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Set attributes on GTS, Encoders or list thereof
 * <p>
 * SETATTRIBUTES expects the following parameters on the stack:
 * <p>
 * 1: a map of attributes
 * <p>
 * If a null key exists, then the attributes will be overridden, otherwise
 * they will be modified. If an attribute has an empty or null associated value,
 * the attribute will be removed.
 */
public class SETATTRIBUTES extends ElementOrListStackFunction {

  public SETATTRIBUTES(String name) {
    super(name);
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {

    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map of attributes as parameter.");
    }

    final Map<?, ?> attributesUpdate = (Map<?, ?>) top;

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof GeoTimeSerie) {
          GeoTimeSerie gts = (GeoTimeSerie) element;

          Map<String, String> newAttributes = updateAttribute(gts.getMetadata().getAttributes(), attributesUpdate);

          gts.getMetadata().setAttributes(newAttributes);

          return gts;
        } else if (element instanceof GTSEncoder) {
          GTSEncoder encoder = (GTSEncoder) element;

          Map<String, String> newAttributes = updateAttribute(encoder.getMetadata().getAttributes(), attributesUpdate);

          encoder.getMetadata().setAttributes(newAttributes);

          return encoder;
        } else {
          throw new WarpScriptException(getName() + " expects a GeoTimeSeries, a GTSEncoder or a list thereof under the attribute map.");
        }
      }
    };
  }


  private Map<String, String> updateAttribute(Map<String, String> originalAttributes, Map<?, ?> attributesUpdate) throws WarpScriptException {
    HashMap<String, String> newAttributes = new HashMap<String, String>();

    if (!attributesUpdate.containsKey(null) && originalAttributes.size() > 0) {
      newAttributes.putAll(originalAttributes);
    }

    for (Entry<?, ?> entry: attributesUpdate.entrySet()) {
      if (null == entry.getKey()) {
        continue;
      }
      if (!(entry.getKey() instanceof String) || (null != entry.getValue() && !(entry.getValue() instanceof String))) {
        throw new WarpScriptException(getName() + " attribute key and value MUST be of type String.");
      }
      if (null == entry.getValue() || "".equals(entry.getValue())) {
        newAttributes.remove(entry.getKey());
      } else {
        newAttributes.put((String) entry.getKey(), (String) entry.getValue());
      }
    }

    return newAttributes;
  }
}
