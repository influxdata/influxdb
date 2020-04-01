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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.MetadataTextComparator;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Sort a list of GTS according to their Metadata
 */
public class METASORT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public static final Comparator<GeoTimeSerie> META_COMPARATOR = new Comparator<GeoTimeSerie>() {
    
    private final Comparator<Metadata> INNER_COMP = new MetadataTextComparator(null);
    
    @Override
    public int compare(GeoTimeSerie o1, GeoTimeSerie o2) {
      return INNER_COMP.compare(o1.getMetadata(), o2.getMetadata());
    }
  };
  
  public METASORT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of fields to consider for sorting on top of the stack.");
    }

    final List<String> fields = new ArrayList<String>();

    for (Object o: ((List) top)) {
      if (null != o && !(o instanceof String)) {
        throw new WarpScriptException(getName() + " expects a list of fields (strings).");
      }
      if (null == o) {
        fields.add(null);
      } else {
        fields.add(o.toString());
      }
    }
    
    top = stack.peek();
    
    //
    // Check if list on the top of the stack is a list of String
    //

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list of Geo Time Series.");
    }

    for (Object o: ((List) top)) {
      if (!(o instanceof GeoTimeSerie)) {
        throw new WarpScriptException(getName() + " operates on a list of Geo Time Series.");
      }
    }
    
    if (fields.isEmpty()) {
      Collections.sort((List<GeoTimeSerie>) top, META_COMPARATOR);
    } else {
      Collections.sort((List<GeoTimeSerie>) top, new Comparator<GeoTimeSerie>() {
        private final Comparator<Metadata> INNER_COMP = new MetadataTextComparator(fields);
        
        @Override
        public int compare(GeoTimeSerie o1, GeoTimeSerie o2) {
          return INNER_COMP.compare(o1.getMetadata(), o2.getMetadata());
        }        
      });
    }
    
    return stack;
  }
}
