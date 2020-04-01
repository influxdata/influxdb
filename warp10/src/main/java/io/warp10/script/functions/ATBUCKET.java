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

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.List;

/**
 * Extract the value/location/elevation at the Nth bucket of the GTS on top of the stack
 */
public class ATBUCKET extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ATBUCKET(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects an index on top of the stack.");
    }
    
    int idx = ((Number) o).intValue();
    
    o = stack.pop();
    
    if (!(o instanceof GeoTimeSerie)) {
      throw new WarpScriptException(getName() + " expects a Geo Time Series instance on top of the stack.");
    }
     
    GeoTimeSerie gts = (GeoTimeSerie) o;
    
    if (!GTSHelper.isBucketized(gts)) {
      throw new WarpScriptException(getName() + " expects the Geo Time Series on top of the stack to be bucketized.");
    }

    int bucketcount = GTSHelper.getBucketCount(gts);

    if (idx >= bucketcount) {
      throw new WarpScriptException(getName() + " cannot retrieve bucket " + idx + " of a GTS with " + bucketcount + " buckets.");
    }
      
    long lastbucket = GTSHelper.getLastBucket(gts);
    long bucketspan = GTSHelper.getBucketSpan(gts);

    //
    // Sort the gts so we can retrieve by timestamp
    //
    
    GTSHelper.sort(gts);
          
    long tick = lastbucket - (bucketcount - 1 - idx) * bucketspan;
      
    idx = GTSHelper.indexAtTick(gts, tick);

    List<Object> result = ATINDEX.getTupleAtIndex(gts, idx);
    
    stack.push(result);

    return stack;
  }
}
