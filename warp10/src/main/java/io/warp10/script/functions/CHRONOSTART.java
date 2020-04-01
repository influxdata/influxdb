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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.concurrent.atomic.AtomicLong;

public class CHRONOSTART extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public CHRONOSTART(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o1 = stack.pop();
    String alias = o1.toString();

    String keyStart = CHRONOSTATS.getStartKey(alias, stack);
    String keyActiveCount = CHRONOSTATS.getActiveCountKey(alias, stack);
    String keyTotalCount = CHRONOSTATS.getTotalCountKey(alias, stack);

    // Increase the active count for this alias
    AtomicLong activeCount = (AtomicLong) stack.getAttribute(keyActiveCount);
    if(null == activeCount){
      activeCount = new AtomicLong();
      stack.setAttribute(keyActiveCount, activeCount);
    }
    activeCount.incrementAndGet();

    // Increase the total count for this alias
    AtomicLong totalCount = (AtomicLong) stack.getAttribute(keyTotalCount);
    if(null == totalCount){
      totalCount = new AtomicLong();
      stack.setAttribute(keyTotalCount, totalCount);
    }
    totalCount.incrementAndGet();

    // Keep start nano only if first start. Takes care of recursivity.
    if (1 == activeCount.intValue()) {
      stack.setAttribute(keyStart, new AtomicLong(System.nanoTime()));
    }

    return stack;
  }
}
