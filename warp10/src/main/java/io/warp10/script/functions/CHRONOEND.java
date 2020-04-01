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
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CHRONOEND extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public CHRONOEND(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    long endNanos = System.nanoTime();

    Object o1 = stack.pop();
    String alias = o1.toString();

    String keyStart = CHRONOSTATS.getStartKey(alias, stack);
    String keyActiveCount = CHRONOSTATS.getActiveCountKey(alias, stack);
    String keyTotalCount = CHRONOSTATS.getTotalCountKey(alias, stack);

    AtomicLong activeCount = (AtomicLong) stack.getAttribute(keyActiveCount);

    if (null == activeCount) {
      throw new WarpScriptException(getName() + " called before " + WarpScriptLib.CHRONOSTART + " for " + alias + ".");
    }

    // Decrease the number of "active" starts
    activeCount.decrementAndGet();

    // If there are active chronos, do nothing. Takes care of recursivity.
    if (activeCount.intValue() > 0) {
      return stack;
    }

    if (activeCount.intValue() < 0) {
      throw new WarpScriptException(getName() + " called more times than " + WarpScriptLib.CHRONOSTART + " for " + alias + ".");
    }

    // No need to check for nullity because it has been done to active count
    AtomicLong startNanos = (AtomicLong) stack.getAttribute(keyStart);
    AtomicLong totalCount = (AtomicLong) stack.getAttribute(keyTotalCount);

    Map<String, AtomicLong[]> stats = (Map<String, AtomicLong[]>) stack.getAttribute(CHRONOSTATS.key);
    if (null == stats) {
      stats = new ConcurrentHashMap<String, AtomicLong[]>();
      stack.setAttribute(CHRONOSTATS.key, stats);
    }

    AtomicLong[] alias_stats = stats.get(alias);
    if (null == alias_stats) {
      alias_stats = new AtomicLong[]{new AtomicLong(), new AtomicLong()};
      stats.put(alias, alias_stats);
    }

    alias_stats[0].addAndGet(endNanos - startNanos.longValue()); // Total elapsed time
    alias_stats[1].addAndGet(totalCount.longValue()); // Total call count

    // Reset total count. No need for start as it will be overridden.
    totalCount.set(0L);

    return stack;
  }
}
