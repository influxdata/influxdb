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

package io.warp10.script.functions.shape;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;

public class HULLSHAPE extends FormattedWarpScriptFunction {

  public static final String LIST = "list";
  public static final String SHAPE = "shape";

  private final Arguments args;
  private final Arguments output;
  protected Arguments getArguments() { return args; }
  protected Arguments getOutput() { return output; }

  public HULLSHAPE(String name) {
    super(name);

    getDocstring().append("Return the shape of a tensor (or multidimensional array) that would be able to contain all the values of an input nested list. The size of the returned shape is equal to the deepest level of nesting plus one. Its i-th value is equal to the size of the largest list that is nested i levels deep.");

    args = new ArgumentsBuilder()
      .addArgument(List.class, LIST, "The input list.")
      .build();

    output = new ArgumentsBuilder()
      .addListArgument(Long.class, SHAPE, "The hull shape of the input list.")
      .build();
  }

  @Override
  protected WarpScriptStack apply(Map<String, Object> formattedArgs, WarpScriptStack stack) throws WarpScriptException {
    List list = (List) formattedArgs.get(LIST);
    stack.push(recHullShape(list));
    return stack;
  }

  private List<Long> maximizeHull(List<Long> a, List<Long> b) {
    List<Long> smaller = a.size() < b.size() ? a : b;
    List<Long> taller = a.size() < b.size() ? b : a;

    if (0 == smaller.size()) {
      return taller;
    }

    List<Long> res = new ArrayList<Long>(taller.size());
    while (smaller.size() > 0) {
      Long x = smaller.remove(0);
      Long y = taller.remove(0);
      res.add(x > y ? x : y);
    }

    res.addAll(taller);
    return res;
  }

  private List<Long> recHullShape(List list) {
    List<Long> res = new ArrayList<Long>();
    res.add((long) list.size());

    List<Long> hull = new ArrayList<>();
    for (Object el: list) {
      if (el instanceof List) {
        hull = maximizeHull(hull, recHullShape((List) el));
      }
    }

    res.addAll(hull);
    return res;
  }
}
