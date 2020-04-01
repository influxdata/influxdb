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

package io.warp10.script.mapper;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.aggregator.CompareTo;

public class MapperCompareTo extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final CompareTo.Compared compared;
  private final CompareTo.Comparison comparison;

  public MapperCompareTo(String name, CompareTo.Compared compared, CompareTo.Comparison comparison) {
    super(name);
    this.compared = compared;
    this.comparison = comparison;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    stack.push(new CompareTo(getName(), o, compared, comparison));

    return stack;
  }
}
