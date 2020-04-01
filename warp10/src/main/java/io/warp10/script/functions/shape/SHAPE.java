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
import io.warp10.script.functions.shape.CHECKSHAPE;

public class SHAPE extends FormattedWarpScriptFunction {

  public static final String LIST = "list";
  public static final String FAST = "fast";
  public static final String SHAPE = "shape";

  private final Arguments args;
  private final Arguments output;
  protected Arguments getArguments() { return args; }
  protected Arguments getOutput() { return output; }

  public SHAPE(String name) {
    super(name);

    getDocstring().append("Return the shape of an input list if it could be a tensor (or multidimensional array), or raise an Exception.");

    args = new ArgumentsBuilder()
      .addArgument(List.class, LIST, "The input list.")
      .addOptionalArgument(Boolean.class, FAST, "If true, it does not check if the sizes of the nested lists are coherent and it returns a shape based on the first nested lists at each level. Default to false.", false)
      .build();

    output = new ArgumentsBuilder()
      .addListArgument(Long.class, SHAPE, "The shape of the input list.")
      .build();
  }

  @Override
  protected WarpScriptStack apply(Map<String, Object> formattedArgs, WarpScriptStack stack) throws WarpScriptException {
    List list = (List) formattedArgs.get(LIST);
    boolean fast = Boolean.TRUE.equals(formattedArgs.get(FAST));

    List<Long> candidateShape = candidate_shape(list);

    if (fast || CHECKSHAPE.recValidateShape(list, candidateShape)) {
      stack.push(candidateShape);
    } else {
      throw new WarpScriptException(getName() + " expects that the sizes of the nested lists are coherent together to form a tensor (or multidimensional array).");
    }
    return stack;
  }

  static List<Long> candidate_shape(List list) {
    List<Long> shape = new ArrayList<Long>();
    Object l = list;

    while(l instanceof List) {
      shape.add((long) ((List) l).size());
      l = ((List) l).get(0);
    }

    return shape;
  }
}