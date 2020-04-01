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

package io.warp10.script;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract class to inherit when a function can be called on a single element or a list of elements.
 * For instance for a function working on GTSs, GTSEncoders and lists thereof, including lists with mixed types.
 * The idea is to generate a function using the parameters on the stack and then to apply this function on the element
 * or the list of elements.
 * This is similar to GTSStackFunction for GTSs but it is a tad faster if the function uses some parameters as this
 * implementation does not use a Map for parameters.
 */
public abstract class ElementOrListStackFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  /**
   * Interface defining the function to be generated which is applied to each element.
   * This method should check the type of the given element and throw a WarpScriptException if the element is of
   * unexpected type.
   */
  @FunctionalInterface
  public interface ElementStackFunction {
    public Object applyOnElement(Object element) throws WarpScriptException;
  }

  /**
   * Generates the function to be applied on the element(s) using the parameters on the stack.
   * @param stack The stack to be used for parameters retrieval.
   * @return An ElementStackFunction which will be applied on each given elements or on the given element.
   * @throws WarpScriptException
   */
  public abstract ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException;

  public ElementOrListStackFunction(String name) {
    super(name);
  }

  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    ElementStackFunction function = generateFunction(stack);

    Object o = stack.pop();

    // Case of a list
    if (o instanceof List) {
      List list = (List) o;
      // Build the result list which will be as long a the given list
      ArrayList<Object> result = new ArrayList<Object>(list.size());

      for (Object element: list) {
        result.add(function.applyOnElement(element));
      }

      stack.push(result);
    } else { // Case of a single element
      stack.push(function.applyOnElement(o));
    }

    return stack;
  }
}

