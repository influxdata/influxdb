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

import java.util.Comparator;
import java.util.EmptyStackException;
import java.util.List;

/**
 * Sorts a list using a comparator macro.
 * The comparator macro is given:
 * TOP: b
 * 2:   a
 * And should push a negative Long if a less than b, a positive one if a more than b or 0L if there are equal.
 * This Long must be within the bounds of a 32-bit integer.
 */
public class SORTWITH extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  // Unchecked exception to wrap checked ones, thus allowing Compare.compare to throw it.
  // Also nicely formats the error message for unchecked exceptions thrown during the comparison.
  private class ComparisonException extends RuntimeException {

    public ComparisonException(Exception e) {
      super(e);
    }
  }

  public SORTWITH(String name) {
    super(name);
  }

  @Override
  public Object apply(final WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();

    if (!(top instanceof WarpScriptStack.Macro)) {
      throw new WarpScriptException(getName() + " expects a macro on top of the stack.");
    }

    final WarpScriptStack.Macro macro = (WarpScriptStack.Macro) top;

    top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " operates on a list.");
    }

    List list = (List) top;

    try {
      list.sort(new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
          try {
            stack.push(o1);
            stack.push(o2);
            stack.exec(macro);

            Object topComp = stack.pop();

            if (!(topComp instanceof Long)) {
              throw new WarpScriptException(getName() + " was given a macro which doesn't return a LONG. This LONG must be within the bounds of a 32-bit integer.");
            }

            return Math.toIntExact((Long) topComp);
          } catch (WarpScriptException | ArithmeticException | EmptyStackException e) {
            // Wrap in a unchecked exception for Comparator.compare to be able to throw it.
            throw new ComparisonException(e);
          }
        }
      });
    } catch (ComparisonException ce) {
      throw new WarpScriptException(getName() + " encountered an error with comparator: " + ce.getCause().getMessage(), ce);
    }

    stack.push(list);

    return stack;
  }

}
