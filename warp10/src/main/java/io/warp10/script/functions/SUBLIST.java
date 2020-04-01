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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Create a new list with the elements whose indices are in the parameter list.
 * If the parameter list contains two indices [a,b] then SUBLIST
 * returns the list of elements from the lesser index to the bigger index (included).
 * If the parameter list contains more than two indices, the result of SUBLIST
 * contains all the elements at the specified indices, with possible duplicates.
 * If, instead of the parameter list, there are number, they are considered to define
 * a range. From top to bottom: step (optional), end(optional), start
 */
public class SUBLIST extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public SUBLIST(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o;
    List indices = null;
    List elements = null;
    ArrayList<Long> longParams = new ArrayList<Long>();

    // Get the 4 elements on top of the stack or until a list is found.
    // After this, either indices is null or longParams contains at least one long.
    for (int i = 0; i < 4; i++) {
      o = stack.pop();

      if (o instanceof List) {
        if (0 == i) { // No range defined as numbers
          indices = (List) o;
          o = stack.pop();
          if (o instanceof List) {
            elements = (List) o;
          } else {
            throw new WarpScriptException(getName() + " expects a list of indices on top of the stack and will operate on the list below it.");
          }
        } else {
          elements = (List) o;
        }
        break;
      } else if (o instanceof Number) {
        longParams.add(0, ((Number) o).longValue()); // Prepend the clamped int parameter
      } else {
        throw new WarpScriptException(getName() + " expects a list of indices on top of the stack or a start end step and will operate on the list below it.");
      }
    }

    // elements can be null if the 4 elements on top of the stack were numbers.
    if (null == elements) {
      throw new WarpScriptException(getName() + " expects a list of indices on top of the stack or a start end step and will operate on the list below it.");
    }

    int size = elements.size();

    List<Object> sublist = new ArrayList<Object>();

    if (null == indices) { // Range definition with longParams
      // If indices is null, there is at least one parameter in longParams: start.
      long start = longParams.get(0);

      // Get end if defined
      long end;
      if (longParams.size() > 1) {
        end = longParams.get(1);
      } else {
        // End is not defined, neither is step. So step will default to 1.
        // Thus end defaults to the end of the list or start, if start is greater than end.
        end = Math.max(size - 1, start);
      }

      // Add the size of the list to defined negative indexes. They can still be negative afterward.
      // No risk of overflow because start/end is strictly negative and size is positive
      if (start < 0) {
        start += size;
      }
      if (end < 0) {
        end += size;
      }

      // Only if the defined range intersects the valid indexes, else the sublist is empty.
      if (!(start < 0 && end < 0 || start >= size && end >= size)) {
        // Get step if defined
        long step = 1L;
        if (longParams.size() > 2) {
          step = longParams.get(2);
        } else {
          if (start > end) {
            step = -1L; // Reverse order
          }
          // else step is already 1
        }

        // Check start/end/step coherency
        if (0 == step) {
          throw new WarpScriptException(getName() + " expects the step parameter to be a strictly positive or negative number.");
        } else if (step > 0) {
          if (start > end) {
            throw new WarpScriptException(getName() + " expects start to be before end when step is positive.");
          }
        } else {
          if (end > start) {
            throw new WarpScriptException(getName() + " expects start to be after end when step is negative.");
          }
        }

        // Jump step by step start to nearest valid index in elements
        start = nearestValidBound(start, step, size);

        // Fill the sublist
        try {
          if (step > 0) {
            end = Math.min(end, size - 1);
            for (long i = start; i <= end; i = Math.addExact(i, step)) {
              sublist.add(elements.get(Math.toIntExact(i)));
            }
          } else {
            end = Math.max(end, 0L);
            for (long i = start; i >= end; i = Math.addExact(i, step)) {
              sublist.add(elements.get(Math.toIntExact(i)));
            }
          }
        } catch (ArithmeticException ae) {
          // Do nothing, that means i + step overflowed int and thus is not a valid index anymore.
          // This is most probably the case of a step near to or greater than max integer.
        }
      }
    } else if (2 == indices.size()) { // Range definition with indices
      Object a = indices.get(0);
      Object b = indices.get(1);

      if (!(a instanceof Long) || !(b instanceof Long)) {
        throw new WarpScriptException(getName() + " expects a list of indices which are numeric integers.");
      }

      int la = ((Long) a).intValue();
      int lb = ((Long) b).intValue();

      if (la < 0) {
        la = size + la;
      }
      if (lb < 0) {
        lb = size + lb;
      }

      if (la < 0 && lb < 0) {
        la = size;
        lb = size;
      } else {
        la = Math.max(0, la);
        lb = Math.max(0, lb);
      }

      //
      // If at least one of the bounds is included in the list indices,
      // fix the other one
      //

      if (la < size || lb < size) {
        if (la >= size) {
          la = size - 1;
        } else if (la < -1 * size) {
          la = -1 * size;
        }

        if (lb >= size) {
          lb = size - 1;
        } else if (lb < -1 * size) {
          lb = -1 * size;
        }
      }

      if (la < lb) {
        if (la < size) {
          lb = Math.min(size - 1, lb);
          for (int i = la; i <= lb; i++) {
            sublist.add(elements.get(i));
          }
        }
      } else {
        if (lb < size) {
          la = Math.min(size - 1, la);
          for (int i = lb; i <= la; i++) {
            sublist.add(elements.get(i));
          }
        }
      }
    } else { // Individual elements selection
      for (Object index: indices) {
        if (!(index instanceof Long)) {
          throw new WarpScriptException(getName() + " expects a list of indices which are numeric integers.");
        }

        int idx = ((Long) index).intValue();

        if (idx >= size || (idx < -1 * size)) {
          throw new WarpScriptException(getName() + " reported an out of bound index.");
        }

        if (idx >= 0) {
          sublist.add(elements.get(idx));
        } else {
          sublist.add(elements.get(size + idx));
        }
      }
    }

    stack.push(sublist);

    return stack;
  }

  /**
   * Moves a bound to the closer valid index by "jumping" by step.
   *
   * Example table for a step of 3 and a size of 8, list given for visualization:
   * Input bound:   [ -5 -4 -3 -2 -1  0  1  2  3  4  5  6  7  8  9 10 11 ]
   * Nearest valid: [  1  2  0  1  2  0  1  2  3  4  5  6  7  5  6  7  5 ]
   * List:          [                 a  b  c  d  e  f  g  h             ]
   *
   * @param bound start or end.
   * @param step Amount to add or subtract to the bound. May be positive or negative.
   * @param size Size of the list.
   * @return A valid index if it exists, else an invalid index.
   */
  private static long nearestValidBound(long bound, long step, int size) {
    if (bound >= size) {
      return ((bound - size) % step) - Math.abs(step) + size;
    } else if (bound < 0) {
      return ((bound + 1) % step) + Math.abs(step) - 1;
    } else {
      return bound;
    }
  }
}
