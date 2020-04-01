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

import io.warp10.Revision;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Return true if the current revision tag is newer or equal to the given revision tag.
 */
public class MINREV extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public MINREV(String name) {
    super(name);
  }

  /**
   * Parse strings of the format \d+(\.\d+)*-.* and returns an array of integers corresponding to the revision.
   *
   * @param revision     The revision string to be parsed.
   * @param errorMessage The error message to be included in the WarpScriptException in case a problem arises.
   * @return An array or integers corresponding to each part of the revision numbering.
   * @throws WarpScriptException In case the input string is malformed.
   */
  private static int[] SplitRev(String revision, String errorMessage) throws WarpScriptException {

    if (null == revision || "".equals(revision)) {
      throw new WarpScriptException(errorMessage);
    }

    // Check for dev version with Git info and discard it.
    String[] gitSplit = revision.split("-");

    if (0 == gitSplit.length) {
      throw new WarpScriptException(errorMessage);
    }

    String[] revSplit = gitSplit[0].split("\\.");

    if (0 == revSplit.length) {
      throw new WarpScriptException(errorMessage);
    }

    int[] revIntSplit = new int[revSplit.length];

    for (int revIndex = 0; revIndex < revSplit.length; revIndex++) {
      try {
        revIntSplit[revIndex] = Integer.valueOf(revSplit[revIndex]);
      } catch (NumberFormatException nfe) {
        throw new WarpScriptException(errorMessage);
      }
    }

    return revIntSplit;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a revision string on top of the stack.");
    }

    // Split revision from "1.2.3-42-abcd" to [1, 2, 3]
    int[] givenRev = SplitRev((String) o, getName() + " expects the given revision to be of the form 'X', 'X.Y' or 'X.Y.Z'. X, Y and Z are integers.");
    int[] currentRev = SplitRev(Revision.REVISION, getName() + " expects the revision to be set on the platform to a string of the form 'X.Y.Z'. X, Y and Z are integers.");

    int revComp = 0;
    
    // Compare corresponding elements on both arrays
    for (int revIndex = 0; revIndex < Math.min(givenRev.length, currentRev.length); revIndex++) {
      revComp = Integer.compare(currentRev[revIndex], givenRev[revIndex]);
      if (0 != revComp) {
        break; // If not equal, stop the comparison here.
      }
    }

    // If corresponding elements are equal and the given revision gives more info (ie given=1.2.3.4 vs current=1.2.3)
    // consider the current revision is older (less) than the given revision.
    if (0 == revComp && givenRev.length > currentRev.length) {
      revComp = -1;
    }

    stack.push(0 <= revComp);

    return stack;
  }
}
