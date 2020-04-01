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
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class TIMED extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  // Define private instances of used functions to avoid potential redefinition by extensions
  private static final CHRONOEND funcCHRONOEND = new CHRONOEND(WarpScriptLib.CHRONOEND);
  private static final CHRONOSTART funcCHRONOSTART = new CHRONOSTART(WarpScriptLib.CHRONOSTART);
  private static final RETHROW funcRETHROW = new RETHROW(WarpScriptLib.RETHROW);
  private static final TRY funcTRY = new TRY(WarpScriptLib.TRY);

  public TIMED(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o1 = stack.pop();
    String alias = o1.toString();

    Object o2 = stack.pop();

    if (!(o2 instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects a macro under the top of the stack which will be timed.");
    }

    Macro macro = (Macro) o2;

    Macro catchMacro = new Macro();
    catchMacro.add(funcRETHROW);

    // Encapsulate the CHRONOEND function in a macro to use it as a finally block.
    // Make TIMED resilient to STOP, RETURN and any exception.
    Macro finallyMacro = new Macro();
    finallyMacro.add(alias);
    finallyMacro.add(funcCHRONOEND);

    // Build a new macro starting with CHRONOSTART, calling the given macro and ending with CHRONOEND in the finally block of a TRY.
    Macro timedMacro = new Macro();
    timedMacro.add(alias);
    timedMacro.add(funcCHRONOSTART);
    timedMacro.add(macro);        // Try
    timedMacro.add(catchMacro);   // Catch
    timedMacro.add(finallyMacro); // Finally
    timedMacro.add(funcTRY);

    stack.push(timedMacro);

    return stack;
  }
}
