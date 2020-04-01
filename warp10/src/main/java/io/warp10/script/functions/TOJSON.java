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

import io.warp10.json.JsonUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.IOException;

/**
 * Converts the object on top of the stack to a JSON representation
 */
public class TOJSON extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public TOJSON(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    //
    // Only allow the serialization of simple lists and maps, otherwise JSON might
    // expose internals
    //

    try {
      Long maxJsonSize = (Long)stack.getAttribute(WarpScriptStack.ATTRIBUTE_JSON_MAXSIZE);
      String json = JsonUtils.objectToJson(o, false, maxJsonSize);
      stack.push(json);
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " failed with to convert to JSON.", ioe);
    } catch (StackOverflowError soe) {
      throw new WarpScriptException(getName() + " failed with to convert to JSON, the structure is too deep or it references itself.", soe);
    }

    return stack;
  }

}
