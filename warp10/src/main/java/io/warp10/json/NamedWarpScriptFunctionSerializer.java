//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;

import java.io.IOException;

public class NamedWarpScriptFunctionSerializer extends StdSerializer<NamedWarpScriptFunction> {

  protected NamedWarpScriptFunctionSerializer() {
    super(NamedWarpScriptFunction.class);
  }

  @Override
  public void serialize(NamedWarpScriptFunction namedWarpScriptFunction, JsonGenerator gen, SerializerProvider provider) throws IOException {
    gen.writeString(WarpScriptStack.MACRO_START + " " + namedWarpScriptFunction.toString() + " " + WarpScriptStack.MACRO_END + " " + WarpScriptLib.EVAL);
  }
}
