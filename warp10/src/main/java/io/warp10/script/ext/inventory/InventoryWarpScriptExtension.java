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

package io.warp10.script.ext.inventory;

import io.warp10.warp.sdk.WarpScriptExtension;

import java.util.HashMap;
import java.util.Map;

/**
 * need to set configuration
 * warpscript.extension.inventory=io.warp10.script.ext.inventory.InventoryWarpScriptExtension
 *
 * Created to list all registered functions in Warp 10.
 *
 */

/**
 * Functions declared by this WarpScript extension must be present in the functions field.
 */
public class InventoryWarpScriptExtension extends WarpScriptExtension {

  private static final Map<String, Object> functions;

  static {
    functions = new HashMap<String, Object>();
    functions.put("FUNCTIONS", new FUNCTIONS("FUNCTIONS"));
  }

  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }

}