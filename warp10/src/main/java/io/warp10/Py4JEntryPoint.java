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

package io.warp10;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Map.Entry;

import io.warp10.WarpConfig;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

/**
 * Entry point class used when using Py4J's JavaGateway
 */
public class Py4JEntryPoint {

  private final String CONFIG_PY4J_STACK_NOLIMITS = "py4j.stack.nolimits";
  
  private final boolean nolimits;

  public Py4JEntryPoint(String path) throws IOException {    
    if (!WarpConfig.isPropertiesSet()) {
      WarpConfig.safeSetProperties(path);
      WarpScriptLib.registerExtensions();
     }
     this.nolimits = "true".equals(WarpConfig.getProperty(CONFIG_PY4J_STACK_NOLIMITS));
  }

  public Py4JEntryPoint(Map<String,String> props) throws IOException {
    if (!WarpConfig.isPropertiesSet()) {
      StringBuilder sb = new StringBuilder();
      for (Entry<String,String> entry: props.entrySet()) {
        sb.append(entry.getKey());
        sb.append("=");
        sb.append(entry.getValue());
        sb.append("\n");
      }
      StringReader reader = new StringReader(sb.toString());
      WarpConfig.safeSetProperties(reader);
      WarpScriptLib.registerExtensions();
    } else if (props.size() > 0) {
      throw new IOException("Initial properties have already been set in this gateway. Use instead io.warp10.WarpConfig.setProperty(key, value), or launch another gateway.");
    }
    this.nolimits = "true".equals(WarpConfig.getProperty(CONFIG_PY4J_STACK_NOLIMITS));
  }

  public Py4JEntryPoint() {
    this.nolimits = "true".equals(WarpConfig.getProperty(CONFIG_PY4J_STACK_NOLIMITS));
  }

  public WarpScriptStack newStack() {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(AbstractWarp10Plugin.getExposedStoreClient(), AbstractWarp10Plugin.getExposedDirectoryClient());
    if (this.nolimits) {
      stack.maxLimits();
    }
    return stack;
  }
}