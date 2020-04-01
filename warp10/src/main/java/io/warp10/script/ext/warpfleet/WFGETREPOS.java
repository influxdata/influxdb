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

package io.warp10.script.ext.warpfleet;

import java.util.ArrayList;
import java.util.Properties;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpFleetMacroRepository;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class WFGETREPOS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final boolean disabled;
  
  static {
    disabled = "true".equals(WarpConfig.getProperty(Configuration.WARPFLEET_GETREPOS_DISABLE));
  }
  
  public WFGETREPOS(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    if (disabled) {
      stack.push(new ArrayList<String>(0));
    } else {
      stack.push(WarpFleetMacroRepository.getRepos(stack));
    }
    
    return stack;
  }
}
