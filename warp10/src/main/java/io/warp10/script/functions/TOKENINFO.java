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

import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.Tokens;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.HashMap;
import java.util.Map;

/**
 * Push on the stack information regarding a token
 */
public class TOKENINFO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOKENINFO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a string on top of the stack.");
    }
    
    Map<String,Object> tokenParams = new HashMap<String, Object>();
        
    String readError = null;
    String writeError = null;
    
    try {
      ReadToken rtoken = Tokens.extractReadToken(o.toString());
      
      tokenParams.put("type", "READ");
      tokenParams.put("issuance", rtoken.getIssuanceTimestamp());
      tokenParams.put("expiry", rtoken.getExpiryTimestamp());
      tokenParams.put("application", rtoken.getAppName());
      if (rtoken.getAppsSize() > 0) {
        tokenParams.put("apps", rtoken.getApps());
      }
      if (rtoken.getLabelsSize() > 0) {
        tokenParams.put("labels", rtoken.getLabels());
      }
    } catch (WarpScriptException ee) {
      readError = ee.getMessage();
    }
    
    try {
      WriteToken wtoken = Tokens.extractWriteToken(o.toString());
      
      tokenParams.put("type", "WRITE");
      tokenParams.put("issuance", wtoken.getIssuanceTimestamp());
      tokenParams.put("expiry", wtoken.getExpiryTimestamp());
      tokenParams.put("application", wtoken.getAppName());
      if (wtoken.getLabelsSize() > 0) {
        tokenParams.put("labels", wtoken.getLabels());
      }
      Map<String,Object> limits = ThrottlingManager.getLimits(Tokens.getUUID(wtoken.getProducerId()), wtoken.getAppName());
      
      tokenParams.put("limits", limits);      
    } catch (WarpScriptException ee) {
      writeError = ee.getMessage();
    }

    if (null != readError && null != writeError) {
      tokenParams.put("ReadTokenDecodeError", readError);
      tokenParams.put("WriteTokenDecodeError", writeError);
    }
    
    stack.push(tokenParams);
    
    return stack;
  }
}
