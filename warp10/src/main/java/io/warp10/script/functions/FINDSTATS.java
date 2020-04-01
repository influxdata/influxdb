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

import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Estimate cardinalities of Geo Time Series matching some criteria
 *
 * The top of the stack must contain a list of the following parameters
 * 
 * @param token The OAuth 2.0 token to use for data retrieval
 * @param classSelector  Class selector.
 * @param labelsSelectors Map of label name to label selector.
 */
public class FINDSTATS extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private WarpScriptStackFunction toList = new TOLIST("");
  private WarpScriptStackFunction listTo = new LISTTO("");
  
  public FINDSTATS(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // Extract parameters from the stack
    //

    Object top = stack.peek();
  
    if (top instanceof List) {      
      if (3 != ((List) top).size()) {
        stack.drop();
        throw new WarpScriptException(getName() + " expects 3 parameters.");
      }

      //
      // Explode list and remove its size
      //
      
      listTo.apply(stack);
      stack.drop();
    }
    
    //
    // Extract labels selector
    //
    
    Object oLabelsSelector = stack.pop();
    
    if (!(oLabelsSelector instanceof Map)) {
      throw new WarpScriptException("Label selectors must be a map.");
    }
    
    Map<String,String> labelSelectors = (Map<String,String>) oLabelsSelector;

    //
    // Extract class selector
    //
    
    Object oClassSelector = stack.pop();

    if (!(oClassSelector instanceof String)) {
      throw new WarpScriptException("Class selector must be a string.");
    }
    
    String classSelector = (String) oClassSelector;

    //
    // Extract token
    //
    
    Object oToken = stack.pop();
    
    if (!(oToken instanceof String)) {
      throw new WarpScriptException("Token must be a string.");
    }
    
    String token = (String) oToken;

    
    DirectoryClient directoryClient = stack.getDirectoryClient();
    
    ReadToken rtoken = Tokens.extractReadToken(token);

    labelSelectors.remove(Constants.PRODUCER_LABEL);
    labelSelectors.remove(Constants.OWNER_LABEL);
    labelSelectors.remove(Constants.APPLICATION_LABEL);
    labelSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));
    
    List<String> clsSels = new ArrayList<String>();
    List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
    
    clsSels.add(classSelector);
    lblsSels.add(labelSelectors);

    Map<String,Object> stats = null;

    try {
      DirectoryRequest drequest = new DirectoryRequest();
      drequest.setClassSelectors(clsSels);
      drequest.setLabelsSelectors(lblsSels);

      stats = directoryClient.stats(drequest);
    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    }

    stack.push(stats);
    
    return stack;
  }  
}
