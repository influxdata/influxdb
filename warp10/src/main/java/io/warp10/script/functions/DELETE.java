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

import io.warp10.WarpConfig;
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.StandaloneAcceleratedStoreClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Delete a set of GTS.
 * 
 * For safety reasons DELETE will first perform a dryrun call to the /delete endpoint to retrieve
 * the number of GTS which would be deleted by the call. If this number is above the expected number provided
 * by the user the actual delete will not be performed and instead an error will be raised. 
 */
public class DELETE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private URL url = null;
  
  public DELETE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    //
    // Extract expected number
    //
    
    Object o = stack.pop();
    
    if (!(o instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a count on top of the stack.");
    }
    
    long expected = (long) o;
    
    //
    // Extract start / end
    //
    
    o = stack.pop();
    
    if (!(o instanceof Long) && !(o instanceof String) && (null != o)) {
      throw new WarpScriptException(getName() + " expects the end timestamp to be a Long, a String or NULL.");
    }
    
    Object end = o;
    
    o = stack.pop();
    
    if (!(o instanceof Long) && !(o instanceof String) && (null != o)) {
      throw new WarpScriptException(getName() + " expects the start timestamp to be a Long, a String or NULL.");
    }
    
    Object start = o;

    if ((null == start && null != end) || (null != start && null == end)) {
      throw new WarpScriptException(getName() + " expects both start and end timestamps MUST be NULL if one of them is.");
    }
    
    //
    // Extract selector
    //
    
    o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a Geo Time Series selector below the time parameters.");
    }
    
    String selector = o.toString();
    
    //
    // Extract token
    //
    
    o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects a token below the selector.");
    }
    
    String token = (String) o;

    //
    // Issue a dryrun call to DELETE
    //
    
    HttpURLConnection conn = null;

    try {
      if (null == url) {
        String url_property = WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_DELETE_ENDPOINT);
        if (null != url_property) {
          try {
            url = new URL(url_property);
          } catch (MalformedURLException mue) {
            throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_DELETE_ENDPOINT + "' does not define a valid URL.");
          }
        } else {
          throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_DELETE_ENDPOINT + "' not set.");
        }
      }

      StringBuilder qsurl = new StringBuilder(url.toString());
      
      if (null == url.getQuery()) {
        qsurl.append("?");
      } else {
        qsurl.append("&");
      }

      if (null != start && null != end) {
        qsurl.append(Constants.HTTP_PARAM_END);
        qsurl.append("=");
        qsurl.append(end);
        qsurl.append("&");
        qsurl.append(Constants.HTTP_PARAM_START);
        qsurl.append("=");
        qsurl.append(start);
      } else {
        qsurl.append(Constants.HTTP_PARAM_DELETEALL);
        qsurl.append("=");
        qsurl.append("true");
      }
      
      qsurl.append("&");
      qsurl.append(Constants.HTTP_PARAM_SELECTOR);
      qsurl.append("=");
      qsurl.append(WarpURLEncoder.encode(selector, StandardCharsets.UTF_8));

      boolean nocache = Boolean.TRUE.equals(stack.getAttribute(StandaloneAcceleratedStoreClient.ATTR_NOCACHE));
      boolean nopersist = Boolean.TRUE.equals(stack.getAttribute(StandaloneAcceleratedStoreClient.ATTR_NOPERSIST));

      if (nocache) {
        qsurl.append("&");
        qsurl.append(StandaloneAcceleratedStoreClient.NOCACHE);
      }
      
      if (nopersist) {
        qsurl.append("&");
        qsurl.append(StandaloneAcceleratedStoreClient.NOPERSIST);
      }

      //
      // Issue the dryrun request
      //
      
      URL requrl = new URL(qsurl.toString() + "&" + Constants.HTTP_PARAM_DRYRUN + "=true");

      conn = (HttpURLConnection) requrl.openConnection();
      
      conn.setDoOutput(false);
      conn.setDoInput(true);
      conn.setRequestMethod("GET");
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DELETE_TOKENX), token);
      conn.connect();
            
      if (200 != conn.getResponseCode()) {
        throw new WarpScriptException(getName() + " failed to complete dryrun request successfully (" + conn.getResponseMessage() + ")");
      }
      
      BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      
      long actualCount = 0;
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        actualCount++;

        // Do an early check for the expected count
        if (expected < actualCount) {
          throw new WarpScriptException(getName() + " expected at most " + expected + " Geo Time Series to be deleted but " + actualCount + " would have been deleted instead.");
        }
      }

      br.close();
      conn.disconnect();
      conn = null;
      
      //
      // Do nothing if no GTS are to be removed
      //
      
      if (0 == actualCount) {
        stack.push(0);
        return stack;
      }
      
      //
      // Now issue the actual call, hoping the deleted count is identical to the one we expected...
      //
      
      requrl = new URL(qsurl.toString());
      
      conn = (HttpURLConnection) requrl.openConnection();
      
      conn.setDoOutput(false);
      conn.setDoInput(true);
      conn.setRequestMethod("GET");
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DELETE_TOKENX), token);
      conn.connect();
            
      if (200 != conn.getResponseCode()) {
        throw new WarpScriptException(getName() + " failed to complete actual request successfully (" + conn.getResponseMessage() + ")");
      }

      br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      
      actualCount = 0;
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        actualCount++;
      }

      br.close();

      conn.disconnect();
      conn = null;

      stack.push(actualCount);
      
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " failed.", ioe);
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }

    return stack;
  }
}
