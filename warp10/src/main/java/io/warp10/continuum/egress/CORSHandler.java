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

package io.warp10.continuum.egress;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.ext.stackps.StackPSWarpScriptExtension;

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class CORSHandler extends AbstractHandler {
  
  private static final String headers;
  static {
    StringBuilder sb = new StringBuilder();
    
    sb.append(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
    sb.append(",");
    sb.append(StackPSWarpScriptExtension.HEADER_SESSION);
    
    String corsHeadersProp = WarpConfig.getProperty(Configuration.CORS_HEADERS);
        
    if (null != corsHeadersProp) {
      String[] hdrs = corsHeadersProp.split(",");
      
      for (String hdr: hdrs) {
        sb.append(",");
        sb.append(hdr.trim());
      }
    }
      
    headers = sb.toString();
      
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if ("OPTIONS".equals(baseRequest.getMethod())) {
      baseRequest.setHandled(true);
    } else if (Constants.API_ENDPOINT_CHECK.equals(target)) {
      baseRequest.setHandled(true);
      response.setStatus(HttpServletResponse.SC_OK);
      return;
    } else {
      return;
    }    
    
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Allow-Methods", "OPTIONS,POST,GET");
    response.setHeader("Access-Control-Allow-Headers", headers);
    // Allow to cache preflight response for 30 days
    response.setHeader("Access-Control-Max-Age", "" + 24 * 3600 * 30);
    
    response.setStatus(HttpServletResponse.SC_OK);
  }
}
