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
package io.warp10.plugins.http;

import io.warp10.WarpConfig;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackRegistry;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class WarpScriptHandler extends AbstractHandler {

  private final HTTPWarp10Plugin plugin;
  private final Properties properties;

  public WarpScriptHandler(HTTPWarp10Plugin plugin) {
    this.plugin = plugin;
    this.properties = WarpConfig.getProperties();
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
    // Only handle REQUEST
    if(DispatcherType.REQUEST != baseRequest.getDispatcherType()){
      baseRequest.setHandled(true);
      return;
    }

    String prefix = this.plugin.getPrefix(target);
    Macro macro = this.plugin.getMacro(prefix);

    if (null == macro) {
      return;
    }

    baseRequest.setHandled(true);

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(HTTPWarp10Plugin.getExposedStoreClient(), HTTPWarp10Plugin.getExposedDirectoryClient(), this.properties);

    try {
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[HTTPWarp10Plugin " + request.getRequestURL() + "]");

      //
      // Push the details onto the stack
      //

      Map<String, Object> params = new HashMap<String, Object>();

      params.put("method", request.getMethod());
      params.put("target", target);
      params.put("pathinfo", target.substring(prefix.length()));

      Enumeration<String> hdrs = request.getHeaderNames();
      Map<String, List<String>> headers = new HashMap<String, List<String>>();
      while (hdrs.hasMoreElements()) {
        String hdr = hdrs.nextElement();
        Enumeration<String> hvalues = request.getHeaders(hdr);
        List<String> hval = new ArrayList<String>();
        while (hvalues.hasMoreElements()) {
          hval.add(hvalues.nextElement());
        }
        if (plugin.isLcHeaders()) {
          headers.put(hdr.toLowerCase(), hval);
        } else {
          headers.put(hdr, hval);
        }
      }
      params.put("headers", headers);

      // Get the payload if the content-type is not application/x-www-form-urlencoded or we do not want to parse the payload
      if (!MimeTypes.Type.FORM_ENCODED.is(request.getContentType()) || !plugin.isParsePayload(prefix)) {
        byte[] payload = IOUtils.toByteArray(request.getInputStream());
        if (0 < payload.length) {
          params.put("payload", payload);
        }
      }

      Map<String, List<String>> httpparams = new HashMap<String, List<String>>();
      Map<String, String[]> pmap = request.getParameterMap();
      for (Entry<String, String[]> param: pmap.entrySet()) {
        httpparams.put(param.getKey(), Arrays.asList(param.getValue()));
      }
      params.put("params", httpparams);

      try {
        stack.push(params);
        stack.exec(macro);

        Object top = stack.pop();

        if (top instanceof Map) {
          Map<String, Object> result = (Map<String, Object>) top;
          if (result.containsKey("status")) {
            response.setStatus(((Number) result.get("status")).intValue());
          }

          if (result.containsKey("headers")) {
            Map<String, Object> respheaders = (Map<String, Object>) result.get("headers");
            for (Entry<String, Object> hdr: respheaders.entrySet()) {
              if (hdr.getValue() instanceof List) {
                for (Object o: (List) hdr.getValue()) {
                  response.addHeader(hdr.getKey(), o.toString());
                }
              } else {
                response.setHeader(hdr.getKey(), hdr.getValue().toString());
              }
            }
          }

          if (result.containsKey("body")) {
            Object body = result.get("body");
            if (body instanceof byte[]) {
              OutputStream out = response.getOutputStream();
              out.write((byte[]) body);
            } else {
              PrintWriter writer = response.getWriter();
              writer.print(body.toString());
            }
          }
        } else {
          if (top instanceof byte[]) {
            response.setContentType("application/octet-stream");
            OutputStream out = response.getOutputStream();
            out.write((byte[]) top);
          } else {
            response.setContentType("text/plain");
            PrintWriter writer = response.getWriter();
            writer.println(String.valueOf(top));
          }
        }
      } catch (WarpScriptException wse) {
        throw new IOException(wse);
      }      
    } finally {
      WarpScriptStackRegistry.unregister(stack);
    }    
  }
}
