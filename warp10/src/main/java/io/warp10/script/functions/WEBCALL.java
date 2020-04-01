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

import io.warp10.continuum.KafkaWebCallService;
import io.warp10.continuum.Tokens;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.thrift.data.WebCallMethod;
import io.warp10.script.thrift.data.WebCallRequest;
import io.warp10.standalone.Warp;
import io.warp10.standalone.StandaloneWebCallService;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Create a WebCallRequest and forwards it to the WebCallService
 */
public class WEBCALL extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public WEBCALL(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {        
    WebCallRequest request = new WebCallRequest();
    
    // Set time in ms
    request.setTimestamp(System.currentTimeMillis());
    
    // Fill stack UUID
    request.setStackUUID(stack.getUUID());
    
    // Generate WebCall UUID
    request.setWebCallUUID(UUID.randomUUID().toString());
    
    //
    // Extract body
    //
    
    Object body = stack.pop();
    
    if (!(body instanceof String)) {
      throw new WarpScriptException(getName() + " expects a request body as a possibly empty string on top of the stack.");
    }
    
    request.setBody(body.toString());
    
    //
    // Extract headers
    //
    
    Object headers = stack.pop();
    
    if (!(headers instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map of header names to header values as second level of the stack.");
    }
    
    for (Entry<Object,Object> entry: ((Map<Object,Object>) headers).entrySet()) {
      request.putToHeaders(entry.getKey().toString(), entry.getValue().toString());
    }
    
    //
    // Extract URL
    //
    
    Object url = stack.pop();
    
    if (!(url instanceof String)) {
      throw new WarpScriptException(getName() + " expects a URL below the headers map.");
    }
    
    try {
      if (!StandaloneWebCallService.getWebAccessController().checkURL(new URL(url.toString()))) {
        throw new WarpScriptException(getName() + " invalid host or scheme in URL.");
      }
    } catch (MalformedURLException mue) {
      throw new WarpScriptException(getName() + " invalid URL.", mue);
    }
    
    request.setUrl(url.toString());
        
    //
    // Extract method
    //
    
    Object method = stack.pop();
    
    if ("GET".equals(method)) {
      request.setMethod(WebCallMethod.GET);
    } else if ("POST".equals(method)) {
      request.setMethod(WebCallMethod.POST);      
    } else {
      throw new WarpScriptException("Invalid method for " + getName() + ", can be one of 'GET' or 'POST'.");
    }
    
    //
    // Extract token
    //
    
    Object token = stack.pop();
    
    if (!(token instanceof String)) {
      throw new WarpScriptException(getName() + " expects a token below the URL.");
    }
    
    request.setToken(token.toString());

    //
    // Check token validity
    //
    
    WriteToken wtoken = Tokens.extractWriteToken(request.getToken());
        
    //
    // Push WebCalLRequest
    //
    
    if (Warp.isStandaloneMode()) {
      if (!StandaloneWebCallService.offer(request)) {
        throw new WarpScriptException("Unable to forward WebCall request.");
      } else {
        stack.push(request.getWebCallUUID());
      }
    } else {
      if (!KafkaWebCallService.offer(request)) {
        throw new WarpScriptException("Unable to forward WebCall request.");        
      } else {
        stack.push(request.getWebCallUUID());
      }
    }
    
    AtomicLong remaining = (AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MAX_WEBCALLS);
    
    if (0 > remaining.get()) {
      throw new WarpScriptException("Maximum number of invocations of " + getName() + " reached.");
    } else {
      remaining.decrementAndGet();
    }
    
    return stack;
  }
}
