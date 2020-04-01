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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.ThrowableUtils;
import io.warp10.continuum.BootstrapManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.LogUtil;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.KeyStore;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.script.WarpScriptStopException;
import io.warp10.script.ext.stackps.StackPSWarpScriptExtension;
import io.warp10.script.functions.AUTHENTICATE;
import io.warp10.sensision.Sensision;

public class EgressExecHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EgressExecHandler.class);
  private static final Logger EVENTLOG = LoggerFactory.getLogger("warpscript.events");
 
  private static StoreClient exposedStoreClient = null;
  private static DirectoryClient exposedDirectoryClient = null;
  
  private final KeyStore keyStore;
  private final StoreClient storeClient;
  private final DirectoryClient directoryClient;
  
  private final BootstrapManager bootstrapManager;
  
  public EgressExecHandler(KeyStore keyStore, Properties properties, DirectoryClient directoryClient, StoreClient storeClient) {
    this.keyStore = keyStore;
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    
    //
    // Check if we have a 'bootstrap' property
    //
    
    if (properties.containsKey(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PATH)) {
      
      final String path = properties.getProperty(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PATH);
      
      long period = properties.containsKey(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PERIOD) ?  Long.parseLong(properties.getProperty(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PERIOD)) : 0L;
      this.bootstrapManager = new BootstrapManager(path, period);      
    } else {
      this.bootstrapManager = new BootstrapManager();
    }
    
    if ("true".equals(properties.getProperty(Configuration.EGRESS_CLIENTS_EXPOSE))) {
      exposedStoreClient = storeClient;
      exposedDirectoryClient = directoryClient;
    }
  }    
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {

    if (target.startsWith(Constants.API_ENDPOINT_EXEC)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }
    
    //
    // CORS header
    //
    
    resp.setHeader("Access-Control-Allow-Origin", "*");

    //
    // Making the Elapsed header available in cross-domain context
    //

    resp.addHeader("Access-Control-Expose-Headers", Constants.getHeader(Configuration.HTTP_HEADER_ELAPSEDX) + "," + Constants.getHeader(Configuration.HTTP_HEADER_OPSX) + "," + Constants.getHeader(Configuration.HTTP_HEADER_FETCHEDX));

    //
    // Generate UUID for this script execution
    //
    
    UUID uuid = UUID.randomUUID();
    
    //
    // FIXME(hbs): Make sure we have at least one valid token
    //
    
    //
    // Create the stack to use
    //
    
    WarpScriptStack stack = new MemoryWarpScriptStack(this.storeClient, this.directoryClient);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[EgressExecHandler " + Thread.currentThread().getName() + "]");
    
    if (null != req.getHeader(StackPSWarpScriptExtension.HEADER_SESSION)) {
      stack.setAttribute(StackPSWarpScriptExtension.ATTRIBUTE_SESSION, req.getHeader(StackPSWarpScriptExtension.HEADER_SESSION));
    }
    
    Throwable t = null;

    StringBuilder scriptSB = new StringBuilder();
    List<Long> times = new ArrayList<Long>();
    
    int lineno = 0;

    long now = System.nanoTime();
    
    try {
      //
      // Replace the context with the bootstrap one
      //
      
      StackContext context = this.bootstrapManager.getBootstrapContext();
      
      if (null != context) {
        stack.push(context);
        stack.restore();
      }
      
      //
      // Expose the headers if instructed to do so
      //
      String expose = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_EXPOSE_HEADERS));
      
      if (null != expose) {
        Map<String,Object> headers = new HashMap<String,Object>();
        Enumeration<String> names = req.getHeaderNames();
        while(names.hasMoreElements()) {
          String name = names.nextElement();
          Enumeration<String> values = req.getHeaders(name);
          List<String> elts = new ArrayList<String>();
          while(values.hasMoreElements()) {
            String value = values.nextElement();
            elts.add(value);
          }
          headers.put(name, elts);
        }
        stack.store(expose, headers);
      }
            
      //
      // Execute the bootstrap code
      //

      stack.exec(WarpScriptLib.BOOTSTRAP);
      
      //
      // Extract parameters from the path info and set their value as symbols
      //
      
      String pathInfo = req.getPathInfo().substring(target.length());
      
      if (null != pathInfo && pathInfo.length() > 0) {
        pathInfo = pathInfo.substring(1);
        String[] tokens = pathInfo.split("/");

        for (String token: tokens) {
          String[] subtokens = token.split("=");
          
          // Legit uses of URLDecoder.decode, do not replace by WarpURLDecoder
          // as the encoding is performed by the browser
          subtokens[0] = URLDecoder.decode(subtokens[0], StandardCharsets.UTF_8.name());
          subtokens[1] = URLDecoder.decode(subtokens[1], StandardCharsets.UTF_8.name());
          
          //
          // Execute values[0] so we can interpret it prior to storing it in the symbol table
          //
    
          scriptSB.append("// @param ").append(subtokens[0]).append("=").append(subtokens[1]).append("\n");

          stack.exec(subtokens[1]);
          
          stack.store(subtokens[0], stack.pop());
        }
      }
      
      //
      // Now read lines of the body, interpreting them
      //
      
      //
      // Determine if content if gzipped
      //

      boolean gzipped = false;
          
      if ("application/gzip".equals(req.getHeader("Content-Type"))) {       
        gzipped = true;
      }
      
      BufferedReader br = null;
          
      if (gzipped) {
        GZIPInputStream is = new GZIPInputStream(req.getInputStream());
        br = new BufferedReader(new InputStreamReader(is));
      } else {    
        br = req.getReader();
      }
                  
      List<Long> elapsed = (List<Long>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_ELAPSED);
      
      elapsed.add(TimeSource.getNanoTime());
      
      boolean terminate = false;
      
      while(!terminate) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }

        lineno++;
        
        // Store line for logging purposes, BEFORE execution is attempted, so we know what line may have caused an exception
        scriptSB.append(line).append("\n");

        long nano = System.nanoTime();
        
        try {
          if (Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_LINENO)) && !((MemoryWarpScriptStack) stack).isInMultiline()) {
            // We call 'exec' so statements are correctly put in macros if we are currently building one
            stack.exec("'[Line #" + Long.toString(lineno) + "]'");
            stack.exec(WarpScriptLib.SECTION);
          }
          stack.exec(line);
        } catch (WarpScriptStopException ese) {
          // Do nothing, this is simply an early termination which should not generate errors
          terminate = true;
        }
        
        long end = System.nanoTime();

        // Record elapsed time
        if (Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_TIMINGS))) {
          elapsed.add(end - now);
        }
        
        times.add(end - nano);
      }

      //
      // Make sure stack is balanced
      //
      
      stack.checkBalanced();
            
      //
      // Check the user defined headers and set them.
      //

      if (null != stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS)) {
        Map<String,String> headers = (Map<String,String>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS);
        if (!Constants.hasReservedHeader(headers)) {
          StringBuilder sb = new StringBuilder();         
          for (Entry<String,String> header: headers.entrySet()) {
            if (sb.length() > 0) {
              sb.append(",");
            }
            sb.append(header.getKey());
            resp.setHeader(header.getKey(), header.getValue());
          }
          resp.addHeader("Access-Control-Expose-Headers", sb.toString());
        }
      }

      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_ELAPSEDX), Long.toString(System.nanoTime() - now));
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_OPSX), stack.getAttribute(WarpScriptStack.ATTRIBUTE_OPS).toString());
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_FETCHEDX), stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT).toString());
      
      //resp.setContentType("application/json");
      //resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
      
      //
      // Output the exported symbols in a map
      //
      
      Object exported = stack.getAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS);
      
      if (exported instanceof Set && !((Set) exported).isEmpty()) {
        Map<String,Object> exports = new HashMap<String,Object>();
        Map<String,Object> symtable = stack.getSymbolTable();
        for (Object symbol: (Set) exported) {
          if (null == symbol) {
            exports.putAll(symtable);
            break;
          }
          exports.put(symbol.toString(), symtable.get(symbol.toString()));
        }
        stack.push(exports);
      }
      
      StackUtils.toJSON(resp.getWriter(), stack);   
    } catch (Throwable e) {
      t = e;      

      int debugDepth = (int) stack.getAttribute(WarpScriptStack.ATTRIBUTE_DEBUG_DEPTH);

      //
      // Check the user defined headers and set them.
      //

      if (null != stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS)) {
        Map<String,String> headers = (Map<String,String>) stack.getAttribute(WarpScriptStack.ATTRIBUTE_HEADERS);
        if (!Constants.hasReservedHeader(headers)) {
          StringBuilder sb = new StringBuilder();         
          for (Entry<String,String> header: headers.entrySet()) {
            if (sb.length() > 0) {
              sb.append(",");
            }
            sb.append(header.getKey());
            resp.setHeader(header.getKey(), header.getValue());
          }
          resp.addHeader("Access-Control-Expose-Headers", sb.toString());
        }
      }

      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_ELAPSEDX), Long.toString(System.nanoTime() - now));
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_OPSX), stack.getAttribute(WarpScriptStack.ATTRIBUTE_OPS).toString());
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_FETCHEDX), stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT).toString());

      resp.addHeader("Access-Control-Expose-Headers", Constants.getHeader(Configuration.HTTP_HEADER_ERROR_LINEX) + "," + Constants.getHeader(Configuration.HTTP_HEADER_ERROR_MESSAGEX));
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_LINEX), Long.toString(lineno));
      String headerErrorMsg = ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_HEADER_LENGTH);
      resp.setHeader(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_MESSAGEX), headerErrorMsg);

      //
      // Output the exported symbols in a map
      //
      
      Object exported = stack.getAttribute(WarpScriptStack.ATTRIBUTE_EXPORTED_SYMBOLS);
      
      if (exported instanceof Set && !((Set) exported).isEmpty()) {
        Map<String,Object> exports = new HashMap<String,Object>();
        Map<String,Object> symtable = stack.getSymbolTable();
        for (Object symbol: (Set) exported) {
          if (null == symbol) {
            exports.putAll(symtable);
            break;
          }
          exports.put(symbol.toString(), symtable.get(symbol.toString()));
        }
        try { stack.push(exports); if (debugDepth < Integer.MAX_VALUE) { debugDepth++; } } catch (WarpScriptException wse) {}
      }

      if(debugDepth > 0) {        
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        PrintWriter pw = resp.getWriter();
        
        try {
          // Set max stack depth to max int value - 1 so we can push our error message
          stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE - 1);
          stack.push("ERROR line #" + lineno + ": " + ThrowableUtils.getErrorMessage(t));
          if (debugDepth < Integer.MAX_VALUE) {
            debugDepth++;
          }
        } catch (WarpScriptException ee) {
        }

        try {
          // As the resulting JSON is streamed, there is no need to limit its size.
          StackUtils.toJSON(pw, stack, debugDepth, Long.MAX_VALUE);
        } catch (WarpScriptException ee) {
        }

      } else {
        // Check if the response is already committed. This may happen if the writer has already been written to and an
        // error happened during the write, for instance a stack overflow caused by infinite recursion.
        if(!resp.isCommitted()) {
          String prefix = "ERROR line #" + lineno + ": ";
          String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
          resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
        }
        return;
      }
    } finally {
      WarpScriptStackRegistry.unregister(stack);
      
      // Clear this metric in case there was an exception
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_REQUESTS, Sensision.EMPTY_LABELS, 1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_TIME_US, Sensision.EMPTY_LABELS, (long) ((System.nanoTime() - now) / 1000));
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_OPS, Sensision.EMPTY_LABELS, (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_OPS));
      
      //
      // Record the JVM free memory
      //
      
      Sensision.set(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_JVM_FREEMEMORY, Sensision.EMPTY_LABELS, Runtime.getRuntime().freeMemory());
      
      LoggingEvent event = LogUtil.setLoggingEventAttribute(null, LogUtil.WARPSCRIPT_SCRIPT, scriptSB.toString());
      
      event = LogUtil.setLoggingEventAttribute(event, LogUtil.WARPSCRIPT_TIMES, times);
      
      if (stack.isAuthenticated()) {
        event = LogUtil.setLoggingEventAttribute(event, WarpScriptStack.ATTRIBUTE_TOKEN, AUTHENTICATE.unhide(stack.getAttribute(WarpScriptStack.ATTRIBUTE_TOKEN).toString()));
      }
      
      if (null != t) {
        event = LogUtil.setLoggingEventStackTrace(event, LogUtil.STACK_TRACE, t);
        Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_ERRORS, Sensision.EMPTY_LABELS, 1);
      }
      
      LogUtil.addHttpHeaders(event, req);
      
      String msg = LogUtil.serializeLoggingEvent(this.keyStore, event);
      
      if (null != t) {
        EVENTLOG.error(msg);
      } else {
        EVENTLOG.info(msg);
      }
    }
  }
  
  public static final StoreClient getExposedStoreClient() {
    return exposedStoreClient;
  }
  
  public static final DirectoryClient getExposedDirectoryClient() {
    return exposedDirectoryClient;
  }
}
