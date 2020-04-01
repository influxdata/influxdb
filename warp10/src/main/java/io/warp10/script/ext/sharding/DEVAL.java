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

package io.warp10.script.ext.sharding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.GZIPInputStream;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.functions.JSONTO;
import io.warp10.script.functions.SNAPSHOT;

/**
 * Distributed EVAL
 */
public class DEVAL extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final ExecutorService executor;
  
  private static final int maxThreadsPerRequest;
  
  private static Map<URL,Set<Long>> endpoints = new HashMap<URL,Set<Long>>();
  
  private static long shardmodulus;
  
  /**
   * Snapshot command to use
   */
  private static byte[] snapshot;
  
  private static JSONTO JSONTO;
  
  static {    
    snapshot = WarpConfig.getProperty(ShardingWarpScriptExtension.SHARDING_SNAPSHOT, WarpScriptLib.SNAPSHOT).trim().getBytes(StandardCharsets.UTF_8);
    
    int poolsize = Integer.parseInt(WarpConfig.getProperty(ShardingWarpScriptExtension.SHARDING_POOLSIZE, "4"));
    maxThreadsPerRequest = Integer.parseInt(WarpConfig.getProperty(ShardingWarpScriptExtension.SHARDING_MAXTHREADSPERCALL, Integer.toString(poolsize)));
        
    BlockingQueue<Runnable> queue = new LinkedBlockingDeque<Runnable>(poolsize * 2);
    
    executor = new ThreadPoolExecutor(poolsize, poolsize, 60, TimeUnit.SECONDS, queue);
    
    //
    // Scan the properties, identifying the endpoints
    //
    
    long shardmodulus = -1;        

    Properties props = WarpConfig.getProperties();

    for (Entry<Object,Object> entry: props.entrySet()) {
      if (!entry.getKey().toString().startsWith(ShardingWarpScriptExtension.SHARDING_ENDPOINT_PREFIX)) {
        continue;
      }
      // Extract shard spec (name.MODULUS:REMAINDER)
      
      String spec = entry.getKey().toString().substring(ShardingWarpScriptExtension.SHARDING_ENDPOINT_PREFIX.length());
      
      // Get rid of the prefix and name
      
      spec = spec.replaceAll(".*\\.", "");
      
      String[] tokens = spec.split(":");
      
      if (2 != tokens.length) {
        continue;
      }
      
      long modulus = Long.parseLong(tokens[0].trim());
      long remainder = Long.parseLong(tokens[1].trim());
      
      if (modulus <= 0 || remainder < 0 || remainder >= modulus) {
        continue;
      }
      
      if (-1L == shardmodulus) {
        shardmodulus = modulus;
      }
      
      if (modulus != shardmodulus) {
        throw new RuntimeException("Invalid modulus " + modulus + " for shard '" + entry.getKey() + "', should be " + shardmodulus);
      }
      
      try {
        URL url = new URL(entry.getValue().toString().trim());
        
        Set<Long> remainders = endpoints.get(url);
        
        if (null == remainders) {
          remainders = new HashSet<Long>();
          endpoints.put(url, remainders);
        }
        
        remainders.add(remainder);        
      } catch (MalformedURLException mue) {
        throw new RuntimeException(mue);
      }
    }

    //
    // Now check that we have all the remainders
    //
    
    Set<Long> allremainders = new HashSet<Long>();
    
    for (Set<Long> rems: endpoints.values()) {
      allremainders.addAll(rems);
    }
    
    if (shardmodulus != allremainders.size()) {
      throw new RuntimeException("Missing shards, only have " + allremainders.size() + " shards defined out of " + shardmodulus);
    }
    
    JSONTO = new JSONTO(WarpScriptLib.JSONTO);
  }
  
  public DEVAL(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " operates on a Macro.");
    }
    
    StringBuilder sb = new StringBuilder();
    
    SNAPSHOT.addElement(sb, top);
    
    sb.append(" ");
    sb.append(WarpScriptLib.EVAL);
    
    final String params = sb.toString();    
    final AtomicInteger pending = new AtomicInteger(0);    
    final AtomicBoolean aborted = new AtomicBoolean(false);
        
    // Get the endpoints and shuffle them
    List<URL> urls = new ArrayList<URL>(endpoints.keySet());
    Collections.shuffle(urls);
    Set<Long> remainders = new HashSet<Long>();
    
    List<URL> finalurls = new ArrayList<URL>();

    for (URL url: urls) {
      
      // If the current url is associated with remainders we already have, skip it
      
      if (remainders.containsAll(endpoints.get(url))) {
        continue;
      }
      
      remainders.addAll(endpoints.get(url));
      finalurls.add(url);

      // If we have enough URLs, bail out
      if (shardmodulus == remainders.size()) {
        break;
      }
    }
        
    @SuppressWarnings("unchecked")
    Future<String>[] futures = new Future[finalurls.size()];

    int i = 0;
    
    while(i < futures.length) {
      // Wait until we have less than maxThreadsPerRequest pending requests
      while(!aborted.get() && pending.get() >= maxThreadsPerRequest) {
        LockSupport.parkNanos(1000000);
      }
      
      if (aborted.get()) {
        break;
      }
      
      try {
        final URL endpoint = finalurls.get(i);
        futures[i] = executor.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
      
            if (aborted.get()) {
              throw new WarpScriptException("Execution aborted.");
            }
            
            HttpURLConnection conn = null;
            
            try {
              // Connect to the endpoint
              conn = (HttpURLConnection) endpoint.openConnection();
              conn.setChunkedStreamingMode(8192);
              conn.setRequestProperty("Accept-Encoding", "gzip");

              // Issue the command
              conn.setDoInput(true);
              conn.setDoOutput(true);
              conn.setRequestMethod("POST");
              
              OutputStream connout = conn.getOutputStream();
              OutputStream out = connout;
              
              out.write(params.getBytes(StandardCharsets.UTF_8));
              out.write('\n');
              out.write(snapshot);
              out.write('\n');
              
              connout.flush();
              
              InputStream in = conn.getInputStream();

              // Retrieve result
              if ("gzip".equals(conn.getContentEncoding())) {
                in = new GZIPInputStream(in);
              }
              
              if (HttpURLConnection.HTTP_OK != conn.getResponseCode()) {
                throw new WarpScriptException(getName() + " remote execution encountered an error: " + conn.getHeaderField(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_MESSAGEX)));
              }
              
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              
              byte[] buf = new byte[1024];
              
              while(true) {
                int len = in.read(buf);
                if (len < 0) {
                  break;
                }
                baos.write(buf, 0, len);
              }

              byte[] bytes = baos.toByteArray();
              
              String result = new String(bytes, StandardCharsets.UTF_8);
              
              return result;
            } catch (IOException ioe) {
              aborted.set(true);
              if (null != conn) {
                throw new IOException(conn.getResponseMessage());
              } else {
                throw ioe;
              }
            } finally {
              if (null != conn) {
                conn.disconnect();
              }
              pending.addAndGet(-1);
            }
          }
        });
        pending.addAndGet(1);
      } catch (RejectedExecutionException ree) {
        continue;
      }
      i++;
    }
    
    //
    // Wait until all tasks have completed or the execution was aborted
    //
    
    while(!aborted.get() && pending.get() > 0) {
      LockSupport.parkNanos(1000000L);
    }

    if (aborted.get()) {
      for (i = 0; i < futures.length; i++) {
        if (null != futures[i]) {
          try {
            futures[i].get();
          } catch (ExecutionException ee) {            
            throw new WarpScriptException(getName() + " execution was aborted.", ee);
          } catch (InterruptedException ie) {
            throw new WarpScriptException(getName() + " execution was interrupted.", ie);
          }
        }
      }
    }
    
    List<Object> results = new ArrayList<Object>();
    
    for (i = 0; i < futures.length; i++) {
      try {
        String result = futures[i].get();
        stack.push(result);
        // Unwrap the JSON
        JSONTO.apply(stack);
        results.add(stack.pop());
      } catch (ExecutionException ee) {
        throw new WarpScriptException(ee.getCause());
      } catch (WarpScriptException wse) {
        throw wse;
      } catch (Exception e) {        
        throw new WarpScriptException(e);
      }      
    }

    stack.push(results);
    
    return stack;
  }  
}
