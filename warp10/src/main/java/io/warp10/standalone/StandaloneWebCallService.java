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

package io.warp10.standalone;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.WebAccessController;
import io.warp10.script.thrift.data.WebCallMethod;
import io.warp10.script.thrift.data.WebCallRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class StandaloneWebCallService extends Thread {
  
  /**
   * User agent to use when making calls
   */
  private static final String ua;
  
  private static boolean launched = false;

  private static final WebAccessController webAccessController;
  
  static {
    //
    // Read properties to set up proxy etc
    //
    
    ua = WarpConfig.getProperty(Configuration.WEBCALL_USER_AGENT);

    String patternConf = WarpConfig.getProperty(Configuration.WEBCALL_HOST_PATTERNS);

    webAccessController = new WebAccessController(patternConf);
  }

  private static final ArrayBlockingQueue<WebCallRequest> requests = new ArrayBlockingQueue<WebCallRequest>(1024);

  public static WebAccessController getWebAccessController() {
    return webAccessController;
  }
  
  public static synchronized boolean offer(WebCallRequest request) {
    //
    // Launch service if not done yet
    //
    
    if (!launched) {
      launch();
    }
    
    try {
      return requests.offer(request, 10, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      return false;
    }
  }
  
  private static void launch() {
    Thread t = new StandaloneWebCallService();
    t.setDaemon(true);
    t.start();
    launched = true;
  }
  
  @Override
  public void run() {
    
    // FIXME(hbs): use an executor to spawn multiple requests in parallel?
    // maybe we only need to do that in the production setup with a dedicated daemon which
    // reads WebCallRequests off of Kafka
    
    //
    // Loop endlessly, emptying the queue and pushing requests
    //
    
    while(true) {
      WebCallRequest request = requests.poll();
      
      //
      // Sleep some if queue was empty
      //
      
      if (null == request) {
        try { Thread.sleep(100L); } catch (InterruptedException ie) {}
        continue;
      }

      doCall(request);
    }
  }
  
  public static void doCall(WebCallRequest request) {
    //
    // Build the URLConnection 
    //
    
    HttpURLConnection conn = null;
    
    try {
      
      URL url = new URL(request.getUrl());

      if (!webAccessController.checkURL(url)) {
        return;
      }
            
      conn = (HttpURLConnection) url.openConnection();
      
      //
      // Add headers
      //
      
      if (request.getHeadersSize() > 0) {
        for (Entry<String,String> entry: request.getHeaders().entrySet()) {
          conn.addRequestProperty(entry.getKey(), entry.getValue());
        }          
      }
      
      if (null != ua) {
        conn.addRequestProperty("User-Agent", ua);
      }
      
      conn.addRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_WEBCALL_UUIDX), request.getWebCallUUID());
            
      //
      // If issuing a POST request, set doOutput
      //
      
      if (WebCallMethod.POST == request.getMethod()) {
        conn.setDoOutput(true);
        // Make sure we use chunking
        conn.setChunkedStreamingMode(2048);
      } else {
        conn.setDoOutput(false);
      }

      //
      // Connect
      //
      
      conn.connect();
      
      if (WebCallMethod.POST == request.getMethod()) {
        OutputStream os = conn.getOutputStream();
        
        os.write(request.getBody().getBytes(StandardCharsets.UTF_8));
        os.close();
      }
      
      //
      // Retrieve response
      //
      
      int code = conn.getResponseCode();                
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }    
  }
}
