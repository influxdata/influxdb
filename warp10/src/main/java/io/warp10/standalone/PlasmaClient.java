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

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

public class PlasmaClient extends Thread {
  
  private final String endpoint;
  
  private CountDownLatch closeLatch;

  private Session session = null;

  private final AtomicBoolean done = new AtomicBoolean(false);
  
  public PlasmaClient(String endpoint) {
    this.endpoint = endpoint;
    this.setDaemon(true);
    this.setName("[Plasma Client " + endpoint + "]");
    this.start();
  }

  public void subscribe(String token, String selector) {
    
  }
  
  public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
    return this.closeLatch.await(duration, unit);
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    this.session = null;
    this.closeLatch.countDown();
  }
 
  @OnWebSocketConnect
  public void onConnect(Session session) {
    this.session = session;
  }
 
  @OnWebSocketMessage
  public void onMessage(String msg) {
    System.out.printf("Got msg: %s%n", msg);
  }        

  @Override
  public void run() {
    
    while(true) {
      WebSocketClient client = null;
      
      try {
        client = new WebSocketClient();
        client.start();
        URI uri = new URI(this.endpoint);
        ClientUpgradeRequest request = new ClientUpgradeRequest();
        
        this.closeLatch = new CountDownLatch(1);
        this.session = null;
        
        client.connect(this, uri, request);
        
        //
        // Wait until we're connected to the endpoint
        //
        
        while(null == this.session) {
          Thread.sleep(1000L);
        }
        
        //
        // Wait until we're told to exit or the socket has closed
        //
        
        while(true) {
          Thread.sleep(1000L);
          //
          // Every now and then re-issue 'SUBSCRIBE' statements to the endpoint
          //
          if (null == this.session || this.done.get()) {
            return;
          }
        }
      } catch (Exception e) {
        try { this.awaitClose(1, TimeUnit.SECONDS); } catch (InterruptedException ie) {}
      } finally {
        try { client.stop(); } catch (Throwable t) {}
      }
    }
  }
}
