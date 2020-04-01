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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.warp10.ThrowableUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.Revision;
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.BootstrapManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.LogUtil;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.KeyStore;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.script.WarpScriptStopException;

/**
 *
 */
public class EgressInteractiveHandler extends WebSocketHandler.Simple implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(EgressInteractiveHandler.class);
  private static final Logger EVENTLOG = LoggerFactory.getLogger("warpscript.events");
  
  private final KeyStore keyStore;
  private final StoreClient storeClient;
  private final DirectoryClient directoryClient;

  private final BootstrapManager bootstrapManager;
  
  private final ServerSocket serverSocket;
  
  private final AtomicInteger connections = new AtomicInteger(0);
  
  private int capacity = 1;
  
  @WebSocket
  public static class InteractiveWebSocket {
    
    private EgressInteractiveHandler handler = null;
    
    private InteractiveProcessor processor = null;
    
    @OnWebSocketConnect
    public void onWebSocketConnect(Session session) {
      if (this.handler.capacity <= this.handler.connections.get()) {
        try {
          session.getRemote().sendString("// Maximum server capacity is reached (" + this.handler.capacity + ").");
          session.disconnect();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
      
      this.handler.connections.incrementAndGet();

      this.processor.init(session);
    }
    
    @OnWebSocketMessage
    public void onWebSocketMessage(Session session, String message) throws Exception {
      message = message + "\n";
      this.processor.pipe(message.getBytes(StandardCharsets.UTF_8));
    }
    
    @OnWebSocketClose    
    public void onWebSocketClose(Session session, int statusCode, String reason) {
      try {
        this.processor.getPipe().close();
      } catch (IOException ioe) {        
      }
    }

    @OnWebSocketError        
    public void onWebSocketError(Session session, Throwable t) {}

    public void setProcessor(InteractiveProcessor processor) {
      this.processor = processor;
    }
    
    public void setHandler(EgressInteractiveHandler handler) {
      this.handler = handler;
    }
  }

  public EgressInteractiveHandler(KeyStore keyStore, Properties properties, DirectoryClient directoryClient, StoreClient storeClient) throws IOException {
    super(InteractiveWebSocket.class);

    this.keyStore = keyStore;
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    
    //
    // Check if we have a 'bootstrap' property
    //
    
    if (properties.containsKey(Configuration.CONFIG_WARPSCRIPT_INTERACTIVE_BOOTSTRAP_PATH)) {
      
      final String path = properties.getProperty(Configuration.CONFIG_WARPSCRIPT_INTERACTIVE_BOOTSTRAP_PATH);
      
      long period = properties.containsKey(Configuration.CONFIG_WARPSCRIPT_INTERACTIVE_BOOTSTRAP_PERIOD) ?  Long.parseLong(properties.getProperty(Configuration.CONFIG_WARPSCRIPT_BOOTSTRAP_PERIOD)) : 0L;
      this.bootstrapManager = new BootstrapManager(path, period);      
    } else {
      this.bootstrapManager = new BootstrapManager();
    }
    
    if (properties.containsKey(Configuration.CONFIG_WARPSCRIPT_INTERACTIVE_CAPACITY)) {
      capacity = Integer.parseInt(properties.getProperty(Configuration.CONFIG_WARPSCRIPT_INTERACTIVE_CAPACITY));
    }
    //
    // Listen for incoming connections
    //

    if (properties.containsKey(Configuration.CONFIG_WARPSCRIPT_INTERACTIVE_TCP_PORT)) {
      this.serverSocket = new ServerSocket(Integer.parseInt(properties.getProperty(Configuration.CONFIG_WARPSCRIPT_INTERACTIVE_TCP_PORT)));

      Thread t = new Thread(this);
      t.setDaemon(true);
      t.setName("[Interactive TCP Handler]");
      t.start();
    } else {
      this.serverSocket = null;
    }
  }
  
  @Override
  public void run() {
    while (true) {
      try {
        Socket connectionSocket = this.serverSocket.accept();
        
        if (this.capacity <= this.connections.get()) {
          PrintWriter pw = new PrintWriter(new OutputStreamWriter(connectionSocket.getOutputStream()));
          pw.println("// Maximum server capacity is reached (" + this.capacity + ").");
          pw.flush();
          LOG.error("Maximum server capacity is reached (" + this.capacity + ").");
          connectionSocket.close();
          continue;
        }
        
        this.connections.incrementAndGet();
        
        InteractiveProcessor processor = new InteractiveProcessor(this, connectionSocket, null, connectionSocket.getInputStream(), connectionSocket.getOutputStream());      
      } catch (IOException ioe) {          
      }
    }
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (Constants.API_ENDPOINT_INTERACTIVE.equals(target)) {
      baseRequest.setHandled(true);
      super.handle(target, baseRequest, request, response);
    }
  }
  
  @Override
  public void configure(final WebSocketServletFactory factory) {
    
    final EgressInteractiveHandler self = this;

    final WebSocketCreator oldcreator = factory.getCreator();
    
    WebSocketCreator creator = new WebSocketCreator() {
      @Override
      public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
        InteractiveWebSocket ws = (InteractiveWebSocket) oldcreator.createWebSocket(req, resp);
        ws.setHandler(self);
        try {
          ws.setProcessor(new InteractiveProcessor(self, null, null, null, null));
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        return ws;
      }
    };

    factory.setCreator(creator);
    super.configure(factory);
  }

  public static class InteractiveProcessor extends Thread {
    
    private final EgressInteractiveHandler rel;
    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private Session session;
    private final PrintWriter pw;
    private PipedOutputStream pipedOut = null;
    
    private MemoryWarpScriptStack stack;
    
    private final LinkedBlockingQueue<byte[]> pipeQueue = new LinkedBlockingQueue<byte[]>(2);
    
    public InteractiveProcessor(EgressInteractiveHandler rel, Socket socket, Session session, InputStream in, OutputStream out) throws IOException {
      this.rel = rel;
      this.socket = socket;
      
      if (null != in && null != out) {
        this.in = in;
        this.out = out;
      } else {
        this.pipedOut = new PipedOutputStream();
        this.in = new PipedInputStream(this.pipedOut);
        this.out = null;
        
        // Start the queue runner thread
        Thread queueRunner = new Thread() {
          @Override
          public void run() {
            while(true) {
              try {
                byte[] data = pipeQueue.take();
                pipedOut.write(data);
                pipedOut.flush();
              } catch (Exception e) {                
              }
            }
          }
        };
        queueRunner.setDaemon(true);
        queueRunner.start();
      }
      
      this.session = session;
      
      if (null != this.out) {
        this.pw = new PrintWriter(this.out);
      } else {
        this.pw = null;
      }
      
      // Delay start until we have a session when using WebSocket
      if (null != socket) {
        this.start();
      }
    }
    
    public void init(Session session) {
      this.session = session;
      this.start();
    }
    
    /**
     * Offer data to the pipe, this will be put
     * into a queue and dequeues by a stable Thread
     * so the PipedInputStream does not consider that
     * the writing Thread died.
     */
    public void pipe(byte[] data) throws InterruptedException {
      this.pipeQueue.put(data);
    }
    
    public PipedOutputStream getPipe() {
      return this.pipedOut;
    }
    
    public String getBanner() {
      StringBuilder sb = new StringBuilder();
      
      sb.append("//");
      sb.append("\n"); sb.append("//");
      sb.append("\n"); sb.append("//  ___       __                           ____________"); 
      sb.append("\n"); sb.append("//  __ |     / /_____ _______________      __<  /_  __ \\");
      sb.append("\n"); sb.append("//  __ | /| / /_  __ `/_  ___/__  __ \\     __  /_  / / /");
      sb.append("\n"); sb.append("//  __ |/ |/ / / /_/ /_  /   __  /_/ /     _  / / /_/ /"); 
      sb.append("\n"); sb.append("//  ____/|__/  \\__,_/ /_/    _  .___/      /_/  \\____/");  
      sb.append("\n"); sb.append("//                           /_/");
      sb.append("\n"); sb.append("//");

      sb.append("\n");
      sb.append("//  Revision ");
      sb.append(Revision.REVISION);
      sb.append("// ");
      sb.append("\n");
      
      return sb.toString();
    }

    public String getPrompt() {
      StringBuilder sb = new StringBuilder();
      
      sb.append("WS");
      
      if (stack.isInSecureScript()) {
        sb.append("S");
      }
      
      if (stack.isInComment()) {
        sb.append("#");
      }
      
      if (stack.isInMultiline()) {
        sb.append("'");
      }
      
      if (stack.getMacroDepth() > 0) {
        sb.append("%");
        sb.append(stack.getMacroDepth());
      }
      
      if (stack.depth() > 0) {
        sb.append("<");
        sb.append(stack.depth());
      }
      
      sb.append("> ");
      
      return sb.toString();
    }
    
    private static class PrintWriterWrapper extends PrintWriter {
      
      private final PrintWriter writer;
      private final Session session;
      
      public PrintWriterWrapper(PrintWriter writer, Session session) {
        super(System.out);
        this.writer = writer;
        this.session = session;
      }
      
      @Override
      public void print(String s) {
        if (null != this.writer) {
          this.writer.print(s);
        } else if (null != this.session) {
          this.session.getRemote().sendStringByFuture(s);
        }
      }

      @Override
      public void println(String s) {
        print(s + "\n");
      }
      
      @Override
      public void print(Object obj) {
        print(obj.toString());
      }
      
      @Override
      public void println(Object x) {
        println(x.toString());
      }
      
      @Override
      public void print(int i) {
        print(Integer.toString(i));
      }

      @Override
      public void println(int i) {
        println(Integer.toString(i));
      }
      
      @Override
      public void flush() {
        if (null != this.writer) {
          this.writer.flush();
        }
      }
    }
    
    @Override
    public void run() {      
      try {
        BufferedReader in = new BufferedReader(new InputStreamReader(this.in));
        PrintWriter out = new PrintWriterWrapper(null != this.out ? new PrintWriter(this.out) : null, session);

        out.print(getBanner());
        
        this.stack = new MemoryWarpScriptStack(this.rel.storeClient, this.rel.directoryClient);
        this.stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[EgressInteractiveHandler " + Thread.currentThread().getName() + "]");

        //
        // Store PrintWriter
        //
        
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_INTERACTIVE_WRITER, out);
        
        String uuid = UUID.randomUUID().toString();
        
        StackContext context = this.rel.bootstrapManager.getBootstrapContext();
        
        try {
          if (null != context) {
            stack.push(context);
            stack.restore();
          }

          //
          // Execute the bootstrap code
          //

          stack.exec(WarpScriptLib.BOOTSTRAP);
        } catch (Throwable t) {
          out.print("// ERROR ");
          out.println(WarpURLEncoder.encode(ThrowableUtils.getErrorMessage(t), StandardCharsets.UTF_8).replaceAll("%20", " ").replaceAll("%27", "'"));

          if (null != this.socket) {
            this.socket.close();
          }
          
          if (null != this.session) {
            this.session.disconnect();
          }
          return;
        }
                

        List<Long> times = new ArrayList<Long>(1);
        
        long seqno = 0;
        
        while(true) {
          
          // Output prompt
          
          out.print(getPrompt());
          out.flush();
          
          String line = in.readLine();
          
          seqno++;
                    
          if (null == line) {
            if (null != this.socket) {
              this.socket.close();
            }
            if (null != this.session) {
              this.session.disconnect();
            }
            return;
          }

          Throwable t = null;
          
          long nano = System.nanoTime();
          long time = 0L;
          
          try {
            stack.exec(line);
            
            time = System.nanoTime() - nano;
            
            if (stack.depth() > 0 && null != stack.getAttribute(WarpScriptStack.ATTRIBUTE_INTERACTIVE_ECHO)) {
              WarpScriptStackFunction npeek = (WarpScriptStackFunction) WarpScriptLib.getFunction("NPEEK");
              stack.push(stack.getAttribute(WarpScriptStack.ATTRIBUTE_INTERACTIVE_ECHO));
              out.println(" ");
              npeek.apply(stack);
              out.println(" ");
            }            
          } catch (WarpScriptStopException ese) {
            continue;
          } catch (Throwable te) {
            t = te;
            out.print("// ERROR ");
            out.println(WarpURLEncoder.encode(ThrowableUtils.getErrorMessage(t), StandardCharsets.UTF_8).replaceAll("%20", " ").replaceAll("%27", "'"));
          } finally {
            times.clear();
            times.add(System.nanoTime() - nano);
            
            LoggingEvent event = LogUtil.setLoggingEventAttribute(null, LogUtil.WARPSCRIPT_SCRIPT, line);
            event = LogUtil.setLoggingEventAttribute(event, LogUtil.WARPSCRIPT_TIMES, times);
            event = LogUtil.setLoggingEventAttribute(event, LogUtil.WARPSCRIPT_UUID, uuid);
            event = LogUtil.setLoggingEventAttribute(event, LogUtil.WARPSCRIPT_SEQNO, seqno);            
            
            if (stack.isAuthenticated()) {
              event = LogUtil.setLoggingEventAttribute(event, WarpScriptStack.ATTRIBUTE_TOKEN, stack.getAttribute(WarpScriptStack.ATTRIBUTE_TOKEN).toString());        
            }
            
            if (null != t) {
              event = LogUtil.setLoggingEventStackTrace(event, LogUtil.STACK_TRACE, t);
            }
            
            String msg = LogUtil.serializeLoggingEvent(this.rel.keyStore, event);
            
            if (null != t) {
              EVENTLOG.error(msg);
            } else {
              EVENTLOG.info(msg);
            }
            
            if (Boolean.TRUE.equals(stack.getAttribute(WarpScriptStack.ATTRIBUTE_INTERACTIVE_TIME))) {
              out.print("// TIME ");
              out.println(time + " ns");
            }
          }
        }        
      } catch (IOException ioe) {
        return;
      } finally {
        WarpScriptStackRegistry.unregister(stack);
        this.rel.connections.decrementAndGet();
        try {
          if (null != this.socket) {
            this.socket.close();
          }
          if (null != this.session) {
            this.session.disconnect();
          }
        } catch (IOException ioe) {          
        }
      }
    }
  }
}
