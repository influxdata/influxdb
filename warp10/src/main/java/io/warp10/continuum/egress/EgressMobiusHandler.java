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
import io.warp10.continuum.BootstrapManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.script.functions.EVERY;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.UpgradeResponse;
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

/**
 * Mobius allows WarpScript scripts to be periodically executed and their result
 * to be pushed back to the caller.
 * 
  */
public class EgressMobiusHandler extends WebSocketHandler.Simple implements Runnable {
  
  private final StoreClient storeClient;
  private final DirectoryClient directoryClient;
  private final BootstrapManager bootstrapManager;

  private int poolsize = 16;
  
  private static final String CONTEXT_SYMBOL = "context";
  
  private static final Comparator<Session> DEADLINE_COMPARATOR = new Comparator<Session>() {
    @Override
    public int compare(Session session1, Session session2) {
      Long deadline1 = deadlines.get(session1);
      Long deadline2 = deadlines.get(session2);
      
      if(null == deadline1) {
        if (null == deadline2) {
          return 0;
        } else {
          return -1;
        }
      } else if (null == deadline2) {
        // we also have null != deadline1
        return 1;
      } else {
        return Long.compare(deadline1, deadline2);
      }
    }      
  };
  
  private static final PriorityQueue<Session> scheduledRuns = new PriorityQueue<Session>(1024, DEADLINE_COMPARATOR);
  
  /**
   * Macros to be scheduled
   */
  private static final Map<Session,Macro> macros = new HashMap<Session, Macro>();
  
  /**
   * Deadline of the next run
   */
  private static final Map<Session,Long> deadlines = new HashMap<Session, Long>();
  
  /**
   * Context carried over from run to run
   */
  private static final Map<Session,Object> contexts = new HashMap<Session, Object>();

  @WebSocket
  public static class MobiusWebSocket {
    
    private EgressMobiusHandler mobius = null;
    
    @OnWebSocketConnect
    public void onWebSocketConnect(Session session) {
    }
    
    @OnWebSocketMessage
    public void onWebSocketMessage(Session session, String message) throws Exception {
      
      if("ABORT".equals(message)) {
        //
        // Abort current background WarpScript execution
        //
        
        mobius.removeSession(session);
        
        return;
      }
      
      //
      // Build a macro
      //
      
      StringBuilder sb = new StringBuilder();
      sb.append(WarpScriptStack.MACRO_START);
      sb.append(" ");
      sb.append(message);
      sb.append("\n");
      sb.append(WarpScriptStack.MACRO_END);
      
      //
      // Execute WarpScript so we retrieve the macro
      //
      
      WarpScriptStack stack = new MemoryWarpScriptStack(null, null);           

      boolean error = false;
      
      try {
        //
        // Replace the context with the bootstrap one
        //
        
        if (null != this.mobius) {
          StackContext context = this.mobius.bootstrapManager.getBootstrapContext();
              
          if (null != context) {
            stack.push(context);
            stack.restore();
          }
              
          //
          // Execute the bootstrap code
          //

          stack.exec(WarpScriptLib.BOOTSTRAP);
        }

        stack.execMulti(sb.toString());
      } catch (WarpScriptException e) {
        stack.push(e.getMessage());
      }

      //
      // Pop the resulting macro off the stack
      //
      
      Macro macro = null;
      
      if (stack.peek() instanceof Macro) {
        macro = (Macro) stack.pop();
      }

      //
      // Return content of the stack post macro parsing
      // This may contain an error, otherwise will be []
      //
      
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      
      StackUtils.toJSON(pw, stack);

      pw.flush();
      session.getRemote().sendStringByFuture(sw.toString());
      
      if (null != macro) {
        //
        // Schedule macro        
        //
        synchronized(macros) {
          // Remove current scheduled execution for this session
          scheduledRuns.remove(session);
          macros.put(session, macro);
          deadlines.put(session, System.currentTimeMillis());
          contexts.put(session, null);
          scheduledRuns.add(session);
        }
      }      
    }
    
    @OnWebSocketClose    
    public void onWebSocketClose(Session session, int statusCode, String reason) {
      mobius.removeSession(session);
    }

    @OnWebSocketError        
    public void onWebSocketError(Session session, Throwable t) {
      mobius.removeSession(session);
    }

    public void setMobiusHandler(EgressMobiusHandler mobius) {
      this.mobius = mobius;
    }
  }
  
  public EgressMobiusHandler(StoreClient storeClient, DirectoryClient directoryClient, Properties properties) {
    super(MobiusWebSocket.class);
    
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    
    //
    // Check if we have a 'bootstrap' property
    //
    
    if (properties.containsKey(Configuration.CONFIG_WARPSCRIPT_MOBIUS_BOOTSTRAP_PATH)) {
      
      final String path = properties.getProperty(Configuration.CONFIG_WARPSCRIPT_MOBIUS_BOOTSTRAP_PATH);
      
      long period = properties.containsKey(Configuration.CONFIG_WARPSCRIPT_MOBIUS_BOOTSTRAP_PERIOD) ?  Long.parseLong(properties.getProperty(Configuration.CONFIG_WARPSCRIPT_MOBIUS_BOOTSTRAP_PERIOD)) : 0L;
      this.bootstrapManager = new BootstrapManager(path, period);      
    } else {
      this.bootstrapManager = new BootstrapManager();
    }

    if (properties.containsKey(Configuration.CONFIG_WARPSCRIPT_MOBIUS_POOL)) {
      this.poolsize = Integer.parseInt(properties.getProperty(Configuration.CONFIG_WARPSCRIPT_MOBIUS_POOL));
    }
            
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("[MobiusHandler]");
    t.start();
  }
        
  public DirectoryClient getDirectoryClient() {
    return this.directoryClient;
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (Constants.API_ENDPOINT_MOBIUS.equals(target)) {
      baseRequest.setHandled(true);
      super.handle(target, baseRequest, request, response);
    }
  }
  
  @Override
  public void configure(final WebSocketServletFactory factory) {
    
    final EgressMobiusHandler self = this;

    final WebSocketCreator oldcreator = factory.getCreator();
    
    WebSocketCreator creator = new WebSocketCreator() {
      @Override
      public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
        MobiusWebSocket ws = (MobiusWebSocket) oldcreator.createWebSocket(req, resp);
        ws.setMobiusHandler(self);
        return ws;
      }
    };

    factory.setCreator(creator);
    super.configure(factory);
  }
  
  
  @Override
  public void run() {
    
    //
    // Configure executor
    //
    
    Executor executor = new ThreadPoolExecutor(poolsize, poolsize, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1 + (poolsize >>> 1)));
    
    while (true) {
      
      Long deadline = deadlines.get(scheduledRuns.peek());
      
      Sensision.set(SensisionConstants.CLASS_WARP_MOBIUS_ACTIVE_SESSIONS, Sensision.EMPTY_LABELS, scheduledRuns.size());

      if (null == deadline || deadline - System.currentTimeMillis() > 100L) {
        LockSupport.parkNanos(99000000L);
      } else {
        long delay = deadline - System.currentTimeMillis() - 1L;
        if (delay > 0) {
          LockSupport.parkNanos(delay * 1000000L);
        }
      }
      
      //
      // Retrieve the head of the priority queue
      // We don't do a peek because we would not be able to
      // remove efficiently 'session' from the queue if it
      // must indeed be run now
      //
      
      Session session = scheduledRuns.poll();
      
      if (null == session) {
        continue;
      }

      //
      // Check if session is still open, if not, remove it and continue
      //
      
      if (!session.isOpen()) {
        try { session.disconnect(); } catch (IOException ioe) {}
        removeSession(session);
        continue;
      }
      
      //
      // Check deadline
      //
      
      Macro macro = null;
      
      synchronized(macros) {
        macro = macros.get(session);
        deadline = deadlines.get(session);
      }
      
      if(null == macro || null == deadline) {
        continue;
      }

      final long now = System.currentTimeMillis();
      
      if (deadline > now) {
        // Reschedule the session, it's too early for now
        scheduledRuns.add(session);
        continue;
      }
      
      //
      // Schedule run of 'macro'
      //
      
      final Macro fmacro = macro;
      final Session fsession = session;
      
      Runnable runner = new Runnable() {
        @Override
        public void run() {
          
          long nano = System.nanoTime();
          
          //
          // Create a stack
          //
          
          WarpScriptStack stack = new MemoryWarpScriptStack(storeClient, directoryClient);
          stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[EgressMobiusHandler " + Thread.currentThread().getName() + "]");

          boolean error = false;
          
          try {
            //
            // Push context
            //
            
            Object o = contexts.get(fsession);
            
            if (null != o) {
              stack.store(CONTEXT_SYMBOL, o);
            }
            
            //
            // Execute macro
            //
            
            stack.exec(fmacro);
            
          } catch (Exception e) {
            error = true;
            try { stack.push(e.getMessage()); } catch (WarpScriptException ee) {}
          } finally {
            WarpScriptStackRegistry.unregister(stack);
          }

          //
          // Save context if needed
          //
          
          if (null != stack.load(CONTEXT_SYMBOL)) {
            contexts.put(fsession, stack.load(CONTEXT_SYMBOL));
          } else {
            contexts.remove(fsession);
          }
          
          //
          // Reschedule macro if needed
          //
    
          if (null != stack.getAttribute(EVERY.EVERY_STACK_ATTRIBUTE)) {
            synchronized(macros) {
              macros.put(fsession, fmacro);
              deadlines.put(fsession, now + (long) stack.getAttribute(EVERY.EVERY_STACK_ATTRIBUTE));
              scheduledRuns.add(fsession);                
            }
          }

          nano = System.nanoTime() - nano;
          
          Sensision.update(SensisionConstants.CLASS_WARP_MOBIUS_MACROS_EXECUTIONS, Sensision.EMPTY_LABELS, 1);
          Sensision.update(SensisionConstants.CLASS_WARP_MOBIUS_MACROS_TIME_NANOS, Sensision.EMPTY_LABELS, nano);
          Sensision.update(SensisionConstants.CLASS_WARP_MOBIUS_MACROS_ERRORS, Sensision.EMPTY_LABELS, error ? 1 : 0);
          
          //
          // Output result
          //
          
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          
          try { StackUtils.toJSON(pw, stack); } catch (WarpScriptException | IOException ee) {}
          
          pw.flush();
          
          fsession.getRemote().sendStringByFuture(sw.toString());
        }
      };
      
      try {
        executor.execute(runner);
      } catch (RejectedExecutionException ree) {
        synchronized(macros) {
          scheduledRuns.add(session);
        }
      }
    }
  }
  
  private void removeSession(Session session) {
    synchronized(macros) {
      scheduledRuns.remove(session);
      macros.remove(session);
      deadlines.remove(session);
      contexts.remove(session);
    }
  }
}
