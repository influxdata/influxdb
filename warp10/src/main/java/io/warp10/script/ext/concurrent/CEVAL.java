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

package io.warp10.script.ext.concurrent;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Execute a list of macros in a concurrent manner
 */
public class CEVAL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final String CONCURRENT_EXECUTION_ATTRIBUTE = "concurrent.execution";
  public static final String CONCURRENT_LOCK_ATTRIBUTE = "concurrent.lock";
  
  public CEVAL(String name) {
    super(name);    
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    //
    // Check if the stack is already in concurrent execution mode
    //
    
    if (Boolean.TRUE.equals(stack.getAttribute(CONCURRENT_EXECUTION_ATTRIBUTE))) {
      throw new WarpScriptException(getName() + " cannot be called from within a concurrent execution.");
    }
        
    Object top = stack.pop();
    
    if (!(top instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a parallelism level on top of the stack.");
    }

    int parallelism = ((Number) top).intValue();
    
    if (parallelism < 1) {
      throw new WarpScriptException(getName() + " parallelism level cannot be less than 1.");
    }
    
    top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of macros below the parallelism level.");
    }

    //
    // Check that all elements of the list are macros
    //
    
    for (Object o: (List) top) {
      if (!(o instanceof Macro)) {
        throw new WarpScriptException(getName() + " expects a list of macros below the parallelism level.");
      }
    }

    //
    // Limit parallelism to number of macros to run
    //
    
    int nmacros = ((List) top).size();
    if (parallelism > nmacros) {
      parallelism = nmacros;
    }
    
    ExecutorService executor = null;
    
    try {
      //
      // Create a Reentrant lock for optional synchronization
      //
      
      ReentrantLock lock = new ReentrantLock();
      
      stack.setAttribute(CONCURRENT_EXECUTION_ATTRIBUTE, true);
      stack.setAttribute(CONCURRENT_LOCK_ATTRIBUTE, lock);
      
      BlockingQueue<Runnable> queue = new LinkedBlockingDeque<Runnable>(nmacros);    
      executor = new ThreadPoolExecutor(parallelism, parallelism, 30, TimeUnit.SECONDS, queue);

      //
      // Copy the current stack context
      //
      
      stack.save();
      StackContext context = (StackContext) stack.pop();
      
      //
      // Submit each macro
      //
      
      final MemoryWarpScriptStack parentStack = (MemoryWarpScriptStack) stack;
      
      List<Future<List<Object>>> futures = new ArrayList<Future<List<Object>>>();
      
      final AtomicBoolean aborted = new AtomicBoolean(false);
      final AtomicInteger pending = new AtomicInteger(0);
      
      int idx = 0;
      
      for (Object o: (List) top) {
        idx++;
        final Macro macro = (Macro) o;
      
        //
        // Create a brand new stack and copy the context
        // We must also make sure that we call the parent's getAttribute and incOps so
        // various counters are common to all the executables 
        //
        
        final MemoryWarpScriptStack newstack = ((MemoryWarpScriptStack) stack).getSubStack();
        
        newstack.push(context);
        newstack.restore();
        
        final int myidx = idx;
        
        Callable task = new Callable<List<Object>>() {
          @Override
          public List<Object> call() throws Exception {
                        
            try {
              if (aborted.get()) {
                throw new WarpScriptException("Early abort.");
              }

              newstack.push(myidx);
              newstack.exec(macro);

              List<Object> results = new ArrayList<Object>();
              while(newstack.depth() > 0) {
                results.add(newstack.pop());
              }
              
              return results;
            } catch (Exception e) {
              aborted.set(true);
              if (e instanceof WarpScriptException) {
                throw e;
              } else {
                throw new WarpScriptException(e);
              }
            } finally {
              pending.addAndGet(-1);
            }            
          }        
        };
        
        pending.addAndGet(1);
        Future<List<Object>> future = executor.submit(task);
        futures.add(future);      
      }
            
      List<Object> results = new ArrayList<Object>();
      
      //
      // Wait until all tasks have completed or they were
      // aborted
      //
      
      while(!aborted.get() && pending.get() > 0) {
        LockSupport.parkNanos(100000000L);
      }
      
      //
      // Abort the executor abruptly if one of the jobs has failed
      //
      if (aborted.get()) {
        try { 
          executor.shutdownNow();
          executor = null;
        } catch (Throwable t) {          
        }
      }
      
      for (Future<List<Object>> future: futures) {
        try {
          if (future.isDone()) {
            results.add(future.get());            
          } else {
            results.add(null);
          }
        } catch (Exception e) {
          if (e.getCause() instanceof WarpScriptException) {
            throw (WarpScriptException) e.getCause();
          } else {
            throw new WarpScriptException(e.getCause());
          }
        }
      }
      
      stack.push(results);
    } finally {      
      if (null != executor) {
        executor.shutdownNow();
      }
      stack.setAttribute(CONCURRENT_EXECUTION_ATTRIBUTE, false);
      stack.setAttribute(CONCURRENT_LOCK_ATTRIBUTE, null);
    }

    return stack;
  }
}
