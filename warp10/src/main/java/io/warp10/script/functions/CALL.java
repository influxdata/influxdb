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

import io.warp10.WarpConfig;
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.Configuration;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Call a subprogram
 */
public class CALL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static int maxCapacity;
  
  static {
    maxCapacity = Integer.parseInt(WarpConfig.getProperty(Configuration.WARPSCRIPT_CALL_MAXCAPACITY, "1"));
  }
    
  private static class ProcessPool {
    
    private List<Process> processes = new ArrayList<Process>();
    
    private Map<Process,BufferedReader> readers = new HashMap<Process,BufferedReader>();
    
    private AtomicInteger loaned = new AtomicInteger(0);
    
    private ProcessBuilder builder;
    
    private int capacity = 0;

    public ProcessPool(String path) {
      this.builder = new ProcessBuilder(path);
    }

    public void provision() throws IOException {
            
      synchronized(processes) {
        if (capacity > 0 && processes.size() + loaned.get() >= capacity) {
          return;
        }
        
        //
        // Create a process, retrieving the configured capacity      
        //
      
        Process proc = this.builder.start();
        BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String cap = br.readLine();

        if (null == cap) {
          throw new RuntimeException("Subprogram '" + this.builder.command().toString() + "' did not return its configured capacity.");
        }
        
        this.capacity = Integer.parseInt(cap);
        
        if (this.capacity > maxCapacity || this.capacity < 0) {
          this.capacity = maxCapacity;
        }
        
        this.processes.add(proc);
        this.readers.put(proc, br);
      }
    }
    
    public Process get() throws IOException {
      Process proc = null;
      
      while(null == proc) {
        synchronized(processes) {
          if (processes.isEmpty()) {
            provision();
          }
          if (!processes.isEmpty()) {
            proc = processes.remove(0);
            if (isAlive(proc)) {
              this.loaned.addAndGet(1);
              return proc;
            } else {
              this.readers.remove(proc);
              proc = null;
            }
          }
        }
        
        LockSupport.parkNanos(100000L);
      }
      
      return proc;
    }
    
    public void release(Process proc) {      
      synchronized(processes) {
        if (isAlive(proc)) {
          processes.add(proc);
        } else {
          this.readers.remove(proc);
        }
        this.loaned.addAndGet(-1);
      }
    }
    
    public BufferedReader getReader(Process proc) {
      return this.readers.get(proc);
    }
  }
  
  /**
   * Map of subprogram name to process pool
   */
  private Map<String,ProcessPool> subprograms = new HashMap<String,ProcessPool>();
  
  public CALL(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object subprogram = stack.pop();
    Object args = stack.pop();
    
    if (!(subprogram instanceof String) || !(args instanceof String)) {
      throw new WarpScriptException(getName() + " expects a subprogram name on top of an argument string.");
    }
    
    init(subprogram.toString());
        
    int attempts = 2;
    
    while(attempts > 0) {
      Process proc = null;

      try {
        attempts--;
        
        proc = subprograms.get(subprogram).get();
        
        if (null == proc) {
          throw new WarpScriptException(getName() + " unable to acquire subprogram.");
        }
        
        //
        // Output the URLencoded string to the subprogram
        //
        
        proc.getOutputStream().write(WarpURLEncoder.encode(args.toString(), StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8));
        proc.getOutputStream().write('\n');
        proc.getOutputStream().flush();
        
        BufferedReader br = subprograms.get(subprogram).getReader(proc);
        
        String ret = br.readLine();

        if (null == ret) {
          throw new WarpScriptException(getName() + " subprogram died unexpectedly.");
        }
        
        //
        // If the return value starts with a space then it is considered an exception whose
        // URL encoded message is the rest of the line. The space cannot otherwise occur since
        // we URL encode the return values.
        //

        // Legit uses of URLDecoder.decode

        if (ret.startsWith(" ")) {
          throw new WarpScriptException(URLDecoder.decode(ret.substring(1), StandardCharsets.UTF_8.name()));
        }
        
        stack.push(URLDecoder.decode(ret, StandardCharsets.UTF_8.name()));
        
        break;
      } catch (IOException ioe) {
        if (attempts > 0) {
          continue;
        }
        throw new WarpScriptException(ioe);
      } finally {
        if (null != proc) {
          subprograms.get(subprogram).release(proc);
        }
      }
    }
    
    return stack;
  }
  
  private synchronized void init(String subprogram) throws WarpScriptException {
    if (this.subprograms.containsKey(subprogram)) {
      return;
    }
    
    String dir = WarpConfig.getProperty(Configuration.WARPSCRIPT_CALL_DIRECTORY);
    
    if (null == dir) {
      throw new WarpScriptException(getName() + " configuration key '" + Configuration.WARPSCRIPT_CALL_DIRECTORY + "' not set, " + getName() + " disabled.");
    }
    
    File root = new File(dir);
    File f = new File(root, subprogram);
    
    //
    // Check if the file exists
    //
    
    if (!f.exists() || !f.canExecute()) {
      throw new WarpScriptException(getName() + " invalid subprogram '" + subprogram + "'.");
    }
    
    //
    // Check if it is under 'root'
    //
    
    if (!f.getAbsolutePath().startsWith(root.getAbsolutePath())) {
      throw new WarpScriptException(getName() + " invalid subprogram, not in the correct directory.");
    }
    
    this.subprograms.put(subprogram, new ProcessPool(f.getAbsolutePath()));
  }
  
  private static final boolean isAlive(Process proc) {
    try {
      proc.exitValue();
      return false;
    } catch(IllegalThreadStateException e) {
      return true;
    }
  }
}
