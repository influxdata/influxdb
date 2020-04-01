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
package io.warp10.continuum;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.continuum.egress.EgressExecHandler;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.sensision.Sensision;

/**
 * This class periodically loads bootstrap code and exposes the resulting stack context
 */
public class BootstrapManager extends Thread {
  
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapManager.class);

  private StackContext bootstrapContext = null;
  
  private final String path;
  private final long period;
  
  public BootstrapManager() {
    this.path = null;
    this.period = 0L;
  }
  
  public BootstrapManager(String path, long period) {
    
    this.path = path;
    this.period = period;
    
    // Do the initial load
    
    loadBootstrap(this.path);
        
    this.setName("[BootstrapManager (" + path + ") every " + period + " ms]");
    this.setDaemon(true);
    this.start();
  }
  
  @Override
  public void run() {
    while(true) {
      long until = 0L == period ? Long.MAX_VALUE : (System.currentTimeMillis() + period);           
            
      while (System.currentTimeMillis() < until) {
        try { Thread.sleep(until - System.currentTimeMillis()); } catch (InterruptedException ie) {}                
      }
            
      loadBootstrap(path);
    }
  }
  
  private void loadBootstrap(String path) {
    
    long lineno = 0;
    
    try {
      BufferedReader br = new BufferedReader(new FileReader(path));
      
      MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        lineno++;
        
        stack.exec(line);
      }
      
      br.close();
      
      //
      // Retrieve the stack context
      //
      
      stack.save();
      
      StackContext context = (StackContext) stack.pop();
      
      //
      // Replace the current bootstrap context
      //
      
      this.bootstrapContext = context;
      
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_BOOTSTRAP_LOADS, Sensision.EMPTY_LABELS, 1);
    } catch (Exception e) {
      LOG.error("Error while loading bootstrap code [" + path + "] at line " + lineno + ". Current bootstrap code will not be replaced.", e);
    }
  }
  
  public StackContext getBootstrapContext() {
    return this.bootstrapContext;
  }
}
