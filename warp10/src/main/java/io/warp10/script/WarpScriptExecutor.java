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

package io.warp10.script;

import io.warp10.WarpConfig;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.script.functions.EVAL;
import io.warp10.script.functions.SNAPSHOT;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.WarpScriptJavaFunction;

import org.apache.hadoop.util.Progressable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class WarpScriptExecutor implements Serializable {

  private Progressable progressable;
  
  private StackContext bootstrapContext = null;
  private String bootstrapCode = null;
  
  public static enum StackSemantics {
    // Single stack which will be shared among calling threads
    SYNCHRONIZED,
    // One stack per thread
    PERTHREAD,
    // A new stack for every execution
    NEW,
  };

  /**
   * Macro this executor will tirelessly apply
   */
  private Macro macro;

  /**
   * Code used to define the macro
   */
  private String macroCode = null;
  
  /**
   * Single stack to use when StackSemantics is SYNCHRONIZED, null otherwise
   */
  private WarpScriptStack stack;
  
  /**
   * Semaphore for synchronizing threads
   */
  private Semaphore sem;

  private StackSemantics semantics;
  
  private Map<String,Object> symbolTable;  
  
  private static Properties properties;
  
  private ThreadLocal<WarpScriptStack> perThreadStack = null;

  static {
    //
    // Initialize WarpConfig
    //
  
    try {
      String config = System.getProperty(WarpConfig.WARP10_CONFIG);
      
      if (null == config) {
        config = System.getenv(WarpConfig.WARP10_CONFIG_ENV);
      }
      
      WarpConfig.safeSetProperties((String) config);
      properties = WarpConfig.getProperties();
      WarpScriptLib.registerExtensions();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public WarpScriptExecutor() {
  }
  
  public WarpScriptExecutor(StackSemantics semantics, String script) throws WarpScriptException {
    this(semantics, script, null);
  }

  public WarpScriptExecutor(StackSemantics semantics, String script, Map<String,Object> symbols) throws WarpScriptException {
    this(semantics,script,symbols,null);
  }

  public WarpScriptExecutor(StackSemantics semantics, String script, Map<String,Object> symbols, Progressable progressable) throws WarpScriptException {
    this(semantics, script, symbols, progressable, true);
  }
  
  public WarpScriptExecutor(StackSemantics semantics, String script, Map<String,Object> symbols, Progressable progressable, boolean wrapInMacro) throws WarpScriptException {

    /**
     * Semantics of the stack
     */
    this.semantics = semantics;
    
    /**
     * Hadoop progressable to update if running under Pig for example
     */
    this.progressable = progressable;
    
    //
    // Load bootstrap
    //
    
    loadBootstrap(null);
    
    //
    // Initialize the stack
    //
    
    initStack();
    
    //
    // Attempt to execute the script code on the stack
    // to define a macro
    //
      
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());
    stack.maxLimits();
    if (null != this.progressable) {
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, this.progressable);
    }
    
    //
    // Execute bootstrap code
    //
    try {
      stack.exec(WarpScriptLib.BOOTSTRAP);
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }

    this.symbolTable = new HashMap<String,Object>();
    
    if (null != symbols) {
      this.symbolTable.putAll(symbols);
    }
        
    StringBuilder sb = new StringBuilder();

    if (wrapInMacro) {
      sb.append(WarpScriptStack.MACRO_START);
      sb.append("\n");
    }
    
    sb.append(script);

    if (wrapInMacro) {
      sb.append("\n");
      sb.append(WarpScriptStack.MACRO_END);
    }

    //
    // Force the symbol table
    //
    
    stack.save();
    MemoryWarpScriptStack.StackContext context = (MemoryWarpScriptStack.StackContext) stack.peek();
    context.symbolTable.putAll(this.symbolTable);
    stack.restore();

    //
    // Execute the script
    //
    
    stack.execMulti(sb.toString());
      
    if (1 != stack.depth()) {
      throw new WarpScriptException("Stack depth was not 1 after the code execution.");
    }
    
    if (!(stack.peek() instanceof Macro)) {
      throw new WarpScriptException("No macro was found on top of the stack.");
    }
    
    Macro macro = (Macro) stack.pop();
    
    this.macro = macro;
    this.macroCode = sb.toString();
  }

  private void initStack() {
    
    this.perThreadStack = new ThreadLocal<WarpScriptStack>() {
      @Override
      protected WarpScriptStack initialValue() {
        MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, properties);
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[WarpScriptExecutor " + semantics.name() + "]");
        stack.maxLimits();
        if (null != progressable) {
          stack.setAttribute(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, progressable);
        }
        try {
          loadBootstrap(stack);
          stack.exec(WarpScriptLib.BOOTSTRAP);
        } catch (WarpScriptException e) {
          throw new RuntimeException(e);
        }

        return stack;
      }
    };
    
    if (StackSemantics.SYNCHRONIZED.equals(semantics)) {
      this.stack = perThreadStack.get();
      if (null != this.progressable) {
        this.stack.setAttribute(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, progressable);
      }
      this.sem = new Semaphore(1);
    } else {
      this.stack = null;
      this.sem = new Semaphore(Integer.MAX_VALUE);
    }
  }
  
  /**
   * Execute the embedded macro on the given stack content.
   * 
   * @param input Stack content to push on the stack, index 0 is the top of the stack.
   * @return Return the state of the stack post execution, index 0 is the top of the stack.
   * @throws WarpScriptException
   */
  public List<Object> exec(List<Object> input) throws WarpScriptException {
    try {
      this.sem.acquire();
    } catch (InterruptedException ie) {
      throw new WarpScriptException("Got interrupted while attempting to acquire semaphore.", ie);
    }
    
    WarpScriptStack stack = this.stack;

    try {
      if (null == stack) {
        stack = getStack();
      }
      
      //
      // Update the symbol table if 'symbolTable' is set
      //
      
      if (null != this.symbolTable) {
        stack.save();
        MemoryWarpScriptStack.StackContext context = (MemoryWarpScriptStack.StackContext) stack.peek();
        context.symbolTable.putAll(this.symbolTable);
        stack.restore();
      }
      
      //
      // Push the parameters onto the stack
      //
      
      for (int i = input.size() - 1; i >= 0; i--) {
        stack.push(input.get(i));
      }
      
      //
      // Execute the macro
      //
      
      try {
        stack.exec(this.macro);
      } catch (WarpScriptStopException wsse) {
        // We catch those as they only mean the script terminated voluntarily early
      }
      
      //
      // Pop the output off the stack
      //
      
      List<Object> output = new ArrayList<Object>();
      
      while(stack.depth() > 0) {
        output.add(stack.pop());
      }
      
      return output;
    } finally {
      if (null != stack) {
        // Clear the stack
        stack.clear();
      }
      // Unregister the stack if it is allocated per call to exec
      if (StackSemantics.NEW.equals(this.semantics)) {
        WarpScriptStackRegistry.unregister(stack);
      }
      this.sem.release();
    }
  }
  
  private WarpScriptStack getStack() throws WarpScriptException {
    WarpScriptStack stack = null;
    
    if (null != this.stack) {
      return this.stack;
    }
    
    if (StackSemantics.PERTHREAD.equals(this.semantics)) {
      stack = perThreadStack.get();
    } else if (StackSemantics.NEW.equals(this.semantics)) {
      stack = new MemoryWarpScriptStack(null, null, properties);
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[WarpScriptExecutor NEW]");
      ((MemoryWarpScriptStack) stack).maxLimits();
      if (null != this.progressable) {
        stack.setAttribute(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, this.progressable);
      }
      try {
        stack.exec(WarpScriptLib.BOOTSTRAP);
      } catch (WarpScriptException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new WarpScriptException("Invalid stack semantics.");
    }
    return stack;
  }
  
  /**
   * Store a symbol in the symbol table.
   */
  public WarpScriptExecutor store(String key, Object value) {
    this.symbolTable.put(key, value);
    return this;
  }
  
  private void writeObject(java.io.ObjectOutputStream stream) throws IOException {
    try {
      stream.writeUTF(semantics.name());
      //
      // Write bootstrap code
      //    
      if (null != this.bootstrapCode) {
        stream.writeUTF(this.bootstrapCode);
      } else {
        stream.writeUTF("");
      }
      
      //
      // Write stack content and current symbol table
      //
      SNAPSHOT snapshot = new SNAPSHOT(WarpScriptLib.SNAPSHOTALL, true, false, true, false);

      if (null != this.stack) {

        snapshot.apply(this.stack);

        String stackSnapshot = this.stack.pop().toString();
        
        stream.writeUTF(stackSnapshot);        
      } else {
        stream.writeUTF("");
      }
      //
      // Write symbol table
      //
      
      MemoryWarpScriptStack tmpstack = new MemoryWarpScriptStack(null, null, new Properties());
      tmpstack.maxLimits();
      tmpstack.save();
      MemoryWarpScriptStack.StackContext context = (MemoryWarpScriptStack.StackContext) tmpstack.peek();
      context.symbolTable.putAll(this.symbolTable);
      tmpstack.restore();

      snapshot.apply(tmpstack);
      
      stream.writeUTF(tmpstack.pop().toString());
      
      //
      // Write macro
      //
      stream.writeUTF(this.macroCode);      
    } catch (WarpScriptException wse) {
      throw new IOException(wse);
    }
  }

  private void readObject(java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException {
    //
    // Read stack semantics
    //
    
    this.semantics = StackSemantics.valueOf(stream.readUTF());
    
    //
    // Read bootstrap
    //
    String bootstrap = stream.readUTF();
    
    if (!"".equals(bootstrap)) {
      int lineno = 0;
      
      try {
        BufferedReader br = new BufferedReader(new StringReader(bootstrap));
        
        MemoryWarpScriptStack stck = new MemoryWarpScriptStack(null, null, new Properties());
        stck.maxLimits();
        
        while(true) {
          String line = br.readLine();
          
          if (null == line) {
            break;
          }
          
          lineno++;
          
          stck.exec(line);
        }
        
        br.close();
        
        //
        // Retrieve the stack context
        //
        
        stck.save();
        
        StackContext context = (StackContext) stck.pop();
        
        //
        // Replace the current bootstrap context
        //
        
        this.bootstrapContext = context;
        this.bootstrapCode = bootstrap;
      } catch (Exception e) {
        throw new IOException("Exception while loading bootstrap code at line " + lineno, e);
      }           
    }
    
    //
    // Initialize stack
    //
    
    initStack();
    
    //
    // Read stack state and symbol table
    //
    
    String state = stream.readUTF();
    
    EVAL eval = new EVAL(WarpScriptLib.EVAL);

    if (!"".equals(state) && null != this.stack) {
      try {
        this.stack.push(state);
        eval.apply(stack);        
      } catch (WarpScriptException wse) {
        throw new IOException(wse);
      }
    }
    
    //
    // Read symbol table
    //
    
    String st = stream.readUTF();
    
    try {
      MemoryWarpScriptStack tmpstack = new MemoryWarpScriptStack(null, null, new Properties());
      tmpstack.maxLimits();
      tmpstack.push(st);
      eval.apply(tmpstack);
      tmpstack.save();
      MemoryWarpScriptStack.StackContext context = (MemoryWarpScriptStack.StackContext) tmpstack.peek();
      this.symbolTable = context.symbolTable;
    } catch (WarpScriptException wse) {
      throw new IOException(wse);
    }

    //
    // Read macro
    //
    
    String macro = stream.readUTF();
    
    try {
      MemoryWarpScriptStack tmpstack = new MemoryWarpScriptStack(null, null, new Properties());
      tmpstack.maxLimits();
      
      tmpstack.save();
      MemoryWarpScriptStack.StackContext context = (MemoryWarpScriptStack.StackContext) tmpstack.peek();
      context.symbolTable.putAll(this.symbolTable);
      tmpstack.restore();

      
      tmpstack.push(macro);
      eval.apply(tmpstack);
      
      this.macro = (Macro) tmpstack.pop();
    } catch (WarpScriptException wse) {
      throw new IOException(wse);
    }
  }
  
  private void loadBootstrap(WarpScriptStack stack) throws WarpScriptException {
    if (null != bootstrapContext) {
      if (null != stack) {
        stack.push(this.bootstrapContext);
        stack.restore();
      }
      return;
    }
    
    String bootstrap = System.getProperty(WarpConfig.WARPSCRIPT_BOOTSTRAP);

    if (null == bootstrap) {
      return;
    }
    
    StringBuilder sb = new StringBuilder();
    
    if (bootstrap.startsWith("@")) {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(bootstrap.substring(1))));
        
        while(true) {
          String line = reader.readLine();
          if (null == line) {
            break;
          }
          sb.append(line);
          sb.append("\n");
        }
        
        reader.close();        
      } catch (IOException ioe) {
        throw new WarpScriptException(ioe);
      }
    } else {
      sb.append(bootstrap);
    }
    
    String code = sb.toString();
    
    int lineno = 0;
    
    try {
      BufferedReader br = new BufferedReader(new StringReader(code));
      
      MemoryWarpScriptStack stck = new MemoryWarpScriptStack(null, null, new Properties());
      stck.maxLimits();
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        lineno++;
        
        stck.exec(line);
      }
      
      br.close();
      
      //
      // Retrieve the stack context
      //
      
      stck.save();
      
      StackContext context = (StackContext) stck.pop();
      
      //
      // Replace the current bootstrap context
      //
      
      this.bootstrapContext = context;
      this.bootstrapCode = code;
      
      if (null != stack) {
        stack.push(this.bootstrapContext);
        stack.restore();
      }
    } catch (Exception e) {
      throw new WarpScriptException("Exception while loading bootstrap code at line " + lineno, e);
    }   
  }
}
