//
//   Copyright 2018-20  SenX S.A.S.
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.ThrowableUtils;
import io.warp10.WarpConfig;
import io.warp10.WarpDist;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.functions.INCLUDE;
import io.warp10.script.functions.MSGFAIL;
import io.warp10.sensision.Sensision;

/**
 * Class which manages file based WarpScript macros from a directory
 */
public class WarpScriptMacroRepository extends Thread {
  
  private static final Logger LOG = LoggerFactory.getLogger(WarpScriptMacroRepository.class);
  
  private static final MSGFAIL MSGFAIL_FUNC = new MSGFAIL("MSGFAIL");
  
  /**
   * Default refresh delay is 60 minutes
   */
  private static final long DEFAULT_DELAY = 3600000L;

  /**
   * Default TTL for macros loaded on demand
   */
  private static final long DEFAULT_MACRO_TTL = 600000L;
  
  /**
   * Default TTL for macros which failed loading
   */
  private static final long DEFAULT_FAILED_MACRO_TTL = 10000L;
  
  private static long[] SIP_KEYS = { 31232312312312L, 543534535435L };
  
  public static final String WARPSCRIPT_FILE_EXTENSION = ".mc2";
  
  /**
   * Directory where the '.mc2' files are
   */
  private static String directory;
  
  /**
   * How often to check for changes
   */
  private static long delay = DEFAULT_DELAY;
  
  /**
   * Default TTL for loaded macros
   */
  private static long ttl = DEFAULT_MACRO_TTL;
  
  /**
   * Default TTL for failed macros
   */
  private static long failedTtl = DEFAULT_FAILED_MACRO_TTL;
  
  /**
   * List of macro names to avoid loops in macro loading
   */
  private static ThreadLocal<List<String>> loading = new ThreadLocal<List<String>>() {
    @Override
    protected List<String> initialValue() {
      return new ArrayList<String>();
    }
  };
  
  /**
   * Should we enable loading macros on demand
   */
  private static boolean ondemand = false;
  
  /**
   * Actual macros
   */
  private final static Map<String,Macro> macros;
 
  private static final int DEFAULT_CACHE_SIZE = 10000;
  
  private static final int maxcachesize;
  
  static {
    //
    // Create macro map
    //
    
    maxcachesize = Integer.parseInt(WarpConfig.getProperty(Configuration.REPOSITORY_CACHE_SIZE, Integer.toString(DEFAULT_CACHE_SIZE)));
    
    macros = new LinkedHashMap<String,Macro>() {
      @Override
      protected boolean removeEldestEntry(java.util.Map.Entry<String,Macro> eldest) {
        int size = this.size();
        Sensision.set(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_REPOSITORY_MACROS, Sensision.EMPTY_LABELS, size);
        return size > maxcachesize;
      }
    };
  }
  private WarpScriptMacroRepository() {       
    this.setName("[Warp Macro Repository (" + directory + ")");
    this.setDaemon(true);
    this.start();
  }
    
  @Override
  public void run() {
    
    //
    // Wait until Continuum has initialized
    //
    
    while(!WarpDist.isInitialized()) {
      try { Thread.sleep(100L); } catch (InterruptedException ie) {}
    }
    
    //
    // Exit now if we do not run an 'egress' service
    //
    
    if (!WarpDist.isEgress()) {
      return;
    }
    
    while(true) {
            
      String rootdir = new File(this.directory).getAbsolutePath();
      
      //
      // Open directory
      //
      
      List<File> files = getWarpScriptFiles(this.directory);

      //
      // Loop over the files, creating the macros
      //
      
      Map<String, Macro> newmacros = new HashMap<String,Macro>();
            
      boolean hasNew = false;
      
      for (File file: files) {
        
        String name = file.getAbsolutePath().substring(rootdir.length() + 1).replaceAll("\\.mc2$", "");
        
        //
        // Ignore '.mc2' files not in a subdir
        //
        
        if (!name.contains(File.separator)) {
          continue;
        }
        
        // Replace file separator with '/'
        name = name.replaceAll(Pattern.quote(File.separator), "/");

        Macro macro = loadMacro(name, file);

        if (null != macro) {
          newmacros.put(name, macro);
          if (!macro.equals(macros.get(name))) {
            hasNew = true;
          }
        }        
      }
      
      //
      // Replace the previous macros
      //
      
      if (hasNew) {
        synchronized(macros) {
          macros.clear();
          macros.putAll(newmacros);
        }        
      }

      if (!ondemand && macros.size() == maxcachesize) {
        LOG.warn("Some cached library macros were evicted.");        
      }
      
      //
      // Update macro count
      //
      
      Sensision.set(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_REPOSITORY_MACROS, Sensision.EMPTY_LABELS, macros.size());
      
      //
      // Sleep a while
      //
      
      try {
        Thread.sleep(this.delay);
      } catch (InterruptedException ie) {        
      }
    }
  }
  
  public static Macro find(String name) throws WarpScriptException {
    Macro macro = null;
    synchronized(macros) {
      macro = (Macro) macros.get(name);
    }
    
    // Check if macro has expired when ondemand loading is activated
    if (ondemand && null != macro && macro.isExpired()) {
      macro = null;
    }
    
    if (null == macro && ondemand) {
      macro = loadMacro(name, null);
      if (null != macro) {
        // Store the recently loaded macro in the map
        synchronized(macros) {
          macros.put(name, macro);
        }
      }
    }
      
    return macro;
  }
  
  public List<File> getWarpScriptFiles(String rootdir) {
    File root = new File(rootdir);
    
    // List to hold the directories
    final List<File> dirs = new ArrayList<File>();
    
    List<File> allfiles = new ArrayList<File>();

    if (!root.exists()) {
      return allfiles;
    }

    dirs.add(root);
    
    int idx = 0;    
    
    while(idx < dirs.size()) {
      File dir = dirs.get(idx++);
      
      File[] files = dir.listFiles(new FilenameFilter() {
        
        @Override
        public boolean accept(File dir, String name) {
          if (".".equals(name) || "..".equals(name)) {
            return false;
          }
          
          File f = new File(dir,name); 
          if (f.isDirectory()) {
            dirs.add(f);
            return false;
          } else if (name.endsWith(WARPSCRIPT_FILE_EXTENSION) || new File(dir, name).isDirectory()) {
            return true;
          }
          
          return false;
        }
      });
      
      if (null == files) {
        continue;
      }
      
      for (File file: files) {
        allfiles.add(file);
      }
    }
    
    return allfiles;
  }
  
  public static void init(Properties properties) {
    
    //
    // Extract root directory
    //
    
    String dir = properties.getProperty(Configuration.REPOSITORY_DIRECTORY);
    
    if (null == dir) {
      return;
    }
    
    //
    // Extract refresh interval
    //
    
    long refreshDelay = DEFAULT_DELAY;
    
    String refresh = properties.getProperty(Configuration.REPOSITORY_REFRESH);

    if (null != refresh) {
      try {
        refreshDelay = Long.parseLong(refresh.toString());
      } catch (Exception e) {            
      }
    }

    directory = dir;
    delay = refreshDelay;
    
    ttl = Long.parseLong(properties.getProperty(Configuration.REPOSITORY_TTL, Long.toString(DEFAULT_MACRO_TTL)));
    failedTtl = Long.parseLong(properties.getProperty(Configuration.REPOSITORY_TTL_FAILED, Long.toString(DEFAULT_FAILED_MACRO_TTL)));

    ondemand = !"false".equals(properties.getProperty(Configuration.REPOSITORY_ONDEMAND));
    new WarpScriptMacroRepository();
  }
  
  /**
   * Load a macro stored in a file
   *
   * @param name of macro
   * @param file containing the macro
   * @return
   */
  private static Macro loadMacro(String name, File file) {
    
    if (null == name && null == file) {
      return null;
    }
    
    // Reject names with relative path components in them or starting with '/'
    if (name.contains("/../") || name.contains("/./") || name.startsWith("../") || name.startsWith("./") || name.startsWith("/")) {
      return null;
    }

    // Name should contain "/" as macros should reside under a subdirectory
    if (!name.contains("/")) {
      return null;
    }
    
    //
    // Read content
    //
    
    String rootdir = new File(directory).getAbsolutePath();
    
    if (null == file) {
      // Replace '/' with the platform separator
      if (!"/".equals(File.separator)) {
        file = new File(rootdir, name.replaceAll("/", File.separator) + ".mc2");
      } else {
        file = new File(rootdir, name + ".mc2");
      }
      
      // Macros should reside in the configured root directory
      if (!file.getAbsolutePath().startsWith(rootdir)) {
        return null;
      }
    }

    if (null == name) {
      name = file.getAbsolutePath().substring(rootdir.length() + 1).replaceAll("\\.mc2$", "");
      name = name.replaceAll(Pattern.quote(File.separator), "/");
    }
    
    byte[] buf = new byte[8192];

    StringBuilder sb = new StringBuilder();
    
    sb.append(" ");
    
    MemoryWarpScriptStack stack = null;
    
    try {
      
      if (loading.get().contains(name)) {
        // Build the macro loading sequence
        StringBuilder seq = new StringBuilder();
        for(String macname: loading.get()) {
          if (seq.length() > 0) {
            seq.append(" >>> ");
          }
          seq.append("@");
          seq.append(macname);
        }
        throw new WarpScriptException("Invalid recursive macro loading (" + seq.toString() + ")");
      }
      
      loading.get().add(name);
      
      if (!file.exists()) {
        return null;
      }
      
      FileInputStream in = new FileInputStream(file);
      ByteArrayOutputStream out = new ByteArrayOutputStream((int) file.length());
      
      while(true) {
        int len = in.read(buf);
        
        if (len < 0) {
          break;
        }
        
        out.write(buf, 0, len);
      }

      in.close();

      byte[] data = out.toByteArray();
      
      // Compute hash to check if the file changed or not
      
      long hash = SipHashInline.hash24_palindromic(SIP_KEYS[0], SIP_KEYS[1], data);

      Macro old = macros.get(name);
      
      // Re-use the same macro if its fingerprint did not change and it has not expired
      if (null != old && hash == old.getFingerprint() && (ondemand && !old.isExpired())) {
        return old;
      }
            
      sb.append(new String(data, StandardCharsets.UTF_8));
      
      sb.append("\n");
      
      stack = new MemoryWarpScriptStack(null, null);
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[WarpScriptMacroRepository " + name + "]");

      stack.maxLimits();
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_MACRO_NAME, name);
      
      //
      // Add 'INCLUDE', 'enabled' will disable 'INCLUDE' after we've used it when loading 
      //

      // Root is the current subdir
      
      //
      // Create an instance of 'INCLUDE' for the current rootdir
      //
      
      // 'enabled' will allow us to disable the INCLUDE after loading the macro
      AtomicBoolean enabled = new AtomicBoolean(true);
      final INCLUDE include = new INCLUDE("INCLUDE", new File(rootdir, name.replaceAll("/.*", "")), enabled);

      stack.define("INCLUDE", new Macro() {
        public boolean isSecure() { return true; }
        public java.util.List<Object> statements() { return new ArrayList<Object>() {{ add(include); }}; }
        }
      );
            
      //
      // Execute the code
      //
      stack.execMulti(sb.toString());
      
      //
      // Disable INCLUDE
      //
      
      enabled.set(false);
      
      //
      // Ensure the resulting stack is one level deep and has a macro on top
      //
      
      if (1 != stack.depth()) {
        throw new WarpScriptException("Stack depth was not 1 after the code execution.");
      }
      
      if (!(stack.peek() instanceof Macro)) {
        throw new WarpScriptException("No macro was found on top of the stack.");
      }
      
      //
      // Store resulting macro under 'name'
      //
      
      Macro macro = (Macro) stack.pop();
                
      // Set expiration if ondemand is set and a ttl was specified
      
      try {
        if (ondemand && null != stack.getAttribute(WarpScriptStack.ATTRIBUTE_MACRO_TTL)) {
          macro.setExpiry(Math.addExact(System.currentTimeMillis(), (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_MACRO_TTL)));
        } else if (ondemand) {
          macro.setExpiry(Math.addExact(System.currentTimeMillis(), ttl));
        }        
      } catch (ArithmeticException ae) {
        macro.setExpiry(Long.MAX_VALUE - 1);
      }
      
      macro.setFingerprint(hash);
      
      // Make macro a secure one
      macro.setSecure(true);
      
      macro.setNameRecursive(name);
      
      return macro;
    } catch(Exception e) {
      // Replace macro with a FAIL indicating the error message
      Macro macro = new Macro();
      macro.add("[" + System.currentTimeMillis() + "] Error while loading macro '" + name + "': " + ThrowableUtils.getErrorMessage(e, 1024));      
      macro.add(MSGFAIL_FUNC);
      // Set the expiry to half the refresh interval if ondemand is true so we get a chance to load a newly provided file
      if (ondemand) {
        macro.setExpiry(System.currentTimeMillis() + Math.max(delay / 2, failedTtl));
      }
      macro.setFingerprint(0L);
      return macro;
    } finally {
      WarpScriptStackRegistry.unregister(stack);
      loading.get().remove(loading.get().size() - 1);
    }
  }
}
