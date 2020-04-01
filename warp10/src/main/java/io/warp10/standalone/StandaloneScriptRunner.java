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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.geoxp.oss.CryptoHelper;
import com.google.common.primitives.Longs;

import io.warp10.WarpConfig;
import io.warp10.continuum.BootstrapManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.ScriptRunner;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.script.WarpScriptStack.StackContext;
import io.warp10.sensision.Sensision;

public class StandaloneScriptRunner extends ScriptRunner {
  
  private final StoreClient storeClient;
  private final DirectoryClient directoryClient;
  private final Properties props;
  private final BootstrapManager bootstrapManager;

  private final Random prng = new Random();

  private final byte[] runnerPSK;
  
  private static final Pattern VAR = Pattern.compile("\\$\\{([^}]+)\\}");

  public StandaloneScriptRunner(Properties properties, KeyStore keystore, StoreClient storeClient, DirectoryClient directoryClient, Properties props) throws IOException {
    super(keystore, props);

    this.props = props;
    this.directoryClient = directoryClient;
    this.storeClient = storeClient;
    
    //
    // Check if we have a 'bootstrap' property
    //
    
    if (properties.containsKey(Configuration.CONFIG_WARPSCRIPT_RUNNER_BOOTSTRAP_PATH)) {     
      final String path = properties.getProperty(Configuration.CONFIG_WARPSCRIPT_RUNNER_BOOTSTRAP_PATH);
      
      long period = properties.containsKey(Configuration.CONFIG_WARPSCRIPT_RUNNER_BOOTSTRAP_PERIOD) ?  Long.parseLong(properties.getProperty(Configuration.CONFIG_WARPSCRIPT_RUNNER_BOOTSTRAP_PERIOD)) : 0L ;
      this.bootstrapManager = new BootstrapManager(path, period);      
    } else {
      this.bootstrapManager = new BootstrapManager();
    }
    
    this.runnerPSK = keystore.getKey(KeyStore.AES_RUNNER_PSK);
  }
  
  @Override
  protected void schedule(final Map<String, Long> nextrun, final String script, final long periodicity) {
    
    try {
      
      final long scheduledat = System.currentTimeMillis();
      
      this.executor.submit(new Runnable() {            
        @Override
        public void run() {
          String name = currentThread().getName();
          currentThread().setName(script);
          
          long nowts = System.currentTimeMillis();

          File f = new File(script);
          
          Map<String,String> labels = new HashMap<String,String>();
          //labels.put(SensisionConstants.SENSISION_LABEL_PATH, Long.toString(periodicity) + "/" + f.getName());
          String path = f.getAbsolutePath().substring(getRoot().length() + 1);
          labels.put(SensisionConstants.SENSISION_LABEL_PATH, path);
          
          long ttl = Math.max(scanperiod * 2, periodicity * 2);
          
          Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_COUNT, labels, ttl, 1);

          long nano = System.nanoTime();
          
          WarpScriptStack stack = new MemoryWarpScriptStack(storeClient, directoryClient, props);
          stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[StandloneScriptRunner " + script + "]");
          
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          
          try {            
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_CURRENT, Sensision.EMPTY_LABELS, 1);

            InputStream in = new FileInputStream(f);
                        
            byte[] buf = new byte[1024];
            
            while(true) {
              int len = in.read(buf);
              
              if (len < 0) {
                break;
              }
              
              baos.write(buf, 0, len);
            }
            
            // Add a 'CLEAR' at the end of the script so we don't return anything
            baos.write(CLEAR);
            
            in.close();

            //
            // Replace the context with the bootstrap one
            //
            
            StackContext context = bootstrapManager.getBootstrapContext();
                  
            if (null != context) {
              stack.push(context);
              stack.restore();
            }
                  
            //
            // Execute the bootstrap code
            //

            stack.exec(WarpScriptLib.BOOTSTRAP);

            stack.store(Constants.RUNNER_PERIODICITY, periodicity);
            stack.store(Constants.RUNNER_PATH, path);
            stack.store(Constants.RUNNER_SCHEDULEDAT, scheduledat);
            
            //
            // Generate a nonce by wrapping the current time with random 64bits
            //
            
            if (null != runnerPSK) {
              byte[] now = Longs.toByteArray(TimeSource.getTime());
              
              byte[] nonce = CryptoHelper.wrapBlob(runnerPSK, now);
              
              stack.store(Constants.RUNNER_NONCE, new String(OrderPreservingBase64.encode(nonce), StandardCharsets.US_ASCII));
            }
            
            String mc2 = new String(baos.toByteArray(), StandardCharsets.UTF_8);
            
            // Replace ${name} and ${name:default} constructs
              
            Matcher m = VAR.matcher(mc2);

            // Strip the period out of the path and add a leading '/'
            String rawpath = "/" + path.replaceFirst("/" + Long.toString(periodicity) + "/", "/");
            // Remove the file extension
            rawpath = rawpath.substring(0, rawpath.length() - 4);

            StringBuffer mc2WithReplacement = new StringBuffer();
            
            while(m.find()) {
              String var = m.group(1);
              String def = m.group(0);

              int colonIndex = var.indexOf(':');
              if (colonIndex >= 0) {
                def = var.substring(colonIndex + 1);
                var = var.substring(0, colonIndex);
              }
                
              // Check in the configuration if we can find a matching key, i.e.
              // name@/path/to/script (with the period omitted) or any shorter prefix
              // of the path, i.e. name@/path/to or name@/path
              String suffix = rawpath;
                
              String value = null;
                
              while (suffix.length() > 1) {
                value = WarpConfig.getProperty(var + "@" + suffix);
                if (null != value) {
                  break;
                }
                suffix = suffix.substring(0, suffix.lastIndexOf('/'));
              }
                
              if (null == value) {
                value = def;
              }
                
              m.appendReplacement(mc2WithReplacement, Matcher.quoteReplacement(value));
            }

            m.appendTail(mc2WithReplacement);
              
            stack.execMulti(mc2WithReplacement.toString());
          } catch (Exception e) {                
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FAILURES, labels, 1);
          } finally {
            WarpScriptStackRegistry.unregister(stack);
            currentThread().setName(name);
            nextrun.put(script, nowts + periodicity);
            nano = System.nanoTime() - nano;
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_TIME_US, labels, ttl, (long) (nano / 1000L));
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_ELAPSED, labels, ttl, nano); 
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_OPS, labels, ttl, (long) stack.getAttribute(WarpScriptStack.ATTRIBUTE_OPS)); 
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FETCHED, labels, ttl, ((AtomicLong) stack.getAttribute(WarpScriptStack.ATTRIBUTE_FETCH_COUNT)).get());            
            Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_CURRENT, Sensision.EMPTY_LABELS, -1);
          }              
        }
      });                  
    } catch (RejectedExecutionException ree) {
      // Reschedule script immediately
      nextrun.put(script, System.currentTimeMillis());
    }    
  }
}
