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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.GZIPOutputStream;
import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.Revision;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

public class REPORT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final Logger LOG = LoggerFactory.getLogger(REPORT.class);
  
  private static final String SECRET;

  private static final AtomicLong seq = new AtomicLong(0L);
  
  private static final String uuid = UUID.randomUUID().toString();

  private static boolean initialized = false;
  
  static {
    String defaultSecret = UUID.randomUUID().toString();
    SECRET = WarpConfig.getProperty(Configuration.WARP10_REPORT_SECRET, defaultSecret);
    
    if (defaultSecret.equals(SECRET)) {
      LOG.info("REPORT secret not set, using '" + defaultSecret + "'.");
      System.out.println("REPORT secret not set, using '" + defaultSecret + "'.");
    }    
  }
  
  public REPORT(String name) {
    super(name);
    if (!initialized && !"false".equals(WarpConfig.getProperty(Configuration.WARP10_TELEMETRY))) {
      telinit();
    }
    initialized = true;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!SECRET.equals(top.toString())) {
      throw new WarpScriptException(getName() + " invalid secret.");
    }
    
    stack.push(genReport(true));
        
    return stack;
  }
  
  public static String genReport(boolean includeConf) throws WarpScriptException {
    try {
      StringBuilder sb = new StringBuilder();
      
      sb.append("\n[revision]\n");
      sb.append(Revision.REVISION);
      sb.append("\n");

      sb.append("\n[jvm]\n");
      sb.append("free=");
      sb.append(Runtime.getRuntime().freeMemory());
      sb.append("\n");
      sb.append("total=");
      sb.append(Runtime.getRuntime().totalMemory());
      sb.append("\n");
      sb.append("max=");
      sb.append(Runtime.getRuntime().maxMemory());
      sb.append("\n");
      sb.append("cpus=");
      sb.append(Runtime.getRuntime().availableProcessors());
      sb.append("\n");
      
      sb.append("\n[sensision]\n");
      
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      
      Sensision.dump(pw);
      
      pw.close();
      sb.append(sw.toString());
      
      sb.append("\n[extensions]\n");
      
      for (String extension: WarpScriptLib.extensions()) {
        sb.append(extension);
        sb.append("\n");
      }
      
      sb.append("\n[plugins]\n");
      
      for (String plugin: AbstractWarp10Plugin.plugins()) {
        sb.append(plugin);
        sb.append("\n");
      }
      
      if (includeConf) {
        sb.append("\n[config]\n");

        Properties properties = WarpConfig.getProperties();
        
        for (Entry<Object,Object> entry: properties.entrySet()) {
          String key = entry.getKey().toString();
          
          //
          // Skip crypto related properties
          //
          
          if (key.contains(".key")
              || key.contains(".aes")
              || key.contains(".hash")
              || key.contains(".mac")
              || key.contains(".psk")
              || key.contains(".secret")) {
            continue;
          }

          sb.append(key);
          sb.append("=");
          
          String value = entry.getValue().toString();
          sb.append(value);
          sb.append("\n");
        }        
      }
      
      byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
      
      int lines = 0;
      for (byte b: data) {
        if ('\n' == b) {
          lines++;
        }
      }
      
      long sip = SipHashInline.hash24(data.length, data.length, data, 0, data.length);
      
      sb.insert(0, "\n");
      sb.insert(0, Long.toHexString(sip));
      sb.insert(0, ".");
      sb.insert(0, lines);
      sb.insert(0, ".");
      sb.insert(0, System.currentTimeMillis());
      sb.insert(0, "[report]\n");
      
      return sb.toString();
    } catch (Exception e) {
      throw new WarpScriptException("Error while generating report.");
    }
  }
  
  private static final void telinit() {
    try {
      Thread telemetry = new Thread() {
        @Override
        public void run() {
          boolean first = true;
          long delay = 8 * 3600L * 1000000000L;
          while(true) {
            
            if (!first) {
              LockSupport.parkNanos(delay);
            }
            
            first = false;

            long newdelay = REPORT.telemetry();
            
            if (newdelay > 0) {
              delay = newdelay;
            }
          }
        }
      };
      
      telemetry.setDaemon(true);
      telemetry.start();
      
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          seq.set(Long.MIN_VALUE);
          REPORT.telemetry();
        }
      });
    } catch (Throwable t) {      
    }
  }
  
  private static long telemetry() {
    HttpURLConnection conn = null;
    
    try {
      String report = genReport(false);
      
      conn = (HttpURLConnection) new URL("https://telemetry.senx.io/report").openConnection();
      conn.setDoOutput(true);
      if (0 == seq.get()) {
        conn.addRequestProperty("X-Warp10-Telemetry-Event", "start");
      } else if (seq.get() < 0) {
        conn.addRequestProperty("X-Warp10-Telemetry-Event", "stop");
      } else {
        conn.addRequestProperty("X-Warp10-Telemetry-Event", "report");
      }      
      seq.addAndGet(1L);
      conn.addRequestProperty("X-Warp10-Telemetry-UUID", uuid);
      conn.addRequestProperty("Content-Type", "application/gzip");
      OutputStream out = conn.getOutputStream();
      OutputStream zout = new GZIPOutputStream(out);
      zout.write(report.getBytes(StandardCharsets.UTF_8));
      zout.close();
      out.close();
      String newdelay = conn.getHeaderField("X-Warp10-Telemetry-Delay");
      
      if (null != newdelay) {
        try {
          return Long.parseLong(newdelay);
        } catch (Throwable t) {                  
        }
      }
    } catch (Throwable t) {   
    } finally {
      if (null != conn) {
        try { conn.disconnect(); } catch (Throwable t) {}
      }
    }
    return 0;
  }
}
