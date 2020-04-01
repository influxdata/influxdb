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

package io.warp10.script.ext.rexec;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WebAccessController;

/**
 * Execute WarpScript on a remote endpoint
 */
public class REXEC extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean compress;

  /**
   * Allowed and excluded host patterns.
   */
  public static final String WARPSCRIPT_REXEC_ENDPOINT_PATTERNS = "warpscript.rexec.endpoint.patterns";

  private static final String DEFAULT_ENDPOINT_PATTERNS = ".*";

  /**
   * Timeouts
   */
  public static final String WARPSCRIPT_REXEC_CONNECT_TIMEOUT = "warpscript.rexec.timeout.connect";
  private static final String DEFAULT_REXEC_CONNECT_TIMEOUT = "0";
  private int connConnectTimeout;
  public static final String WARPSCRIPT_REXEC_READ_TIMEOUT = "warpscript.rexec.timeout.read";
  private static final String DEFAULT_REXEC_READ_TIMEOUT = "0";
  private int connReadTimeout;

  private final WebAccessController webAccessController;
  
  public REXEC(String name) {
    this(name, false);
  }
  
  public REXEC(String name, boolean compress) {
    super(name);
  
    String patternConf = WarpConfig.getProperty(WARPSCRIPT_REXEC_ENDPOINT_PATTERNS, DEFAULT_ENDPOINT_PATTERNS);
    String readTimeout = WarpConfig.getProperty(WARPSCRIPT_REXEC_READ_TIMEOUT, DEFAULT_REXEC_READ_TIMEOUT);
    this.connReadTimeout= Integer.parseInt(readTimeout);
    String connectTimeout = WarpConfig.getProperty(WARPSCRIPT_REXEC_CONNECT_TIMEOUT, DEFAULT_REXEC_CONNECT_TIMEOUT);
    this.connConnectTimeout = Integer.parseInt(connectTimeout);
    this.webAccessController = new WebAccessController(patternConf);

    this.compress = compress;    
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    String endpoint = stack.pop().toString();
    
    String warpscript = stack.pop().toString();
    
    HttpURLConnection conn = null;
    
    try {
      URL url = new URL(endpoint);

      if (!"http".equals(url.getProtocol()) && !"https".equals(url.getProtocol())) {
        throw new WarpScriptException(getName() + " invalid endpoint protocol.");
      }
      
      if (!webAccessController.checkURL(url)) {
        throw new WarpScriptException(getName() + " encountered a forbidden URL '" + url + "'");
      }
      
      conn = (HttpURLConnection) url.openConnection();
      conn.setReadTimeout(this.connReadTimeout);
      conn.setConnectTimeout(this.connConnectTimeout);
      conn.setChunkedStreamingMode(8192);
      conn.setRequestProperty("Accept-Encoding", "gzip");
      
      if (this.compress) {
        conn.setRequestProperty("Content-Type", "application/gzip");
      }
      
      conn.setDoInput(true);
      conn.setDoOutput(true);
      conn.setRequestMethod("POST");
      
      OutputStream connout = conn.getOutputStream();
      OutputStream out = connout;
      
      if (this.compress) {
        out = new GZIPOutputStream(out);
      }

      out.write(warpscript.getBytes(StandardCharsets.UTF_8));
      out.write('\n');
      out.write(WarpScriptLib.SNAPSHOT.getBytes(StandardCharsets.UTF_8));
      out.write('\n');
      out.write(WarpScriptLib.TOOPB64.getBytes(StandardCharsets.UTF_8));
      out.write('\n');
      
      if (this.compress) {
        out.close();
      }
      
      connout.flush();

      if (HttpURLConnection.HTTP_OK != conn.getResponseCode()) {
        String errorMessage = conn.getHeaderField(Constants.getHeader(Configuration.HTTP_HEADER_ERROR_MESSAGEX));
        if (null != errorMessage) {
          throw new WarpScriptException(getName() + " remote execution failed: " + errorMessage);
        } else {
          throw new WarpScriptException(getName() + " remote execution failed with HTTP code " + conn.getResponseCode() + ".'");
        }
      }
      
      InputStream in = conn.getInputStream();
      
      if ("gzip".equals(conn.getContentEncoding())) {
        in = new GZIPInputStream(in);
      }
      
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      
      byte[] buf = new byte[1024];
      
      while(true) {
        int len = in.read(buf);
        if (len < 0) {
          break;
        }
        baos.write(buf, 0, len);
      }

      byte[] bytes = baos.toByteArray();
      
      // Strip '[ ' ' ]'
      String result = new String(bytes, 2, bytes.length - 4, StandardCharsets.US_ASCII);
      
      stack.push(result);
      
      stack.exec(WarpScriptLib.OPB64TO);
      stack.push(StandardCharsets.UTF_8.name());
      stack.exec(WarpScriptLib.BYTESTO);
      stack.exec(WarpScriptLib.EVAL);
      
    } catch (WarpScriptException e) {
      throw e;
    } catch(SocketTimeoutException e) {
      throw new WarpScriptException(getName() + " Timeout: check configurations " + WARPSCRIPT_REXEC_CONNECT_TIMEOUT + " and " + WARPSCRIPT_REXEC_READ_TIMEOUT, e);
    } catch (Exception e) {
      throw new WarpScriptException(e);
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }
    
    return stack;
  }
}
