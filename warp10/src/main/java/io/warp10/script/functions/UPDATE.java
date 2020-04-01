//
//   Copyright 2018-2020  SenX S.A.S.
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
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.StandaloneAcceleratedStoreClient;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Updates datapoints from the GTS on the stack.
 * 
 * In the standalone mode, this connects to the Ingress endpoint on localhost.
 * In the distributed mode, this uses a proxy to connect to an HTTP endpoint which will push data into continuum.
 */
public class UPDATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private URL url = null;
  private boolean dynURL = false;
  
  public UPDATE(String name) {
    super(name);
  }

  public UPDATE(String name, URL url) {
    super(name);
    this.url = url;
  }
  
  public UPDATE(String name, boolean dynURL) {
    super(name);
    this.dynURL = dynURL;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // Extract token
    //
    
    Object otoken = stack.pop();
    
    if (!(otoken instanceof String)) {
      throw new WarpScriptException(getName() + " expects a token on top of the stack.");
    }
    
    String token = (String) otoken;
    
    URL url = this.url;
    
    if (this.dynURL) {
      Object urlstr = stack.pop();
      
      try {
        url = new URL(urlstr.toString());
      } catch (MalformedURLException mue) {
        throw new WarpScriptException(mue);
      }
    }
    
    List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
    List<GTSEncoder> encoders = new ArrayList<GTSEncoder>();
    
    Object o = stack.pop();
    
    if (o instanceof GeoTimeSerie) {
      if (GTSHelper.nvalues((GeoTimeSerie) o) > 0) {
        series.add((GeoTimeSerie) o);
      }
    } else if (o instanceof GTSEncoder) {
      if (((GTSEncoder) o).getCount() > 0) {
        encoders.add((GTSEncoder) o);
      }
    } else if (o instanceof List) {
      for (Object oo: (List<Object>) o) {
        if (oo instanceof GeoTimeSerie) {
          if (GTSHelper.nvalues((GeoTimeSerie) oo) > 0) {
            series.add((GeoTimeSerie) oo);
          }
        } else if (oo instanceof GTSEncoder) {
          if (((GTSEncoder) oo).getCount() > 0) {
            encoders.add((GTSEncoder) oo);
          }          
        } else {
          throw new WarpScriptException(getName() + " can only operate on Geo Time Series, encoders or a list thereof.");
        }
      }
    } else {
      throw new WarpScriptException(getName() + " can only operate on Geo Time Series, encoders or a list thereof.");
    }
    
    //
    // Return immediately if 'series' is empty
    //
    
    if (0 == series.size() && 0 == encoders.size()) {
      return stack;
    }
    
    //
    // Check that all GTS have a name and were renamed
    //
    
    for (GeoTimeSerie gts: series) {
      if (null == gts.getName() || "".equals(gts.getName())) {
        throw new WarpScriptException(getName() + " can only update Geo Time Series which have a non empty name.");
      }
      if (!gts.isRenamed()) {
        throw new WarpScriptException(getName() + " can only update Geo Time Series which have been renamed.");
      }
    }
    
    for (GTSEncoder encoder: encoders) {
      if (null == encoder.getName() || "".equals(encoder.getName())) {
        throw new WarpScriptException(getName() + " can only update encoders which have a non empty name.");
      }
    }
    
    //
    // Create the OutputStream
    //
    
    HttpURLConnection conn = null;

    try {
      if (null == url) {
        String url_property = WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_UPDATE_ENDPOINT);
        if (null != url_property) {
          try {
            this.url = new URL(url_property);
            url = this.url;
          } catch (MalformedURLException mue) {
            throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_UPDATE_ENDPOINT + "' does not define a valid URL.");
          }
        } else {
          throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_UPDATE_ENDPOINT + "' not set.");
        }
      }

      /*
      if (null == this.proxy) {
        conn = (HttpURLConnection) this.url.openConnection();
      } else {
        conn = (HttpURLConnection) this.url.openConnection(this.proxy);
      }
      */
      conn = (HttpURLConnection) url.openConnection();
      
      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setRequestMethod("POST");
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_UPDATE_TOKENX), token);
      conn.setRequestProperty("Content-Type", "application/gzip");
      
      boolean nocache = Boolean.TRUE.equals(stack.getAttribute(StandaloneAcceleratedStoreClient.ATTR_NOCACHE));
      boolean nopersist = Boolean.TRUE.equals(stack.getAttribute(StandaloneAcceleratedStoreClient.ATTR_NOPERSIST));
      
      String accel = "";
      if (nocache) {
        accel = accel + StandaloneAcceleratedStoreClient.NOCACHE + " ";
      }
      if (nopersist) {
        accel = accel + StandaloneAcceleratedStoreClient.NOPERSIST;
      }
      if (!"".equals(accel)) {
        conn.setRequestProperty(StandaloneAcceleratedStoreClient.ACCELERATOR_HEADER, accel);
      }
      conn.setChunkedStreamingMode(16384);
      conn.connect();
      
      OutputStream os = conn.getOutputStream();
      GZIPOutputStream out = new GZIPOutputStream(os);
      PrintWriter pw = new PrintWriter(out);
      
      for (GeoTimeSerie gts: series) {
        gts.dump(pw);
      }
    
      for (GTSEncoder encoder: encoders) {
        GTSHelper.dump(encoder, pw);
        stack.handleSignal();
      }
      
      pw.close();
      
      if (200 != conn.getResponseCode()) {
        throw new WarpScriptException(getName() + " failed to complete successfully (" + conn.getResponseMessage() + ")");
      }
      
      conn.disconnect();
      conn = null;
    } catch (IOException ioe) { 
      throw new WarpScriptException(getName() + " failed.", ioe);
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }

    return stack;
  }
}
