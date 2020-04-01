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
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Set the metadata (attributes) for the GTS on the stack
 * 
 */
public class META extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private URL url = null;
  
  private final boolean delta;
  
  public META(String name) {
    this(name, false);
  }

  public META(String name, boolean delta) {
    super(name);
    this.delta = delta;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // Extract token
    //
    
    Object otoken = stack.pop();
    
    if (!(otoken instanceof String)) {
      throw new WarpScriptException(getName() + " expects a token.");
    }
    
    String token = (String) otoken;

    List<Metadata> metas = new ArrayList<Metadata>();
    
    Object o = stack.pop();
    
    if (o instanceof GeoTimeSerie) {
      metas.add(((GeoTimeSerie) o).getMetadata());
    } else if (o instanceof GTSEncoder) {
      metas.add(((GTSEncoder) o).getMetadata());      
    } else if (o instanceof List) {
      for (Object oo: (List<Object>) o) {
        if (oo instanceof GeoTimeSerie) {
          metas.add(((GeoTimeSerie) oo).getMetadata());
        } else if (oo instanceof GTSEncoder) {
          metas.add(((GTSEncoder) oo).getMetadata());
        } else {
          throw new WarpScriptException(getName() + " can only operate on Geo Time Series, GTS Encoders or a list thereof.");
        }
      }
    } else {
      throw new WarpScriptException(getName() + " can only operate on Geo Time Series, GTS Encoders or a list thereof");
    }
    
    //
    // Return immediately if 'series' is empty
    //
    
    if (0 == metas.size()) {
      return stack;
    }
    
    //
    // Check that all GTS have a name and attributes
    //
    
    for (Metadata meta: metas) {
      if (null == meta.getName() || "".equals(meta.getName())) {
        throw new WarpScriptException(getName() + " can only set attributes of Geo Time Series or GTS Encoders which have a non empty name.");
      }
      if (null == meta.getAttributes()) {
        throw new WarpScriptException(getName() + " can only operate on Geo Time Series or GTS Encoders which have attributes.");
      }
    }
    
    //
    // Create the OutputStream
    //
    
    HttpURLConnection conn = null;

    try {
      if (null == url) {
        String url_property = WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT);
        if (null != url_property) {
          try {
            url = new URL(url_property);
          } catch (MalformedURLException mue) {
            throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT + "' does not define a valid URL.");
          }
        } else {
          throw new WarpScriptException(getName() + " configuration parameter '" + Configuration.CONFIG_WARPSCRIPT_META_ENDPOINT + "' not set.");
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
      
      if (this.delta) {
        conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES), "delta");        
      }
      
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_META_TOKENX), token);
      conn.setRequestProperty("Content-Type", "application/gzip");
      conn.setChunkedStreamingMode(16384);
      conn.connect();
      
      OutputStream os = conn.getOutputStream();
      GZIPOutputStream out = new GZIPOutputStream(os);
      PrintWriter pw = new PrintWriter(out);
      
      StringBuilder sb = new StringBuilder();
      
      for (Metadata meta: metas) {
        sb.setLength(0);
        // Expose all labels as producer/owner will have been dropped by the meta endpoint
        GTSHelper.metadataToString(sb, meta.getName(), meta.getLabels(), true);
        Map<String,String> attributes = null != meta.getAttributes() ? meta.getAttributes() : new HashMap<String,String>();
        // Always expose attributes
        GTSHelper.labelsToString(sb, attributes, true);
        pw.println(sb.toString());
        stack.handleSignal();
      }
      
      pw.close();
      
      //
      // Update was successful, delete all batchfiles
      //
      
      if (200 != conn.getResponseCode()) {
        throw new WarpScriptException(getName() + " failed to complete successfully (" + conn.getResponseMessage() + ")");
      }
      
      //is.close();
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
