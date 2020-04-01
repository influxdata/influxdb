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
package io.warp10.plugins.influxdb;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.store.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.common.base.Splitter;

public class InfluxDBHandler extends AbstractHandler {
  
  private final URL url;
  private final String token;
  
  public InfluxDBHandler(URL url, String token) {
    this.url = url;
    this.token = token;
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    baseRequest.setHandled(true);

    // Only run code on REQUEST
    if(DispatcherType.REQUEST != baseRequest.getDispatcherType()){
      return;
    }
    
    long nsPerTimeUnit = 1;
    
    String qs = request.getQueryString();
    
    Map<String,String> params = null;
    
    if (null != qs) {
      params = Splitter.on("&").withKeyValueSeparator("=").split(qs);  
    } else {
      params = new HashMap();
    }
    
    String precision = params.get("precision");
    
    if (null != precision) { 
      if ("n".equals(precision)) {
        nsPerTimeUnit = 1;
      } else if ("u".equals(precision)) {
        nsPerTimeUnit = 1000L;
      } else if ("ms".equals(precision)) {
        nsPerTimeUnit = 1000000L;
      } else if ("s".equals(precision)) {
        nsPerTimeUnit = 1000000000L;
      } else if ("m".equals(precision)) {
        nsPerTimeUnit = 60 * 1000000000L;
      } else if ("h".equals(precision)) {
        nsPerTimeUnit = 3600 * 1000000000L;
      } else {
        throw new IOException("Invalid precision.");
      }
    }

    String token = this.token;
    
    String basicAuth = request.getHeader("Authorization");
    
    if (null != basicAuth) {
      String[] userpass = new String(Base64.decodeBase64(basicAuth.substring(6))).split(":");
      token = userpass[1];
    } else if (params.containsKey("p")) {
      token = params.get("p");
    }

    if (null == token) {
      throw new IOException("Missing password and no default token.");
    }
    
    HttpURLConnection conn = null;
    
    try {
      
      conn = (HttpURLConnection) url.openConnection();
      
      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setRequestMethod("POST");
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_UPDATE_TOKENX), token);
      //conn.setRequestProperty("Content-Type", "application/gzip");
      conn.setChunkedStreamingMode(16384);
      conn.connect();
      
      OutputStream os = conn.getOutputStream();
      //GZIPOutputStream out = new GZIPOutputStream(os);
      PrintWriter pw = new PrintWriter(os);

      String contentType = request.getHeader("Content-Type"); 
      BufferedReader br;
      
      if (null == contentType || !"application/gzip".equals(contentType)) {
        br = request.getReader();
      } else {
        GZIPInputStream gzin = new GZIPInputStream(request.getInputStream());
        br = new BufferedReader(new InputStreamReader(gzin));
      }
      
      while(true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }
        try {
        parse(pw,line,nsPerTimeUnit);
        } catch (Throwable t) {
          t.printStackTrace();
          throw t;
        }
      }
      
      br.close();
      
      pw.flush();
      
      if (HttpServletResponse.SC_OK != conn.getResponseCode()) {
        throw new IOException(conn.getResponseMessage());
      }
    } finally {
      if (null != conn) {
        conn.disconnect();
      }
    }
  }
  
  private static void parse(PrintWriter pw, String line, long nsPerTimeUnit) throws IOException {
    
    line = line.trim();
    
    // Skip comments and empty lines
    if ("".equals(line) || line.startsWith("#")) {
      return;
    }
    
    //
    // Replace escape sequences
    //
    
    if (line.indexOf('\\') >= 0) {
      line = line.replaceAll("%", "%25").replaceAll("\\\\=", "%3D").replaceAll("\\\\,", "%2C").replaceAll("\\\\ ", "%20").replaceAll("\\\\\"", "%22").replaceAll("\\\\\\{", "%7B").replaceAll("\\\\\\}", "%7D");
    }

    // Replace spaces in strings
    if (line.indexOf('\"') >= 0) {
      StringBuilder sb = new StringBuilder();
      int idx0 = 0;
      int idx = line.indexOf('\"');
      while (idx >= 0) {
        sb.append(line.substring(idx0, idx + 1));
        idx0 = idx + 1;
        // Find the end quote
        idx = line.indexOf('\"', idx0);
        // Replace single quotes and spaces
        sb.append(line.substring(idx0, idx + 1).replaceAll("'", "%27").replaceAll(" ", "%20"));
        idx0 = idx + 1;
        idx = line.indexOf('\"', idx0);
      }
      
      if (idx0 <= line.length()) {
        sb.append(line.substring(idx0));
      }
      line = sb.toString();
    }
    
    String[] tokens = line.split(" ");
    
    String measurementtags = tokens[0];
    String[] fields = tokens[1].split(",");
    
    long timestamp = tokens.length > 2 ? Long.parseLong(tokens[2]) * nsPerTimeUnit : TimeSource.getNanoTime();

    timestamp = timestamp / (1000000000L / Constants.TIME_UNITS_PER_S);
    
    int comma = measurementtags.indexOf(',');
    
    String measurement = measurementtags.substring(0, comma);
    String labels = measurementtags.substring(comma + 1);

    StringBuilder sb = new StringBuilder();

    for (String field: fields) {
      sb.setLength(0);
      int equal = field.indexOf('=');
      
      String name = field.substring(0, equal);
      
      sb.append(timestamp);
      sb.append("// ");
      sb.append(measurement);
      sb.append(".");
      sb.append(name);
      sb.append("{");
      sb.append(labels);
      sb.append("}");
      sb.append(" ");

      String val = field.substring(equal + 1);
      
      if ("t".equals(val) || "T".equals(val) || "true".equals(val) || "TRUE".equals(val)) {
        sb.append("T");
      } else if ("f".equals(val) || "F".equals(val) || "false".equals(val) || "FALSE".equals(val)) {
        sb.append("F");
      } else if (val.endsWith("i")) {
        sb.append(val.substring(0, val.length() - 1));
      } else if (val.startsWith("\"") && val.endsWith("\"")) {
        sb.append("'");
        sb.append(val.substring(1, val.length() - 1));
        sb.append("'");
      } else {
        sb.append(val);
      }
      pw.println(sb.toString());
    }
  }
}
