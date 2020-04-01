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

package io.warp10.continuum.egress;

import io.warp10.WarpURLEncoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;

public class StreamingMetadataIterator extends MetadataIterator {
  
  /**
   * Index on classSelectors
   */
  int idx = 0;
  
  /**
   * Index in URL list
   */
  int urlidx = 0;
  
  InputStream stream = null;
  
  HttpURLConnection conn = null;
  
  BufferedReader reader = null;
  
  Metadata metadata = null;
  
  private final long[] SIPHASH_PSK; 
  
  private final DirectoryRequest directoryRequest;
  
  private final List<URL> urls;

  private final boolean noProxy;
  
  public StreamingMetadataIterator(long[] SIPHASH_PSK, DirectoryRequest request, List<URL> urls, boolean noProxy) {        
    this.SIPHASH_PSK = SIPHASH_PSK;
    this.directoryRequest = request;
    this.urls = urls;
    this.noProxy = noProxy;
  }
    
  @Override
  public boolean hasNext() {
    try {
      return hasNextInternal();
    } catch (Exception e) {
      return false;
    }
  }
    
  private synchronized boolean hasNextInternal() throws Exception {
    
    //
    // If there is a pending Metadata, return true
    //
    
    if (null != metadata) {
      return true;
    }
    
    //
    // If we ran out of selectors, return false
    //
    
    // TODO(hbs): swap idx and urlidx. Add support for multiple selectors in query string
    
    if (idx >= directoryRequest.getClassSelectorsSize()) {
      return false;
    }
            
    if (null == reader) {
      if (urlidx >= urls.size()) {
        urlidx = 0;
        idx++;
        // Call us recursively
        return hasNext();
      }
      
      // Compute request signature
      
      long now = System.currentTimeMillis();
      
      // Rebuild selector
      
      StringBuilder selector = new StringBuilder();
      selector.append(WarpURLEncoder.encode(directoryRequest.getClassSelectors().get(idx), StandardCharsets.UTF_8));
      selector.append("{");
      
      boolean first = true;
      
      for (Entry<String,String> entry: directoryRequest.getLabelsSelectors().get(idx).entrySet()) {
        if (!first) {
          selector.append(","); // ','
        }
        selector.append(entry.getKey());
        if (entry.getValue().startsWith("=")) {
          selector.append("=");
          selector.append(WarpURLEncoder.encode(entry.getValue().substring(1), StandardCharsets.UTF_8));
        } else if (entry.getValue().startsWith("~")) {
          selector.append("~");
          selector.append(WarpURLEncoder.encode(entry.getValue().substring(1), StandardCharsets.UTF_8));
        } else {
          selector.append("=");
          selector.append(WarpURLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
        }
        first = false;
      }
      
      selector.append("}");

      String tssel = now + ":" + selector.toString();

      byte[] data = tssel.getBytes(StandardCharsets.UTF_8);
      long hash = SipHashInline.hash24(SIPHASH_PSK[0], SIPHASH_PSK[1], data, 0, data.length);
      
      String signature = Long.toHexString(now) + ":" + Long.toHexString(hash);
      
      // Open connection
      
      String qs = Constants.HTTP_PARAM_SELECTOR + "=" + new String(OrderPreservingBase64.encode(selector.toString().getBytes(StandardCharsets.UTF_8)), StandardCharsets.US_ASCII);

      if (directoryRequest.isSetActiveAfter()) {
        qs = qs + "&" + Constants.HTTP_PARAM_ACTIVEAFTER + "=" + Long.toString(directoryRequest.getActiveAfter());
      }

      if (directoryRequest.isSetQuietAfter()) {
        qs = qs + "&" + Constants.HTTP_PARAM_QUIETAFTER + "=" + Long.toString(directoryRequest.getQuietAfter());
      }

      //URL url = new URL(urls.get(urlidx) + "?" + qs);
      URL url = urls.get(urlidx);
      
      conn = (HttpURLConnection) (this.noProxy ? url.openConnection(Proxy.NO_PROXY) : url.openConnection());
      
      conn.setRequestMethod("POST");      
      conn.setChunkedStreamingMode(8192);
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DIRECTORY_SIGNATURE), signature);
      conn.setDoInput(true);
      conn.setDoOutput(true);
      
      OutputStream out = conn.getOutputStream();
      out.write(qs.getBytes(StandardCharsets.US_ASCII));
      out.flush();
      
      reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));          
    }
    
    //
    // Attempt to read the next line
    //
    
    String line = reader.readLine();
    
    if (null == line) {
      reader.close();
      conn.disconnect();
      reader = null;
      metadata = null;
      urlidx++;
      return hasNext();
    }
    
    //
    // Decode Metadata
    //
    
    byte[] bytes = OrderPreservingBase64.decode(line.getBytes(StandardCharsets.US_ASCII));
    
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    Metadata meta = new Metadata();
    
    deserializer.deserialize(meta, bytes);
    
    metadata = meta;
    
    return true;
  }
    
  @Override
  public Metadata next() throws NoSuchElementException {        
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    synchronized(this) {
      Metadata meta = metadata;
      metadata = null;
      GTSHelper.internalizeStrings(meta);
      return meta;
    }
  }
  
  @Override
  public void close() throws Exception {
    if (null != this.reader) {
      try { this.reader.close(); } catch (Exception e) {}
      this.reader = null;
    }
    if (null != this.stream) {
      try { this.stream.close(); } catch (Exception e) {}
      this.stream = null;
    }
    if (null != this.conn) {
      try { this.conn.disconnect(); } catch (Exception e) {}
      this.conn = null;
    }
  }
}

