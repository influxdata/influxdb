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

package io.warp10.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.util.concurrent.RateLimiter;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;

public class Warp10RecordWriter extends RecordWriter<Writable, Writable> {
  
  public static final String WARP10_GZIP = "warp10.gzip";
  public static final String WARP10_ENDPOINT = "warp10.endpoint";
  public static final String WARP10_TOKEN = "warp10.token";
  public static final String WARP10_MAXRATE = "warp10.maxrate";
  
  private final Properties props;
  
  private boolean init = false;
  
  private HttpURLConnection conn = null;

  private PrintWriter pw = null;
  
  private RateLimiter limiter = null;
  
  public Warp10RecordWriter(Properties props) {
    this.props = props;    
  }
  
  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      if (null != pw) {
        pw.flush();
        pw.close();
        int respcode = conn.getResponseCode();
        
        if (HttpURLConnection.HTTP_OK != respcode) {
          throw new IOException("HTTP code: " + respcode + " - " + conn.getResponseMessage());
        }        
      }
    } finally {
      if (null != pw) { try { pw.close(); } catch (Exception e) {} }
      if (null != conn) { try { conn.disconnect(); } catch (Exception e) {} }
    }    
  }
  
  @Override
  public void write(Writable key, Writable value) throws IOException, InterruptedException {
    if (!init) {
      synchronized(props) {
        if (!init) {
          init();
        }
      }
    }
    
    //
    // Assume the value is a GTSWrapper
    //
    
    long count = 0L;

    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      if (value instanceof BytesWritable) {
        deserializer.deserialize(gtsWrapper, ((BytesWritable) value).copyBytes());
      } else if (value instanceof Text) {
        deserializer.deserialize(gtsWrapper, OrderPreservingBase64.decode(((Text) value).copyBytes()));
      } else {
        throw new IOException("Invalid value class, expecting BytesWritable or Text, was " + value.getClass());
      }
    } catch (TException te) {
      throw new IOException(te);
    }

    Metadata metadataChunk;
    
    if (gtsWrapper.isSetMetadata()) {
      metadataChunk = new Metadata(gtsWrapper.getMetadata());
    } else {
      metadataChunk = new Metadata();
    }

    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(gtsWrapper);

    StringBuilder metasb = new StringBuilder();
    // We don't care about exposing labels since they are forced by the token
    GTSHelper.metadataToString(metasb, metadataChunk.getName(), metadataChunk.getLabels(), false);

    boolean first = true;

    while (decoder.next()) {
      if (null != this.limiter) {
        this.limiter.acquire(1);
      }

      if (!first) {
        this.pw.print("=");
        this.pw.println(GTSHelper.tickToString(null, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue()));
      } else {
        pw.println(GTSHelper.tickToString(metasb, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue()));
        first = false;
      }
      count++;
    }
  }  
  
  private void init() throws IOException {
    
    boolean gzip = "true".equals(props.getProperty(WARP10_GZIP));
    String endpoint = props.getProperty(WARP10_ENDPOINT);
    String token = props.getProperty(WARP10_TOKEN);
    String maxrate = props.getProperty(WARP10_MAXRATE);
    
    if (null != maxrate) {
      this.limiter = RateLimiter.create(Double.parseDouble(maxrate));
    }
    
    conn = (HttpURLConnection) new URL(endpoint).openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setDoInput(true);
    conn.setRequestProperty(Constants.HTTP_HEADER_TOKEN_DEFAULT, token);
    conn.setChunkedStreamingMode(65536);

    if (gzip) {
      conn.setRequestProperty("Content-Type", "application/gzip");
    }

    conn.connect();

    OutputStream out = conn.getOutputStream();

    if (gzip) {
      out = new GZIPOutputStream(out);
    }

    pw = new PrintWriter(out);
    
    this.init = true;
  }
}
