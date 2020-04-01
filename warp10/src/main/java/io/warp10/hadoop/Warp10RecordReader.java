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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.crypto.OrderPreservingBase64;

public class Warp10RecordReader extends RecordReader<Text, BytesWritable> implements Progressable {

  private BufferedReader br = null;
  private HttpURLConnection conn = null;

  private Text key;
  private BytesWritable value;

  private long count = 0;

  private static final Logger LOG = LoggerFactory.getLogger(Warp10RecordReader.class);

  private Progressable progress = null;

  private final String suffix;
  
  public Warp10RecordReader() {
    this.suffix = "";
  }

  public Warp10RecordReader(String suffix) {
    this.suffix = suffix;
  }
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    if (!(split instanceof Warp10InputSplit)) {
      throw new IOException("Invalid split type.");
    }

    this.progress = context;

    //
    // Retrieve now and timespan parameters
    //

    long now = Long.valueOf(getProperty(context, Warp10InputFormat.PROPERTY_WARP10_FETCH_NOW));
    long timespan = Long.valueOf(getProperty(context, Warp10InputFormat.PROPERTY_WARP10_FETCH_TIMESPAN));

    int connectTimeout = Integer.valueOf(getProperty(context, Warp10InputFormat.PROPERTY_WARP10_HTTP_CONNECT_TIMEOUT, Warp10InputFormat.DEFAULT_WARP10_HTTP_CONNECT_TIMEOUT));
    int readTimeout = Integer.valueOf(getProperty(context, Warp10InputFormat.PROPERTY_WARP10_HTTP_READ_TIMEOUT, Warp10InputFormat.DEFAULT_WARP10_HTTP_READ_TIMEOUT));

    //
    // Call each provided fetcher until one answers
    //

    String protocol = getProperty(context, Warp10InputFormat.PROPERTY_WARP10_FETCHER_PROTOCOL, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PROTOCOL);
    String port = getProperty(context, Warp10InputFormat.PROPERTY_WARP10_FETCHER_PORT, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PORT);
    String path = getProperty(context, Warp10InputFormat.PROPERTY_WARP10_FETCHER_PATH, Warp10InputFormat.DEFAULT_WARP10_FETCHER_PATH);

    // FIXME: use Constants instead ?? but warp.timeunits is mandatory and property file must be provided..
    String nowHeader = getProperty(context, Configuration.HTTP_HEADER_NOW_HEADERX, Warp10InputFormat.HTTP_HEADER_NOW_HEADER_DEFAULT);
    String timespanHeader = getProperty(context, Configuration.HTTP_HEADER_TIMESPAN_HEADERX, Warp10InputFormat.HTTP_HEADER_TIMESPAN_HEADER_DEFAULT);

    for (String fetcher: split.getLocations()) {
      try {

        StringBuilder endpointSb = new StringBuilder();
        endpointSb.append(protocol);
        endpointSb.append("://");
        endpointSb.append(fetcher);
        endpointSb.append(":");
        endpointSb.append(port);

        StringBuilder sb = new StringBuilder();
        sb.append(endpointSb.toString());
        sb.append(path);

        URL url = new URL(sb.toString());

        LOG.info("Fetcher: " + endpointSb.toString());

        conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(connectTimeout);
        conn.setReadTimeout(readTimeout);
        conn.setChunkedStreamingMode(16384);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setRequestProperty(nowHeader, Long.toString(now));
        conn.setRequestProperty(timespanHeader, Long.toString(timespan));
        conn.setRequestProperty("Content-Type", "application/gzip");
        conn.connect();

        OutputStream out = conn.getOutputStream();

        out.write(((Warp10InputSplit)split).getBytes());

        if (HttpURLConnection.HTTP_OK != conn.getResponseCode()) {
          System.err.println(url + " failed - error code: " + conn.getResponseCode());
          InputStream is = conn.getErrorStream();
          BufferedReader errorReader = new BufferedReader(new InputStreamReader(is));
          String line = errorReader.readLine();
          while (null != line) {
            System.err.println(line);
            line = errorReader.readLine();
          }
          is.close();
          continue;
        }

        this.br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

        break;
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error(e.getMessage(),e);
      } finally {
        if (null == this.br && null != conn) {
          try { conn.disconnect(); } catch (Exception e) {}
          conn = null;
        }
      }
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (null == br) {
      return false;
    }

    String line = br.readLine();
    
    if (null == line) {
      return false;
    }

    if (line.startsWith(Constants.EGRESS_FETCH_ERROR_PREFIX)) {
      throw new IOException("Fetcher reported an error, aborting: " + line);
    }
    
    // Format: GTSWrapperId <WSP> HASH <WSP> GTSWrapper

    String[] tokens = line.split("\\s+");

    if (null == key) {
      key = new Text();
    }

    key.set(tokens[0]);

    if (null == value) {
      value = new BytesWritable();
    }

    byte[] wrapper = OrderPreservingBase64.decode(tokens[2].getBytes(StandardCharsets.US_ASCII));

    value.setCapacity(wrapper.length);
    value.set(wrapper, 0, wrapper.length);
    
    count++;
    
    return true;
  }
  
  @Override
  public void close() throws IOException {
    if (null != this.br) {
      this.br.close();
    }
    if (null != this.conn) {
      this.conn.disconnect();
    }
  }

  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() {
    return value;
  }
  
  @Override
  public float getProgress() throws IOException {
    return -1.0F;
  }

  @Override public void progress() {
    if (null != this.progress) {
      this.progress.progress();
    }
  }
  
  private String getProperty(JobContext context, String property) {
    return getProperty(context, property, null);
  }
  
  private String getProperty(JobContext context, String property, String defaultValue) {
    if (null != context.getConfiguration().get(property + suffix)) {
      return context.getConfiguration().get(property + suffix);      
    } else if (null != context.getConfiguration().get(property)) {
      return context.getConfiguration().get(property);
    } else if (null != defaultValue) {
      return defaultValue;
    } else {
      return null;
    }
  }
}
