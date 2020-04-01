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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.std.RawTextLineWriter;
import com.fasterxml.sort.std.TextFileSorter;

import io.warp10.WarpURLEncoder;
import io.warp10.continuum.TextFileShuffler;
import io.warp10.continuum.store.Constants;

public class Warp10InputFormat extends InputFormat<Text, BytesWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(Warp10InputFormat.class);

  /**
   * Suffix as set via the configuration
   */
  public static final String PROPERTY_WARP10_INPUTFORMAT_SUFFIX = "warp10.inputformat.suffix";
  
  /**
   * URL of split endpoint
   */
  public static final String PROPERTY_WARP10_SPLITS_ENDPOINT = "warp10.splits.endpoint";
  
  /**
   * List of fallback fetchers
   */
  public static final String PROPERTY_WARP10_FETCHER_FALLBACKS = "warp10.fetcher.fallbacks";
  
  /**
   * Boolean indicating whether to use the fetchers or only the fallbacks
   */
  public static final String PROPERTY_WARP10_FETCHER_FALLBACKSONLY = "warp10.fetcher.fallbacksonly";

  /**
   * Protocol to use when contacting the fetcher (http or https), defaults to http
   */
  public static final String PROPERTY_WARP10_FETCHER_PROTOCOL = "warp10.fetcher.protocol";
  public static final String DEFAULT_WARP10_FETCHER_PROTOCOL = "http";
  
  /**
   * Port to use when contacting the fetcher, defaults to 8881
   */
  public static final String PROPERTY_WARP10_FETCHER_PORT = "warp10.fetcher.port";
  public static final String DEFAULT_WARP10_FETCHER_PORT = "8881";
  
  /**
   * URL Path of the fetcher, defaults to "/api/v0/sfetch"
   */
  public static final String PROPERTY_WARP10_FETCHER_PATH = "warp10.fetcher.path";
  public static final String DEFAULT_WARP10_FETCHER_PATH = Constants.API_ENDPOINT_SFETCH;

  /**
   * GTS Selector
   */
  public static final String PROPERTY_WARP10_SPLITS_SELECTOR = "warp10.splits.selector";
  
  /**
   * Token to use for selecting GTS
   */
  public static final String PROPERTY_WARP10_SPLITS_TOKEN = "warp10.splits.token";

  /**
   * Connection timeout to the splits and sfetch endpoints, defaults to 10000 ms
   */
  public static final String PROPERTY_WARP10_HTTP_CONNECT_TIMEOUT = "warp10.http.connect.timeout";
  public static final String DEFAULT_WARP10_HTTP_CONNECT_TIMEOUT = "10000";

  /**
   * Read timeout to the splits and sfetch endpoints, defaults to 10000 ms
   */
  public static final String PROPERTY_WARP10_HTTP_READ_TIMEOUT = "warp10.http.read.timeout";
  public static final String DEFAULT_WARP10_HTTP_READ_TIMEOUT = "10000";

  /**
   * Now parameter
   */
  public static final String PROPERTY_WARP10_FETCH_NOW = "warp10.fetch.now";

  /**
   * Timespan parameter
   */
  public static final String PROPERTY_WARP10_FETCH_TIMESPAN = "warp10.fetch.timespan";

  /**
   * Maximum number of splits to combined into a single split
   */
  public static final String PROPERTY_WARP10_MAX_COMBINED_SPLITS = "warp10.max.combined.splits";
  
  /**
   * Maximum number of splits we wish to produce
   */
  public static final String PROPERTY_WARP10_MAX_SPLITS = "warp10.max.splits";
  
  /**
   * Default Now HTTP Header
   */
  public static final String HTTP_HEADER_NOW_HEADER_DEFAULT = "X-Warp10-Now";

  /**
   * Default Timespan HTTP Header
   */
  public static final String HTTP_HEADER_TIMESPAN_HEADER_DEFAULT = "X-Warp10-Timespan";

  /**
   * Suffix for the properties
   */
  private String suffix = "";
  
  public Warp10InputFormat(String suffix) {
    if (null != suffix) {
      this.suffix = "." + suffix;
    } else {
      this.suffix = "";
    }
  }
  
  public Warp10InputFormat() {
    this.suffix = "";
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    
    String sfx = getProperty(context, PROPERTY_WARP10_INPUTFORMAT_SUFFIX);
    if (null != sfx) {
      if (!"".equals(sfx)) {
        this.suffix = "." + sfx;
      } else {
        this.suffix = "";
      }
    }
    
    List<String> fallbacks = new ArrayList<>();
    
    boolean fallbacksonly = "true".equals(getProperty(context, PROPERTY_WARP10_FETCHER_FALLBACKSONLY));
    
    if (null != getProperty(context, PROPERTY_WARP10_FETCHER_FALLBACKS)) {
      String[] servers = getProperty(context, PROPERTY_WARP10_FETCHER_FALLBACKS).split(",");
      for (String server: servers) {
        fallbacks.add(server);
      }
    }

    int connectTimeout = Integer.valueOf(getProperty(context, Warp10InputFormat.PROPERTY_WARP10_HTTP_CONNECT_TIMEOUT, Warp10InputFormat.DEFAULT_WARP10_HTTP_CONNECT_TIMEOUT));
    int readTimeout = Integer.valueOf(getProperty(context, Warp10InputFormat.PROPERTY_WARP10_HTTP_READ_TIMEOUT, Warp10InputFormat.DEFAULT_WARP10_HTTP_READ_TIMEOUT));

    //
    // Issue a call to the /splits endpoint to retrieve the individual splits
    //

    String splitEndpoint = getProperty(context, PROPERTY_WARP10_SPLITS_ENDPOINT);

    StringBuilder sb = new StringBuilder();
    sb.append(splitEndpoint);
    sb.append("?");
    sb.append(Constants.HTTP_PARAM_SELECTOR);
    sb.append("=");
    sb.append(WarpURLEncoder.encode(getProperty(context, PROPERTY_WARP10_SPLITS_SELECTOR), StandardCharsets.UTF_8));
    sb.append("&");
    sb.append(Constants.HTTP_PARAM_TOKEN);
    sb.append("=");
    sb.append(getProperty(context, PROPERTY_WARP10_SPLITS_TOKEN));
    
    URL url = new URL(sb.toString());

    LOG.info("Get splits from: " + splitEndpoint);

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();

    conn.setConnectTimeout(connectTimeout);
    conn.setReadTimeout(readTimeout);

    conn.setDoInput(true);
    
    InputStream in = conn.getInputStream();
    
    File infile = File.createTempFile("Warp10InputFormat-", "-in");
    infile.deleteOnExit();

    OutputStream out = new FileOutputStream(infile);
    
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    PrintWriter pw = new PrintWriter(out);
    
    int count = 0;
    
    Map<String,AtomicInteger> perServer = new HashMap<String,AtomicInteger>();
    
    while(true) {
      String line = br.readLine();
      if (null == line) {
        break;
      }
      // Count the total number of splits
      count++;
      // Count the number of splits per RS
      String server = line.substring(0, line.indexOf(' '));
      
      AtomicInteger scount = perServer.get(server);
      if (null == scount) {
        scount = new AtomicInteger(0);
        perServer.put(server, scount);
      }
      scount.addAndGet(1);

      pw.println(line);
    }

    pw.flush();
    out.close();
    br.close();
    in.close();
    conn.disconnect();

    TextFileSorter sorter = new TextFileSorter(new SortConfig().withMaxMemoryUsage(64000000L));

    File outfile = File.createTempFile("Warp10InputFormat-", "-out");
    outfile.deleteOnExit();

    in = new FileInputStream(infile);
    out = new FileOutputStream(outfile);
    
    try {
      sorter.sort(new TextFileShuffler.CustomReader<byte[]>(in), new RawTextLineWriter(out));
    } finally {      
      out.close();
      in.close();
      sorter.close();
      infile.delete();
    }
        
    //
    // Do a naive split generation, using the RegionServer as the ideal fetcher. We will need
    // to adapt this later so we ventilate the splits on all fetchers if we notice that a single
    // fetcher gets pounded too much
    //
    
    // Compute the maximum number of splits which can be combined given the number of servers (RS)
    int avgsplitcount = (int) Math.ceil((double) count / perServer.size());
    
    if (null != getProperty(context, PROPERTY_WARP10_MAX_SPLITS)) {
      int maxsplitavg = (int) Math.ceil((double) count / Integer.parseInt(getProperty(context, PROPERTY_WARP10_MAX_SPLITS)));
      
      avgsplitcount = maxsplitavg;
    }
    
    if (null != getProperty(context, PROPERTY_WARP10_MAX_COMBINED_SPLITS)) {
      int maxcombined = Integer.parseInt(getProperty(context, PROPERTY_WARP10_MAX_COMBINED_SPLITS));
      
      if (maxcombined < avgsplitcount) {
        avgsplitcount = maxcombined;
      }
    }

    List<InputSplit> splits = new ArrayList<>();
    
    br = new BufferedReader(new FileReader(outfile));
    
    Warp10InputSplit split = new Warp10InputSplit();
    String lastserver = null;
    int subsplits = 0;
    
    while(true) {
      String line = br.readLine();

      if (null == line) {
        break;
      }
      
      String[] tokens = line.split("\\s+");
      
      // If the server changed or we've reached the maximum split size, flush the current split.
      
      if (null != lastserver && !lastserver.equals(tokens[0]) || avgsplitcount == subsplits) {
        // Add fallback fetchers, shuffle them first
        Collections.shuffle(fallbacks);
        for (String fallback: fallbacks) {
          split.addFetcher(fallback);
        }
        splits.add(split.build());

        split = new Warp10InputSplit();
        subsplits = 0;
      }
      
      subsplits++;

      split.addEntry(fallbacksonly ? null : tokens[0], tokens[2]);
    }
    
    br.close();

    outfile.delete();
    
    if (subsplits > 0) {
      // Add fallback fetchers, shuffle them first
      Collections.shuffle(fallbacks);
      for (String fallback: fallbacks) {
        split.addFetcher(fallback);
      }
      splits.add(split.build());

    }

    LOG.info("Number of splits: " + splits.size());

    return splits;

//    //
//    // We know we have 'count' splits to combine and we know how many splits are hosted on each
//    // server
//    //
//    
//    // Compute the average number of splits per combined split
//    int avgsplitcount = (int) Math.ceil((double) count / numSplits);
//    
//    // Compute the average number of splits per server
//    int avgsplitpersrv = (int) Math.ceil((double) count / perServer.size());
//    
//    //
//    // Determine the number of ideal (i.e. associated with the right server) combined splits
//    // per server
//    //
//    
//    Map<String,AtomicInteger> idealcount = new HashMap<String,AtomicInteger>();
//    
//    for (Entry<String,AtomicInteger> entry: perServer.entrySet()) {
//      idealcount.put(entry.getKey(), new AtomicInteger(Math.min((int) Math.ceil(entry.getValue().doubleValue() / avgsplitcount), avgsplitpersrv)));
//    }
//    
//    //
//    // Compute the number of available slots per server after the maximum ideal combined splits
//    // have been allocated
//    //
//    
//    Map<String,AtomicInteger> freeslots = new HashMap<String,AtomicInteger>();
//    
//    for (Entry<String,AtomicInteger> entry: perServer.entrySet()) {
//      if (entry.getValue().get() < avgsplitpersrv) {
//        freeslots.put(entry.getKey(), new AtomicInteger(avgsplitpersrv - entry.getValue().get()));
//      }
//    }
//
//    //
//    // Generate splits
//    // We know the input file is sorted by server then region
//    //
//    
//    br = new BufferedReader(new FileReader(outfile));
//    
//    Warp10InputSplit split = null;
//    String lastsrv = null;
//    int subsplits = 0;
//    
//    List<Warp10InputSplit> splits = new ArrayList<Warp10InputSplit>();
//    
//    while(true) {
//      String line = br.readLine();
//      
//      if (null == line) {
//        break;
//      }
//      
//      // Split line into tokens
//      String[] tokens = line.split("\\s+");
//      
//      // If the srv changed, flush the split
//      if (null != lastsrv && lastsrv != tokens[0]) {
//        splits.add(split);
//        split = null;
//      }
//      
//      
//      if (null == splitsrv) {
//        splitsrv = tokens[0];
//        // Check if 'splitsrv' can host more splits
//        if (idealcount.get(splitsrv))
//      }
//      // Emit current split if it is full
//      
//      if (avgsplitcount == subsplits) {
//        
//      }
//    }
//    
//    System.out.println("NSPLITS=" + count);
//    
//    System.out.println("AVG=" + avgsplit);
//    System.out.println(perServer);
//    return null;
  }
  
  @Override
  public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    if (!(split instanceof Warp10InputSplit)) {
      throw new IOException("Invalid split type.");
    }
    return new Warp10RecordReader(this.suffix);
  }

  private String getProperty(JobContext context, String property) {
    return getProperty(context, property, null);
  }

  private String getProperty(JobContext context, String property, String defaultValue) {
    return getProperty(context.getConfiguration(), this.suffix, property, defaultValue);
  }

  public static String getProperty(Configuration conf, String suffix, String property, String defaultValue) {
    if (null != conf.get(property + suffix)) {
      return conf.get(property + suffix);      
    } else if (null != conf.get(property)) {
      return conf.get(property);
    } else if (null != defaultValue) {
      return defaultValue;
    } else {
      return null;
    }
  }
}
