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
package io.warp10.continuum.ingress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.GZIPOutputStream;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.SortedPathIterator;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.DatalogRequest;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandaloneAcceleratedStoreClient;

/**
 * Forward UPDATA/META/DELETE requests to another Warp 10 instance
 */
public class DatalogForwarder extends Thread {

  private final String name;
  
  private static final Logger LOG = LoggerFactory.getLogger(DatalogForwarder.class);
  
  public static final String DATALOG_SUFFIX = ".datalog";
  
  /**
   * Queues to forward datalog actions according to token
   */
  private final LinkedBlockingDeque<DatalogAction>[] queues;
  
  private final Path rootdir;
  
  private final byte[] datalogPSK;
  
  private final long[] modulus;
  private final long[] remainder;
  
  /**
   * Number of bits to shift the shard key right.
   * If this is 24, then only the class Id will be considered
   */
  private final long shardkeyshift;
  
  /**
   * URL for the UPDATE endpoint
   */
  private final URL updateUrl;

  /**
   * URL for the DELETE endpoint
   */
  private final URL deleteUrl;
  
  /**
   * URL for the META endpoint
   */
  private final URL metaUrl;
  
  /**
   * Target directory where processed files are moved
   */
  private final File targetDir;
  
  /**
   * Period between directory scans
   */
  private final long period;
  
  /**
   * Should we compress update/meta requests we forward
   */
  private final boolean compress;
  
  /**
   * Should we forward the datalog request or act as a regular client
   */
  private final boolean actasclient;
  
  /**
   * IDs we should ignore and not forward, usually to avoid loops.
   */
  private final Set<String> ignoredIds;
  
  private static final String DEFAULT_PERIOD = "1000";
    
  /**
   * Set of files currently processed
   */
  private final Set<String> processing = ConcurrentHashMap.newKeySet();

  /**
   * Flag to indicate whether or not to delete forwarded requests
   */
  private final boolean deleteForwarded;

  /**
   * Flag to indicate whether or not to delete ignored requests
   */
  private final boolean deleteIgnored;
  
  public static enum DatalogActionType {
    UPDATE,
    DELETE,
    META
  }
  
  private static final class DatalogAction {
    private DatalogRequest request;
    private String encodedRequest;
    private File file;
  }
  
  private static final class DatalogForwarderWorker extends Thread {

    private final LinkedBlockingDeque<DatalogAction> queue;
    
    private final DatalogForwarder forwarder;
        
    private final String name;
    
    public DatalogForwarderWorker(DatalogForwarder forwarder, LinkedBlockingDeque<DatalogAction> queue) {
      this.name = forwarder.name;
      this.queue = queue;
      this.forwarder = forwarder;
      this.setDaemon(true);
      if (null != name) {
        this.setName("[Datalog Forwarder Worker '" + this.name + "']");
      } else {
        this.setName("[Datalog Forwarder Worker]");
      }
      this.start();
    }
    
    @Override
    public void run() {
      
      DatalogAction action = null;
      
      while(true) {        
        action = queue.peek();
        
        if (null == action) {
          LockSupport.parkNanos(100000000L);
          continue;
        }

        boolean processed = false;
        
        try {
          switch (DatalogActionType.valueOf(action.request.getType())) {
            case DELETE:
              if (!doDelete(action)) {
                continue;
              }
              break;
            case META:
              if (!doMeta(action)) {
                continue;
              }
              break;
            case UPDATE:
              if (!doUpdate(action)) {
                continue;
              }
              break;
          }
          
          //
          // Move the file to our target directory and remove the action from the queue as we forwarded it successfully
          //
          
          if (this.forwarder.deleteForwarded) {
            if (!action.file.delete()) {
              continue;
            }
          } else {
            if (!action.file.renameTo(new File(this.forwarder.targetDir, action.file.getName()))) {
              continue;
            }            
          }
          
          queue.poll();
          forwarder.processing.remove(action.file.getName());
          processed = true;
        } finally {
          if (!processed) {
            // Wait 10s before re-attempting the same file.
            LockSupport.parkNanos(10000000000L);
          }
        }
      }
    }
    
    private boolean doUpdate(DatalogAction action) {
      if (!DatalogActionType.UPDATE.equals(DatalogActionType.valueOf(action.request.getType()))) {
        return false;
      }
      
      BufferedReader br = null;

      HttpURLConnection conn = null;
      
      try {
        br = new BufferedReader(new FileReader(action.file));
        
        conn = (HttpURLConnection) forwarder.updateUrl.openConnection();
        
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestMethod("POST");
        
        if (forwarder.actasclient) {
          if (action.request.isDeltaAttributes()) {
            conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES), "delta");
          }
          conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX), action.request.getToken());
          if (action.request.isSetNow()) {
            conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX), action.request.getNow());
          }
          if (action.request.getAttributesSize() > 0) {
            String accel = "";
            if (null != action.request.getAttributes().get(StandaloneAcceleratedStoreClient.ATTR_NOCACHE)) {
              accel += StandaloneAcceleratedStoreClient.NOCACHE + " ";
            }
            if (null != action.request.getAttributes().get(StandaloneAcceleratedStoreClient.ATTR_NOPERSIST)) {
              accel += StandaloneAcceleratedStoreClient.NOPERSIST;              
            }
            if (!"".equals(accel)) {
              conn.setRequestProperty(StandaloneAcceleratedStoreClient.ACCELERATOR_HEADER, accel);
            }
          }
        } else {
          conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DATALOG), action.encodedRequest);
        }
        
        if (forwarder.compress) {
          conn.setRequestProperty("Content-Type", "application/gzip");
        }
        
        conn.setChunkedStreamingMode(16384);
        conn.connect();
        
        OutputStream os = conn.getOutputStream();
        
        OutputStream out = null;
        
        if (forwarder.compress) {
          out = new GZIPOutputStream(os);
        } else {
          out = os;
        }
        
        PrintWriter pw = new PrintWriter(out);
                
        boolean first = true;
        
        boolean include = false;

        while(true) {
          String line = br.readLine();
          if (null == line) {
            break;
          }
          // Ignore first line as it is the DatalogRequest
          if (first) {
            first = false;
            continue;
          }

          // If shards are defined, check the shard key          
          if (null != forwarder.modulus && null != forwarder.remainder) {
            if (line.length() >= 3 && '#' == line.charAt(0) && 'K' == line.charAt(1)) {
              // Extract the shard key
              long shardkey = Long.parseLong(line.substring(2));
              
              shardkey >>>= forwarder.shardkeyshift;
              
              include = false;
              
              // Check if one shard matches, in which case we print out the line and continue
              for (int i = 0; i < forwarder.modulus.length; i++) {
                if (shardkey % forwarder.modulus[i] == forwarder.remainder[i]) {
                  include = true;
                  break;
                }
              }
              continue;
            } else {
              // Ignore line if shard is not included in those we forward
              if (!include) {
                continue;
              }
            }
          } else {
            // No shards defined, include everything
            include = true;
          }
          
          if (include) {
            pw.println(line);
          }
        }
        
        pw.close();

        br.close();
        
        //
        // Update was successful, delete all batchfiles
        //
        
        boolean success = 200 == conn.getResponseCode();
        
        if (!success) {
          LOG.error(conn.getResponseMessage());
        }
        
        conn.disconnect();
        conn = null;

        Map<String,String> labels = new HashMap<String,String>();
        
        if (null != name) {
          labels.put(SensisionConstants.SENSISION_LABEL_FORWARDER, name);
        }

        labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(action.request.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
        labels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogActionType.UPDATE.name());
        
        if (success) {
          Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FORWARDED, labels, 1);
        } else {
          Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FAILED, labels, 1);
        }

        return success;
      } catch (IOException ioe) {
        Map<String,String> labels = new HashMap<String,String>();
        
        if (null != name) {
          labels.put(SensisionConstants.SENSISION_LABEL_FORWARDER, name);
        }

        labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(action.request.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
        labels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogActionType.UPDATE.name());
        
        Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FAILED, labels, 1);

        return false;
      } finally {
        if (null != conn) {
          conn.disconnect();
        }
        if (null != br) { try { br.close(); } catch (IOException ioe) {} }
      }
    }
    
    private boolean doDelete(DatalogAction action) {
      if (!DatalogActionType.DELETE.equals(DatalogActionType.valueOf(action.request.getType()))) {
        return false;
      }
      
      HttpURLConnection conn = null;
      
      try {
        URL urlAndQS = new URL(forwarder.deleteUrl.toString() + "?" + action.request.getDeleteQueryString());
        
        conn = (HttpURLConnection) urlAndQS.openConnection();
        
        conn.setDoInput(true);
        conn.setRequestMethod("GET");
        if (forwarder.actasclient) {
          conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX), action.request.getToken());
        } else {
          conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DATALOG), action.encodedRequest);
        }
        conn.connect();
                
        //
        // Update was successful, delete all batchfiles
        //
        
        boolean success = 200 == conn.getResponseCode();
        
        if (!success) {
          LOG.error(conn.getResponseMessage());
        }
        
        conn.disconnect();
        conn = null;

        Map<String,String> labels = new HashMap<String,String>();
        
        if (null != name) {
          labels.put(SensisionConstants.SENSISION_LABEL_FORWARDER, name);
        }

        labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(action.request.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
        labels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogActionType.DELETE.name());
        if (success) {
          Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FORWARDED, labels, 1);
        } else {
          Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FAILED, labels, 1);
        }
        
        return success;
      } catch (IOException ioe) {
        Map<String,String> labels = new HashMap<String,String>();
        
        if (null != name) {
          labels.put(SensisionConstants.SENSISION_LABEL_FORWARDER, name);
        }

        labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(action.request.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
        labels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogActionType.DELETE.name());
        Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FAILED, labels, 1);

        return false;
      } finally {
        if (null != conn) {
          conn.disconnect();
        }
      }
    }
    
    private boolean doMeta(DatalogAction action) {
      if (!DatalogActionType.META.equals(DatalogActionType.valueOf(action.request.getType()))) {
        return false;
      }
      
      BufferedReader br = null;

      HttpURLConnection conn = null;
      
      try {
        br = new BufferedReader(new FileReader(action.file));
        
        conn = (HttpURLConnection) forwarder.metaUrl.openConnection();
        
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestMethod("POST");
        
        if (forwarder.actasclient) {
          if (action.request.isDeltaAttributes()) {
            conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES), "delta");
          }
          
          conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX), action.request.getToken());
        } else {
          conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_DATALOG), action.encodedRequest);
        }
        
        if (forwarder.compress) {
          conn.setRequestProperty("Content-Type", "application/gzip");
        }
        
        conn.setChunkedStreamingMode(16384);
        conn.connect();
        
        OutputStream os = conn.getOutputStream();
        
        OutputStream out = null;
        
        if (forwarder.compress) {
          out = new GZIPOutputStream(os);
        } else {
          out = os;
        }
        
        PrintWriter pw = new PrintWriter(out);
                
        boolean first = true;
        
        while(true) {
          String line = br.readLine();
          if (null == line) {
            break;
          }
          // Discard the DatalogRequest line
          if (first) {
            first = false;
            continue;
          }
          pw.println(line);
        }
        
        pw.close();

        br.close();
        
        //
        // Update was successful, delete all batchfiles
        //
        
        boolean success = 200 == conn.getResponseCode();
        
        if (!success) {
          LOG.error(conn.getResponseMessage());
        }

        conn.disconnect();
        conn = null;

        Map<String,String> labels = new HashMap<String,String>();
        
        if (null != name) {
          labels.put(SensisionConstants.SENSISION_LABEL_FORWARDER, name);
        }

        labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(action.request.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
        labels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogActionType.META.name());
        if (success) {
          Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FORWARDED, labels, 1);
        } else {
          Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FAILED, labels, 1);
        }

        return success;
      } catch (IOException ioe) {
        Map<String,String> labels = new HashMap<String,String>();
        
        if (null != name) {
          labels.put(SensisionConstants.SENSISION_LABEL_FORWARDER, name);
        }

        labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(action.request.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
        labels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogActionType.META.name());
        Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_FAILED, labels, 1);

        return false;
      } finally {
        if (null != conn) {
          conn.disconnect();
        }
        if (null != br) { try { br.close(); } catch (IOException ioe) {} }
      }
    }
    
  }
  
  public DatalogForwarder(KeyStore keystore, Properties properties) throws Exception {
    this(null, keystore, properties);
  }
  
  public DatalogForwarder(String name, KeyStore keystore, Properties properties) throws Exception {
  
    this.name = name;
    
    String suffix = "";
    
    if (null != name) {
      suffix = "." + name;
      LOG.info("Initializing datalog forwarder '" + name + "'.");
    } else {
      LOG.info("Initializing datalog forwarder.");
    }
    
    this.rootdir = new File(properties.getProperty(Configuration.DATALOG_FORWARDER_SRCDIR + suffix)).toPath().toRealPath();
    
    if (!this.rootdir.toFile().isDirectory()) {
      throw new RuntimeException("Invalid datalog forwarder source directory '" + this.rootdir + "'.");
    }

    if (properties.containsKey(Configuration.DATALOG_PSK)) {
      this.datalogPSK = keystore.decodeKey(properties.getProperty(Configuration.DATALOG_PSK));
    } else {
      this.datalogPSK = null;
    }
    
    this.period = Long.parseLong(properties.getProperty(Configuration.DATALOG_FORWARDER_PERIOD + suffix, DEFAULT_PERIOD));
    
    this.compress = "true".equals(properties.getProperty(Configuration.DATALOG_FORWARDER_COMPRESS + suffix));
    
    this.actasclient = "true".equals(properties.getProperty(Configuration.DATALOG_FORWARDER_ACTASCLIENT + suffix));
    
    this.ignoredIds = new HashSet<String>();
    
    if (properties.containsKey(Configuration.DATALOG_FORWARDER_IGNORED + suffix)) {
      String[] ids = properties.getProperty(Configuration.DATALOG_FORWARDER_IGNORED + suffix).split(",");
      
      for (String id: ids) {
        ignoredIds.add(id.trim());
      }
    }
    
    if (!properties.containsKey(Configuration.DATALOG_FORWARDER_DSTDIR + suffix)) {
      throw new RuntimeException("Datalog forwarder target directory (" +  Configuration.DATALOG_FORWARDER_DSTDIR + suffix + ") not set.");
    }

    this.targetDir = new File(properties.getProperty(Configuration.DATALOG_FORWARDER_DSTDIR + suffix));

    if (!this.targetDir.isDirectory()) {
      throw new RuntimeException("Invalid datalog forwarder target directory '" + this.targetDir + "'.");
    }
    
    this.deleteForwarded = "true".equals(properties.getProperty(Configuration.DATALOG_FORWARDER_DELETEFORWARDED + suffix));
    this.deleteIgnored = "true".equals(properties.getProperty(Configuration.DATALOG_FORWARDER_DELETEIGNORED + suffix));
    
    int nthreads = Integer.parseInt(properties.getProperty(Configuration.DATALOG_FORWARDER_NTHREADS + suffix, "1"));
    
    if (!properties.containsKey(Configuration.DATALOG_FORWARDER_ENDPOINT_UPDATE + suffix)) {
      throw new RuntimeException("Missing UPDATE endpoint.");
    }
    this.updateUrl = new URL(properties.getProperty(Configuration.DATALOG_FORWARDER_ENDPOINT_UPDATE + suffix));

    if (!properties.containsKey(Configuration.DATALOG_FORWARDER_ENDPOINT_DELETE + suffix)) {
      throw new RuntimeException("Missing DELETE endpoint.");
    }
    this.deleteUrl = new URL(properties.getProperty(Configuration.DATALOG_FORWARDER_ENDPOINT_DELETE + suffix));

    if (!properties.containsKey(Configuration.DATALOG_FORWARDER_ENDPOINT_META + suffix)) {
      throw new RuntimeException("Missing META endpoint.");
    }
    this.metaUrl = new URL(properties.getProperty(Configuration.DATALOG_FORWARDER_ENDPOINT_META + suffix));

    if (properties.containsKey(Configuration.DATALOG_FORWARDER_SHARDS + suffix)) {
      
      this.shardkeyshift = Long.parseLong(properties.getProperty(Configuration.DATALOG_FORWARDER_SHARDKEY_SHIFT + suffix, "0"));
      
      if (this.shardkeyshift >= 48 || this.shardkeyshift < 0) {
        throw new RuntimeException("Invalid shard key shifting.");
      }
      
      String[] shards = properties.getProperty(Configuration.DATALOG_FORWARDER_SHARDS + suffix).split(",");
      
      this.modulus = new long[shards.length];
      this.remainder = new long[shards.length];
        
      int idx = 0;
        
      for (String shard: shards) {
        String[] tokens = shard.trim().split(":");
        if (2 != tokens.length) {
          throw new RuntimeException("Invalid shard specification " + shard);
        }
        this.modulus[idx] = Long.parseLong(tokens[0]);
        this.remainder[idx] = Long.parseLong(tokens[1]);
          
        if (this.modulus[idx] < 1 || this.remainder[idx] >= this.modulus[idx] || this.remainder[idx] < 0) {
          throw new RuntimeException("Invalid shard specification " + shard);
        }
        
        idx++;
      }
    } else {
      this.modulus = null;
      this.remainder = null;
      this.shardkeyshift = 0;
    }

    queues = new LinkedBlockingDeque[nthreads];
    
    for (int i = 0; i < nthreads; i++) {
      queues[i] = new LinkedBlockingDeque<DatalogAction>(64);
      DatalogForwarderWorker forwarder = new DatalogForwarderWorker(this, queues[i]);
    }
    
    if (null == name) {
      this.setName("[Datalog Forwarder]");
    } else {
      this.setName("[Datalog Forwarder '" + name + "']");
    }
    this.setDaemon(true);
    this.start();
  }
  
  @Override
  public void run() {
    while (true) {
      
      //
      // Copy the list of files currently being processed so we don't risk
      // attempting to process a file that was still present when we scanned the
      // directory but which has been processed since. This can happen when there
      // are lots of files.
      //
      
      Set<String> ongoingProcessing = new HashSet<String>(this.processing);
      
      //
      // Scan the datalog directory
      //
      
      DirectoryStream<Path> ds = null;
      
      try {
        ds = Files.newDirectoryStream(rootdir, "*" + DATALOG_SUFFIX);        
      } catch (IOException ioe) {
        LOG.error("Error while getting file list for directory " + rootdir, ioe);
        LockSupport.parkNanos(1000000000L);
        continue;
      }
      
      Iterator<Path> iter = null;
      
      try {
        iter = new SortedPathIterator(ds.iterator());
      } catch (IOException ioe) {
        LOG.error("Error while getting path iterator.");
        LockSupport.parkNanos(1000000000L);
        continue;
      }
      
      while(iter.hasNext()) {
        //
        // Extract timestamp/id
        //
        
        Path p = iter.next();
        String filename = p.getFileName().toString();
        
        //
        // Skip file if it is currently being processed
        //
        
        if (ongoingProcessing.contains(filename)) {
          continue;
        }
        
        String[] subtokens = filename.split("-");
        
        long ts = new BigInteger(subtokens[0], 16).longValue();
        String id = subtokens[1];
        
        DatalogAction action = new DatalogAction();
        
        action.file = p.toFile();
        
        // Delete and skip empty files
        if (0 == action.file.length()) {
          action.file.delete();
          continue;
        }
        
        //
        // Read DatalogRequest
        //
        
        String encoded;
        
        try {
          BufferedReader br = new BufferedReader(new FileReader(action.file));
          encoded = br.readLine();
          
          if ('#' == encoded.charAt(0)) {
            encoded = encoded.substring(1);
          }
          
          br.close();          
        } catch (IOException ioe) {
          LOG.error("Error while reading Datalog Request", ioe);
          break;
        }
        
        byte[] bytes = encoded.getBytes(StandardCharsets.US_ASCII);
        byte[] data = null;

        data = OrderPreservingBase64.decode(bytes);          
                
        if (null != this.datalogPSK) {
          data = CryptoUtils.unwrap(this.datalogPSK, data);
        }
        
        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
        
        DatalogRequest dr = new DatalogRequest();

        try {
          deser.deserialize(dr, data);
        } catch (TException te) {
          LOG.error("Error while deserializing Datalog Request", te);
          break;
        }
        
        action.request = dr;
        action.encodedRequest = encoded;

        //
        // Check that timestamp and id match
        //
        
        if (ts != dr.getTimestamp()) {
          LOG.error("Datalog Request '" + action.file + "' has a timestamp which differs from that of its file, timestamp is 0x" + Long.toHexString(dr.getTimestamp()));
          break;
        }
        
        if (!id.equals(dr.getId())) {
          LOG.error("Datalog Request '" + action.file + "' has an id which differs from that of its file, id is " + dr.getId());
          break;          
        }
        
        //
        // Check if id should be ignored
        //
        
        String decodedId = new String(OrderPreservingBase64.decode(id.getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8);
        if (this.ignoredIds.contains(decodedId)) {
          Map<String,String> labels = new HashMap<String,String>();
          
          if (null != name) {
            labels.put(SensisionConstants.SENSISION_LABEL_FORWARDER, name);
          }
          
          labels.put(SensisionConstants.SENSISION_LABEL_ID, decodedId);
          labels.put(SensisionConstants.SENSISION_LABEL_TYPE, DatalogActionType.UPDATE.name());
          Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_FORWARDER_REQUESTS_IGNORED, labels, 1);

          // File should be ignored, move it directly to the target directory
          if (this.deleteIgnored) {
            action.file.delete();
          } else {
            action.file.renameTo(new File(this.targetDir, action.file.getName()));
          }
          continue;
        }
        
        //
        // Dispatch the action to the correct queue according to the producer/app/owner of the token
        //
        
        WriteToken wtoken;
        
        try {
          wtoken = Tokens.extractWriteToken(dr.getToken());
        } catch (WarpScriptException ee) {
          LOG.error("Encountered error while extracting token.", ee);
          break;
        }

        String application = wtoken.getAppName();
        String producer = Tokens.getUUID(wtoken.getProducerId());
        String owner = Tokens.getUUID(wtoken.getOwnerId());

        String hashkey = producer + "/" + application + "/" + owner;
        
        int q = ((hashkey.hashCode() % queues.length) + queues.length) % queues.length;
        
        try {
          queues[q].put(action);
          processing.add(action.file.getName());
        } catch (InterruptedException ie) {
          break;
        }                                
      }
      
      try {
        ds.close();
      } catch (IOException ioe) {        
      }
      
      LockSupport.parkNanos(this.period * 1000000L);
    }
  }
  
  public Path getRootDir() {
    return this.rootdir;
  }
}
