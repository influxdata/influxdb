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

package io.warp10.standalone;

import io.warp10.ThrowableUtils;
import io.warp10.WarpConfig;
import io.warp10.WarpManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.ingress.DatalogForwarder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.sensision.Sensision;
import io.warp10.warp.sdk.IngressPlugin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocket handler which handles streaming updates
 * 
 * WARNING: since we push GTSEncoders only after we reached a threshold of we've changed GTS, plasma consumers
 *          will only see updates once the GTSEncoder has been transmitted to the StoreClient
 */
public class StandaloneStreamUpdateHandler extends WebSocketHandler.Simple {
    
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneStreamUpdateHandler.class);
  
  private final KeyStore keyStore;
  private final Properties properties;
  private final StoreClient storeClient;
  private final StandaloneDirectoryClient directoryClient;
  private final long maxValueSize;
  
  private final String datalogId;
  private final boolean logShardKey;
  private final byte[] datalogPSK;
  private final boolean datalogSync;
  private final File loggingDir;
  
  private final long[] classKeyLongs;
  private final long[] labelsKeyLongs;

  private final boolean updateActivity;
  private final boolean parseAttributes;
  private final boolean allowDeltaAttributes;
  private final Long maxpastDefault;
  private final Long maxfutureDefault;
  private final Long maxpastOverride;
  private final Long maxfutureOverride;
  private final boolean ignoreOutOfRange;
  
  private IngressPlugin plugin = null;
  
  private final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSS").withZoneUTC();

  @WebSocket(maxTextMessageSize=1024 * 1024, maxBinaryMessageSize=1024 * 1024)
  public static class StandaloneStreamUpdateWebSocket {
    
    private static final int METADATA_CACHE_SIZE = 1000;
    
    private StandaloneStreamUpdateHandler handler;
    
    private boolean errormsg = false;
    
    private boolean deltaAttributes = false;
    
    private long seqno = 0L;
    
    private long maxsize;
    private WriteToken wtoken;

    private Boolean ignoor = null;
    
    private boolean expose = false;
    
    private Long maxpastdelta = null;
    private Long maxfuturedelta = null;
    
    private String encodedToken;
        
    /**
     * Cache used to determine if we should push metadata into Kafka or if it was previously seen.
     * Key is a BigInteger constructed from a byte array of classId+labelsId (we cannot use byte[] as map key)
     */
    private final Map<BigInteger, Object> metadataCache = new LinkedHashMap<BigInteger, Object>(100, 0.75F, true) {
      @Override
      protected boolean removeEldestEntry(java.util.Map.Entry<BigInteger, Object> eldest) {
        return this.size() > METADATA_CACHE_SIZE;
      }
    };
    
    private Map<String,String> sensisionLabels = new HashMap<String,String>();
    
    private Map<String,String> extraLabels = null;
    
    @OnWebSocketConnect
    public void onWebSocketConnect(Session session) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_REQUESTS, sensisionLabels, 1);
    }
    
    @OnWebSocketMessage
    public void onWebSocketMessage(Session session, String message) throws Exception {
      
      try {
            
        if (null != WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)) {
          throw new IOException(String.valueOf(WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)));
        }

        //
        // Split message on whitespace boundary if the message starts by a known verb
        //
        
        String[] tokens = null;
      
        if (message.startsWith("TOKEN") || message.startsWith("CLEARTOKEN") || message.startsWith("NOOP") || message.startsWith("ONERROR")) {
          tokens = message.split("\\s+");
          tokens[0] = tokens[0].trim();
        }
              
        
        if (null != tokens && "TOKEN".equals(tokens[0])) {
          
          setToken(tokens[1]);
          
          session.getRemote().sendString("OK " + (seqno++) + " TOKEN");
        } else if (null != tokens && "CLEARTOKEN".equals(tokens[0])) {
          // Clear the current token
          this.wtoken = null;
          session.getRemote().sendString("OK " + (seqno++) + " CLEARTOKEN");
        } else if (null != tokens && "NOOP".equals(tokens[0])) {
          // Do nothing...
          session.getRemote().sendString("OK " + (seqno++) + " NOOP");
        } else if (null != tokens && "ONERROR".equals(tokens[0])) {
          if ("message".equalsIgnoreCase(tokens[1])) {
            this.errormsg = true;
          } else if ("close".equalsIgnoreCase(tokens[1])) {
            this.errormsg = false;
          }
          session.getRemote().sendString("OK " + (seqno++) + " ONERROR");
        } else if ("DELTAON".equals(tokens[0])) {
          if (!this.handler.allowDeltaAttributes) {
            throw new IOException("Delta update of attributes is disabled.");
          }
          this.deltaAttributes = true;
        } else if ("DELTAOFF".equals(tokens[0])) {
          this.deltaAttributes = false;
        } else {
          //
          // Anything else is considered a measurement
          //
          
          long nano = System.nanoTime();
          
          //
          // Loop on all lines
          //
          
          int count = 0;
          
          long now = TimeSource.getTime();
          
          //
          // Extract time limits
          //
          
          Long maxpast = null;
          
          if (null != this.handler.maxpastDefault) {
            try {
              maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, this.handler.maxpastDefault));
            } catch (ArithmeticException ae) {
              maxpast = null;
            }
          }
          
          Long maxfuture = null;
          
          if (null != this.handler.maxfutureDefault) {
            try {
              maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, this.handler.maxfutureDefault));
            } catch (ArithmeticException ae) {
              maxfuture = null;
            }
          }

          if (null != this.maxpastdelta) {
            try {
              maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, this.maxpastdelta));
            } catch (ArithmeticException ae) {
              maxpast = null;
            }
          }

          if (null != this.maxfuturedelta) {
            try {
              maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, this.maxfuturedelta));
            } catch (ArithmeticException ae) {
              maxfuture = null;
            }
          }
          
          if (null != this.handler.maxpastOverride) {
            try {
              maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, this.handler.maxpastOverride));
            } catch (ArithmeticException ae) {
              maxpast = null;
            }
          }

          if (null != this.handler.maxfutureOverride) {
            try {
              maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, this.handler.maxfutureOverride));
            } catch (ArithmeticException ae) {
              maxfuture = null;
            }
          }

          File loggingFile = null;   
          PrintWriter loggingWriter = null;
          FileDescriptor loggingFD = null;
          DatalogRequest dr = null;

          long shardkey = 0L;

          AtomicLong ignoredCount = null;
          
          if ((this.handler.ignoreOutOfRange && !Boolean.FALSE.equals(ignoor)) || Boolean.TRUE.equals(ignoor)) {
            ignoredCount = new AtomicLong(0L);            
          }
          
          try {
            GTSEncoder lastencoder = null;
            GTSEncoder encoder = null;

            BufferedReader br = new BufferedReader(new StringReader(message));

            // Atomic boolean to track if attributes were parsed
            AtomicBoolean hadAttributes = this.handler.parseAttributes ? new AtomicBoolean(false) : null;

            boolean lastHadAttributes = false;
            
            do {
              
              if (this.handler.parseAttributes) {
                lastHadAttributes = lastHadAttributes || hadAttributes.get();
                hadAttributes.set(false);
              }
              
              String line = br.readLine();
              
              if (null == line) {
                break;
              }

              //
              // Check if we encountered an 'UPDATE xxx' line
              //
              
              if (line.startsWith("UPDATE ")) {
                String[] subtokens = line.split("\\s+");
                setToken(subtokens[1]);
                
                //
                // Close the current datalog file if it exists
                //
                
                if (null != loggingWriter) {
                  Map<String,String> labels = new HashMap<String,String>();
                  labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(dr.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
                  labels.put(SensisionConstants.SENSISION_LABEL_TYPE, dr.getType());
                  Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_REQUESTS_LOGGED, labels, 1);

                  if (handler.datalogSync) {
                    loggingWriter.flush();
                    loggingFD.sync();
                  }
                  loggingWriter.close();
                  // Create hard links when multiple datalog forwarders are configured
                  for (Path srcDir: Warp.getDatalogSrcDirs()) {
                    try {
                      Files.createLink(new File(srcDir.toFile(), loggingFile.getName() + DatalogForwarder.DATALOG_SUFFIX).toPath(), loggingFile.toPath());              
                    } catch (Exception e) {
                      throw new RuntimeException("Encountered an error while attempting to link " + loggingFile + " to " + srcDir);
                    }
                  }
                  //loggingFile.renameTo(new File(loggingFile.getAbsolutePath() + DatalogForwarder.DATALOG_SUFFIX));
                  loggingFile.delete();
                  loggingFile = null;
                  loggingWriter = null;
                }
                
                continue;
              }

              if (null == this.wtoken) {
                throw new IOException("Missing token.");
              }

              //
              // Open the logging file if it is not open yet and if datalogging is enabled
              //
              
              if (null != handler.loggingDir && null == loggingFile) {
                long nanos = TimeSource.getNanoTime();
                StringBuilder sb = new StringBuilder();
                sb.append(Long.toHexString(nanos));
                sb.insert(0, "0000000000000000", 0, 16 - sb.length());
                sb.append("-");
                sb.append(handler.datalogId);
                
                sb.append("-");
                sb.append(handler.dtf.print(nanos / 1000000L));
                sb.append(Long.toString(1000000L + (nanos % 1000000L)).substring(1));
                sb.append("Z");
                
                dr = new DatalogRequest();
                dr.setTimestamp(nanos);
                dr.setType(Constants.DATALOG_UPDATE);
                dr.setId(handler.datalogId);
                dr.setToken(encodedToken); 
                dr.setDeltaAttributes(deltaAttributes);
                
                //
                // Force 'now'
                //
                
                dr.setNow(Long.toString(now));
                
                //
                // Serialize the request
                //
                
                TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
                
                byte[] encoded;
                
                try {
                  encoded = ser.serialize(dr);
                } catch (TException te) {
                  throw new IOException(te);
                }
                
                if (null != handler.datalogPSK) {
                  encoded = CryptoUtils.wrap(handler.datalogPSK, encoded);
                }
                
                encoded = OrderPreservingBase64.encode(encoded);
                        
                loggingFile = new File(handler.loggingDir, sb.toString());
                
                FileOutputStream fos = new FileOutputStream(loggingFile);
                loggingFD = fos.getFD();
                OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
                loggingWriter = new PrintWriter(osw);
                
                //
                // Write request
                //
                
                loggingWriter.println(new String(encoded, StandardCharsets.US_ASCII));
              }

              try {
                encoder = GTSHelper.parse(lastencoder, line, extraLabels, now, this.maxsize, hadAttributes, maxpast, maxfuture, ignoredCount, this.deltaAttributes);
                if (null != this.handler.plugin) {
                  if (!this.handler.plugin.update(this.handler, wtoken, line, encoder)) {
                    hadAttributes.set(false);
                    continue;
                  }
                }
              } catch (ParseException pe) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_PARSEERRORS, sensisionLabels, 1);
                throw new IOException("Parse error at '" + line + "'", pe);
              }

              //
              // Force PRODUCER/OWNER
              //
                            
              if (encoder != lastencoder || lastencoder.size() > StandaloneIngressHandler.ENCODER_SIZE_THRESHOLD) {
                
                //
                // Check throttling
                //
                
                if (null != lastencoder) {
                  String producer = extraLabels.get(Constants.PRODUCER_LABEL);
                  String owner = extraLabels.get(Constants.OWNER_LABEL);
                  String application = extraLabels.get(Constants.APPLICATION_LABEL);
                  ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId(), expose);
                  ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount(), expose);
                }
                
                //
                // Build metadata object to push
                //
                
                if (encoder != lastencoder) {
                  Metadata metadata = new Metadata(encoder.getMetadata());
                  
                  if (this.handler.updateActivity) {
                    metadata.setLastActivity(System.currentTimeMillis());
                  }
                  
                  metadata.setSource(Configuration.INGRESS_METADATA_SOURCE);
                  this.handler.directoryClient.register(metadata);
                  
                  // Extract shardkey 128BITS
                  // Shard key is 48 bits, 24 upper from the class Id and 24 lower from the labels Id
                  shardkey =  (GTSHelper.classId(this.handler.classKeyLongs, encoder.getMetadata().getName()) & 0xFFFFFF000000L) | (GTSHelper.labelsId(this.handler.labelsKeyLongs, encoder.getMetadata().getLabels()) & 0xFFFFFFL);
                }

                if (null != lastencoder) {
                  // 128BITS
                  lastencoder.setClassId(GTSHelper.classId(this.handler.classKeyLongs, lastencoder.getName()));
                  lastencoder.setLabelsId(GTSHelper.labelsId(this.handler.labelsKeyLongs, lastencoder.getLabels()));
                  this.handler.storeClient.store(lastencoder);
                  count += lastencoder.getCount();
                  
                  
                  if (this.handler.parseAttributes && lastHadAttributes) {
                    // We need to push lastencoder's metadata update as they were updated since the last
                    // metadata update message sent
                    Metadata meta = new Metadata(lastencoder.getMetadata());
                    if (this.deltaAttributes) {
                      meta.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);                      
                    } else {
                      meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                    }
                    this.handler.directoryClient.register(meta);
                    lastHadAttributes = false;
                  }
                }
                
                if (encoder != lastencoder) {
                  lastencoder = encoder;
                  
                  // This is the case when we just parsed either the first input line or one for a different
                  // GTS than the previous one.
                } else {
                  //lastencoder = null
                  //
                  // Allocate a new GTSEncoder and reuse Metadata so we can
                  // correctly handle a continuation line if this is what occurs next
                  //
                  Metadata metadata = lastencoder.getMetadata();
                  lastencoder = new GTSEncoder(0L);
                  lastencoder.setMetadata(metadata);
                  
                  // This is the case when lastencoder and encoder are identical, but lastencoder was too big and needed
                  // to be flushed
                }
              }
              
              if (null != loggingWriter) {
                if (this.handler.logShardKey && '=' != line.charAt(0)) {
                  loggingWriter.print("#K");
                  loggingWriter.println(shardkey);
                }                         
                loggingWriter.println(line);
              }
            } while (true); 
            
            br.close();
            
            if (null != lastencoder && lastencoder.size() > 0) {
              
              //
              // Check throttling
              //
              
              String producer = extraLabels.get(Constants.PRODUCER_LABEL);
              String owner = extraLabels.get(Constants.OWNER_LABEL);
              String application = extraLabels.get(Constants.APPLICATION_LABEL);
              ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId(), expose);
              ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount(), expose);

              lastencoder.setClassId(GTSHelper.classId(this.handler.classKeyLongs, lastencoder.getName()));
              lastencoder.setLabelsId(GTSHelper.labelsId(this.handler.labelsKeyLongs, lastencoder.getLabels()));
              this.handler.storeClient.store(lastencoder);
              count += lastencoder.getCount();
              
              if (this.handler.parseAttributes && lastHadAttributes) {
                // Push a metadata UPDATE message so attributes are stored
                // Build metadata object to push
                Metadata meta = new Metadata(lastencoder.getMetadata());
                // Set source to indicate we
                if (this.deltaAttributes) {
                  meta.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);                  
                } else {
                  meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                }
                this.handler.directoryClient.register(meta);
              }
            }              
          } finally {
            if (null != loggingWriter) {              
              Map<String,String> labels = new HashMap<String,String>();
              labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(dr.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
              labels.put(SensisionConstants.SENSISION_LABEL_TYPE, dr.getType());
              Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_REQUESTS_LOGGED, labels, 1);

              loggingWriter.close();
              // Create hard links when multiple datalog forwarders are configured
              for (Path srcDir: Warp.getDatalogSrcDirs()) {
                try {
                  Files.createLink(new File(srcDir.toFile(), loggingFile.getName() + DatalogForwarder.DATALOG_SUFFIX).toPath(), loggingFile.toPath());              
                } catch (Exception e) {
                  throw new RuntimeException("Encountered an error while attempting to link " + loggingFile + " to " + srcDir);
                }
              }
              //loggingFile.renameTo(new File(loggingFile.getAbsolutePath() + DatalogForwarder.DATALOG_SUFFIX));
              loggingFile.delete();
              loggingFile = null;
              loggingWriter = null;
            }

            this.handler.storeClient.store(null);
            this.handler.directoryClient.register(null);

            nano = System.nanoTime() - nano;
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);          
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_MESSAGES, sensisionLabels, 1);
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_STREAM_UPDATE_TIME_US, sensisionLabels, nano / 1000);      
          }
          if (null != this.handler.plugin) {
            this.handler.plugin.flush(this.handler);
          }
          session.getRemote().sendString("OK " + (seqno++) + " UPDATE " + count + " " + nano);
        }        
      } catch (Throwable t) {
        if (this.errormsg) {
          String msg = "ERROR " + ThrowableUtils.getErrorMessage(t);
          session.getRemote().sendString(msg);
        } else {
          throw t;
        }
      }
    }
    
    @OnWebSocketClose    
    public void onWebSocketClose(Session session, int statusCode, String reason) {
    }
    
    public void setHandler(StandaloneStreamUpdateHandler handler) {
      this.handler = handler;
    }
    
    private void setToken(String token) throws IOException {
      //
      // TOKEN <TOKEN>
      //
      
      //
      // Extract token
      //
      
      WriteToken wtoken = null;
      
      try {
        wtoken = Tokens.extractWriteToken(token);          
      } catch (Exception e) {
        wtoken = null;
      }
      
      if (null == wtoken) {
        throw new IOException("Invalid token.");
      }

      if (wtoken.getAttributesSize() > 0 && wtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOUPDATE)) {
        throw new IOException("Token cannot be used for updating data.");
      }

      this.maxsize = this.handler.maxValueSize;
      
      if (wtoken.getAttributesSize() > 0 && null != wtoken.getAttributes().get(Constants.TOKEN_ATTR_MAXSIZE)) {
        maxsize = Long.parseLong(wtoken.getAttributes().get(Constants.TOKEN_ATTR_MAXSIZE));
      }

      this.maxpastdelta = null;
      this.maxfuturedelta = null;
      
      Boolean ignoor = null;
      
      if (wtoken.getAttributesSize() > 0) {
        
        this.expose = wtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);
        
        if (wtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_IGNOOR)) {
          String v = wtoken.getAttributes().get(Constants.TOKEN_ATTR_IGNOOR).toLowerCase();
          if ("true".equals(v) || "t".equals(v)) {
            ignoor = Boolean.TRUE;
          } else if ("false".equals(v) || "f".equals(v)) {
            ignoor = Boolean.FALSE;
          }
        }
        
        String deltastr = wtoken.getAttributes().get(Constants.TOKEN_ATTR_MAXPAST);

        if (null != deltastr) {
          long delta = Long.parseLong(deltastr);
          if (delta < 0) {
            throw new IOException("Invalid '" + Constants.TOKEN_ATTR_MAXPAST + "' token attribute, MUST be positive.");
          }
          maxpastdelta = delta;
        }
        
        deltastr = wtoken.getAttributes().get(Constants.TOKEN_ATTR_MAXFUTURE);
        
        if (null != deltastr) {
          long delta = Long.parseLong(deltastr);
          if (delta < 0) {
            throw new IOException("Invalid '" + Constants.TOKEN_ATTR_MAXFUTURE + "' token attribute, MUST be positive.");
          }
          maxfuturedelta = delta;
        }          
      }

      String application = wtoken.getAppName();
      String producer = Tokens.getUUID(wtoken.getProducerId());
      String owner = Tokens.getUUID(wtoken.getOwnerId());
        
      this.sensisionLabels.clear();
      this.sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      if (null == producer || null == owner) {
        throw new IOException("Invalid token.");
      }
        
      //
      // Build extra labels
      //
        
      this.extraLabels = new HashMap<String,String>();
      
      // Add labels from the WriteToken if they exist
      if (wtoken.getLabelsSize() > 0) {
        extraLabels.putAll(wtoken.getLabels());
      }
      
      // Force internal labels
      this.extraLabels.put(Constants.PRODUCER_LABEL, producer);
      this.extraLabels.put(Constants.OWNER_LABEL, owner);
      // FIXME(hbs): remove me
      if (null != application) {
        this.extraLabels.put(Constants.APPLICATION_LABEL, application);
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      this.ignoor = ignoor;
      this.wtoken = wtoken;
      this.encodedToken = token;
    }
  }
  
  public StandaloneStreamUpdateHandler(KeyStore keystore, Properties properties, StandaloneDirectoryClient directoryClient, StoreClient storeClient) {
    super(StandaloneStreamUpdateWebSocket.class);

    this.keyStore = keystore;
    
    this.classKeyLongs = SipHashInline.getKey(this.keyStore.getKey(KeyStore.SIPHASH_CLASS));    
    this.labelsKeyLongs = SipHashInline.getKey(this.keyStore.getKey(KeyStore.SIPHASH_LABELS));

    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    this.properties = properties;
    this.updateActivity = "true".equals(properties.getProperty(Configuration.INGRESS_ACTIVITY_UPDATE));

    this.maxValueSize = Long.parseLong(properties.getProperty(Configuration.STANDALONE_VALUE_MAXSIZE, StandaloneIngressHandler.DEFAULT_VALUE_MAXSIZE));
    
    this.parseAttributes = "true".equals(properties.getProperty(Configuration.INGRESS_PARSE_ATTRIBUTES));
    this.allowDeltaAttributes = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_ATTRIBUTES_ALLOWDELTA));

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_DEFAULT)) {
      this.maxpastDefault = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_DEFAULT));
      if (this.maxpastDefault < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXPAST_DEFAULT + "' MUST be positive.");
      }
    } else {
      this.maxpastDefault = null;
    }
    
    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_DEFAULT)) {
      this.maxfutureDefault = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_DEFAULT));
      if (this.maxfutureDefault < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXFUTURE_DEFAULT + "' MUST be positive.");
      }
    } else {
      this.maxfutureDefault = null;
    }

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_OVERRIDE)) {
      this.maxpastOverride = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_OVERRIDE));
      if (this.maxpastOverride < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXPAST_OVERRIDE + "' MUST be positive.");
      }
    } else {
      this.maxpastOverride = null;
    }
    
    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_OVERRIDE)) {
      this.maxfutureOverride = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_OVERRIDE));
      if (this.maxfutureOverride < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXFUTURE_OVERRIDE + "' MUST be positive.");
      }
    } else {
      this.maxfutureOverride = null;
    }
    
    this.ignoreOutOfRange = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_OUTOFRANGE_IGNORE));
    
    if ("false".equals(properties.getProperty(Configuration.DATALOG_LOGSHARDKEY))) {
      logShardKey = false;
    } else {
      logShardKey = true;
    }

    if (properties.containsKey(Configuration.DATALOG_DIR)) {
      File dir = new File(properties.getProperty(Configuration.DATALOG_DIR));
      
      if (!dir.exists()) {
        throw new RuntimeException("Data logging target '" + dir + "' does not exist.");
      } else if (!dir.isDirectory()) {
        throw new RuntimeException("Data logging target '" + dir + "' is not a directory.");
      } else {
        loggingDir = dir;
        LOG.info("Data logging enabled in directory '" + dir + "'.");
      }
      
      String id = properties.getProperty(Configuration.DATALOG_ID);
      
      if (null == id) {
        throw new RuntimeException("Property '" + Configuration.DATALOG_ID + "' MUST be set to a unique value for this instance.");
      } else {
        datalogId = new String(OrderPreservingBase64.encode(id.getBytes(StandardCharsets.UTF_8)), StandardCharsets.US_ASCII);
      }
      
    } else {
      loggingDir = null;
      datalogId = null;
    }

    this.datalogSync = "true".equals(WarpConfig.getProperty(Configuration.DATALOG_SYNC));
    if (properties.containsKey(Configuration.DATALOG_PSK)) {
      this.datalogPSK = this.keyStore.decodeKey(properties.getProperty(Configuration.DATALOG_PSK));
    } else {
      this.datalogPSK = null;
    }    
  }
     
  public void setPlugin(IngressPlugin plugin) {
    this.plugin = plugin;
  }
  
  public DirectoryClient getDirectoryClient() {
    return this.directoryClient;
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (Constants.API_ENDPOINT_PLASMA_UPDATE.equals(target)) {
      baseRequest.setHandled(true);
      super.handle(target, baseRequest, request, response);
    }
  }
  
  @Override
  public void configure(final WebSocketServletFactory factory) {
    
    final StandaloneStreamUpdateHandler self = this;

    final WebSocketCreator oldcreator = factory.getCreator();
    
    WebSocketCreator creator = new WebSocketCreator() {
      @Override
      public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
        StandaloneStreamUpdateWebSocket ws = (StandaloneStreamUpdateWebSocket) oldcreator.createWebSocket(req, resp);
        ws.setHandler(self);
        return ws;
      }
    };

    factory.setCreator(creator);
    
    //
    // Update the maxMessageSize if need be
    //
    if (this.properties.containsKey(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)) {
      factory.getPolicy().setMaxTextMessageSize((int) Long.parseLong(this.properties.getProperty(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)));
      factory.getPolicy().setMaxBinaryMessageSize((int) Long.parseLong(this.properties.getProperty(Configuration.INGRESS_WEBSOCKET_MAXMESSAGESIZE)));
    }

    super.configure(factory);
  }    
}
