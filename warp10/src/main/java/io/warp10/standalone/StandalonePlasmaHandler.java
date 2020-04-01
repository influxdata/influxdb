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

import io.warp10.json.JsonUtils;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.plasma.PlasmaSubscriptionListener;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.json.MetadataSerializer;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import com.geoxp.GeoXPLib;
public class StandalonePlasmaHandler extends WebSocketHandler.Simple implements Runnable, StandalonePlasmaHandlerInterface {
  
  private enum OUTPUT_FORMAT {
    RAW,
    JSON,
    TEXT,
    FULLTEXT,
    WRAPPER,
  };

  protected final KeyStore keystore;
  private final Properties properties;
  
  private DirectoryClient directoryClient;

  private final Random random = new Random();
  
  private byte[] metadataKey;
  
  private LinkedBlockingQueue<GTSEncoder> encoders = new LinkedBlockingQueue<GTSEncoder>(256);
  
  /**
   * Map of classId+labelsId to Metadata
   */
  private Map<BigInteger, Metadata> metadatas = new HashMap<BigInteger, Metadata>();
  
  /**
   * Map of Session to subscription
   */
  private Map<Session, Set<BigInteger>> subscriptions = new ConcurrentHashMap<Session, Set<BigInteger>>();
  
  /**
   * Map of Session to JSON format
   */
  private Map<Session, Boolean> format = new HashMap<Session, Boolean>();
  
  /**
   * Map of Session to output format
   */
  private Map<Session, OUTPUT_FORMAT> outputFormat = new HashMap<Session, OUTPUT_FORMAT>();
  
  /**
   * Mp of Session to sample rate
   */
  private Map<Session, Long> sampleRate = new HashMap<Session, Long>();
  
  /**
   * Map of Session flag to expose owner/producer, based on the tokens used
   */
  private Map<Session, Boolean> exposeOwnerProducer = new HashMap<Session, Boolean>();
  
  /**
   * Number of 
   */
  private Map<BigInteger, AtomicInteger> refcounts = new ConcurrentHashMap<BigInteger, AtomicInteger>();
  
  private boolean hasclients = false;
  
  private PlasmaSubscriptionListener subscriptionListener = null;
  
  /**
   * Max number of subscriptions per session
   */
  private final int maxSubscriptions;
  
  @WebSocket
  public static class StandalonePlasmaWebSocket {
    
    private StandalonePlasmaHandler handler;
    
    @OnWebSocketConnect
    public void onWebSocketConnect(Session session) {
    }
    
    @OnWebSocketMessage
    public void onWebSocketMessage(Session session, String message) throws Exception {
      
      //
      // Split message on whitespace boundary
      //
      
      String[] tokens = message.split("\\s+");
            
      tokens[0] = tokens[0].trim();
      
      if ("SUBSCRIBE".equals(tokens[0]) || "UNSUBSCRIBE".equals(tokens[0])) {
        //
        // [UN]SUBSCRIBE <TOKEN> <SELECTOR>
        //
        
        Matcher m = EgressFetchHandler.SELECTOR_RE.matcher(tokens[2].trim());
        
        if (!m.matches()) {
          session.getRemote().sendString("KO Invalid subscription selector.");
          return;
        }
        
        String classSelector = m.group(1);
        Map<String,String> labelsSelector = GTSHelper.parseLabelsSelectors(m.group(2));
        
        //
        // Extract token
        //
        
        ReadToken rtoken = null;
        
        try {
          rtoken = Tokens.extractReadToken(tokens[1]);
          
          if (rtoken.getHooksSize() > 0) {
            throw new IOException("Tokens with hooks cannot be used with Plasma.");        
          }
        } catch (Exception e) {
          rtoken = null;
        }
        
        if (null == rtoken) {
          session.getRemote().sendString("KO Invalid token.");
          return;
        }
        
        labelsSelector.remove(Constants.PRODUCER_LABEL);
        labelsSelector.remove(Constants.OWNER_LABEL);
        labelsSelector.remove(Constants.APPLICATION_LABEL);

        labelsSelector.putAll(Tokens.labelSelectorsFromReadToken(rtoken));
        
        List<String> clsSels = new ArrayList<String>();
        List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
        clsSels.add(classSelector);
        lblsSels.add(labelsSelector);
        
        List<Metadata> metadatas = new ArrayList<Metadata>();
        
        DirectoryRequest drequest = new DirectoryRequest();
        drequest.setClassSelectors(clsSels);
        drequest.setLabelsSelectors(lblsSels);
        Iterator<Metadata> iter = this.handler.getDirectoryClient().iterator(drequest);

        int subs = this.handler.getSubscriptionCount(session);
        
        try {
          while(iter.hasNext()) {
            metadatas.add(iter.next());
            
            //
            // Process subscriptions 10000 at a time
            //
            
            if (metadatas.size() >= 10000) {
              if ('S' == tokens[0].charAt(0)) {
                this.handler.subscribe(session, metadatas);
              } else {
                this.handler.unsubscribe(session, metadatas);
              }
              metadatas.clear();
            }
          }
          
          if ('S' == tokens[0].charAt(0)) {
            this.handler.subscribe(session, metadatas);
          } else {
            this.handler.unsubscribe(session, metadatas);
          }
        } finally {
          if (iter instanceof MetadataIterator) {
            try { ((MetadataIterator) iter).close(); } catch (Exception e) {}
          }
        }
        
        //
        // Update the expose flag. If the token has the .expose attribute set
        // then if the subscription list is currently empty or the expose flag is
        // already true, then set it to true. If the token has the .expose attribute
        // unset, reset the expose flag to false. This is to prevent metadata that were subscribed
        // to with a token without the .expose attribute to be exposed.
        //

        if (0 == this.handler.getSubscriptionCount(session)) {
          // Reset the expose flag to false if there are no more subscriptions
          this.handler.setExposeOwnerProducer(session, false);
        } else if (this.handler.getSubscriptionCount(session) - subs > 0) {
          // We added some metadata
          boolean expose = rtoken.getAttributesSize() > 0 && rtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);

          // If the flag was false and the subscription list empty or we subscribed to more GTS and the
          // flag was already true, then set it or keep it to true, otherwise set it to false
          if (expose && ((0 == subs && !this.handler.getExposeOwnerProducer(session))
                        || (subs > 0 && this.handler.getExposeOwnerProducer(session)))) {
              this.handler.setExposeOwnerProducer(session, true);
          } else {
            this.handler.setExposeOwnerProducer(session, false);
          }            
        }
      } else if ("SUBSCRIPTIONS".equals(tokens[0])) {
        //
        // List subscriptions
        //
        
        this.handler.listSubscriptions(session);
      } else if ("CLEAR".equals(tokens[0])) {
        //
        // Clear all subscriptions
        //
        
        this.handler.clearSubscriptions(session);
      } else if ("TEXT".equals(tokens[0])) {
        this.handler.setOutputFormat(session, OUTPUT_FORMAT.TEXT);
      } else if ("FULLTEXT".equals(tokens[0])) {
        this.handler.setOutputFormat(session, OUTPUT_FORMAT.FULLTEXT);
      } else if ("JSON".equals(tokens[0])) {
        this.handler.setOutputFormat(session, OUTPUT_FORMAT.JSON);
      } else if ("RAW".equals(tokens[0])) {
        // Output raw GTSEncoders
        this.handler.setOutputFormat(session, OUTPUT_FORMAT.RAW);
      } else if ("WRAPPER".equals(tokens[0])) {
        this.handler.setOutputFormat(session, OUTPUT_FORMAT.WRAPPER);
      } else if ("GEO".equals(tokens[0])) {
        //
        // Geofencing
        //
      } else if ("NOOP".equals(tokens[0]) || "".equals(tokens[0])) {
        //
        // Do nothing, this is just so we can keep the socket alive
        //
      } else if ("SAMPLE".equals(tokens[0])) {
        //
        // Set the sample rate of data
        //
                
        double rate = Double.parseDouble(tokens[1]);
        
        if (rate > 0.0D && rate <= 1.0D) {
          this.handler.setSampleRate(session, rate);
        } else {
          this.handler.sampleRate.remove(session);
        }
      } else {
        throw new IOException("Invalid verb.");
      }
    }
    
    @OnWebSocketClose    
    public void onWebSocketClose(Session session, int statusCode, String reason) {
      this.handler.deregister(session);
    }
    
    public void setHandler(StandalonePlasmaHandler handler) {
      this.handler = handler;
    }
  }
  
  public StandalonePlasmaHandler(KeyStore keystore, Properties properties, DirectoryClient directoryClient) {
    this(keystore, properties, directoryClient, true);
  }
  
  public StandalonePlasmaHandler(KeyStore keystore, Properties properties, DirectoryClient directoryClient, boolean startThread) {
    super(StandalonePlasmaWebSocket.class);
    
    this.keystore = keystore;
    this.properties = properties;
    this.directoryClient = directoryClient;
    if (properties.containsKey(Configuration.WARP_PLASMA_MAXSUBS)) {
      this.maxSubscriptions = Integer.parseInt(properties.getProperty(Configuration.WARP_PLASMA_MAXSUBS));
    } else {
      this.maxSubscriptions = Constants.WARP_PLASMA_MAXSUBS_DEFAULT;
    }
    this.metadataKey = keystore.getKey(KeyStore.AES_KAFKA_METADATA);
    
    if (startThread) {
      Thread t = new Thread(this);
      t.setDaemon(true);
      t.setName("[StandalonePlasmaHandler]");
      t.start();      
    }
  }

  public void setDirectoryClient(DirectoryClient directoryClient) {
    this.directoryClient = directoryClient;    
  }
    
  public DirectoryClient getDirectoryClient() {
    return this.directoryClient;
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (Constants.API_ENDPOINT_PLASMA_SERVER.equals(target)) {
      baseRequest.setHandled(true);
      super.handle(target, baseRequest, request, response);
    } else if (Constants.API_ENDPOINT_CHECK.equals(target)) {
      baseRequest.setHandled(true);
      response.setStatus(HttpServletResponse.SC_OK);
      return;
    }
  }
  
  @Override
  public void configure(final WebSocketServletFactory factory) {
        
    final StandalonePlasmaHandler self = this;

    final WebSocketCreator oldcreator = factory.getCreator();
    
    WebSocketCreator creator = new WebSocketCreator() {
      @Override
      public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
        StandalonePlasmaWebSocket ws = (StandalonePlasmaWebSocket) oldcreator.createWebSocket(req, resp);
        ws.setHandler(self);
        return ws;
      }
    };

    factory.setCreator(creator);
    
    //
    // Update the maxMessageSize if need be
    //
    if (this.properties.containsKey(Configuration.PLASMA_FRONTEND_WEBSOCKET_MAXMESSAGESIZE)) {
      factory.getPolicy().setMaxTextMessageSize((int) Long.parseLong(this.properties.getProperty(Configuration.PLASMA_FRONTEND_WEBSOCKET_MAXMESSAGESIZE)));
      factory.getPolicy().setMaxBinaryMessageSize((int) Long.parseLong(this.properties.getProperty(Configuration.PLASMA_FRONTEND_WEBSOCKET_MAXMESSAGESIZE)));
    }

    super.configure(factory);
  }
  
  private synchronized void subscribe(Session session, List<Metadata> metadatas) {
    
    if (metadatas.isEmpty()) {
      return;
    }
    
    // 128BITS
    byte[] bytes = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    
    if (!this.subscriptions.containsKey(session)) {
      this.subscriptions.put(session, new HashSet<BigInteger>());
    }
    
    for (Metadata metadata: metadatas) {
      bb.rewind();
      bb.putLong(metadata.getClassId());
      bb.putLong(metadata.getLabelsId());
      
      BigInteger id = new BigInteger(bytes);
      
      //
      // Limit the number of subscriptions per session to 'maxSubscriptions'
      //
      
      if (subscriptions.get(session).size() >= maxSubscriptions) {
        break;
      }

      this.metadatas.put(id, metadata);
      
      if (!this.refcounts.containsKey(id)) {
        this.refcounts.put(id, new AtomicInteger(0));
      }
      
      if (!subscriptions.get(session).contains(id)) {
        subscriptions.get(session).add(id);
        this.refcounts.get(id).addAndGet(1);
      }
      hasclients = true;
    }
    
    if (null != this.subscriptionListener) {
      this.subscriptionListener.onChange();
    }
  }

  private synchronized void unsubscribe(Session session, List<Metadata> metadatas) {   
    
    if (metadatas.isEmpty()) {
      return;
    }
    
    // 128BITS
    byte[] bytes = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    
    if (!this.subscriptions.containsKey(session)) {
      return;
    }
    
    for (Metadata metadata: metadatas) {
      bb.rewind();
      bb.putLong(metadata.getClassId());
      bb.putLong(metadata.getLabelsId());
      
      BigInteger id = new BigInteger(bytes);

      if (subscriptions.get(session).contains(id)) {
        subscriptions.get(session).remove(id);
        if (0 == this.refcounts.get(id).addAndGet(-1)) {
          this.metadatas.remove(id);
          this.refcounts.remove(id);
        }
      }
    }
    
    if (null != this.subscriptionListener) {
      this.subscriptionListener.onChange();
    }
  }

  public void setSubscriptionListener(PlasmaSubscriptionListener listener) {
    this.subscriptionListener = listener;
  }
  
  private synchronized void deregister(Session session) {    
    clearSubscriptions(session);
    this.format.remove(session);
    this.sampleRate.remove(session);
    this.exposeOwnerProducer.remove(session);
  }
  
  private synchronized void clearSubscriptions(Session session) {
    //
    // Decrease refcount for each gts subscribed
    //

    boolean mustRepublish = false;
    
    if (this.subscriptions.containsKey(session)) {
      Set<BigInteger> ids = this.subscriptions.get(session);
      this.subscriptions.remove(session);
      for (BigInteger id: ids) {
        if (0 == this.refcounts.get(id).addAndGet(-1)) {
          // FIXME(hbs): we need to ensure refcount is not incremented by another thread, otherwise
          // we may remove some Metadata even though another client just subscribed to it
          this.metadatas.remove(id);
          this.refcounts.remove(id);
          mustRepublish = true;
        }        
      }
    }    
    
    if (this.refcounts.isEmpty()) {
      hasclients = false;
    }
    
    if (null != this.subscriptionListener && mustRepublish) {
      this.subscriptionListener.onChange();
    }
  }
  
  private synchronized void listSubscriptions(Session session) throws IOException {
    if (this.subscriptions.containsKey(session)) {
      StringBuilder sb = new StringBuilder();
      
      for (BigInteger id: this.subscriptions.get(session)) {
        sb.setLength(0);
        sb.append("SUB ");
        GTSHelper.metadataToString(sb, metadatas.get(id).getName(), metadatas.get(id).getLabels(), getExposeOwnerProducer(session));
        session.getRemote().sendString(sb.toString());
      }
    }
  }
  
  private synchronized int getSubscriptionCount(Session session) {
    Set<BigInteger> subs = this.subscriptions.get(session); 
    if (null != subs) {
      return subs.size();
    } else {
      return 0;
    }
  }
  
  public void publish(GTSEncoder encoder) {
    try {      
      // FIXME(hbs): this will block the pushing of data
      this.encoders.offer(encoder, 1000L, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      // FIXME(hbs): Sensision metrics
    }
  }
  
  @Override
  public boolean hasSubscriptions() {
    return hasclients;
  }
  
  //
  // FIXME(hbs): dispatch will forward a given encoder to every single session which subscribed to it
  // This is done in the Kafka consuming thread. It might be a good idea to add some loosely coupled
  // logic in this by having a set of threads doing the actual dispatch.
  // Or maybe first add a metric to know how many different sessions were targeted
  //
  
  protected void dispatch(GTSEncoder encoder) throws IOException {
        
    long nano = System.nanoTime();
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_DISPATCH_CALLS, Sensision.EMPTY_LABELS, 1);
    
    // 128BITS
    byte[] bytes = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    bb.putLong(encoder.getClassId());
    bb.putLong(encoder.getLabelsId());
    
    BigInteger id = new BigInteger(bytes);
    
    AtomicInteger count = refcounts.get(id);
    
    if (null == count) {
      return;
    }
       
    int refcount = count.get();
    
    if (refcount > 0) {
      
      long maxmessagesize = Math.min(this.getWebSocketFactory().getPolicy().getMaxTextMessageSize(), this.getWebSocketFactory().getPolicy().getMaxBinaryMessageSize());
      
      StringBuilder metasb = new StringBuilder();
      StringBuilder exposedmetasb = new StringBuilder();
      StringBuilder sb = new StringBuilder();

      Metadata metadata = this.metadatas.get(id);

      if (null == metadata) {
        return;
      }
    
      GTSHelper.metadataToString(metasb, metadata.getName(), metadata.getLabels(), false);
      GTSHelper.metadataToString(exposedmetasb, metadata.getName(), metadata.getLabels(), true);
      
      Set<Entry<Session, Set<BigInteger>>> subs = subscriptions.entrySet();
      
      for (Entry<Session, Set<BigInteger>> entry: subs) {
        
        //
        // We might have missed the close of a session, we get a chance to correct that here
        // FIXME(hbs): if we missed a close it's probably a bug though!
        //
        
        if (!entry.getKey().isOpen()) {
          deregister(entry.getKey());
          continue;
        }
        
        try {
          if (entry.getValue().contains(id)) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_DISPATCH_SESSIONS, Sensision.EMPTY_LABELS, 1);
            OUTPUT_FORMAT format = getOutputFormat(entry.getKey());
            boolean exposeOwnerProducer = getExposeOwnerProducer(entry.getKey());
            StringBuilder curmetasb = exposeOwnerProducer ? exposedmetasb : metasb;
            
            if (OUTPUT_FORMAT.RAW.equals(format)) {
              sb.setLength(0);
              
              sb.append(encoder.getBaseTimestamp());
              sb.append("// ");
              
              TSerializer tserializer = new TSerializer(new TCompactProtocol.Factory());
              
              try {
                byte[] serialized = tserializer.serialize(metadata);

                // FIXME(hbs): should we use a specific key?
                // FIXME(hbs): create chunks so we stay below maxmessagesize
                byte[] encrypted = CryptoUtils.wrap(this.metadataKey, serialized);
                sb.append(new String(OrderPreservingBase64.encode(encrypted), StandardCharsets.US_ASCII));
                sb.append(":");              
                sb.append(new String(OrderPreservingBase64.encode(encoder.getBytes()), StandardCharsets.US_ASCII));
                
                entry.getKey().getRemote().sendStringByFuture(sb.toString());                
              } catch (TException te) {
                // Oh well, skip it!
              }
              
              continue;
            } else if (OUTPUT_FORMAT.WRAPPER.equals(format)) {
              encoder.setMetadata(metadata);
              
              //
              // Remove producer/owner
              //
              
              if (!Constants.EXPOSE_OWNER_PRODUCER && !exposeOwnerProducer) {
                encoder.getMetadata().getLabels().remove(Constants.PRODUCER_LABEL);
                encoder.getMetadata().getLabels().remove(Constants.OWNER_LABEL);                
              }

              // Compress with two pass max
              GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true, GTSWrapperHelper.DEFAULT_COMP_RATIO_THRESHOLD, 2);
              
              TSerializer tserializer = new TSerializer(new TCompactProtocol.Factory());
              
              try {
                byte[] serialized = tserializer.serialize(wrapper);

                sb.setLength(0);
                sb.append(new String(OrderPreservingBase64.encode(serialized), StandardCharsets.US_ASCII));
                
                entry.getKey().getRemote().sendStringByFuture(sb.toString());                
              } catch (TException te) {
                // Oh well, skip it!
              }

              continue;
            }
            
            GTSDecoder decoder = encoder.getDecoder();
                      
            boolean first = true;
                        
            double rate = getSampleRate(entry.getKey());
            
            long budget = maxmessagesize;
            
            //
            // Reset StringBuilder
            //
            
            sb.setLength(0);
            
            while(decoder.next()) {
              
              if (1.0D != rate && random.nextDouble() > rate) {
                continue;
              }
              
              if (OUTPUT_FORMAT.JSON.equals(format)) {
                Map<String,Object> json = new HashMap<String,Object>();
                    
                HashMap<String,String> labels = new HashMap<String,String>();

                json.put("c", metadata.getName());
                
                labels.putAll(metadata.getLabels());
                
                //
                // Remove PRODUCER/OWNER
                //
                
                if (!Constants.EXPOSE_OWNER_PRODUCER && !exposeOwnerProducer) {
                  labels.remove(Constants.PRODUCER_LABEL);
                  labels.remove(Constants.OWNER_LABEL);
                }
                
                json.put("l", labels);              
                
                json.put("t", decoder.getTimestamp());
                // Requested format is JSON so we do not use getBinaryValue as JSON cannot represent byte arrays
                json.put("v", decoder.getValue());
                if (GeoTimeSerie.NO_LOCATION != decoder.getLocation()) {
                  double[] latlon = GeoXPLib.fromGeoXPPoint(decoder.getLocation());
                  json.put("lat", latlon[0]);
                  json.put("lon", latlon[1]);
                }
                if (GeoTimeSerie.NO_ELEVATION != decoder.getElevation()) {
                  json.put("elev", decoder.getElevation());
                }
                
                if (first) {
                  sb.append("[");
                } else {
                  sb.append(",");                
                }
                sb.append(JsonUtils.objectToJson(json));
                
                first = false;
              } else {
                
                if (!first && OUTPUT_FORMAT.TEXT.equals(format)) {
                  sb.append("=");
                }
                
                sb.append(decoder.getTimestamp());
                sb.append("/");
                if (GeoTimeSerie.NO_LOCATION != decoder.getLocation()) {
                  double[] latlon = GeoXPLib.fromGeoXPPoint(decoder.getLocation());
                  sb.append(latlon[0]);
                  sb.append(":");
                  sb.append(latlon[1]);
                }
                sb.append("/");
                if (GeoTimeSerie.NO_ELEVATION != decoder.getElevation()) {
                  sb.append(decoder.getElevation());
                }
                sb.append(" ");
                if (first || !OUTPUT_FORMAT.TEXT.equals(format)) {
                  sb.append(curmetasb);
                  sb.append(" ");
                }
                GTSHelper.encodeValue(sb, decoder.getBinaryValue());
                sb.append("\n");
                first = false;
              }
              
              //
              // If we've reached 90% of the max message size, flush the current message
              // FIXME(hbs): we really should check beforehand that we will not overflow the buffer.
              // With specially crafted content (String values) we could overflow the message size.
              // Given we're in a try/catch we would simply ignore the message, but still...
              //
              
              if (sb.length() > 0.9 * maxmessagesize) {
                if (OUTPUT_FORMAT.JSON.equals(format) && sb.length() > 0) {
                  sb.append("]");
                }

                entry.getKey().getRemote().sendStringByFuture(sb.toString());
                sb.setLength(0);
                first = true;
              }
            }
            
            if (OUTPUT_FORMAT.JSON.equals(format) && sb.length() > 0) {
              sb.append("]");
            }

            if (sb.length() > 0) {
              entry.getKey().getRemote().sendStringByFuture(sb.toString());
              sb.setLength(0);              
            }
            
            refcount--;
            
            if (0 == refcount) {
              break;
            }
          }          
        } catch (WebSocketException wse) {          
        }
      }      
    }
    
    nano = System.nanoTime() - nano;
    
    Sensision.update(SensisionConstants.SENSISION_CLASS_PLASMA_FRONTEND_DISPATCH_TIME_US, Sensision.EMPTY_LABELS, nano/1000L);
  }
  
  /**
   * Return the current set of subscribed classId/labelsId
   * 
   * @return
   */
  public Set<BigInteger> getSubscriptions() {
    Set<BigInteger> ids = new HashSet<BigInteger>();
    
    Collection<Set<BigInteger>> subs = this.subscriptions.values();
    
    for (Set<BigInteger> sub: subs) {
      ids.addAll(sub);
    }
    
    return ids;
  }
  
  private boolean getExposeOwnerProducer(Session session) {
    return this.exposeOwnerProducer.getOrDefault(session, false);  
  }
  
  private synchronized void setExposeOwnerProducer(Session session, boolean expose) {
    if (expose) {
      this.exposeOwnerProducer.put(session, expose);
    } else {
      this.exposeOwnerProducer.remove(session);
    }
  }
  
  private OUTPUT_FORMAT getOutputFormat(Session session) {
    if (this.outputFormat.containsKey(session)) {
      return this.outputFormat.get(session);
    } else {
      return OUTPUT_FORMAT.TEXT;
    }
  }
  
  private synchronized void setOutputFormat(Session session, OUTPUT_FORMAT format) {
    this.outputFormat.put(session, format);
  }

  private synchronized void setSampleRate(Session session, double rate) {
    this.sampleRate.put(session, Double.doubleToLongBits(rate));
  }
  
  private synchronized double getSampleRate(Session session) {
    if (!this.sampleRate.containsKey(session)) {
      return 1.0D;
    } else {
      return Double.longBitsToDouble(this.sampleRate.get(session));
    }
  }
  
  @Override
  public void run() {
    while (true) {
      try {
        GTSEncoder encoder = this.encoders.poll(Long.MAX_VALUE, TimeUnit.DAYS);
        
        if (null == encoder) {
          continue;
        }

        // TODO(hbs): have several threads actually do the dispatch so we
        // speed things up? We might need to synchronize things so encoders are
        // pushed in the order they arrived to every client. For this we can dispatch
        // encoders using a partitioning scheme based on a modulus of the classId and/or labelsId
        // so encoders for the same GTS will be delivered by the same thread, but this would cause
        // heavily subscribed GTS to be served by a single Thread, which may cause performance
        // problems
        //
        // Or use 'Disruptor' or 'Chronicle' or 'BigQueue' from the HFT world?
        
        dispatch(encoder);
      } catch (IOException ioe) {
        // FIXME(hbs): sensision metric
      } catch (InterruptedException ie) {        
      }
    }
  }
  
}
