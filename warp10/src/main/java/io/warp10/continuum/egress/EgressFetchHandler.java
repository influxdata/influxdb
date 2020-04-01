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

package io.warp10.continuum.egress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.warp10.json.GeoTimeSerieSerializer;
import io.warp10.json.JsonUtils;
import io.warp10.json.MetadataSerializer;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.geoxp.GeoXPLib;
import com.google.common.primitives.Longs;

import io.warp10.ThrowableUtils;
import io.warp10.WarpURLDecoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.GTSDecoderIterator;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.GTSSplit;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.FETCH;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandaloneAcceleratedStoreClient;

public class EgressFetchHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EgressFetchHandler.class);
  
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  private final StoreClient storeClient;
    
  private final DirectoryClient directoryClient;
  
  private final byte[] fetchPSK;
  
  private final byte[] fetchAES;

  private final long maxSplitAge;
  
  public static final Pattern SELECTOR_RE = Pattern.compile("^([^{]+)\\{(.*)\\}$");

  /**
   * Maximum number of GTS per call to the fetch endpoint
   */
  public static long FETCH_BATCHSIZE = 100000;

  public static final String FIELD_ID = "i";
  
  public EgressFetchHandler(KeyStore keystore, Properties properties, DirectoryClient directoryClient, StoreClient storeClient) {
    this.fetchPSK = keystore.getKey(KeyStore.SIPHASH_FETCH_PSK);
    this.fetchAES = keystore.getKey(KeyStore.AES_FETCHER);
    this.storeClient = storeClient;
    this.directoryClient = directoryClient;
    
    if (properties.containsKey(Configuration.EGRESS_FETCH_BATCHSIZE)) {
      FETCH_BATCHSIZE = Long.parseLong(properties.getProperty(Configuration.EGRESS_FETCH_BATCHSIZE));
    }
    
    if (properties.containsKey(Configuration.EGRESS_FETCHER_MAXSPLITAGE)) {
      this.maxSplitAge = Long.parseLong(properties.getProperty(Configuration.EGRESS_FETCHER_MAXSPLITAGE));
    } else {
      this.maxSplitAge = Long.MAX_VALUE;
    }
  }
  
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
    boolean fromArchive = false;
    boolean splitFetch = false;
    boolean writeTimestamp = false;
    
    if (Constants.API_ENDPOINT_FETCH.equals(target)) {
      baseRequest.setHandled(true);
      fromArchive = false;
    } else if (Constants.API_ENDPOINT_AFETCH.equals(target)) {
      baseRequest.setHandled(true);
      fromArchive = true;
    } else if (Constants.API_ENDPOINT_SFETCH.equals(target)) {
      baseRequest.setHandled(true);
      splitFetch = true;
    } else if (Constants.API_ENDPOINT_CHECK.equals(target)) {
      baseRequest.setHandled(true);
      resp.setStatus(HttpServletResponse.SC_OK);
      return;      
    } else {
      return;
    }

    try {
      // Labels for Sensision
      Map<String,String> labels = new HashMap<String,String>();

      labels.put(SensisionConstants.SENSISION_LABEL_TYPE, target);

      //
      // Add CORS header
      //
      
      resp.setHeader("Access-Control-Allow-Origin", "*");
      
      long now = Long.MIN_VALUE;
      long then = Long.MIN_VALUE;
      long count = -1;
      long skip = 0;
      double sample = 1.0D;
      int preBoundary = 0;
      int postBoundary = 0;

      String startParam = null;
      String stopParam = null;
      String nowParam = null;
      String endParam = null;
      String timespanParam = null;
      String dedupParam = null;
      String showErrorsParam = null;
      String countParam = null;
      String skipParam = null;
      String sampleParam = null;
      String preBoundaryParam = null;
      String postBoundaryParam = null;

      if (splitFetch) {
        nowParam = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX));
        timespanParam = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TIMESPAN_HEADERX));
        showErrorsParam = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_SHOW_ERRORS_HEADERX));
      } else {
        startParam = req.getParameter(Constants.HTTP_PARAM_START);
        stopParam = req.getParameter(Constants.HTTP_PARAM_STOP);
        nowParam = req.getParameter(Constants.HTTP_PARAM_NOW);
        endParam = req.getParameter(Constants.HTTP_PARAM_END);
        timespanParam = req.getParameter(Constants.HTTP_PARAM_TIMESPAN);
        dedupParam = req.getParameter(Constants.HTTP_PARAM_DEDUP);      
        showErrorsParam = req.getParameter(Constants.HTTP_PARAM_SHOW_ERRORS);
        countParam = req.getParameter(Constants.HTTP_PARAM_COUNT);
        skipParam = req.getParameter(Constants.HTTP_PARAM_SKIP);
        sampleParam = req.getParameter(Constants.HTTP_PARAM_SAMPLE);
        preBoundaryParam = req.getParameter(Constants.HTTP_PARAM_PREBOUNDARY);
        postBoundaryParam = req.getParameter(Constants.HTTP_PARAM_POSTBOUNDARY);
      }
          
      boolean nocache = null != req.getParameter(StandaloneAcceleratedStoreClient.NOCACHE);
      boolean nopersist = null != req.getParameter(StandaloneAcceleratedStoreClient.NOPERSIST);
      
      String maxDecoderLenParam = req.getParameter(Constants.HTTP_PARAM_MAXSIZE);
      int maxDecoderLen = null != maxDecoderLenParam ? Integer.parseInt(maxDecoderLenParam) : Constants.DEFAULT_PACKED_MAXSIZE;
      
      String suffix = req.getParameter(Constants.HTTP_PARAM_SUFFIX);
      if (null == suffix) {
        suffix = Constants.DEFAULT_PACKED_CLASS_SUFFIX;
      }
      
      boolean unpack = null != req.getParameter(Constants.HTTP_PARAM_UNPACK);
   
      long chunksize = Long.MAX_VALUE;
      
      if (null != req.getParameter(Constants.HTTP_PARAM_CHUNKSIZE)) {
        chunksize = Long.parseLong(req.getParameter(Constants.HTTP_PARAM_CHUNKSIZE));      
      }
      
      if (chunksize <= 0) {
        throw new IOException("Invalid chunksize.");
      }    
      
      boolean showErrors = null != showErrorsParam;
      boolean dedup = null != dedupParam && "true".equals(dedupParam);

      //
      // Handle aliases
      //

      // negative timespan is count
      try {
        long numericTimespan = Long.parseLong(timespanParam);
        if (numericTimespan < 0) {
          if (null != countParam) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Cannot specify both '" + Constants.HTTP_PARAM_COUNT + "' and negative '" + Constants.HTTP_PARAM_TIMESPAN + "'.");
            return;
          }
          timespanParam = null;
          countParam = Long.toString(-numericTimespan);
        }
      } catch (NumberFormatException nfe) {
        // do nothing, timespan is not a long, should be a ISO8601 duration
      }

      // stop, now and end are aliases. Put the defined value in endParam.
      String endParamName = Constants.HTTP_PARAM_END;
      if (null != stopParam) {
        if (null != endParam) {
          resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Cannot specify both '" + Constants.HTTP_PARAM_END + "' and '" + Constants.HTTP_PARAM_STOP + "'.");
          return;
        } else {
          endParam = stopParam;
          endParamName = Constants.HTTP_PARAM_STOP;
        }
      }
      if (null != nowParam) {
        if (null != endParam) {
          resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Cannot specify both '" + Constants.HTTP_PARAM_END + "' and '" + Constants.HTTP_PARAM_NOW + "'.");
          return;
        } else {
          endParam = nowParam;
          endParamName = Constants.HTTP_PARAM_NOW;
        }
      }

      Long[] timerange = FETCH.computeTimeRange(startParam, Constants.HTTP_PARAM_START, endParam, endParamName, timespanParam, Constants.HTTP_PARAM_TIMESPAN, countParam, Constants.HTTP_PARAM_COUNT);
      then = timerange[0];
      now = timerange[1];

      if (null != countParam) {
        count = Long.parseLong(countParam);
      }
      
      if (null != skipParam) {
        skip = Long.parseLong(skipParam);        
      }
      
      if (null != sampleParam) {
        sample = Double.parseDouble(sampleParam);
      }

      if (null != preBoundaryParam) {
        preBoundary = Integer.parseInt(preBoundaryParam);
      }

      if (null != postBoundaryParam) {
        postBoundary = Integer.parseInt(postBoundaryParam);
      }
      
      String selector = splitFetch ? null : req.getParameter(Constants.HTTP_PARAM_SELECTOR);
   
      //
      // Extract token from header
      //
      
      String token = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
      
      // If token was not found in header, extract it from the 'token' parameter
      if (null == token && !splitFetch) {
        token = req.getParameter(Constants.HTTP_PARAM_TOKEN);
      }
      
      String fetchSig = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_FETCH_SIGNATURE));

      //
      // Check token signature if it was provided
      //
      
      boolean signed = false;
      
      if (splitFetch) {
        // Force showErrors
        showErrors = true;
        signed = true;
      }
      
      if (null != fetchSig) {
        if (null != fetchPSK) {
          String[] subelts = fetchSig.split(":");
          if (2 != subelts.length) {
            throw new IOException("Invalid fetch signature.");
          }
          long nowts = System.currentTimeMillis();
          long sigts = new BigInteger(subelts[0], 16).longValue();
          long sighash = new BigInteger(subelts[1], 16).longValue();
          
          if (nowts - sigts > 10000L) {
            throw new IOException("Fetch signature has expired.");
          }
          
          // Recompute hash of ts:token
          
          String tstoken = Long.toString(sigts) + ":" + token;
          
          long checkedhash = SipHashInline.hash24(fetchPSK, tstoken.getBytes(StandardCharsets.ISO_8859_1));
          
          if (checkedhash != sighash) {
            throw new IOException("Corrupted fetch signature");
          }
      
          signed = true;
        } else {
          throw new IOException("Fetch PreSharedKey is not set.");
        }
      }
      
      ReadToken rtoken = null;
      
      String format = splitFetch ? "wrapper" : req.getParameter(Constants.HTTP_PARAM_FORMAT);
      
      if (!splitFetch) {
        try {
          rtoken = Tokens.extractReadToken(token);
          
          if (rtoken.getHooksSize() > 0) {
            throw new IOException("Tokens with hooks cannot be used for fetching data.");        
          }
        } catch (WarpScriptException ee) {
          throw new IOException(ee);
        }
              
        if (null == rtoken) {
          resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing token.");
          return;
        }      
      }
      
      boolean showAttr = "true".equals(req.getParameter(Constants.HTTP_PARAM_SHOWATTR));
      
      Long activeAfter = null == req.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER) ? null : Long.parseLong(req.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER));
      Long quietAfter = null == req.getParameter(Constants.HTTP_PARAM_QUIETAFTER) ? null : Long.parseLong(req.getParameter(Constants.HTTP_PARAM_QUIETAFTER));
      boolean sortMeta = "true".equals(req.getParameter(Constants.HTTP_PARAM_SORTMETA));
      
      //
      // Extract the class and labels selectors
      // The class selector and label selectors are supposed to have
      // values which use percent encoding, i.e. explicit percent encoding which
      // might have been re-encoded using percent encoding when passed as parameter
      //
      //
      
      Set<Metadata> metadatas = new HashSet<Metadata>();
      List<Iterator<Metadata>> iterators = new ArrayList<Iterator<Metadata>>();
      
      if (!splitFetch) {      
        
        if (null == selector) {
          throw new IOException("Missing '" + Constants.HTTP_PARAM_SELECTOR + "' parameter.");
        }
        
        String[] selectors = selector.split("\\s+");
        
        for (String sel: selectors) {
          Matcher m = SELECTOR_RE.matcher(sel);
          
          if (!m.matches()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
          }
          
          String classSelector = WarpURLDecoder.decode(m.group(1), StandardCharsets.UTF_8);
          String labelsSelection = m.group(2);
          
          Map<String,String> labelsSelectors;

          try {
            labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
          } catch (ParseException pe) {
            throw new IOException(pe);
          }
          
          //
          // Force 'producer'/'owner'/'app' from token
          //
          
          labelsSelectors.remove(Constants.PRODUCER_LABEL);
          labelsSelectors.remove(Constants.OWNER_LABEL);
          labelsSelectors.remove(Constants.APPLICATION_LABEL);
          
          labelsSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));

          List<Metadata> metas = null;
          
          List<String> clsSels = new ArrayList<String>();
          List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
          
          clsSels.add(classSelector);
          lblsSels.add(labelsSelectors);
          
          DirectoryRequest request = new DirectoryRequest();
          request.setClassSelectors(clsSels);
          request.setLabelsSelectors(lblsSels);
          
          if (null != activeAfter) {
            request.setActiveAfter(activeAfter);
          }
          if (null != quietAfter) {
            request.setQuietAfter(quietAfter);
          }
          
          try {
            metas = directoryClient.find(request);
            metadatas.addAll(metas);
          } catch (Exception e) {
            //
            // If metadatas is not empty, create an iterator for it, then clear it
            //
            if (!metadatas.isEmpty()) {
              iterators.add(metadatas.iterator());
              metadatas.clear();
            }
            iterators.add(directoryClient.iterator(request));
          }
        }      
      } else {
        //
        // Add an iterator which reads splits from the request body
        //
        
        boolean gzipped = false;
        
        if (null != req.getHeader("Content-Type") && "application/gzip".equals(req.getHeader("Content-Type"))) {
          gzipped = true;
        }
        
        BufferedReader br = null;
            
        if (gzipped) {
          GZIPInputStream is = new GZIPInputStream(req.getInputStream());
          br = new BufferedReader(new InputStreamReader(is));
        } else {    
          br = req.getReader();
        }

        final BufferedReader fbr = br;
        

        MetadataIterator iterator = new MetadataIterator() {
          
          private List<Metadata> metadatas = new ArrayList<Metadata>();
          
          private boolean done = false;

          private String lasttoken = "";
          
          @Override
          public void close() throws Exception {
            fbr.close();
          }
          
          @Override
          public Metadata next() {
            if (!metadatas.isEmpty()) {
              Metadata meta = metadatas.get(metadatas.size() - 1);
              metadatas.remove(metadatas.size() - 1);
              return meta;
            } else {
              if (hasNext()) {
                return next();
              } else {
                throw new NoSuchElementException();
              }
            }
          }
          
          @Override
          public boolean hasNext() {
            if (!metadatas.isEmpty()) {
              return true;
            }

            if (done) {
              return false;
            }

            String line = null;

            try {
              line = fbr.readLine();
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }

            if (null == line) {
              done = true;
              return false;
            }

            //
            // Decode/Unwrap/Deserialize the split
            //

            byte[] data = OrderPreservingBase64.decode(line.getBytes(StandardCharsets.US_ASCII));
            if (null != fetchAES) {
              data = CryptoUtils.unwrap(fetchAES, data);
            }

            if (null == data) {
              throw new RuntimeException("Invalid wrapped content.");
            }

            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

            GTSSplit split = new GTSSplit();

            try {
              deserializer.deserialize(split, data);
            } catch (TException te) {
              throw new RuntimeException(te);
            }

            //
            // Check the expiry
            //

            long instant = System.currentTimeMillis();

            if (instant - split.getTimestamp() > maxSplitAge || instant > split.getExpiry()) {
              throw new RuntimeException("Split has expired.");
            }

            this.metadatas.addAll(split.getMetadatas());

            // We assume there was at least one metadata instance in the split!!!
            return true;
          }
        };
        
        iterators.add(iterator);
      }
         
      List<Metadata> metas = new ArrayList<Metadata>();
      metas.addAll(metadatas);
      
      if (!metas.isEmpty()) {
        iterators.add(metas.iterator());
      }
      
      //
      // Loop over the iterators, storing the read metadata to a temporary file encrypted on disk
      // Data is encrypted using a onetime pad
      //
      
      final byte[] onetimepad = new byte[(int) Math.min(65537, System.currentTimeMillis() % 100000)];
      new Random().nextBytes(onetimepad);
      
      final File cache = File.createTempFile(Long.toHexString(System.currentTimeMillis()) + "-" + Long.toHexString(System.nanoTime()), ".dircache");
      cache.deleteOnExit();
      
      FileWriter writer = new FileWriter(cache);

      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      
      int padidx = 0;
      
      for (Iterator<Metadata> itermeta: iterators){
        try {
          while(itermeta.hasNext()) {
            Metadata metadata = itermeta.next();
            
            try {
              byte[] bytes = serializer.serialize(metadata);
              // Apply onetimepad
              for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (bytes[i] ^ onetimepad[padidx++]);
                if (padidx >= onetimepad.length) {
                  padidx = 0;
                }
              }
              OrderPreservingBase64.encodeToWriter(bytes, writer);
              writer.write('\n');
            } catch (TException te) {          
            }         
          }
          
          if (!itermeta.hasNext() && (itermeta instanceof MetadataIterator)) {
            try {
              ((MetadataIterator) itermeta).close();
            } catch (Exception e) {          
            }
          }              
        } catch (Throwable t) {
          throw t;
        } finally {
          if (itermeta instanceof MetadataIterator) {
            try {
              ((MetadataIterator) itermeta).close();
            } catch (Exception e) {        
            }
          }
        }
      }
      
      writer.close();
      
      //
      // Create an iterator based on the cache
      //
      
      MetadataIterator cacheiterator = new MetadataIterator() {
        
        BufferedReader reader = new BufferedReader(new FileReader(cache));
        
        private Metadata current = null;
        private boolean done = false;
        
        private TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
        
        int padidx = 0;
        
        @Override
        public boolean hasNext() {
          if (done) {
            return false;
          }
          
          if (null != current) {
            return true;
          }
          
          try {
            String line = reader.readLine();
            if (null == line) {
              done = true;
              return false;
            }
            byte[] raw = OrderPreservingBase64.decode(line.getBytes(StandardCharsets.US_ASCII));
            // Apply one time pad
            for (int i = 0; i < raw.length; i++) {
              raw[i] = (byte) (raw[i] ^ onetimepad[padidx++]);
              if (padidx >= onetimepad.length) {
                padidx = 0;
              }
            }
            Metadata metadata = new Metadata();
            try {
              deserializer.deserialize(metadata, raw);
              this.current = metadata;
              return true;
            } catch (TException te) {
              LOG.error("", te);
            }
          } catch (IOException ioe) {
            LOG.error("", ioe);
          }
          
          return false;
        }
        
        @Override
        public Metadata next() {
          if (null != this.current) {
            Metadata metadata = this.current;
            this.current = null;
            return metadata;
          } else {
            throw new NoSuchElementException();
          }
        }
        
        @Override
        public void close() throws Exception {
          this.reader.close();
          cache.delete();
        }
      };
      
      iterators.clear();
      iterators.add(cacheiterator);
          
      metas = new ArrayList<Metadata>();

      PrintWriter pw = resp.getWriter();
      
      AtomicReference<Metadata> lastMeta = new AtomicReference<Metadata>(null);
      AtomicLong lastCount = new AtomicLong(0L);
      
      boolean expose = rtoken.getAttributesSize() > 0 && rtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);
      
      if (nocache) {
        StandaloneAcceleratedStoreClient.nocache();
      } else {
        StandaloneAcceleratedStoreClient.cache();        
      }
      
      if (nopersist) {
        StandaloneAcceleratedStoreClient.nopersist();
      } else {
        StandaloneAcceleratedStoreClient.persist();        
      }
      
      for (Iterator<Metadata> itermeta: iterators) {
        while(itermeta.hasNext()) {
          metas.add(itermeta.next());
          
          //
          // Access the data store every 'FETCH_BATCHSIZE' GTS or at the end of each iterator
          //
          
          if (metas.size() > FETCH_BATCHSIZE || !itermeta.hasNext()) {
            try(GTSDecoderIterator iterrsc = storeClient.fetch(rtoken, metas, now, then, count, skip, sample, writeTimestamp, preBoundary, postBoundary)) {
              GTSDecoderIterator iter = iterrsc;
                          
              if (unpack) {
                iter = new UnpackingGTSDecoderIterator(iter, suffix);
                count = Long.MAX_VALUE;
              }
              
              if ("text".equals(format)) {
                textDump(pw, iter, now, count, false, dedup, signed, showAttr, lastMeta, lastCount, sortMeta, expose);
              } else if ("fulltext".equals(format)) {
                textDump(pw, iter, now, count, true, dedup, signed, showAttr, lastMeta, lastCount, sortMeta, expose);
              } else if ("raw".equals(format)) {
                rawDump(pw, iter, dedup, signed, count, lastMeta, lastCount, sortMeta, expose);
              } else if ("wrapper".equals(format)) {
                wrapperDump(pw, iter, dedup, signed, fetchPSK, count, lastMeta, lastCount);
              } else if ("json".equals(format)) {
                jsonDump(pw, iter, now, count, dedup, signed, lastMeta, lastCount, expose);
              } else if ("tsv".equals(format)) {
                tsvDump(pw, iter, now, count, false, dedup, signed, lastMeta, lastCount, sortMeta, expose);
              } else if ("fulltsv".equals(format)) {
                tsvDump(pw, iter, now, count, true, dedup, signed, lastMeta, lastCount, sortMeta, expose);
              } else if ("pack".equals(format)) {
                packedDump(pw, iter, now, count, dedup, signed, lastMeta, lastCount, maxDecoderLen, suffix, chunksize, sortMeta, expose);
              } else if ("null".equals(format)) {
                nullDump(iter);
              } else {
                textDump(pw, iter, now, count, false, dedup, signed, showAttr, lastMeta, lastCount, sortMeta, expose);
              }
            } catch (Throwable t) {
              LOG.error("",t);
              Sensision.update(SensisionConstants.CLASS_WARP_FETCH_ERRORS, Sensision.EMPTY_LABELS, 1);
              if (showErrors) {
                pw.println();
                StringWriter sw = new StringWriter();
                PrintWriter pw2 = new PrintWriter(sw);
                t.printStackTrace(pw2);
                pw2.close();
                sw.flush();
                String error = URLEncoder.encode(sw.toString(), StandardCharsets.UTF_8.name());
                pw.println(Constants.EGRESS_FETCH_ERROR_PREFIX + error);
              }
              throw new IOException(t);
            } finally {      
              if (!itermeta.hasNext() && (itermeta instanceof MetadataIterator)) {
                try {
                  ((MetadataIterator) itermeta).close();
                } catch (Exception e) {          
                }
              }
            }                  
            
            //
            // Reset 'metas'
            //
            
            metas.clear();
          }        
        }
        
        if (!itermeta.hasNext() && (itermeta instanceof MetadataIterator)) {
          try {
            ((MetadataIterator) itermeta).close();
          } catch (Exception e) {          
          }
        }
      }

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_FETCH_REQUESTS, labels, 1);      
    } catch (Exception e) {
      if (!resp.isCommitted()) {
        resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ThrowableUtils.getErrorMessage(e, Constants.MAX_HTTP_REASON_LENGTH));
        return;
      }
    }
  }
  
  private static void rawDump(PrintWriter pw, GTSDecoderIterator iter, boolean dedup, boolean signed, long count, AtomicReference<Metadata> lastMeta, AtomicLong lastCount, boolean sortMeta, boolean expose) throws IOException {
    
    String name = null;
    Map<String,String> labels = null;
    
    StringBuilder sb = new StringBuilder();
    
    Metadata lastMetadata = lastMeta.get();
    long currentCount = lastCount.get();
    
    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();

      if (dedup) {
        decoder = decoder.dedup();
      }
      
      if(!decoder.next()) {
        continue;
      }

      long toDecodeCount = Long.MAX_VALUE;
      
      if (count >= 0) {
        Metadata meta = decoder.getMetadata();
        if (!meta.equals(lastMetadata)) {
          lastMetadata = meta;
          currentCount = 0;
        }
        toDecodeCount = Math.max(0, count - currentCount);
      }

      GTSEncoder encoder = decoder.getEncoder(true);
      
      //
      // Only display the class + labels if they have changed since the previous GTS
      //
      
      Map<String,String> lbls = decoder.getLabels();
      
      //
      // Compute the name
      //

      name = decoder.getName();
      labels = lbls;
      sb.setLength(0);
      GTSHelper.encodeName(sb, name);
      sb.append("{");
      boolean first = true;
      
      if (sortMeta) {
        lbls = new TreeMap<String,String>(lbls);
      }
      
      for (Entry<String, String> entry: lbls.entrySet()) {
        //
        // Skip owner/producer labels and any other 'private' labels
        //
        if (!signed && !Constants.EXPOSE_OWNER_PRODUCER && !expose) {
          if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
            continue;
          }
          if (Constants.OWNER_LABEL.equals(entry.getKey())) {
            continue;
          }          
        }
        
        if (!first) {
          sb.append(",");
        }
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        first = false;
      }
      sb.append("}");

      if (encoder.getCount() > toDecodeCount) {
        // We have too much data, shrink the encoder
        GTSEncoder enc = new GTSEncoder();
        enc.safeSetMetadata(decoder.getMetadata());
        while(decoder.next() && toDecodeCount > 0) {
          enc.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
          toDecodeCount--;
        }
        encoder = enc;
      }

      if (count >= 0) {
        currentCount += encoder.getCount();
      }

      if (encoder.size() > 0) {
        pw.print(encoder.getBaseTimestamp());
        pw.print("//");
        pw.print(encoder.getCount());
        pw.print(" ");
        pw.print(sb.toString());
        pw.print(" ");

        //pw.println(new String(OrderPreservingBase64.encode(encoder.getBytes())));
        OrderPreservingBase64.encodeToWriter(encoder.getBytes(), pw);
        pw.write('\r');
        pw.write('\n');        
      }
    }
    
    lastMeta.set(lastMetadata);
    lastCount.set(currentCount);
  }

  private static void wrapperDump(PrintWriter pw, GTSDecoderIterator iter, boolean dedup, boolean signed, byte[] fetchPSK, long count, AtomicReference<Metadata> lastMeta, AtomicLong lastCount) throws IOException {

    if (!signed) {
      throw new IOException("Unsigned request.");
    }

    // Labels for Sensision
    Map<String,String> labels = new HashMap<String,String>();

    StringBuilder sb = new StringBuilder();
    
    Metadata lastMetadata = lastMeta.get();
    long currentCount = lastCount.get();
    
    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();

      if (dedup) {
        decoder = decoder.dedup();
      }
      
      if(!decoder.next()) {
        continue;
      }

      long toDecodeCount = Long.MAX_VALUE;
      
      if (count >= 0) {
        Metadata meta = decoder.getMetadata();
        if (!meta.equals(lastMetadata)) {
          lastMetadata = meta;
          currentCount = 0;
        }
        toDecodeCount = Math.max(0, count - currentCount);
      }
      
      GTSEncoder encoder = decoder.getEncoder(true);

      if (encoder.getCount() > toDecodeCount) {
        // We have too much data, shrink the encoder
        GTSEncoder enc = new GTSEncoder();
        enc.safeSetMetadata(decoder.getMetadata());
        while(decoder.next() && toDecodeCount > 0) {
          enc.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
          toDecodeCount--;
        }
        encoder = enc;
      }

      if (count >= 0) {
        currentCount += encoder.getCount();
      }

      if (encoder.size() <= 0) {
        continue;
      }
      
      //
      // Build a GTSWrapper
      //
      
      GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true);
      
//      GTSWrapper wrapper = new GTSWrapper();
//      wrapper.setBase(encoder.getBaseTimestamp());
//      wrapper.setMetadata(encoder.getMetadata());
//      wrapper.setCount(encoder.getCount());
//      wrapper.setEncoded(encoder.getBytes());
      
      //
      // Serialize the wrapper
      //
      
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      byte[] data = null;
      
      try {
        data = serializer.serialize(wrapper);
      } catch (TException te) {
        throw new IOException(te);
      }
      
      //
      // Output is GTSWrapperId <WSP> HASH <WSP> GTSWrapper
      //
      
      pw.write(Hex.encodeHex(GTSWrapperHelper.getId(wrapper)));
      
      pw.write(' ');
      
      if (null != fetchPSK) {
        //
        // Compute HMac for the wrapper
        //
        
        long hash = SipHashInline.hash24(fetchPSK, data);
        
        //
        // Output the MAC before the data, as hex digits
        //
        pw.write(Hex.encodeHex(Longs.toByteArray(hash)));               
      } else {
        pw.write('-');
      }
      
      pw.write(' ');
      
      //
      // Base64 encode the wrapper
      //
      
      OrderPreservingBase64.encodeToWriter(data, pw);
      pw.write('\r');
      pw.write('\n');

      //
      // Sensision metrics
      //

      labels.clear();
      if (wrapper.isSetMetadata()) {
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, wrapper.getMetadata().getLabels().get(Constants.APPLICATION_LABEL));
      } else {
        labels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, "");
      }

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS, Sensision.EMPTY_LABELS, 1);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_PERAPP, labels, 1);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_SIZE, Sensision.EMPTY_LABELS, data.length);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_SIZE_PERAPP, labels, data.length);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_DATAPOINTS, Sensision.EMPTY_LABELS, wrapper.getCount());
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_SFETCH_WRAPPERS_DATAPOINTS_PERAPP, labels, wrapper.getCount());

    }
    
    lastMeta.set(lastMetadata);
    lastCount.set(currentCount);
  }
  
  /**
   * Output a text version of fetched data. Deduplication is done on the fly so we don't decode twice.
   * 
   */
  private static void textDump(PrintWriter pw, GTSDecoderIterator iter, long now, long count, boolean raw, boolean dedup, boolean signed, boolean showAttributes, AtomicReference<Metadata> lastMeta, AtomicLong lastCount, boolean sortMeta, boolean expose) throws IOException {
    
    String name = null;
    Map<String,String> labels = null;
    
    StringBuilder sb = new StringBuilder();
    
    Metadata lastMetadata = lastMeta.get();
    long currentCount = lastCount.get();
    
    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();
      
      if (!decoder.next()) {
        continue;
      }

      long toDecodeCount = Long.MAX_VALUE;
      
      if (count >= 0) {
        Metadata meta = decoder.getMetadata();
        if (!meta.equals(lastMetadata)) {
          lastMetadata = meta;
          currentCount = 0;
        }
        toDecodeCount = Math.max(0, count - currentCount);
      }
      
      //
      // Only display the class + labels if they have changed since the previous GTS
      //
      
      Map<String,String> lbls = decoder.getLabels();
      
      //
      // Compute the new name
      //

      boolean displayName = false;
      
      if (null == name || (!name.equals(decoder.getName()) || !labels.equals(lbls))) {
        displayName = true;
        name = decoder.getName();
        labels = lbls;
        sb.setLength(0);
        GTSHelper.encodeName(sb, name);
        sb.append("{");
        boolean first = true;

        if (sortMeta) {
          lbls = new TreeMap<String, String>(lbls);
        }
        
        for (Entry<String, String> entry: lbls.entrySet()) {
          //
          // Skip owner/producer labels and any other 'private' labels
          //
          if (!signed && !Constants.EXPOSE_OWNER_PRODUCER && !expose) {
            if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
              continue;
            }
            if (Constants.OWNER_LABEL.equals(entry.getKey())) {
              continue;
            }            
          }
          
          if (!first) {
            sb.append(",");
          }
          GTSHelper.encodeName(sb, entry.getKey());
          sb.append("=");
          GTSHelper.encodeName(sb, entry.getValue());
          first = false;
        }
        sb.append("}");
        
        if (showAttributes) {
          Metadata meta = decoder.getMetadata();
          if (meta.getAttributesSize() > 0) {
            
            if (sortMeta) {
              meta.setAttributes(new TreeMap<String,String>(meta.getAttributes()));
            }
            
            GTSHelper.labelsToString(sb, meta.getAttributes(), true);
          } else {
            sb.append("{}");
          }          
        }
      }
      
      long timestamp = 0L;
      long location = GeoTimeSerie.NO_LOCATION;
      long elevation = GeoTimeSerie.NO_ELEVATION;
      Object value = null;
      
      boolean dup = true;
      
      long decoded = 0;
      
      do {
      
        if (toDecodeCount == decoded) {
          break;
        }
        
        // FIXME(hbs): only display the results which match the authorized (according to token) timerange and geo zones
        
        long newTimestamp = decoder.getTimestamp();

        //
        // TODO(hbs): filter out values with no location or outside the selected geozone when a geozone was set
        //
        
        long newLocation = decoder.getLocation();
        long newElevation = decoder.getElevation();
        Object newValue = decoder.getBinaryValue();
        
        dup = true;
        
        if (dedup) {
          if (location != newLocation || elevation != newElevation) {
            dup = false;
          } else {
            if (null == newValue) {
              // Consider nulls as duplicates (can't happen!)
              dup = false;
            } else if (newValue instanceof Number) {
              if (!((Number) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof String) {
              if (!((String) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof Boolean) {
              if (!((Boolean) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof byte[]) {
              if (!(value instanceof byte[]) || 0 != Bytes.compareTo((byte[]) newValue, (byte[]) value)) {
                dup = false;
              }
            }
          }          
        }

        decoded++;
        
        location = newLocation;
        elevation = newElevation;
        timestamp = newTimestamp;
        value = newValue;
            
        if (raw) {
          if (!dedup || !dup) {
            pw.println(GTSHelper.tickToString(sb, timestamp, location, elevation, value));
          }
        } else {
          // Display the name only if we have at least one value to display
          // We force 'dup' to be false when we must show the name
          if (displayName) {
            pw.println(GTSHelper.tickToString(sb, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue()));
            displayName = false;
            dup = false;
          } else {
            if (!dedup || !dup) {
              pw.print("=");
              pw.println(GTSHelper.tickToString(timestamp,
                  location,
                  elevation,
                  value));                                    
            }
          }
        }
      } while (decoder.next());        
      
      // Update currentcount
      if (count >= 0) {
        currentCount += decoded;
      }
      
      // Print any remaining value
      if (dedup && dup) {
        if (raw) {
          pw.println(GTSHelper.tickToString(sb, timestamp, location, elevation, value));          
        } else {
          pw.print("=");
          pw.println(GTSHelper.tickToString(timestamp,
              location,
              elevation,
              value));                                              
        }
      }
      
      //
      // If displayName is still true it means we should have displayed the name but no value matched,
      // so set name to null so we correctly display the name for the next decoder if it has values
      //
      
      if (displayName) {
        name = null;
      }
    }
    
    lastMeta.set(lastMetadata);
    lastCount.set(currentCount);
  }

  static void jsonDump(PrintWriter pw, Iterator<GTSDecoder> iter, long now, long count, boolean dedup, boolean signed, AtomicReference<Metadata> lastMeta, AtomicLong lastCount, boolean expose) throws IOException {
    
    String name = null;
    Map<String,String> labels = null;
    
    pw.print("[");
    
    boolean hasValues = false;

    Metadata lastMetadata = lastMeta.get();
    long currentCount = lastCount.get();
    
    try {
      StringBuilder sb = new StringBuilder();
      
      boolean firstgts = true;
      
      long mask = (long) (Math.random() * Long.MAX_VALUE);
      
      while(iter.hasNext()) {
        GTSDecoder decoder = iter.next();
        
        if (dedup) {
          decoder = decoder.dedup();
        }
        
        if (!decoder.next()) {
          continue;
        }
            
        long toDecodeCount = Long.MAX_VALUE;
        
        if (count >= 0) {
          Metadata meta = decoder.getMetadata();
          if (!meta.equals(lastMetadata)) {
            lastMetadata = meta;
            currentCount = 0;
          }
          toDecodeCount = Math.max(0, count - currentCount);
        }

        //
        // Only display the class + labels if they have changed since the previous GTS
        //
        
        Map<String,String> lbls = decoder.getLabels();
        
        //
        // Compute the new name
        //

        boolean displayName = false;
        
        if (null == name || (!name.equals(decoder.getName()) || !labels.equals(lbls))) {
          displayName = true;
          name = decoder.getName();
          labels = lbls;
          sb.setLength(0);

          sb.append("{\"");
          sb.append(MetadataSerializer.FIELD_NAME);
          sb.append("\":");

          sb.append(JsonUtils.objectToJson(name));

          boolean first = true;

          sb.append(",\"");
          sb.append(MetadataSerializer.FIELD_LABELS);
          sb.append("\":{");

          for (Entry<String, String> entry: lbls.entrySet()) {
            //
            // Skip owner/producer labels and any other 'private' labels
            //
            if (!signed && !Constants.EXPOSE_OWNER_PRODUCER && !expose) {
              if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
                continue;
              }
              if (Constants.OWNER_LABEL.equals(entry.getKey())) {
                continue;
              }            
            }
            
            if (!first) {
              sb.append(",");
            }
            
            sb.append(JsonUtils.objectToJson(entry.getKey()));
            sb.append(":");
            sb.append(JsonUtils.objectToJson(entry.getValue()));
            first = false;
          }
          sb.append("}");

          sb.append(",\"");
          sb.append(MetadataSerializer.FIELD_ATTRIBUTES);
          sb.append("\":{");

          first = true;
          for (Entry<String, String> entry: decoder.getMetadata().getAttributes().entrySet()) {
            if (!first) {
              sb.append(",");
            }
            
            sb.append(JsonUtils.objectToJson(entry.getKey()));
            sb.append(":");
            sb.append(JsonUtils.objectToJson(entry.getValue()));
            first = false;
          }
          
          sb.append("}");
          sb.append(",\"");
          sb.append(FIELD_ID);
          sb.append("\":\"");
          sb.append(decoder.getLabelsId() & mask);
          sb.append("\",\"");
          sb.append(MetadataSerializer.FIELD_LASTACTIVITY);
          sb.append("\":");
          sb.append(decoder.getMetadata().getLastActivity());

          sb.append(",\"");
          sb.append(GeoTimeSerieSerializer.FIELD_VALUES);
          sb.append("\":[");
        }
        
        long decoded = 0L;
        
        do {
          
          if (toDecodeCount == decoded) {
            break;
          }

          // FIXME(hbs): only display the results which match the authorized (according to token) timerange and geo zones
          
          decoded++;
          
          //
          // TODO(hbs): filter out values with no location or outside the selected geozone when a geozone was set
          //
          
          // Display the name only if we have at least one value to display
          if (displayName) {
            if (!firstgts) {
              pw.print("]},");
            }
            pw.print(sb.toString());
            firstgts = false;
            displayName = false;
          } else {
            pw.print(",");
          }
          hasValues = true;
          pw.print("[");
          pw.print(decoder.getTimestamp());
          if (GeoTimeSerie.NO_LOCATION != decoder.getLocation()) {
            double[] latlon = GeoXPLib.fromGeoXPPoint(decoder.getLocation());
            pw.print(",");
            pw.print(latlon[0]);
            pw.print(",");
            pw.print(latlon[1]);
          }
          if (GeoTimeSerie.NO_ELEVATION != decoder.getElevation()) {
            pw.print(",");
            pw.print(decoder.getElevation());
          }
          pw.print(",");
          // For JSON representation we do not extract the binary value as byte[] cannot be
          // represented in JSON
          Object value = decoder.getValue();
          
          if (value instanceof Number) {
            pw.print(value);
          } else if (value instanceof Boolean) {
            pw.print(Boolean.TRUE.equals(value) ? "true" : "false");
          } else {
            pw.print(JsonUtils.objectToJson(value.toString()));
          }
          pw.print("]");
        } while (decoder.next());        
        
        if (count >= 0) {
          currentCount += decoded;
        }

        //
        // If displayName is still true it means we should have displayed the name but no value matched,
        // so set name to null so we correctly display the name for the next decoder if it has values
        //
        
        if (displayName) {
          name = null;
        }
      }
            
    } catch (Throwable t) {
      throw t;
    } finally {
      if (hasValues) {
        pw.print("]}");
      }
      pw.print("]");      
    }
    
    lastMeta.set(lastMetadata);
    lastCount.set(currentCount);
  }
  
  /**
   * Output a tab separated version of fetched data. Deduplication is done on the fly so we don't decode twice.
   * 
   */
  private static void tsvDump(PrintWriter pw, GTSDecoderIterator iter, long now, long count, boolean raw, boolean dedup, boolean signed, AtomicReference<Metadata> lastMeta, AtomicLong lastCount, boolean sortMeta, boolean expose) throws IOException {
    
    String name = null;
    Map<String,String> labels = null;
    
    StringBuilder classSB = new StringBuilder();
    StringBuilder labelsSB = new StringBuilder();
    StringBuilder attributesSB = new StringBuilder();
    StringBuilder valueSB = new StringBuilder();

    Metadata lastMetadata = lastMeta.get();
    long currentCount = lastCount.get();

    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();
      
      if (!decoder.next()) {
        continue;
      }
    
      long toDecodeCount = Long.MAX_VALUE;
      
      if (count >= 0) {
        Metadata meta = decoder.getMetadata();
        if (!meta.equals(lastMetadata)) {
          lastMetadata = meta;
          currentCount = 0;
        }
        toDecodeCount = Math.max(0, count - currentCount);
      }

      //
      // Only display the class + labels if they have changed since the previous GTS
      //
      
      Map<String,String> lbls = decoder.getLabels();
      
      //
      // Compute the new name
      //

      boolean displayName = false;
      
      if (null == name || (!name.equals(decoder.getName()) || !labels.equals(lbls))) {
        displayName = true;
        name = decoder.getName();
        labels = lbls;
        classSB.setLength(0);
        GTSHelper.encodeName(classSB, name);
        labelsSB.setLength(0);
        attributesSB.setLength(0);
        boolean first = true;
        
        if (sortMeta) {
          lbls = new TreeMap<String,String>(lbls);
        }
        for (Entry<String, String> entry: lbls.entrySet()) {
          //
          // Skip owner/producer labels and any other 'private' labels
          //
          if (!signed && !Constants.EXPOSE_OWNER_PRODUCER && !expose) {
            if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
              continue;
            }
            if (Constants.OWNER_LABEL.equals(entry.getKey())) {
              continue;
            }            
          }
          
          if (!first) {
            labelsSB.append(",");
          }
          GTSHelper.encodeName(labelsSB, entry.getKey());
          labelsSB.append("=");
          GTSHelper.encodeName(labelsSB, entry.getValue());
          first = false;
        }

        first = true;
        if (decoder.getMetadata().getAttributesSize() > 0) {
          
          if (sortMeta) {
            decoder.getMetadata().setAttributes(new TreeMap<String,String>(decoder.getMetadata().getAttributes()));
          }
          
          for (Entry<String, String> entry: decoder.getMetadata().getAttributes().entrySet()) {
            if (!first) {
              attributesSB.append(",");
            }
            GTSHelper.encodeName(attributesSB, entry.getKey());
            attributesSB.append("=");
            GTSHelper.encodeName(attributesSB, entry.getValue());
            first = false;
          }          
        }

      }
      
      long timestamp = 0L;
      long location = GeoTimeSerie.NO_LOCATION;
      long elevation = GeoTimeSerie.NO_ELEVATION;
      Object value = null;
      
      boolean dup = true;
      
      long decoded = 0;
      
      do {
        
        if (toDecodeCount == decoded) {
          break;
        }
        
        long newTimestamp = decoder.getTimestamp();
        
        //
        // TODO(hbs): filter out values with no location or outside the selected geozone when a geozone was set
        //
        
        long newLocation = decoder.getLocation();
        long newElevation = decoder.getElevation();
        Object newValue = decoder.getBinaryValue();
        
        dup = true;
        
        if (dedup) {
          if (location != newLocation || elevation != newElevation) {
            dup = false;
          } else {
            if (null == newValue) {
              // Consider nulls as duplicates (can't happen!)
              dup = false;
            } else if (newValue instanceof Number) {
              if (!((Number) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof String) {
              if (!((String) newValue).equals(value)) {
                dup = false;
              }
            } else if (newValue instanceof byte[]) {
              if (!(value instanceof byte[]) || 0 != Bytes.compareTo((byte[]) value, (byte[]) newValue)) {
                dup = false;
              }
            } else if (newValue instanceof Boolean) {
              if (!((Boolean) newValue).equals(value)) {
                dup = false;
              }
            }
          }          
        }
                
        decoded++;

        location = newLocation;
        elevation = newElevation;
        timestamp = newTimestamp;
        value = newValue;
            
        if (raw) {
          if (!dedup || !dup) {
            pw.print(classSB);
            pw.print('\t');
            pw.print(labelsSB);
            pw.print('\t');
            pw.print(attributesSB);
            pw.print('\t');
            
            pw.print(timestamp);
            pw.print('\t');
            
            if (GeoTimeSerie.NO_LOCATION != location) {
              double[] latlon = GeoXPLib.fromGeoXPPoint(location);
              pw.print(latlon[0]);
              pw.print('\t');
              pw.print(latlon[1]);
            } else {
              pw.print('\t');
            }

            pw.print('\t');

            if (GeoTimeSerie.NO_ELEVATION != elevation) {
              pw.print(elevation);
            }
            pw.print('\t');
            
            valueSB.setLength(0);
            GTSHelper.encodeValue(valueSB, value);
            pw.println(valueSB);
          }
        } else {
          // Display the name only if we have at least one value to display
          // We force 'dup' to be false when we must show the name
          if (displayName) {
            pw.print("# ");
            pw.print(classSB);
            pw.print("{");
            pw.print(labelsSB);
            pw.print("}");
            pw.print("{");
            pw.print(attributesSB);
            pw.println("}");
            displayName = false;
            dup = false;
          }
          
          if (!dedup || !dup) {
            pw.print(timestamp);
            pw.print('\t');           
            if (GeoTimeSerie.NO_LOCATION != location) {
              double[] latlon = GeoXPLib.fromGeoXPPoint(location);
              pw.print(latlon[0]);
              pw.print('\t');
              pw.print(latlon[1]);
            } else {
              pw.print('\t');
            }
            
            pw.print('\t');

            if (GeoTimeSerie.NO_ELEVATION != elevation) {
              pw.print(elevation);
            }
            pw.print('\t');
            
            valueSB.setLength(0);
            GTSHelper.encodeValue(valueSB, value);
            pw.println(valueSB);
          }
        }
      } while (decoder.next());        
      
      // Update currentcount
      if (count >= 0) {
        currentCount += decoded;
      }

      // Print any remaining value
      if (dedup && dup) {
        if (raw) {
          pw.print(classSB);
          pw.print('\t');
          pw.print(labelsSB);
          pw.print('\t');
          pw.print(attributesSB);
          pw.print('\t');
            
          pw.print(timestamp);
          pw.print('\t');

          if (GeoTimeSerie.NO_LOCATION != location) {
            double[] latlon = GeoXPLib.fromGeoXPPoint(location);
            pw.print(latlon[0]);
            pw.print('\t');
            pw.print(latlon[1]);
          } else {
            pw.print('\t');
          }

          pw.print('\t');

          if (GeoTimeSerie.NO_ELEVATION != elevation) {
            pw.print(elevation);
          }
          pw.print('\t');
            
          valueSB.setLength(0);
          GTSHelper.encodeValue(valueSB, value);
          pw.println(valueSB);
        } else {
          pw.print(timestamp);
          pw.print('\t');
          if (GeoTimeSerie.NO_LOCATION != location) {
            double[] latlon = GeoXPLib.fromGeoXPPoint(location);
            pw.print(latlon[0]);
            pw.print('\t');
            pw.print(latlon[1]);
          } else {
            pw.print('\t');
          }

          pw.print('\t');
          
          if (GeoTimeSerie.NO_ELEVATION != elevation) {
            pw.print(elevation);
          }
          pw.print('\t');
            
          valueSB.setLength(0);
          GTSHelper.encodeValue(valueSB, value);
          pw.println(valueSB);
        }
        
      }
      
      //
      // If displayName is still true it means we should have displayed the name but no value matched,
      // so set name to null so we correctly display the name for the next decoder if it has values
      //
      
      if (displayName) {
        name = null;
      }
    }
    
    lastMeta.set(lastMetadata);
    lastCount.set(currentCount);
  }

  private void nullDump(GTSDecoderIterator iter) {
    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();
    }
  }
  
  private void packedDump(PrintWriter pw, GTSDecoderIterator iter, long now, long count, boolean dedup, boolean signed, AtomicReference<Metadata> lastMeta, AtomicLong lastCount, int maxDecoderLen, String classSuffix, long chunksize, boolean sortMeta, boolean expose) throws IOException {
    
    String name = null;
    Map<String,String> labels = null;
    
    StringBuilder sb = new StringBuilder();
    
    Metadata lastMetadata = lastMeta.get();
    long currentCount = lastCount.get();
    
    List<GTSEncoder> encoders = new ArrayList<GTSEncoder>();

    while(iter.hasNext()) {
      GTSDecoder decoder = iter.next();

      if (dedup) {
        decoder = decoder.dedup();
      }
      
      if(!decoder.next()) {
        continue;
      }

      long toDecodeCount = Long.MAX_VALUE;
      
      if (count >= 0) {
        Metadata meta = decoder.getMetadata();
        if (!meta.equals(lastMetadata)) {
          lastMetadata = meta;
          currentCount = 0;
        }
        toDecodeCount = Math.max(0, count - currentCount);
      }

      GTSEncoder encoder = decoder.getEncoder(true);
      
      //
      // Only display the class + labels if they have changed since the previous GTS
      //
      
      Map<String,String> lbls = decoder.getLabels();
      
      //
      // Compute the name
      //

      name = decoder.getName();
      labels = lbls;
      sb.setLength(0);
      GTSHelper.encodeName(sb, name + classSuffix);
      sb.append("{");
      boolean first = true;
      
      if (sortMeta) {
        lbls = new TreeMap<String,String>(lbls);
      }
      
      for (Entry<String, String> entry: lbls.entrySet()) {
        //
        // Skip owner/producer labels and any other 'private' labels
        //
        if (!signed && !Constants.EXPOSE_OWNER_PRODUCER && !expose) {
          if (Constants.PRODUCER_LABEL.equals(entry.getKey())) {
            continue;
          }
          if (Constants.OWNER_LABEL.equals(entry.getKey())) {
            continue;
          }          
        }
        
        if (!first) {
          sb.append(",");
        }
        GTSHelper.encodeName(sb, entry.getKey());
        sb.append("=");
        GTSHelper.encodeName(sb, entry.getValue());
        first = false;
      }
      sb.append("}");

      // We treat the case where encoder.getCount() is 0 in a special way
      // as this may be because the encoder was generated from a partly
      // consumed decoder and thus its count was reset to 0
      if (0 == encoder.getCount() || encoder.getCount() > toDecodeCount) {
        // We have too much data, shrink the encoder
        GTSEncoder enc = new GTSEncoder();
        enc.safeSetMetadata(decoder.getMetadata());
        while(decoder.next() && toDecodeCount > 0) {
          enc.addValue(decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getBinaryValue());
          toDecodeCount--;
        }
        encoder = enc;
      }

      if (count >= 0) {
        currentCount += encoder.getCount();
      }

      encoders.clear();
      
      //
      // Add encoders per chunk
      //
      
      GTSDecoder chunkdec = encoder.getDecoder(true);
      
      GTSEncoder chunkenc = null;
      
      Long lastchunk = null;
      
      if (Long.MAX_VALUE == chunksize) {
        encoders.add(encoder);
      } else {
        while(chunkdec.next()) {
          long ts = chunkdec.getTimestamp();
          long chunk = ts >= 0 ? ts / chunksize : ((ts + 1) / chunksize) - 1;

          //
          // If it is the first chunk or we changed chunk, create a new encoder
          //
          
          if (null == chunkenc || (null != lastchunk && chunk != lastchunk)) {
            chunkenc = new GTSEncoder(0L);
            chunkenc.setMetadata(encoder.getMetadata());
            encoders.add(chunkenc);
          }
         
          lastchunk = chunk;
          
          chunkenc.addValue(ts, chunkdec.getLocation(), chunkdec.getElevation(), chunkdec.getBinaryValue());
        }        
      }
            
      while(!encoders.isEmpty()) {
        encoder = encoders.remove(0);

        if (encoder.size() > 0) {
          //
          // Determine most recent timestamp
          //
          
          GTSDecoder dec = encoder.getDecoder(true);

          dec.next();
          
          long timestamp = dec.getTimestamp();

          //
          // Build GTSWrapper
          //

          encoder.setMetadata(new Metadata());
          // Clear labels
          encoder.setName("");
          encoder.setLabels(new HashMap<String,String>());
          encoder.getMetadata().setAttributes(new HashMap<String,String>());
          
          GTSWrapper wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(encoder, true);
          
          TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
          byte[] serialized;
          
          try {
            serialized = ser.serialize(wrapper);
          } catch (TException te) {
            throw new IOException(te);
          }

          //
          // Check the size of the generatd wrapper. If it is over 75% of maxDecoderLen,
          // split the original encoder in two
          //
                    
          if (serialized.length >= Math.floor(0.75D * maxDecoderLen) && encoder.getCount() > 2) {
            GTSEncoder split = new GTSEncoder(0L);
            split.setMetadata(encoder.getMetadata());
            
            List<GTSEncoder> splits = new ArrayList<GTSEncoder>();
            
            splits.add(split);
            
            int threshold = encoder.size() / 2;
            
            GTSDecoder deco = encoder.getDecoder(true);
            
            while(deco.next()) {
              split.addValue(deco.getTimestamp(), deco.getLocation(), deco.getElevation(), deco.getBinaryValue());
              if (split.size() > threshold) {
                split = new GTSEncoder(0L);
                splits.add(split);
              }
            }
            
            //
            // Now insert the splits at the beginning of 'encoders'
            //
            
            for (int i = splits.size() - 1; i >= 0; i--) {
              encoders.add(0, splits.get(i));
            }
            continue;
          }
          
          if (serialized.length > Math.ceil(0.75D * maxDecoderLen)) {
            throw new IOException("Encountered a value whose length is above the configured threshold of " + maxDecoderLen);
          }
          
          pw.print(timestamp);
          pw.print("//");
          pw.print(encoder.getCount());
          pw.print(" ");
          pw.print(sb.toString());
          pw.print(" '");

          OrderPreservingBase64.encodeToWriter(serialized, pw);
          
          pw.print("'");
          pw.write('\r');
          pw.write('\n');        
        }
      }
    }

    lastMeta.set(lastMetadata);
    lastCount.set(currentCount);
  }
}
