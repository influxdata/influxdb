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

package io.warp10.continuum;

import io.warp10.json.JsonUtils;
import io.warp10.WarpURLEncoder;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.script.HyperLogLogPlus;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class generates a Throttling configuration by merging a base configuration
 * with the limits and a set of estimators collected from the various ingestion endpoints (and soon geo directories).
 * 
 * Relies on macro ops/throttling-ddp.mc2 from repo 'devops'
 */
public class ThrottlingConfigGenerator {
  
  /**
   * We have our own constant here to avoid loading the ThrottlingManager class
   * which will fail when it attempts to call init in its static block.
   */
  private static final double MINIMUM_RATE_LIMIT = 1.0D / 3600.0D;
  
  public static void genConfiguration(String baseUrl, String warpEndPoint, String token, String cell, String ddp_spike) throws Exception {
    
    if (null == ddp_spike) {
      ddp_spike = "86400";
    }
    
    //
    // Compute the period on which we allow the DDP to be produced
    //
    
    double ddpSpike = Double.parseDouble(ddp_spike);
    
    //
    // Retrieve the base configuration
    //
    
    URL url = new URL(baseUrl);
    
    BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
    
    Map<String,Long> MADS = new HashMap<String,Long>();
    Map<String,Double> DPS = new HashMap<String, Double>();
    Map<String,HyperLogLogPlus> HLLP = new HashMap<String, HyperLogLogPlus>();
    
    while (true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      line = line.trim();
      
      if ("".equals(line)) {
        continue;
      }
      
      // Skip comments
      if (line.charAt(0) == '#') {
        continue;
      }
      
      String[] subtokens = line.split(":");
      
      // Entity concerned by the line
      String entity = subtokens[0];
      // Monthly Active Device Streams
      MADS.put(entity, Long.parseLong(subtokens[1]));
      // Recompute the DDP from the Datapoint rate (dp/s)
      DPS.put(entity, 86400.0D * Double.parseDouble(subtokens[2]));            
    }
    
    br.close();
    
    //
    // Compute the daily ingested datapoints for each producer/application
    //
    
    String mc2 = "'" + WarpURLEncoder.encode(cell, StandardCharsets.UTF_8) + "' '" + WarpURLEncoder.encode(token, StandardCharsets.UTF_8) + "' @ops/throttling-ddp";

    
//    String mc2 = "" +
//        "'" + URLEncoder.encode(cell, StandardCharsets.UTF_8.name()) + "' 'cell' STORE\n" +
//        "'" + URLEncoder.encode(token, StandardCharsets.UTF_8.name()) + "' 'token' STORE\n" +
//        "//\n" + 
//        "// Retrieve raw datapoints ingestion metrics\n" + 
//        "//\n" + 
//        "\n" + 
//        "$token\n" +
//        //"'ZYcRABCkn+CLO/GUWmrvelLOnvAqO9PEmo8PQtOM1tJ5o'\n" + 
//        "'~warp.*datapoints.raw'\n" + 
//        "'cell' $cell 2 ->MAP\n" + 
//        "NOW DUP 86400000000 % -\n" + 
//        "86400000000 +\n" + 
//        "86400000000\n" + 
//        "5 ->LIST FETCH\n" + 
//        "false RESETS\n" + 
//        "'gts' STORE\n" + 
//        "\n" + 
//        "//\n" + 
//        "// Compute delta between first and last datapoints\n" + 
//        "//\n" + 
//        "[]\n" + 
//        "\n" + 
//        "0 $gts SIZE 1 -\n" + 
//        "<%\n" + 
//        "  $gts SWAP GET 'g' STORE $g\n" + 
//        "  // Sort ticks\n" + 
//        "  SORT\n" + 
//        "  // Extract values\n" + 
//        "  VALUES\n" + 
//        "  DUP 0 GET\n" + 
//        "  SWAP DUP SIZE 1 - GET\n" + 
//        "  SWAP - 'delta' STORE\n" + 
//        "  $delta 0 >\n" + 
//        "  <% %>\n" + 
//        "  <%\n" + 
//        "  NEWGTS\n" + 
//        "  0 NaN NaN NaN $delta ADDVALUE\n" + 
//        "  $g LABELS RELABEL\n" + 
//        "  $g ATTRIBUTES SETATTRIBUTES\n" + 
//        "  $g NAME RENAME\n" + 
//        "  +\n" + 
//        "  %>\n" + 
//        "  IFT\n" + 
//        "%>\n" + 
//        "FOR\n" + 
//        "\n" + 
//        "'gts' STORE\n" + 
//        "\n" + 
//        "//\n" + 
//        "// Sum by producer\n" + 
//        "//\n" + 
//        "\n" + 
//        "$gts\n" + 
//        "'producer' 'cell' 2 ->LIST\n" + 
//        "reducer.sum\n" + 
//        "3 ->LIST REDUCE\n" + 
//        "'byproducer' STORE\n" + 
//        "\n" + 
//        "//\n" + 
//        "// Build a String representation\n" + 
//        "//\n" + 
//        "\n" + 
//        "''\n" + 
//        "0 $byproducer SIZE 1 -\n" + 
//        "<%\n" + 
//        "  $byproducer SWAP GET DUP LABELS 'producer' GET SWAP VALUES 0 GET '%09' SWAP TOSTRING + + + '%0A' +\n" + 
//        "%>\n" + 
//        "FOR\n" + 
//        "\n" + 
//        "//\n" + 
//        "// Sum by application\n" + 
//        "//\n" + 
//        "\n" + 
//        "$gts\n" + 
//        "'app' 'cell' 2 ->LIST\n" + 
//        "reducer.sum\n" + 
//        "3 ->LIST REDUCE\n" + 
//        "'byapp' STORE\n" + 
//        "\n" + 
//        "//\n" + 
//        "// Complete the String representation\n" + 
//        "//\n" + 
//        "\n" + 
//        "0 $byapp SIZE 1 -\n" + 
//        "<%\n" + 
//        "  $byapp SWAP GET DUP LABELS 'app' GET DUP ISNULL <% %> <% DROP 'sensision' %> IFT '%2B' SWAP + SWAP VALUES 0 GET  '%09' SWAP TOSTRING + + + '%0A' +\n" + 
//        "%>\n" + 
//        "FOR\n" + 
//        "\n" + 
//        "";
    
    url = new URL(warpEndPoint + "/exec");
    
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    
    conn.setDoOutput(true);
    conn.setDoInput(true);
    conn.setChunkedStreamingMode(8192);
    conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    
    OutputStream out = conn.getOutputStream();
    
    out.write(mc2.getBytes(StandardCharsets.UTF_8));
    
    br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    
    StringBuilder sb = new StringBuilder();
    
    while(true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      sb.append(line);
      sb.append("\n");
    }
    
    br.close();
    
    Object json = JsonUtils.jsonToObject(sb.toString());
    
    String str = ((List) json).get(0).toString();
    
    br = new BufferedReader(new StringReader(str));
    
    while(true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      String[] tokens = line.split("\t");
      
      String key = tokens[0];
      double val = Double.parseDouble(tokens[1]);

      //
      // If threshold was passed, set rate at 1/3600 per second (1/hour, almost 0...)
      //
      
      if (DPS.containsKey(key) && val > DPS.get(key)) {
        DPS.put(key, 0.0);
      }
    }
       
    //
    // Fetch the estimators for the producers
    //
    
    // Map to keep track of cardinalities of the various estimators, we will only output
    // the fused estimator if one of the following situations is true:
    //
    // - The delta span of estimates is more than 10% of the max
    // - The maximum estimate is over 80% of the threshold
    //
    
    Map<String,Set<Long>> estimates = new HashMap<String,Set<Long>>();
    
    // We use /fetch so we know we can stream many values
    url = new URL(warpEndPoint + "/fetch?format=fulltext&token=" + WarpURLEncoder.encode(token, StandardCharsets.UTF_8) + "&selector=" + SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_ESTIMATOR + "{cell=" + WarpURLEncoder.encode(cell, StandardCharsets.UTF_8) + "}&now=0&timespan=-1");
    
    br = new BufferedReader(new InputStreamReader(url.openStream()));
    
    Set<String> expired = new HashSet<String>();
    
    while (true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      GTSEncoder encoder = GTSHelper.parse(null, line);
      
      String producer = encoder.getMetadata().getLabels().get(SensisionConstants.SENSISION_LABEL_PRODUCER);
      
      // Skip producer if there is no associated MADS limit
      if (!MADS.containsKey(producer)) {
        continue;
      }
      
      GTSDecoder decoder = encoder.getDecoder();

      if (decoder.next()) {        
        // We don't call getBinaryValue because we know the value was stored as OPB64
        HyperLogLogPlus estimator = HyperLogLogPlus.fromBytes(OrderPreservingBase64.decode(decoder.getValue().toString().getBytes(StandardCharsets.US_ASCII)));
        
        if (estimator.hasExpired()) {
          expired.add(producer);
          continue;
        }

        HyperLogLogPlus currentEstimator = HLLP.get(producer);
        
        if (null == currentEstimator) {
          estimates.put(producer, new HashSet<Long>());
          estimates.get(producer).add(estimator.cardinality());
          HLLP.put(producer, estimator);
          continue;
        }
        
        if (currentEstimator.getInitTime() < estimator.getInitTime()) {
          estimates.get(producer).add(estimator.cardinality());
          estimator.fuse(currentEstimator);
          HLLP.put(producer, estimator);
        } else {
          estimates.get(producer).add(estimator.cardinality());
          currentEstimator.fuse(estimator);
        }
      }
    }
    
    //
    // Output per producer ThrottlingConfiguration
    //
    
    for (Map.Entry<String, Long> keyAndMad: MADS.entrySet()) {
      String key = keyAndMad.getKey();
      Long mad = keyAndMad.getValue();
      // Skip per application detail for now
      if (key.charAt(0) == '+') {
        continue;
      }
      sb = new StringBuilder();
      sb.append(key);
      sb.append(":");
      sb.append(mad);
      sb.append(":");
      if (DPS.containsKey(key)) {
        double rate = DPS.get(key);
        if (rate > 0.0D) {
          sb.append(rate / ddpSpike);
        } else {
          sb.append(MINIMUM_RATE_LIMIT);
        }
      }
      sb.append(":");
      
      
      if (HLLP.containsKey(key)) {
        //
        // Determine if we should output the estimator 
        //
        
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        
        // Add cardinality of fused estimator
        estimates.get(key).add(HLLP.get(key).cardinality());
        
        for (long l: estimates.get(key)) {
          if (l < min) { min = l; }
          if (l > max) { max = l; }
        }
        
        if (max - min > 0.10 * max || max > 0.8 * mad || max * 1.25 < HLLP.get(key).cardinality()) {
          sb.append(new String(OrderPreservingBase64.encode(HLLP.get(key).toBytes()), StandardCharsets.US_ASCII));
        }
      } else {
        //
        // There is no estimator for the producer, this means they have all expired, so we issue '-' as the
        // estimator so they are reset at all ingress
        //
        if (expired.contains(key)) {
          sb.append("-");
        }
      }
      
      sb.append(":#");
      System.out.println(sb.toString());
    }
    
    // Clear HLLP
    
    //
    // Now retrieve per app estimators
    //
    
    estimates.clear();
    
    url = new URL(warpEndPoint + "/fetch?format=fulltext&token=" + WarpURLEncoder.encode(token, StandardCharsets.UTF_8) + "&selector=" + SensisionConstants.SENSISION_CLASS_CONTINUUM_GTS_ESTIMATOR_PER_APP + "{cell=" + WarpURLEncoder.encode(cell, StandardCharsets.UTF_8) + "}&now=0&timespan=-1");
    
    br = new BufferedReader(new InputStreamReader(url.openStream()));
    
    expired.clear();
    
    while (true) {
      String line = br.readLine();
    
      if (null == line) {
        break;
      }
      
      GTSEncoder encoder = GTSHelper.parse(null, line);
      
      String app = "+" + encoder.getMetadata().getLabels().get(SensisionConstants.SENSISION_LABEL_APPLICATION);
      
      // Skip producer if there is no associated MADS limit
      if (!MADS.containsKey(app)) {
        continue;
      }
      
      GTSDecoder decoder = encoder.getDecoder();

      if (decoder.next()) {
        // We don't call getBinaryValue because we know the value was stored as OPB64
        HyperLogLogPlus estimator = HyperLogLogPlus.fromBytes(OrderPreservingBase64.decode(decoder.getValue().toString().getBytes(StandardCharsets.US_ASCII)));
        
        if (estimator.hasExpired()) {
          expired.add(app);
          continue;
        }
        
        HyperLogLogPlus currentEstimator = HLLP.get(app);
        
        if (null == currentEstimator) {
          estimates.put(app, new HashSet<Long>());
          estimates.get(app).add(estimator.cardinality());
          HLLP.put(app, estimator);
          continue;
        }
        
        if (currentEstimator.getInitTime() < estimator.getInitTime()) {
          estimates.get(app).add(estimator.cardinality());
          estimator.fuse(currentEstimator);
          HLLP.put(app, estimator);
        } else {
          estimates.get(app).add(estimator.cardinality());
          currentEstimator.fuse(estimator);
        }
      }
    }

    br.close();

    for (Map.Entry<String, Long> keyAndMad: MADS.entrySet()) {
      String key = keyAndMad.getKey();
      Long mad = keyAndMad.getValue();
      if (key.charAt(0) != '+') {
        continue;
      }
      sb = new StringBuilder();
      sb.append(key);
      sb.append(":");
      sb.append(mad);
      sb.append(":");
      if (DPS.containsKey(key)) {
        double rate = DPS.get(key);
        
        if (rate > 0.0D) {
          sb.append(rate / ddpSpike);
        } else {
          sb.append(MINIMUM_RATE_LIMIT);
        }
      }
      sb.append(":");
      if (HLLP.containsKey(key)) {
        //
        // Determine if we should output the estimator 
        //
        
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        
        // Add cardinality of fused estimator
        estimates.get(key).add(HLLP.get(key).cardinality());
        
        for (long l: estimates.get(key)) {
          if (l < min) { min = l; }
          if (l > max) { max = l; }
        }
        
        if (max - min > 0.10 * max || max > 0.8 * mad || max * 1.25 < HLLP.get(key).cardinality()) {
          sb.append(new String(OrderPreservingBase64.encode(HLLP.get(key).toBytes()), StandardCharsets.US_ASCII));
        }
      } else {
        //
        // No estimator because they all have expired, remove them on 'ingress' side
        //
        if (expired.contains(key)) {
          sb.append("-");
        }
      }
      sb.append(":#");
      System.out.println(sb.toString());
    }
  }
  
  /**
   * ThrottlingConfigGenerator PARSE_URL WARP_ENDPOINT TOKEN CELL DDP_SPIKE
   * 
   * DDP_SPIKE indicates on which time interval we allow the DDP to be pushed. It is expressed
   * in seconds.
   * 
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (5 != args.length) {
      System.err.println(ThrottlingConfigGenerator.class.getCanonicalName() + " PARSE_URL WARP_ENDPOINT TOKEN CELL DDP_SPIKE");
    }
    genConfiguration(args[0], args[1], args[2], args[3], args[4]);
  }
}
