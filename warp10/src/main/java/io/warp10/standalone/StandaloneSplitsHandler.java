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

package io.warp10.standalone;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import io.warp10.continuum.Tokens;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.GTSSplit;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.PARSESELECTOR;

/**
 * This handler will generate splits from a selector and a token, those
 * splits will then be used by the InputFormat to retrieve data for MR job
 */
public class StandaloneSplitsHandler extends AbstractHandler {
  
  private final DirectoryClient directoryClient;
  
  private final byte[] fetcherKey;
  
  public StandaloneSplitsHandler(KeyStore keystore, DirectoryClient directoryClient) {
    this.directoryClient = directoryClient;
    this.fetcherKey = keystore.getKey(KeyStore.AES_FETCHER);
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (!target.equals(Constants.API_ENDPOINT_SPLITS)) {
      return;
    }
    
    baseRequest.setHandled(true);
    
    //
    // Extract parameters
    //
    
    String token = request.getParameter(Constants.HTTP_PARAM_TOKEN);
    String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);
    String now = request.getParameter(Constants.HTTP_PARAM_NOW);
    Long activeAfter = null == request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER) ? null : Long.parseLong(request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER));
    Long quietAfter = null == request.getParameter(Constants.HTTP_PARAM_QUIETAFTER) ? null : Long.parseLong(request.getParameter(Constants.HTTP_PARAM_QUIETAFTER));

    //
    // Validate token
    //
    
    ReadToken rtoken;

    if (null == token) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing token.");
      return;
    }
    
    try {
      rtoken = Tokens.extractReadToken(token);
      
      if (rtoken.getHooksSize() > 0) {
        throw new IOException("Tokens with hooks cannot be used for generating splits.");        
      }
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }

    //
    // Parse selector
    //
    
    Object[] elts = null;
    
    try {
      elts = PARSESELECTOR.parse(selector);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }

    //
    // Force app/owner/producer from token
    //
    
    String classSelector = elts[0].toString();
    Map<String,String> labelsSelector = (Map<String,String>) elts[1];
    
    labelsSelector.remove(Constants.PRODUCER_LABEL);
    labelsSelector.remove(Constants.OWNER_LABEL);
    labelsSelector.remove(Constants.APPLICATION_LABEL);
    
    labelsSelector.putAll(Tokens.labelSelectorsFromReadToken(rtoken));
    
    List<String> clsSels = new ArrayList<String>();
    List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
    
    clsSels.add(classSelector);
    lblsSels.add(labelsSelector);
    
    //
    // Determine the list of fetchers we can use
    //
    
    DirectoryRequest dr = new DirectoryRequest();
    dr.setClassSelectors(clsSels);
    dr.setLabelsSelectors(lblsSels);
    
    if (null != activeAfter) {
      dr.setActiveAfter(activeAfter);
    }
    if (null != quietAfter) {
      dr.setQuietAfter(quietAfter);
    }

    try (MetadataIterator metadatas = directoryClient.iterator(dr)) {
      
      //
      // We output a single split per Metadata, split combining is the
      // responsibility of the InputFormat
      // 128bits
      //
        
      PrintWriter pw = response.getWriter();
      
      while(metadatas.hasNext()) {
        Metadata metadata = metadatas.next();
        
        //
        // Build row of the GTS
        // 128bits
        //
        
        long classId = metadata.getClassId();
        long labelsId = metadata.getLabelsId();
        
        //
        // Build Split
        //
        
        GTSSplit split = new GTSSplit();
        
        split.setTimestamp(System.currentTimeMillis());
        split.setExpiry(rtoken.getExpiryTimestamp());
        split.addToMetadatas(metadata);
        
        //
        // Serialize and encrypt Split
        //
        
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] data = null;
        
        try {
          data = serializer.serialize(split);
        } catch (TException te) {
          throw new IOException(te);
        }

        if (null != fetcherKey) {
          data = CryptoUtils.wrap(fetcherKey, data);
        }
        
        pw.print("0.0.0.0");
        pw.print(" ");
        // 128 bits
        pw.print(Long.toHexString(classId));
        pw.print(" ");
        pw.println(new String(OrderPreservingBase64.encode(data), StandardCharsets.US_ASCII));
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
