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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.crypto.OrderPreservingBase64;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * Reads individual GTS readings, produce GTSEncoders
 */
public class Encode {
  public void encode(InputStream in, OutputStream out) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    
    GTSEncoder lastencoder = null;
    GTSEncoder encoder = null;
    
    StringBuilder sb = new StringBuilder();
    
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(out));
    
    while (true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      encoder = GTSHelper.parse(lastencoder, line);
      
      if (null != lastencoder && encoder != lastencoder) {
        sb.setLength(0);
        sb.append(lastencoder.getBaseTimestamp());
        sb.append("//");
        sb.append(lastencoder.getCount());
        sb.append(" ");
        GTSHelper.metadataToString(sb, lastencoder.getMetadata().getName(), lastencoder.getMetadata().getLabels(), true);
        sb.append(" ");
        sb.append(new String(OrderPreservingBase64.encode(lastencoder.getBytes()), StandardCharsets.US_ASCII));
        pw.println(sb.toString());
        lastencoder = encoder;
      } else if (null == lastencoder) {
        lastencoder = encoder;
      }
    }
    
    br.close();
    
    if (encoder.size() > 0) {
      sb.setLength(0);
      sb.append(encoder.getBaseTimestamp());
      sb.append("// ");
      GTSHelper.metadataToString(sb, encoder.getMetadata().getName(), encoder.getMetadata().getLabels(), true);
      sb.append(" ");
      sb.append(new String(OrderPreservingBase64.encode(encoder.getBytes()), StandardCharsets.US_ASCII));
      pw.println(sb.toString());      
    }        
    
    in.close();
    pw.flush();
    pw.close();
  }
  
  public static void main(String[] args) throws Exception {
    Encode encode = new Encode();
    encode.encode(System.in, System.out);
    //encode.encode(new FileInputStream("/var/tmp/debs.gts"), new FileOutputStream("/var/tmp/debs.encoders"));
  }
}
