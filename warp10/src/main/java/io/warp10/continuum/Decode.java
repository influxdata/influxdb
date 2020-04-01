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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.crypto.OrderPreservingBase64;

/**
 * Reads GTSEncoders, outputs GTS values
 */
public class Decode {
  public void decode(InputStream in, OutputStream out) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    
    StringBuilder sb = new StringBuilder();
    
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(out));
    
    while (true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }

      sb.setLength(0);
      sb.append(line);
      
      // Add single quotes around value if it does not contain any
      
      int lastwsp = sb.lastIndexOf(" ");
      
      sb.insert(lastwsp + 1, "'");
      sb.append("'");
      
      GTSEncoder encoder = GTSHelper.parse(null, sb.toString());
      
      GTSDecoder decoder = encoder.getDecoder(true);
      
      while(decoder.next()) {
        long ts = decoder.getTimestamp();
        Object value = decoder.getBinaryValue();
        
        byte[] bytes;
        
        if (value instanceof byte[]) {
          bytes = (byte[]) value;
        } else {
          bytes = OrderPreservingBase64.decode(value.toString().getBytes(StandardCharsets.UTF_8));
        }
        decoder = new GTSDecoder(ts, ByteBuffer.wrap(bytes));
        decoder.setMetadata(encoder.getMetadata());

        decoder.dump(pw);
        
        break;
      }      
    }
    
    br.close();
    
    in.close();
    pw.flush();
    pw.close();
  }
  
  public static void main(String[] args) throws Exception {
    Decode decode = new Decode();
    decode.decode(System.in, System.out);
  }
}
