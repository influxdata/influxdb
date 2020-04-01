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

import io.warp10.continuum.store.Constants;
import io.warp10.crypto.SipHashInline;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.geoxp.oss.jarjar.org.bouncycastle.util.encoders.Hex;
public class Fetch {
  public static void main(String[] args) throws Exception {
    URL url = new URL(args[0]);
    
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    
    conn.setDoOutput(false);
    conn.setDoInput(true);
    
    String psk = System.getProperty(Configuration.CONFIG_FETCH_PSK);

    if (null != psk) {
      String token = System.getProperty("fetch.token");
      long now = System.currentTimeMillis();
      StringBuilder sb = new StringBuilder(Long.toHexString(now));
      sb.append(":");
      byte[] fetchKey = Hex.decode(psk);
      long hash = SipHashInline.hash24(fetchKey, (Long.toString(now) + ":" + token).getBytes(StandardCharsets.ISO_8859_1));
      sb.append(Long.toHexString(hash));
      conn.setRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_FETCH_SIGNATURE), sb.toString());      
    }
    
    InputStream in = conn.getInputStream();
    
    byte[] buf = new byte[8192];
    
    while(true) {
      int len = in.read(buf);
      
      if (len <= 0) {
        break;
      }
      
      System.out.write(buf, 0, len);
    }
    
    System.out.flush();
    conn.disconnect();
  }
}
