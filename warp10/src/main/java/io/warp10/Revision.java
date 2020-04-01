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
package io.warp10;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.output.ByteArrayOutputStream;

public class Revision {
  private static final String REVFILE = "REVISION";
  
  public static final String REVISION;
  
  static {
    String rev = "UNAVAILABLE";
    try {
      InputStream is = WarpConfig.class.getClassLoader().getResourceAsStream(REVFILE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[128];
      while(true) {
        int len = is.read(buf);
        if (len <= 0) {
          break;
        }
        baos.write(buf, 0, len);
      }
      is.close();
      rev = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    } catch (Exception e) {
    }
    
    REVISION = rev;
  }
}
