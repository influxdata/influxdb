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

import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OSSKeyStore;
import io.warp10.crypto.OrderPreservingBase64;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.bouncycastle.util.encoders.Hex;

public class LogDumper {
  
  public static void main(String[] args) throws Exception {
    
    boolean dump = false;
    boolean decrypt = false;
    
    if ("dump".equals(args[0])) {
      dump = true;
    } else if ("decrypt".equals(args[0])) {
      decrypt = true;
    }
    
    KeyStore ks = new OSSKeyStore(System.getProperty("oss.master"));
    
    byte[] key = ks.decodeKey(args[1]);
    
    BufferedReader br = new BufferedReader(new FileReader(args[2]));
    
    TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
    
    while(true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      LoggingEvent event = LogUtil.unwrapLog(key, line);
      
      if (null == event) {
        continue;
      }
      
      if (dump) {
        System.out.println(event);
      } else if (decrypt) {
        byte[] serialized = ser.serialize(event);
        
        OrderPreservingBase64.encodeToStream(serialized, System.out);
        System.out.println();
      }
    }
    
    br.close();
  }
}
