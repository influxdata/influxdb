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

import io.warp10.continuum.store.thrift.data.DirectoryFindRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsRequest;
import io.warp10.crypto.SipHashInline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.google.common.primitives.Longs;

public class DirectoryUtil {
  /**
   * Compute the hash of a DirectoryFindRequest for the provided SipHash key
   * 
   * @param k0 first half of SipHash key
   * @param k1 second half of SipHash key
   * @param request DirectoryFindRequest to hash
   * @return
   */
  public static long computeHash(long k0, long k1, DirectoryFindRequest request) {
    return computeHash(k0, k1, request.getTimestamp(), request.getClassSelector(), request.getLabelsSelectors());
  }

  public static long computeHash(long k0, long k1, DirectoryStatsRequest request) {
    return computeHash(k0, k1, request.getTimestamp(), request.getClassSelector(), request.getLabelsSelectors());
  }

  private static long computeHash(long k0, long k1, long timestamp, List<String> classSelectors, List<Map<String,String>> labelsSelectors) {
    //
    // Create a ByteArrayOutputStream into which the content will be dumped
    //
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
    // Add timestamp
    
    try {
      baos.write(Longs.toByteArray(timestamp));
      
      if (null != classSelectors) {
        for (String classSelector: classSelectors) {
          baos.write(classSelector.getBytes(StandardCharsets.UTF_8));
        }
      }
      
      if (null != labelsSelectors) {
        for (Map<String, String> map: labelsSelectors) {
          TreeMap<String,String> tm = new TreeMap<String, String>();
          tm.putAll(map);
          for (Entry<String,String> entry: tm.entrySet()) {
            baos.write(entry.getKey().getBytes(StandardCharsets.UTF_8));
            baos.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
          }
        }
      }      
    } catch (IOException ioe) {
      return 0L;
    }
    
    // Compute hash
    
    byte[] data = baos.toByteArray();
    
    long hash = SipHashInline.hash24(k0, k1, data, 0, data.length);
    
    return hash;    
  }
}
