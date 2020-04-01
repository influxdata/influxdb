//
//   Copyright 2019  SenX S.A.S.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.warp10.continuum.Configuration;

/**
 * Class used to control various aspects of the Warp 10 platform
 */
public class WarpManager {
  private static final String MANAGER_SECRET;
  public static final String UPDATE_DISABLED = "update.disabled";
  public static final String META_DISABLED = "meta.disabled";
  public static final String DELETE_DISABLED = "delete.disabled";
  
  private static final Map<String,Object> attributes = new HashMap<String,Object>();
    
  static {
    attributes.put(UPDATE_DISABLED, WarpConfig.getProperty(Configuration.WARP_UPDATE_DISABLED));
    attributes.put(META_DISABLED, WarpConfig.getProperty(Configuration.WARP_META_DISABLED));
    attributes.put(DELETE_DISABLED, WarpConfig.getProperty(Configuration.WARP_DELETE_DISABLED));
    
    MANAGER_SECRET = WarpConfig.getProperty(Configuration.WARP10_MANAGER_SECRET);
  }
  
  public static Object getAttribute(String attr) {
    return attributes.get(attr);
  }
  
  public static synchronized Object setAttribute(String attr, Object value) {
    return attributes.put(attr, value);
  }
  
  public static boolean checkSecret(String secret) {
    if (null == MANAGER_SECRET) {
      return false;
    } else {
      return MANAGER_SECRET.equals(secret);
    }
  }
}
