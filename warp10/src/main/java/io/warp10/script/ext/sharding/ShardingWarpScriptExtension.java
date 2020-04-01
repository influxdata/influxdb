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

package io.warp10.script.ext.sharding;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class ShardingWarpScriptExtension extends WarpScriptExtension {
  /**
   * Size of thread pool to use for issuing requests to shards
   */
  public static final String SHARDING_POOLSIZE = "sharding.poolsize";
  
  /**
   * Maximum number of threads a single call to DEVAL may use
   */
  public static final String SHARDING_MAXTHREADSPERCALL = "sharding.maxthreadspercall";
  
  /**
   * Prefix for shard endpoint configuration. Overall key name is sharding.endpoint.NAME.MODULUS:REMAINDER
   */
  public static final String SHARDING_ENDPOINT_PREFIX = "sharding.endpoint.";
  
  /**
   * Snapshot command to use when talking to shards, defaults to SNAPSHOT
   */
  public static final String SHARDING_SNAPSHOT = "sharding.snapshot";
    
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String,Object>();
    
    functions.put("DEVAL", new DEVAL("DEVAL"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
