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

package io.warp10.warp.sdk;

import java.util.BitSet;
import java.util.Map;

public class WarpScriptJavaFunctionGTS {
  public String gtsClass;
  public Map<String, String> gtsLabels;
  public Map<String, String> gtsAttributes;
  
  public boolean bucketized;
  public long bucketspan;
  public int bucketcount;    
  public long lastbucket;
  
  public long[] ticks;
  
  public float[] latitudes;
  public float[] longitudes;  
  public long[] elevations;

  public long[] longValues;
  public double[] doubleValues;
  public String[] stringValues;
  public BitSet booleanValues;
}