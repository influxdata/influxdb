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

package io.warp10.quasar.filter;

public class QuasarConfiguration {

  /**
   * root directory where trl files are stored.
   */
  public final static String WARP_TRL_PATH = "warp.trl.dir";

  /**
   * Scan period of the of the trl directory
   */
  public final static String WARP_TRL_PERIOD = "warp.trl.scan.period";

  /**
   * Default trl directory scan period
   */
  public final static String WARP_TRL_PERIOD_DEFAULT = "60000";

  /**
   * TRL Startup delay in ms, accept tokens during this delay even if TRL in not loaded
   * 0 for a TRL mandatory at the startup
   */
  public final static String WARP_TRL_STARTUP_DELAY ="warp.trl.startup.delay";

  /**
   * application prefix for TRL files
   */
  public final static String WARP_APPLICATION_PREFIX="+";
}
