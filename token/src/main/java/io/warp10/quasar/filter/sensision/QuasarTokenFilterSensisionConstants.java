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

package io.warp10.quasar.filter.sensision;

public class QuasarTokenFilterSensisionConstants {

  public static final String SENSISION_CLASS_QUASAR_FILTER_TOKEN_COUNT = "warp.quasar.filter.token.count";

  public static final String SENSISION_CLASS_QUASAR_FILTER_TOKEN_TIME_US = "warp.quasar.filter.token.time.us";

  /**
   * Heart beat +1 each time the TRL Thread is looking if a new trl is available
   */
  public static final String SENSISION_CLASS_QUASAR_FILTER_TRL_COUNT = "warp.quasar.filter.trl.thread.count";

  /**
   * Number of errors in the loading thread
   */
  public static final String SENSISION_CLASS_QUASAR_FILTER_TRL_ERROR_COUNT = "warp.quasar.filter.trl.thread.error.count";

  /**
   * Time taken for loading a new trl
   */
  public static final String SENSISION_CLASS_QUASAR_FILTER_TRL_LOAD_TIME = "warp.quasar.filter.trl.load.time";

  /**
   * number of tokens loaded in the trl
   */
  public static final String SENSISION_CLASS_QUASAR_FILTER_TRL_TOKENS_COUNT = "warp.quasar.filter.trl.tokens.count";

  /**
   * Token validated without trl available
   */
  public static final String SENSISION_CLASS_QUASAR_FILTER_TRL_UNAVAILABLE_COUNT = "warp.quasar.filter.trl.unavailable.count";

}
