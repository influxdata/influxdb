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

package io.warp10.script.ext.urlfetch;

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptStack;
import io.warp10.warp.sdk.WarpScriptExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Extension for URLFETCH and the associated function to change limits: MAXURLFETCHCOUNT and MAXURLFETCHSIZE
 */
public class UrlFetchWarpScriptExtension extends WarpScriptExtension {

  //
  // CONFIGURATION
  //

  /**
   * Maximum number of calls to URLFETCH in a session
   */
  public static final String WARPSCRIPT_URLFETCH_LIMIT = "warpscript.urlfetch.limit";
  public static final String WARPSCRIPT_URLFETCH_LIMIT_HARD = "warpscript.urlfetch.limit.hard";

  /**
   * Maximum cumulative size of content retrieved via calls to URLFETCH in a session
   */
  public static final String WARPSCRIPT_URLFETCH_MAXSIZE = "warpscript.urlfetch.maxsize";
  public static final String WARPSCRIPT_URLFETCH_MAXSIZE_HARD = "warpscript.urlfetch.maxsize.hard";

  /**
   * Allowed and excluded host patterns.
   */
  public static final String WARPSCRIPT_URLFETCH_HOST_PATTERNS = "warpscript.urlfetch.host.patterns";

  //
  // STACK
  //

  /**
   * Maximum number of calls to URLFETCH in a session
   */
  public static final String ATTRIBUTE_URLFETCH_LIMIT = "urlfetch.limit";
  public static final String ATTRIBUTE_URLFETCH_LIMIT_HARD = "urlfetch.limit.hard";

  /**
   * Number of calls to URLFETCH so far in the sessions
   */
  public static final String ATTRIBUTE_URLFETCH_COUNT = "urlfetch.count";

  /**
   * Maximum cumulative size of content retrieved via calls to URLFETCH in a session
   */
  public static final String ATTRIBUTE_URLFETCH_MAXSIZE = "urlfetch.maxsize";
  public static final String ATTRIBUTE_URLFETCH_MAXSIZE_HARD = "urlfetch.maxsize.hard";

  /**
   * Current  URLFETCH so far in the sessions
   */
  public static final String ATTRIBUTE_URLFETCH_SIZE = "urlfetch.size";

  //
  // DEFAULTS
  //

  public static final long DEFAULT_URLFETCH_LIMIT = 64L;
  public static final long DEFAULT_URLFETCH_MAXSIZE = 1000000L;

  //
  // ASSOCIATIONS attributes to either configuration or defaults
  //

  /**
   * Associates the attribute name to the configuration name
   */
  private static final Map<String, String> attributeToConf;

  /**
   * Associates the attribute name to its default value
   */
  private static final Map<String, Long> attributeToDefault;

  private static final Map<String, Object> functions;

  static {
    // Initialize attribute->configuration
    Map<String, String> a2c = new HashMap<String, String>();
    a2c.put(ATTRIBUTE_URLFETCH_LIMIT, WARPSCRIPT_URLFETCH_LIMIT);
    a2c.put(ATTRIBUTE_URLFETCH_MAXSIZE, WARPSCRIPT_URLFETCH_MAXSIZE);
    a2c.put(ATTRIBUTE_URLFETCH_LIMIT_HARD, WARPSCRIPT_URLFETCH_LIMIT_HARD);
    a2c.put(ATTRIBUTE_URLFETCH_MAXSIZE_HARD, WARPSCRIPT_URLFETCH_MAXSIZE_HARD);
    attributeToConf = Collections.unmodifiableMap(a2c);

    // Initialize attribute->default
    Map<String, Long> a2d = new HashMap<String, Long>();
    a2d.put(ATTRIBUTE_URLFETCH_LIMIT, DEFAULT_URLFETCH_LIMIT);
    a2d.put(ATTRIBUTE_URLFETCH_MAXSIZE, DEFAULT_URLFETCH_MAXSIZE);
    a2d.put(ATTRIBUTE_URLFETCH_LIMIT_HARD, DEFAULT_URLFETCH_LIMIT);
    a2d.put(ATTRIBUTE_URLFETCH_MAXSIZE_HARD, DEFAULT_URLFETCH_MAXSIZE);
    attributeToDefault = Collections.unmodifiableMap(a2d);

    // Create functions and map
    functions = new HashMap<String, Object>();

    functions.put("URLFETCH", new URLFETCH("URLFETCH"));
    functions.put("MAXURLFETCHCOUNT", new MAXURLFETCHCOUNT("MAXURLFETCHCOUNT"));
    functions.put("MAXURLFETCHSIZE", new MAXURLFETCHSIZE("MAXURLFETCHSIZE"));
  }

  public Map<String, Object> getFunctions() {
    return functions;
  }

  /**
   * Get the value of the attribute in the stack, if not present get the value in the configuration and it not present either, get the default value.
   * Also set the attribute in the stack once the value is found.
   *
   * @param stack     The stack the get the attribute from, if present.
   * @param attribute The attribute name to get.
   * @return The first available value in the list: attribute value, configuration value, default.
   */
  public static Long getLongAttribute(WarpScriptStack stack, String attribute) {
    // Get the value from stack attributes if available
    Object attributeValue = stack.getAttribute(attribute);

    if (null != attributeValue) {
      return (Long) attributeValue;
    }

    // Get the value from conf or default
    String associatedConf = attributeToConf.get(attribute);

    Long longValue = attributeToDefault.get(attribute);

    // Overwrite the default with the conf, if available
    String confValue = WarpConfig.getProperty(associatedConf);
    if (null != confValue) {
      longValue = Long.valueOf(confValue);
    }

    // The the stack attribute for future usage
    stack.setAttribute(attribute, longValue);

    return longValue;
  }
}
