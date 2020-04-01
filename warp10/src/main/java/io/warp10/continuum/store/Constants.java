//
//   Copyright 2018-2020  SenX S.A.S.
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

package io.warp10.continuum.store;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Constants {
  
  //
  //  A T T E N T I O N
  //  
  //  Once the time_units and modulus have been set, their values must not be modified.
  //  
  //  Doing so would render the storage system unusable
  //
  
  private static boolean timeUnitsAlreadySet = false;
  
  /**
   * Number of continuum time units per millisecond
   * 1000000 means we store nanoseconds
   * 1000 means we store microseconds
   * 1 means we store milliseconds
   * 0.001 means we store seconds (N/A since we use a long for the constant)
   */
  public static final long TIME_UNITS_PER_MS;
  
  /**
   * Number of time units per second
   */
  public static final long TIME_UNITS_PER_S;

  /**
   * Number of nanoseconds per time unit
   */
  public static final long NS_PER_TIME_UNIT;
  
  /**
   * Banner, based on Figlet
   * @see http://patorjk.com/software/taag/#p=display&f=Speed&t=Warp%2010
   */  
  public static final String WARP10_BANNER = "  ___       __                           ____________ \n" + 
      "  __ |     / /_____ _______________      __<  /_  __ \\\n" + 
      "  __ | /| / /_  __ `/_  ___/__  __ \\     __  /_  / / /\n" + 
      "  __ |/ |/ / / /_/ /_  /   __  /_/ /     _  / / /_/ / \n" + 
      "  ____/|__/  \\__,_/ /_/    _  .___/      /_/  \\____/  \n" + 
      "                           /_/                        \n"; 

  /**
   * Row key time boundary in time units
   */
  public static final long DEFAULT_MODULUS = 1L;
  
  /**
   * Number of elevation units per meter.
   */
  public static final long ELEVATION_UNITS_PER_M = 1000L;
  
  /**
   * Name of the 'producer' label
   */
  public static final String PRODUCER_LABEL = ".producer";
  
  /**
   * Name of the 'owner' label
   */
  public static final String OWNER_LABEL = ".owner";
  
  /**
   * Name of the 'uuid' attribute
   */
  public static final String UUID_ATTRIBUTE = ".uuid";
  
  /**
   * Name of the 'application' label
   */
  public static final String APPLICATION_LABEL = ".app";

  /**
   * Prefix used when Egress Fetch reports an error
   */
  public static final String EGRESS_FETCH_ERROR_PREFIX = "# ERROR: ";

  /**
   * Prefix used when Egress find reports an error
   */
  public static final String EGRESS_FIND_ERROR_PREFIX = "# ERROR: ";

  /**
   * Prefix used when Ingress Delete reports an error
   */
  public static final String INGRESS_DELETE_ERROR_PREFIX = "# ERROR: ";

  public static final String RUNNER_PERIODICITY = "runner.periodicity";
  public static final String RUNNER_PATH = "runner.path";
  public static final String RUNNER_SCHEDULEDAT = "runner.scheduledat";
  public static final String RUNNER_NONCE = "runner.nonce";
  
  private static final Map<String,String> HEADERS = new HashMap<String,String>();
  
  /**
   * Header containing the request UUID when calling the endpoint
   */
  public static final String HTTP_HEADER_WEBCALL_UUID_DEFAULT = "X-Warp10-WebCall";
    
  /**
   * HTTP Header for elapsed time of WarpScript scripts
   */  
  public static final String HTTP_HEADER_ELAPSED_DEFAULT = "X-Warp10-Elapsed";
  
  /**
   * HTTP Header for number of operations performed by a WarpScript
   */
  public static final String HTTP_HEADER_OPS_DEFAULT = "X-Warp10-Ops";
  
  /**
   * HTTP Header for number of datapoints fetched during a WarpScript execution
   */
  public static final String HTTP_HEADER_FETCHED_DEFAULT = "X-Warp10-Fetched";
  
  /**
   * Script line where an error was encountered
   */
  public static final String HTTP_HEADER_ERROR_LINE_DEFAULT = "X-Warp10-Error-Line";
  
  /**
   * Message for the error that was encountered
   */
  public static final String HTTP_HEADER_ERROR_MESSAGE_DEFAULT = "X-Warp10-Error-Message";
  
  /**
   * HTTP Header for access tokens
   */
  public static final String HTTP_HEADER_TOKEN_DEFAULT = "X-Warp10-Token";

  /**
   * HTTP Header to provide the token for outgoing META requests
   */
  public static final String HTTP_HEADER_META_TOKEN_DEFAULT = "X-Warp10-Token";

  /**
   * HTTP Header to provide the token for outgoing DELETE requests
   */
  public static final String HTTP_HEADER_DELETE_TOKEN_DEFAULT = "X-Warp10-Token";

  /**
   * HTTP Header to provide the token for outgoing UPDATE requests
   */
  public static final String HTTP_HEADER_UPDATE_TOKEN_DEFAULT = "X-Warp10-Token";

  /**
   * HTTP Header for setting the base timestamp for relative timestamps
   */
  public static final String HTTP_HEADER_NOW_HEADER_DEFAULT = "X-Warp10-Now";
  
  /**
   * HTTP Header for specifying the timespan for /sfetch requests
   */
  public static final String HTTP_HEADER_TIMESPAN_HEADER_DEFAULT = "X-Warp10-Timespan";
  
  /**
   * HTTP Header to specify if we should show errors in /sfetch responses
   */
  public static final String HTTP_HEADER_SHOW_ERRORS_HEADER_DEFAULT = "X-Warp10-ShowErrors";
  
  /**
   * Name of header containing the signature of the token used for the fetch
   */
  public static final String HTTP_HEADER_FETCH_SIGNATURE_DEFAULT = "X-Warp10-Fetch-Signature";

  /**
   * Name of header containing the signature of the token used for the update
   */
  public static final String HTTP_HEADER_UPDATE_SIGNATURE_DEFAULT = "X-Warp10-Update-Signature";
  
  /**
   * Name of header containing the signature of streaming directory requests
   */
  public static final String HTTP_HEADER_DIRECTORY_SIGNATURE_DEFAULT = "X-Warp10-Directory-Signature";

  /**
   * Name of header specifying the name of the symbol in which to expose the request headers
   */
  public static final String HTTP_HEADER_EXPOSE_HEADERS_DEFAULT = "X-Warp10-ExposeHeaders";
  
  /**
   * Name of header containing the wrapped Datalog request
   */
  public static final String HTTP_HEADER_DATALOG_DEFAULT = "X-Warp10-Datalog";
  
  /**
   * Header name for specifying attribute updates are delta
   */
  public static final String HTTP_HEADER_ATTRIBUTES_DEFAULT = "X-Warp10-Attributes";
  
  public static final String DATALOG_UPDATE = "UPDATE";
  public static final String DATALOG_META = "META";
  public static final String DATALOG_DELETE = "DELETE";
  
  /**
   * Empty column qualifier for HBase writes
   */
  public static final byte[] EMPTY_COLQ = new byte[0];
  
  /**
   * Endpoint for checks
   */
  public static final String API_ENDPOINT_CHECK = "/api/v0/check";
  
  /**
   * Endpoint for splits generation
   */
  public static final String API_ENDPOINT_SPLITS = "/api/v0/splits";
  
  /**
   * Endpoint for script submission
   */
  public static final String API_ENDPOINT_EXEC = "/api/v0/exec";
  
  /**
   * Update endpoint for the API
   */
  public static final String API_ENDPOINT_UPDATE = "/api/v0/update";
  
  /**
   * Find endpoint for the API
   */
  public static final String API_ENDPOINT_FIND = "/api/v0/find";
  
  /**
   * Fetch endpoint for the API
   */
  public static final String API_ENDPOINT_FETCH = "/api/v0/fetch";

  /**
   * Split fetch endpoint
   */
  public static final String API_ENDPOINT_SFETCH = "/api/v0/sfetch";
  
  /**
   * Archive Fetch endpoint for the API
   */
  public static final String API_ENDPOINT_AFETCH = "/api/v0/afetch";

  /**
   * Delete endpoint for the API
   */
  public static final String API_ENDPOINT_DELETE = "/api/v0/delete";
  
  /**
   * Plasma client endpoint for the API
   */
  public static final String API_ENDPOINT_PLASMA_CLIENT = "/api/v0/plasma/client";
  
  /**
   * Plasma server endpoint
   */
  public static final String API_ENDPOINT_PLASMA_SERVER = "/api/v0/plasma";

  /**
   * Plasma update endpoint
   */
  public static final String API_ENDPOINT_PLASMA_UPDATE = "/api/v0/streamupdate";
  
  /**
   * Mobius server endpoint
   */
  public static final String API_ENDPOINT_MOBIUS = "/api/v0/mobius";

  /**
   * Read Execute Loop endpoint
   */
  public static final String API_ENDPOINT_INTERACTIVE = "/api/v0/interactive";
  
  /**
   * Meta endpoint
   */
  public static final String API_ENDPOINT_META = "/api/v0/meta";
  
  /**
   * Geo root endpoint
   */
  public static final String API_ENDPOINT_GEO = "/api/v0/geo";
  
  /**
   * Geo endpoint subpath for 'list'
   */
  public static final String API_ENDPOINT_GEO_LIST = "/list";
  
  /**
   * Geo endpoint subpath for 'add'
   */
  public static final String API_ENDPOINT_GEO_ADD = "/add";
  
  /**
   * Geo endpoint subpath for 'remove'
   */
  public static final String API_ENDPOINT_GEO_REMOVE = "/remove";

  /**
   * Geo endpoint subpath for 'index'
   */
  public static final String API_ENDPOINT_GEO_INDEX = "/index";

  /**
   * Endpoint for internal directory streaming requests
   */
  public static final String API_ENDPOINT_DIRECTORY_STREAMING_INTERNAL = "/directory-streaming";
  
  /**
   * Endpoint for internal stats requests
   */
  public static final String API_ENDPOINT_DIRECTORY_STATS_INTERNAL = "/directory-stats";
  
  /**
   * Header to extract POP from OVH CDN
   */
  public static final String OVH_CDN_GEO_HEADER = "X-CDN-Geo";
  
  /**
   * Maximum size for internal encoders. MUST be less than Kafka's max message size.
   */
  public static final int MAX_ENCODER_SIZE;
  
  public static final String HTTP_PARAM_TOKEN = "token";
  public static final String HTTP_PARAM_SELECTOR = "selector";
  public static final String HTTP_PARAM_START = "start";
  public static final String HTTP_PARAM_STOP = "stop";
  public static final String HTTP_PARAM_NOW = "now";
  public static final String HTTP_PARAM_TIMESPAN = "timespan";
  public static final String HTTP_PARAM_DEDUP = "dedup";
  public static final String HTTP_PARAM_SHOW_ERRORS = "showerrors";
  public static final String HTTP_PARAM_FORMAT = "format";
  public static final String HTTP_PARAM_END = "end";
  public static final String HTTP_PARAM_DELETEALL = "deleteall";
  public static final String HTTP_PARAM_DRYRUN = "dryrun";
  public static final String HTTP_PARAM_MINAGE = "minage";
  public static final String HTTP_PARAM_SHOWUUID = "showuuid";
  public static final String HTTP_PARAM_SHOWATTR = "showattr";
  public static final String HTTP_PARAM_SORTMETA = "sortmeta";
  public static final String HTTP_PARAM_MINSPLITS = "minsplits";
  public static final String HTTP_PARAM_MAXSPLITS = "maxsplits";
  public static final String HTTP_PARAM_MAXSIZE = "maxsize";
  public static final String HTTP_PARAM_SUFFIX = "suffix";
  public static final String HTTP_PARAM_UNPACK = "unpack";
  public static final String HTTP_PARAM_CHUNKSIZE = "chunksize";
  public static final String HTTP_PARAM_ACTIVEAFTER = "activeafter";
  public static final String HTTP_PARAM_QUIETAFTER = "quietafter";
  public static final String HTTP_PARAM_LIMIT = "limit";
  public static final String HTTP_PARAM_COUNT = "count";
  public static final String HTTP_PARAM_SKIP = "skip";
  public static final String HTTP_PARAM_SAMPLE = "sample";
  public static final String HTTP_PARAM_PREBOUNDARY = "boundary.pre";
  public static final String HTTP_PARAM_POSTBOUNDARY = "boundary.post";
  public static final String HTTP_PARAM_METAONLY = "metaonly";

  public static final String DEFAULT_PACKED_CLASS_SUFFIX = ":packed";
  public static final int DEFAULT_PACKED_MAXSIZE = 65536;
  
  public static final String WARP10_DOC_URL = "http://www.warp10.io/";
  public static final String WARP10_FUNCTION_DOC_URL = "http://www.warp10.io/doc/";

  public static final int WARP_PLASMA_MAXSUBS_DEFAULT = 256000;
  
  public static final String KEY_MODULUS = "modulus";
  public static final String KEY_ALGORITHM = "algorithm";
  public static final String KEY_EXPONENT = "exponent";
  
  private static final int DEFAULT_MAX_ENCODER_SIZE = 100000;
  
  //
  // Token Attributes
  //
  
  /**
   * Attribute used to specify a WRITE token cannot be used for delete
   */
  public static final String TOKEN_ATTR_NODELETE = ".nodelete";
  public static final String TOKEN_ATTR_NOUPDATE = ".noupdate";
  public static final String TOKEN_ATTR_NOMETA = ".nometa";
  
  /**
   * Attribute to specify the maximum value size
   */
  public static final String TOKEN_ATTR_MAXSIZE = ".maxsize";

  /**
   * Timestamp limits for WRITE tokens (expressed in ms delta from current time)
   */
  public static final String TOKEN_ATTR_MAXFUTURE = ".maxfuture";
  public static final String TOKEN_ATTR_MAXPAST = ".maxpast";
  public static final String TOKEN_ATTR_IGNOOR = ".ignoor";
  
  /**
   * TTL for the written data (in ms)
   */
  public static final String TOKEN_ATTR_TTL = ".ttl";
  
  /**
   * Use the timestamp of the datapoints as the HBase cell timestamp.
   * Use of this attribute has no effect on a standalone version of Warp 10
   */
  public static final String TOKEN_ATTR_DPTS = ".dpts";
  
  /**
   * Attribute to specify that owner and producer should be exposed instead of hidden
   */
  public static final String TOKEN_ATTR_EXPOSE = ".expose";
  
  //
  // KafkaMessage Store attributes
  //
  
  public static final String STORE_ATTR_TTL = "ttl";
  public static final String STORE_ATTR_USEDATAPOINTTS = "dpts";

  /**
   * Limit to the size of errors message returned as the HTTP reason. In Jetty, this is limited to 1024 character.
   * See https://github.com/eclipse/jetty.project/blob/jetty-9.4.2.v20170220/jetty-http/src/main/java/org/eclipse/jetty/http/HttpGenerator.java#L624-L625
   */
  public static  final int MAX_HTTP_REASON_LENGTH = 1024;

  /**
   * Limit to the size of messages set in HTTP headers. In Jetty, the default limit is 8*1024 for all the headers.
   * See https://github.com/eclipse/jetty.project/blob/jetty-9.4.2.v20170220/jetty-server/src/main/java/org/eclipse/jetty/server/HttpConfiguration.java#L56
   * We set this limit to 1/8 of this value, expecting that this is highly unlikely that 8 values of this length will
   * be added to the headers.
   */
  public static  final int MAX_HTTP_HEADER_LENGTH = 1024;

  public static final boolean EXPOSE_OWNER_PRODUCER;
  
  /**
   * Does Directory support missing label selectors (using an empty STRING as exact match)
   */
  public static final boolean ABSENT_LABEL_SUPPORT;
  
  /**
   * Does the /delete endpoint allow the use of the 'nodata' parameter to only remove metadata
   */
  public static final boolean DELETE_METAONLY_SUPPORT;
  
  public static final boolean DELETE_ACTIVITY_SUPPORT;
  
  static {
    String tu = WarpConfig.getProperty(Configuration.WARP_TIME_UNITS);
  
    EXPOSE_OWNER_PRODUCER = "true".equals(WarpConfig.getProperty(Configuration.WARP10_EXPOSE_OWNER_PRODUCER));
    
    ABSENT_LABEL_SUPPORT = "true".equals(WarpConfig.getProperty(Configuration.WARP10_ABSENT_LABEL_SUPPORT));
    
    DELETE_METAONLY_SUPPORT = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_DELETE_METAONLY_SUPPORT));
    
    DELETE_ACTIVITY_SUPPORT = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_DELETE_ACTIVITY_SUPPORT));
    
    if (null == tu) {
      throw new RuntimeException("Missing time units.");
    } else if ("ms".equals(tu)) {
      TIME_UNITS_PER_MS = 1L;
    } else if ("us".equals(tu)) {
      TIME_UNITS_PER_MS = 1000L;
    } else if ("ns".equals(tu)) {
      TIME_UNITS_PER_MS = 1000000L;
    } else {
      throw new RuntimeException("Invalid time unit.");
    }

    TIME_UNITS_PER_S =  1000L * TIME_UNITS_PER_MS;
    NS_PER_TIME_UNIT = 1000000L / TIME_UNITS_PER_MS;
    //DEFAULT_MODULUS = 600L * TIME_UNITS_PER_S;

    MAX_ENCODER_SIZE = Integer.parseInt(WarpConfig.getProperty(Configuration.MAX_ENCODER_SIZE, Integer.toString(DEFAULT_MAX_ENCODER_SIZE)));
    
    if (null == System.getProperty(Configuration.WARP10_QUIET)) {
      System.out.println("########[ Initialized with " + TIME_UNITS_PER_MS + " time units per millisecond ]########");
    }
    
    //
    // Initialize headers
    //
    
    HEADERS.put(Configuration.HTTP_HEADER_WEBCALL_UUIDX, WarpConfig.getProperty(Configuration.HTTP_HEADER_WEBCALL_UUIDX, HTTP_HEADER_WEBCALL_UUID_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_ELAPSEDX, WarpConfig.getProperty(Configuration.HTTP_HEADER_ELAPSEDX, HTTP_HEADER_ELAPSED_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_OPSX, WarpConfig.getProperty(Configuration.HTTP_HEADER_OPSX, HTTP_HEADER_OPS_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_FETCHEDX, WarpConfig.getProperty(Configuration.HTTP_HEADER_FETCHEDX, HTTP_HEADER_FETCHED_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_ERROR_LINEX, WarpConfig.getProperty(Configuration.HTTP_HEADER_ERROR_LINEX, HTTP_HEADER_ERROR_LINE_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_ERROR_MESSAGEX, WarpConfig.getProperty(Configuration.HTTP_HEADER_ERROR_MESSAGEX, HTTP_HEADER_ERROR_MESSAGE_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_TOKENX, WarpConfig.getProperty(Configuration.HTTP_HEADER_TOKENX, HTTP_HEADER_TOKEN_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_META_TOKENX, WarpConfig.getProperty(Configuration.HTTP_HEADER_META_TOKENX, HTTP_HEADER_META_TOKEN_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_DELETE_TOKENX, WarpConfig.getProperty(Configuration.HTTP_HEADER_DELETE_TOKENX, HTTP_HEADER_DELETE_TOKEN_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_UPDATE_TOKENX, WarpConfig.getProperty(Configuration.HTTP_HEADER_UPDATE_TOKENX, HTTP_HEADER_UPDATE_TOKEN_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_NOW_HEADERX, WarpConfig.getProperty(Configuration.HTTP_HEADER_NOW_HEADERX, HTTP_HEADER_NOW_HEADER_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_TIMESPAN_HEADERX, WarpConfig.getProperty(Configuration.HTTP_HEADER_TIMESPAN_HEADERX, HTTP_HEADER_TIMESPAN_HEADER_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_SHOW_ERRORS_HEADERX, WarpConfig.getProperty(Configuration.HTTP_HEADER_SHOW_ERRORS_HEADERX, HTTP_HEADER_SHOW_ERRORS_HEADER_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_FETCH_SIGNATURE, WarpConfig.getProperty(Configuration.HTTP_HEADER_FETCH_SIGNATURE, HTTP_HEADER_FETCH_SIGNATURE_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_UPDATE_SIGNATURE, WarpConfig.getProperty(Configuration.HTTP_HEADER_UPDATE_SIGNATURE, HTTP_HEADER_UPDATE_SIGNATURE_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_DIRECTORY_SIGNATURE, WarpConfig.getProperty(Configuration.HTTP_HEADER_DIRECTORY_SIGNATURE, HTTP_HEADER_DIRECTORY_SIGNATURE_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_EXPOSE_HEADERS, WarpConfig.getProperty(Configuration.HTTP_HEADER_EXPOSE_HEADERS, HTTP_HEADER_EXPOSE_HEADERS_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_DATALOG, WarpConfig.getProperty(Configuration.HTTP_HEADER_DATALOG, HTTP_HEADER_DATALOG_DEFAULT));
    HEADERS.put(Configuration.HTTP_HEADER_ATTRIBUTES, WarpConfig.getProperty(Configuration.HTTP_HEADER_ATTRIBUTES, HTTP_HEADER_ATTRIBUTES_DEFAULT));    
  }
  
  public static String getHeader(String name) {
    return HEADERS.get(name);
  }
  
  public static boolean hasReservedHeader(Map<String,String> headers) {
    Set<String> hdrs = new HashSet<String>();
    
    for (String definedHeader: headers.keySet()) {
      hdrs.add(definedHeader.toLowerCase());
    }
    
    for (String key: HEADERS.keySet()) {
      if (hdrs.contains(key.toLowerCase())) {
        return true;
      }
    }
    
    return false;
  }

  /**
   * row key prefix for metadata
   */
  public static final byte[] HBASE_METADATA_KEY_PREFIX = "M".getBytes(StandardCharsets.UTF_8);

  /**
   * Prefix for 'raw' (individual datapoints) data
   */
  public static final byte[] HBASE_RAW_DATA_KEY_PREFIX = "R".getBytes(StandardCharsets.UTF_8);
}
