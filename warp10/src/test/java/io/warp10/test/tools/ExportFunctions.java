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

package io.warp10.test.tools;


import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.warp10.json.JsonUtils;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.ext.concurrent.ConcurrentWarpScriptExtension;
import io.warp10.script.ext.sensision.SensisionWarpScriptExtension;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Export the FULL functions list slitted into these categories
 * Function
 * operator (comparaison / arithemtic / logical )
 * Constants
 * frameworks (bucketize / map / reduce / apply )
 * stack structures <% %> <S S> [] {}
 */
public class ExportFunctions {

  private final static String OP_ARITMETIC = "operator.artimetic";
  private final static String OP_LOGICAL = "operator.logical";
  private final static String OP_COMPARAISON = "operator.comparison";
  private final static String OP_BITWISE = "operator.bitwise";

  private final static String FCT_MATH = "functions.math";
  private final static String FCT_TIMEUNIT = "functions.timeunit";
  private final static String FCT_TRIGO = "functions.trigonometry";
  private final static String FCT_COUNTER = "functions.counter";
  private final static String FCT_COMPRESSION = "functions.compression";
  private final static String FCT_DATE = "functions.date";
  private final static String FCT_UDF = "functions.udf";
  private final static String FCT_UTIL = "functions.util";
  private final static String FCT_STRING = "functions.string";
  private final static String FCT_LIST = "functions.list";
  private final static String FCT_STACK = "functions.stack";
  private final static String FCT_GTS = "functions.gts";
  private final static String FCT_LOGIC_STRUCTURE = "functions.logicStructures";
  private final static String FCT_PLATFORM = "functions.platform";
  private final static String FCT_OUTLIER = "functions.outlier";
  private final static String FCT_BUCKETIZED  = "functions.bucketized";
  private final static String FCT_GEO  = "functions.geo";
  private final static String FCT_TYPE_CONVERSION  = "functions.typeConversion";
  private final static String FCT_CRYPTO = "functions.crypto";
  private final static String FCT_BITSET = "functions.bitset";
  private final static String FCT_QUATERNION= "functions.quaternion";
  private final static String FCT_PROCESSING = "functions.processing";
  private final static String FCT_MISC  = "functions.misc";
  private final static String FCT_BETA  = "functions.beta";

  private final static String FCT_EXT_CONCURRENT  = "functions.ext.concurrent";
  private final static String FCT_EXT_SENSISION  = "functions.ext.sensision";

  private final static String SINGLE_VALUE_MAPPER = "mapper.singleValue";
  private final static String SLIDING_WINDOW_MAPPER = "mapper.slidingWindow";
  private final static String GEO_MAPPER = "mapper.geo";
  private final static String CUSTOM_MAPPER = "mapper.custom";

  private final static String DEFAULT_REDUCER = "reducer.default";
  private final static String CUSTOM_REDUCER = "reducer.custom";

  private final static String DEFAULT_FILTER = "filter.default";
  private final static String CUSTOM_FILTER = "filter.custom";

  private final static String DEFAULT_BUCKETIZER = "bucketizer.default";
  private final static String CUSTOM_BUCKETIZER = "bucketizer.custom";

  private final static String DEFAULT_OP = "op.default";

  // WARP SCRIPT FRAMEWORKS
  private final static String FMK_MAP = "MAP";
  private final static String FMK_REDUCE = "REDUCE";
  private final static String FMK_FILTER = "FILTER";
  private final static String FMK_APPLY = "APPLY";
  private final static String FMK_BUCKETIZE = "BUCKETIZE";

  public static void main(String[] args) throws Exception {
    List<String> functionsFullList = new ArrayList<>();
    try {
      // THIS is a very bad thing, used only for extraction purpose

      // Extract WarpScript functions
      Field warpScriptField = WarpScriptLib.class.getDeclaredField("functions");
      warpScriptField.setAccessible(true);
      Map<String,Object> functions  = (Map<String,Object>) warpScriptField.get(null);

      // Extract Sensision WarpScript ext functions
      Field sensisionWarpScriptField = SensisionWarpScriptExtension.class.getDeclaredField("functions");
      sensisionWarpScriptField.setAccessible(true);
      functions.putAll((Map<String,Object>) sensisionWarpScriptField.get(null));

      Field concurrentWarpScriptField = ConcurrentWarpScriptExtension.class.getDeclaredField("functions");
      concurrentWarpScriptField.setAccessible(true);
      functions.putAll((Map<String,Object>) concurrentWarpScriptField.get(null));


      functionsFullList.addAll(functions.keySet());
    } catch (Exception exp) {
      exp.printStackTrace();
      throw exp;
    }

    // -------------------------------------------------------------
    // output functions by category
    // -------------------------------------------------------------
    Map<String, Map<String,List<String>>> frameworksFunctions = new HashMap<>();
    frameworksFunctions.put(FMK_MAP, getFrameworkStructure(SINGLE_VALUE_MAPPER, SLIDING_WINDOW_MAPPER, GEO_MAPPER, CUSTOM_MAPPER));
    frameworksFunctions.put(FMK_REDUCE, getFrameworkStructure(DEFAULT_REDUCER, CUSTOM_REDUCER) );
    frameworksFunctions.put(FMK_APPLY, getFrameworkStructure(DEFAULT_OP));
    frameworksFunctions.put(FMK_FILTER, getFrameworkStructure(DEFAULT_FILTER, CUSTOM_FILTER));
    frameworksFunctions.put(FMK_BUCKETIZE, getFrameworkStructure(DEFAULT_BUCKETIZER, CUSTOM_BUCKETIZER));

    Map<String, List<String>> operators = new HashMap<>();
    operators.put(OP_ARITMETIC, new ArrayList<String>());
    operators.put(OP_LOGICAL, new ArrayList<String>());
    operators.put(OP_COMPARAISON, new ArrayList<String>());
    operators.put(OP_BITWISE, new ArrayList<String>());

    Map<String, List<String>> functions = new HashMap<>();
    functions.put(FCT_MATH, new ArrayList<String>());
    functions.put(FCT_TIMEUNIT, new ArrayList<String>());
    functions.put(FCT_TRIGO, new ArrayList<String>());
    functions.put(FCT_DATE, new ArrayList<String>());
    functions.put(FCT_STRING, new ArrayList<String>());
    functions.put(FCT_LIST, new ArrayList<String>());
    functions.put(FCT_STACK, new ArrayList<String>());
    functions.put(FCT_LOGIC_STRUCTURE, new ArrayList<String>());
    functions.put(FCT_PLATFORM, new ArrayList<String>());
    functions.put(FCT_GTS, new ArrayList<String>());
    functions.put(FCT_OUTLIER, new ArrayList<String>());
    functions.put(FCT_BUCKETIZED, new ArrayList<String>());
    functions.put(FCT_GEO, new ArrayList<String>());
    functions.put(FCT_TYPE_CONVERSION, new ArrayList<String>());
    functions.put(FCT_MISC, new ArrayList<String>());
    functions.put(FCT_BETA, new ArrayList<String>());
    functions.put(FCT_UTIL, new ArrayList<String>());
    functions.put(FCT_CRYPTO, new ArrayList<String>());
    functions.put(FCT_COMPRESSION, new ArrayList<String>());
    functions.put(FCT_COUNTER, new ArrayList<String>());
    functions.put(FCT_UDF, new ArrayList<String>());
    functions.put(FCT_BITSET, new ArrayList<String>());
    functions.put(FCT_QUATERNION, new ArrayList<String>());

    functions.put(FCT_EXT_CONCURRENT, new ArrayList<String>());
    functions.put(FCT_EXT_SENSISION, new ArrayList<String>());
    functions.put(FCT_PROCESSING, new ArrayList<String>());

    List<String> constants = Lists.newArrayList("true","false");

    // -------------------------------------------------------------
    // patterns
    // -------------------------------------------------------------
    Pattern mapPattern = Pattern.compile("mapper\\..*");
    Pattern reducePattern = Pattern.compile("reducer\\..*");
    Pattern bucketizePattern = Pattern.compile("bucketizer\\..*");
    Pattern applyPattern = Pattern.compile("op\\..*");
    Pattern filterPattern = Pattern.compile("filter\\..*");
    Pattern processingPattern = Pattern.compile("P[Ga-z].*");


    // -------------------------------------------------------------
    // STATIC CATEGORISATION
    // -------------------------------------------------------------
    List<String> frameworks = Lists.newArrayList(FMK_MAP, FMK_REDUCE, FMK_BUCKETIZE, FMK_APPLY, FMK_FILTER);
    List<String> structures = Lists.newArrayList("[", "]", "[]", "{", "}", "{}", "<%", "%>", "<S", "S>", "<'", "'>");

    // -------------------------------------------------------------
    // OPERATORS
    // -------------------------------------------------------------
    Map<String, List<String>> staticOperators = new HashMap<>();
    staticOperators.put(OP_ARITMETIC, Lists.newArrayList("+", "-", "*", "**", "/", "%"));
    staticOperators.put(OP_LOGICAL, Lists.newArrayList("&&", "||", "!", "AND", "OR", "NOT"));
    staticOperators.put(OP_COMPARAISON, Lists.newArrayList("==", "~=", "!=", "<=", ">=", "<", ">"));
    staticOperators.put(OP_BITWISE, Lists.newArrayList("<<", ">>", ">>>", "&", "|", "^", "~"));

    // -------------------------------------------------------------
    // CONSTS
    // -------------------------------------------------------------
    List<String> staticConstants = Lists.newArrayList("E", "e","MAXLONG","MINLONG","NaN","NULL","PI","pi", "max.time.sliding.window", "max.tick.sliding.window");

    // -------------------------------------------------------------
    // FRAMEWORK MAP
    // -------------------------------------------------------------
    List<String> singleValueMapper = Lists.newArrayList("mapper.abs","mapper.add","mapper.mul","mapper.ceil","mapper.floor","mapper.round","mapper.toboolean","mapper.todouble","mapper.tolong","mapper.tostring","mapper.tick","mapper.yearmapper.month","mapper.day","mapper.weekday","mapper.hour","mapper.minute","mapper.second","mapper.exp","mapper.log","mapper.pow","mapper.tanh","mapper.sigmoid");
    List<String> geoMapper = Lists.newArrayList("mapper.geo.within","mapper.geo.outside");
    List<String> customMapper = Lists.newArrayList("MACROMAPPER","STRICTMAPPER");

    // -------------------------------------------------------------
    // FRAMEWORK REDUCE
    // -------------------------------------------------------------
    List<String> customReducer = Lists.newArrayList("MACROREDUCER");

    // -------------------------------------------------------------
    // FRAMEWORK BUCKETIZE
    // -------------------------------------------------------------
    List<String> customBucketizer = Lists.newArrayList("MACROBUCKETIZER");

    // -------------------------------------------------------------
    // FRAMEWORK FILTER
    // -------------------------------------------------------------
    List<String> customFilter = Lists.newArrayList("MACROFILTER");


    // FUNCTIONS
    Map<String, List<String>> staticFunctions = new HashMap<>();
    staticFunctions.put(FCT_MATH, Lists.newArrayList("+!","RANDPDF","->DOUBLEBITS","->FLOATBITS","DOUBLEBITS->","FLOATBITS->","ABS","CBRT","CEIL","COPYSIGN","EXP","FLOOR","IEEEREMAINDER","LBOUNDS","LOG","LOG10","MAX","MIN","NBOUNDS","NEXTAFTER","NEXTUP","NPDF","PROBABILITY","RAND","REVBITS","RINT","ROUND","SIGNUM","SQRT"));
    staticFunctions.put(FCT_TIMEUNIT, Lists.newArrayList("w","d","h","m","s", "ms","us", "ns", "ps"));
    staticFunctions.put(FCT_TRIGO, Lists.newArrayList("COS","COSH","ACOS","SIN","SINH","ASIN","TAN","TANH","ATAN","TODEGREES","TORADIANS"));
    staticFunctions.put(FCT_DATE, Lists.newArrayList("ADDDAYS","ADDMONTHS","ADDYEARS","AGO","DURATION","HUMANDURATION","ISO8601","ISODURATION","MSTU","NOTAFTER", "NOTBEFORE", "NOW","STU","TSELEMENTS","->TSELEMENTS","TSELEMENTS->"));
    staticFunctions.put(FCT_STRING, Lists.newArrayList("BIN->","B64TOHEX","B64->","B64URL->","BINTOHEX","BYTES->","FROMBIN","FROMBITS","FROMHEX","HASH","HEX->","HEXTOB64","HEXTOBIN","JOIN","MATCH","MATCHER","OPB64->","OPB64TOHEX","SPLIT","SUBSTRING","REPLACE","REPLACEALL","TEMPLATE","->B64URL","->B64","->BIN","->BYTES","->HEX","->OPB64","->V","->VEC","TOBIN","TOBITS","TOHEX","TOLOWER","TOUPPER","TRIM","URLDECODE","URLENCODE","UUID","V->","VEC->"));
    staticFunctions.put(FCT_LIST, Lists.newArrayList("->LIST","->MAP","->MAT","->SET","APPEND","CLONEREVERSE","CONTAINSKEY","CONTAINS","CONTAINSVALUE","DIFFERENCE","FLATTEN","GET","INTERSECTION","KEYLIST","LFLATMAP","LIST->","LMAP","LSORT","MAP->","MAT->","MAPID","MSORT","PACK","PUT","REMOVE","REVERSE","SET","SET->","SIZE","SUBLIST","SUBMAP","UNION","UNIQUE","UNLIST","UNMAP","UNPACK","VALUELIST","ZIP"));
    staticFunctions.put(FCT_LOGIC_STRUCTURE, Lists.newArrayList("ISNaN","ISNULL","ASSERT","BREAK","CEVAL","CONTINUE","DEFINED","DEFINEDMACRO","EVAL","FAIL","FOREACH","FOR","FORSTEP","IFTE","IFT","MSGFAIL","NRETURN","RETURN","STOP","SWITCH","SYNC","UNTIL","WHILE"));
    staticFunctions.put(FCT_PLATFORM, Lists.newArrayList("EVALSECURE","HEADER","IDENT","JSONLOOSE","JSONSTRICT","LIMIT","MAXBUCKETS","MAXDEPTH","MAXGTS","MAXLOOP","MAXOPS","MAXSYMBOLS","NOOP","OPS","RESET","RESTORE","REV","RTFM","SAVE","SECUREKEY","TOKENINFO","UNSECURE","URLFETCH","WEBCALL"));
    staticFunctions.put(FCT_GTS, Lists.newArrayList("REXEC","LOCATIONOFFSET","TLTTB","PROB","PARSE","LTTB","LOCSTRINGS","ADDVALUE","ATINDEX","ATTICK","ATTRIBUTES","BBOX","CHUNK","CLIP","CLONE","CLONEEMPTY","COMMONTICKS","COMPACT","COPYGEO","CORRELATE","CPROB","DEDUP","DELETE","DISCORDS","DTW","DWTSPLIT","ELEVATIONS","FDWT","FETCH","FETCHBOOLEAN","FETCHDOUBLE","FETCHLONG","FETCHSTRING","FFT","FFTAP","FILLTICKS","FIND","FINDSTATS","FIRSTTICK","IDWT","IFFT","INTEGRATE","ISONORMALIZE","LABELS","LASTSORT","LASTTICK","LOCATIONS","LOWESS","MAKEGTS","MERGE","META","METASORT","MODE","MONOTONIC","MUSIGMA","NAME","NEWGTS","NONEMPTY","NORMALIZE","NSUMSUMSQ","OPTDTW","PATTERNDETECTION","PATTERNS","PARSESELECTOR","PARTITION","QUANTIZE","RANGECOMPACT","RELABEL","RENAME","RESETS","RLOWESS","RSORT","RVALUESORT","SETATTRIBUTES","SETVALUE","SHRINK","SINGLEEXPONENTIALSMOOTHING","SORT","SORTBY","STANDARDIZE","TICKINDEX","TICKLIST","TICKS","TIMECLIP","TIMEMODULO","TIMESCALE","TIMESHIFT","TIMESPLIT","TOSELECTOR","UNWRAP","UPDATE","VALUEDEDUP","VALUEHISTOGRAM","VALUES","VALUESORT","VALUESPLIT","WRAP","WRAPRAW","ZDISCORDS","ZPATTERNDETECTION", "ZPATTERNS","ZSCORE"));
    staticFunctions.put(FCT_OUTLIER, Lists.newArrayList("THRESHOLDTEST","ZSCORETEST","GRUBBSTEST","ESDTEST","STLESDTEST","HYBRIDTEST","HYBRIDTEST2"));
    staticFunctions.put(FCT_BUCKETIZED, Lists.newArrayList("ATBUCKET","BUCKETCOUNT","BUCKETSPAN","CROP","FILLNEXT","FILLPREVIOUS","FILLVALUE","INTERPOLATE","LASTBUCKET","STL","UNBUCKETIZE"));
    staticFunctions.put(FCT_GEO, Lists.newArrayList("->GEOHASH","->HHCODE","GEO.DIFFERENCE","GEO.INTERSECTION","GEO.INTERSECTS","GEO.REGEXP","GEO.UNION","GEO.WITHIN","GEO.WKT","GEOHASH->","GEOPACK","GEOUNPACK","HAVERSINE","HHCODE->"));
    staticFunctions.put(FCT_TYPE_CONVERSION, Lists.newArrayList("->PICKLE","PICKLE->","->JSON","JSON->","TOBOOLEAN","TODOUBLE","TOLONG","TOSTRING","TOTIMESTAMP"));
    staticFunctions.put(FCT_STACK, Lists.newArrayList("AUTHENTICATE","BOOTSTRAP","COUNTTOMARK","CLEAR","CLEARDEFS","CLEARSYMBOLS","CLEARTOMARK","CSTORE","DEF","DEPTH","DEBUGON","DEBUGOFF","DOC","DOCMODE","DROP","DROPN","DUP","DUPN","EXPORT","ELAPSED","TIMINGS","NOTIMINGS","FORGET","LOAD","MARK","NDEBUGON","PICK","ROLL","ROLLD","ROT","RUN","SNAPSHOT","SNAPSHOTALL","SNAPSHOTALLTOMARK","SNAPSHOTTOMARK","STACKATTRIBUTE","STACKTOLIST","STORE","SWAP","SYMBOLS","TYPEOF"));
    staticFunctions.put(FCT_UDF, Lists.newArrayList("CALL","CUDF","UDF"));
    staticFunctions.put(FCT_COUNTER, Lists.newArrayList("RANGE","COUNTER", "COUNTERDELTA", "COUNTERVALUE"));
    staticFunctions.put(FCT_COMPRESSION, Lists.newArrayList("->Z","Z->","GZIP","UNGZIP"));
    staticFunctions.put(FCT_CRYPTO, Lists.newArrayList("AESWRAP","AESUNWRAP","MD5","SHA1","SHA256","SHA1HMAC","SHA256HMAC","RSAGEN","RSAPRIVATE","RSAPUBLIC","RSADECRYPT","RSAENCRYPT","RSASIGN","RSAVERIFY"));
    staticFunctions.put(FCT_BITSET, Lists.newArrayList("BITCOUNT","BITGET","BITSTOBYTES","BYTESTOBITS"));
    staticFunctions.put(FCT_QUATERNION, Lists.newArrayList("->Q","Q->","QCONJUGATE","QDIVIDE","QMULTIPLY","QROTATE","QROTATION","ROTATIONQ"));
    staticFunctions.put(FCT_EXT_CONCURRENT, Lists.newArrayList("CEVAL","SYNC"));
    staticFunctions.put(FCT_BETA, Lists.newArrayList("LORAENC","LORAMIC","GETHOOK"));
    staticFunctions.put(FCT_EXT_SENSISION, Lists.newArrayList("SENSISION.EVENT","SENSISION.GET","SENSISION.SET","SENSISION.UPDATE"));


    List<String> bannedFunctions = Lists.newArrayList("TICKSHIFT");

    // Sort all functions list
    for (String function: functionsFullList) {
      // exclude frameworks  and structures
      if (frameworks.contains(function) || structures.contains(function) || bannedFunctions.contains(function)) {
        continue;
      }


      // -----------------------------------------------
      // test frameworks patterns & processing
      // -------------------------------------

      // Processing
      if (processingPattern.matcher(function).matches()) {
        functions.get(FCT_PROCESSING).add(function);
        continue;
      }

      // MAP
      if (mapPattern.matcher(function).matches()) {
        if (singleValueMapper.contains(function)) {
          frameworksFunctions.get(FMK_MAP).get(SINGLE_VALUE_MAPPER).add(function);
        } else if (geoMapper.contains(function)) {
          frameworksFunctions.get(FMK_MAP).get(GEO_MAPPER).add(function);
        } else {
          frameworksFunctions.get(FMK_MAP).get(SLIDING_WINDOW_MAPPER).add(function);
        }

        continue;
      }

      // CUSTOM MAPEPR
      if (customMapper.contains(function)) {
        frameworksFunctions.get(FMK_MAP).get(CUSTOM_MAPPER).add(function);
        continue;
      }

      // REDUCE
      if (reducePattern.matcher(function).matches()) {
        frameworksFunctions.get(FMK_REDUCE).get(DEFAULT_REDUCER).add(function);
        continue;
      }

      // CUSTOM REDUCER
      if (customReducer.contains(function)) {
        frameworksFunctions.get(FMK_REDUCE).get(CUSTOM_REDUCER).add(function);
        continue;
      }

      // BUCKETIZE
      if (bucketizePattern.matcher(function).matches()) {
        frameworksFunctions.get(FMK_BUCKETIZE).get(DEFAULT_BUCKETIZER).add(function);
        continue;
      }

      // CUSTOM BUCKETIZE
      if (customBucketizer.contains(function)) {
        frameworksFunctions.get(FMK_BUCKETIZE).get(CUSTOM_BUCKETIZER).add(function);
        continue;
      }

      // FILTER
      if (filterPattern.matcher(function).matches()) {
        frameworksFunctions.get(FMK_FILTER).get(DEFAULT_FILTER).add(function);
        continue;
      }

      // CUSTOM FILTER
      if (customFilter.contains(function)) {
        frameworksFunctions.get(FMK_FILTER).get(CUSTOM_FILTER).add(function);
        continue;
      }

      // APPLY
      if (applyPattern.matcher(function).matches()) {
        frameworksFunctions.get(FMK_APPLY).get(DEFAULT_OP).add(function);
        continue;
      }

      boolean functionMatch = false;

      // ---------------------------------------------
      // Values with static categorisation (constants + macros)
      // ---------------------------------------------
      if (staticConstants.contains(function)) {
        constants.add(function);
        continue;
      }


      // ---------------------------------------------
      // Values with static categorisation (operators)
      // ---------------------------------------------
      for (Map.Entry<String, List<String>> entry : staticOperators.entrySet()) {
        // match
        if (entry.getValue().contains(function)) {
          // add if to the key
          operators.get(entry.getKey()).add(function);
          functionMatch = true;
          break;
        }
      }

      if (functionMatch) {
        continue;
      }

      // ---------------------------------------------
      // Values with static categorisation (functions)
      // ---------------------------------------------
      for (Map.Entry<String, List<String>> entry : staticFunctions.entrySet()) {
        // match
        if (entry.getValue().contains(function)) {
          // add if to the key
          functions.get(entry.getKey()).add(function);
          functionMatch = true;
          break;
        }
      }

      if (functionMatch) {
        continue;
      }

      // -------------------------------------------------------------
      // Uncategorized function. Add it to function.misc
      // -------------------------------------------------------------
      functions.get(FCT_MISC).add(function);
    }

    // -------------------------------------------------------------
    // -- SORT functions
    // -------------------------------------------------------------
    for (Map.Entry<String, List<String>> entry : functions.entrySet()) {
      List<String> sortedFunctions = Ordering.natural().sortedCopy(entry.getValue());
      functions.put(entry.getKey(), sortedFunctions);
    }


    // -------------------------------------------------------------
    // build output json object
    // -------------------------------------------------------------
    Map<String, Object> output = new HashMap<>();

    output.put("frameworks", frameworksFunctions);
    output.put("operators", operators);
    output.put("functions", functions);
    output.put("constants", constants);
    output.put("structures", structures);

    System.out.println(JsonUtils.objectToJson(output, true));

    // -------------------------------
    // ---- COUNT TOTAL FOUNCTIONS ---
    // -------------------------------
    int totalFunctions = 0;
    int miscFunctions = 0;

    for (Map.Entry<String, Map<String,List<String>>> entry : frameworksFunctions.entrySet()) {
      for(Map.Entry<String,List<String>> fmkEntry: entry.getValue().entrySet() ) {
        totalFunctions += fmkEntry.getValue().size();
      }
    }

    for(Map.Entry<String,List<String>> operatorsEntry: operators.entrySet() ) {
      totalFunctions += operatorsEntry.getValue().size();
    }
    for(Map.Entry<String,List<String>> functionsEntry: functions.entrySet() ) {
      if (functionsEntry.getKey().equals(FCT_MISC)) {
        miscFunctions = functionsEntry.getValue().size();
      } else {
        totalFunctions += functionsEntry.getValue().size();
      }
    }

    totalFunctions += constants.size();
    totalFunctions += structures.size();

    System.out.println("documented functions = " + totalFunctions);
    System.out.println("undocumented function = " + miscFunctions);

  }

  private static Map<String, List<String>> getFrameworkStructure(String... categories) {
    Map<String, List<String>> fmk = new HashMap<>();

    for (String category : categories) {
      fmk.put(category, new ArrayList<String>());
    }

    return fmk;
  }
}



