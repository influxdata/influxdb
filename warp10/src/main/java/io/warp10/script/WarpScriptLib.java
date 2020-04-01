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

package io.warp10.script;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.WarpClassLoader;
import io.warp10.WarpConfig;
import io.warp10.WarpManager;
import io.warp10.WarpURLDecoder;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.CORRELATE;
import io.warp10.continuum.gts.DISCORDS;
import io.warp10.continuum.gts.FFT;
import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.gts.IFFT;
import io.warp10.continuum.gts.INTERPOLATE;
import io.warp10.continuum.gts.LOCATIONOFFSET;
import io.warp10.continuum.gts.ZIP;
import io.warp10.script.aggregator.And;
import io.warp10.script.aggregator.Argminmax;
import io.warp10.script.aggregator.CircularMean;
import io.warp10.script.aggregator.CompareTo;
import io.warp10.script.aggregator.Count;
import io.warp10.script.aggregator.Delta;
import io.warp10.script.aggregator.First;
import io.warp10.script.aggregator.HDist;
import io.warp10.script.aggregator.HSpeed;
import io.warp10.script.aggregator.Highest;
import io.warp10.script.aggregator.Join;
import io.warp10.script.aggregator.Last;
import io.warp10.script.aggregator.Lowest;
import io.warp10.script.aggregator.MAD;
import io.warp10.script.aggregator.Max;
import io.warp10.script.aggregator.Mean;
import io.warp10.script.aggregator.Median;
import io.warp10.script.aggregator.Min;
import io.warp10.script.aggregator.Or;
import io.warp10.script.aggregator.Percentile;
import io.warp10.script.aggregator.RMS;
import io.warp10.script.aggregator.Rate;
import io.warp10.script.aggregator.ShannonEntropy;
import io.warp10.script.aggregator.StandardDeviation;
import io.warp10.script.aggregator.Sum;
import io.warp10.script.aggregator.TrueCourse;
import io.warp10.script.aggregator.VDist;
import io.warp10.script.aggregator.VSpeed;
import io.warp10.script.aggregator.Variance;
import io.warp10.script.binary.ADD;
import io.warp10.script.binary.BitwiseAND;
import io.warp10.script.binary.BitwiseOR;
import io.warp10.script.binary.BitwiseXOR;
import io.warp10.script.binary.CondAND;
import io.warp10.script.binary.CondOR;
import io.warp10.script.binary.DIV;
import io.warp10.script.binary.EQ;
import io.warp10.script.binary.GE;
import io.warp10.script.binary.GT;
import io.warp10.script.binary.INPLACEADD;
import io.warp10.script.binary.LE;
import io.warp10.script.binary.LT;
import io.warp10.script.binary.MOD;
import io.warp10.script.binary.MUL;
import io.warp10.script.binary.NE;
import io.warp10.script.binary.POW;
import io.warp10.script.binary.SHIFTLEFT;
import io.warp10.script.binary.SHIFTRIGHT;
import io.warp10.script.binary.SUB;
import io.warp10.script.filler.FillerInterpolate;
import io.warp10.script.filler.FillerNext;
import io.warp10.script.filler.FillerPrevious;
import io.warp10.script.filler.FillerTrend;
import io.warp10.script.filter.FilterByClass;
import io.warp10.script.filter.FilterByLabels;
import io.warp10.script.filter.FilterByMetadata;
import io.warp10.script.filter.FilterBySelector;
import io.warp10.script.filter.FilterLastEQ;
import io.warp10.script.filter.FilterLastGE;
import io.warp10.script.filter.FilterLastGT;
import io.warp10.script.filter.FilterLastLE;
import io.warp10.script.filter.FilterLastLT;
import io.warp10.script.filter.FilterLastNE;
import io.warp10.script.filter.FilterAny;
import io.warp10.script.filter.FilterBySize;
import io.warp10.script.filter.LatencyFilter;
import io.warp10.script.functions.*;
import io.warp10.script.functions.math.ACOS;
import io.warp10.script.functions.math.ADDEXACT;
import io.warp10.script.functions.math.ASIN;
import io.warp10.script.functions.math.ATAN;
import io.warp10.script.functions.math.ATAN2;
import io.warp10.script.functions.math.CBRT;
import io.warp10.script.functions.math.CEIL;
import io.warp10.script.functions.math.COPYSIGN;
import io.warp10.script.functions.math.COS;
import io.warp10.script.functions.math.COSH;
import io.warp10.script.functions.math.DECREMENTEXACT;
import io.warp10.script.functions.math.EXP;
import io.warp10.script.functions.math.EXPM1;
import io.warp10.script.functions.math.FLOOR;
import io.warp10.script.functions.math.FLOORDIV;
import io.warp10.script.functions.math.FLOORMOD;
import io.warp10.script.functions.math.GETEXPONENT;
import io.warp10.script.functions.math.HYPOT;
import io.warp10.script.functions.math.IEEEREMAINDER;
import io.warp10.script.functions.math.INCREMENTEXACT;
import io.warp10.script.functions.math.LOG;
import io.warp10.script.functions.math.LOG10;
import io.warp10.script.functions.math.LOG1P;
import io.warp10.script.functions.math.MAX;
import io.warp10.script.functions.math.MIN;
import io.warp10.script.functions.math.MULTIPLYEXACT;
import io.warp10.script.functions.math.NEGATEEXACT;
import io.warp10.script.functions.math.NEXTAFTER;
import io.warp10.script.functions.math.NEXTDOWN;
import io.warp10.script.functions.math.NEXTUP;
import io.warp10.script.functions.math.RANDOM;
import io.warp10.script.functions.math.RINT;
import io.warp10.script.functions.math.ROUND;
import io.warp10.script.functions.math.SCALB;
import io.warp10.script.functions.math.SIGNUM;
import io.warp10.script.functions.math.SIN;
import io.warp10.script.functions.math.SINH;
import io.warp10.script.functions.math.SQRT;
import io.warp10.script.functions.math.SUBTRACTEXACT;
import io.warp10.script.functions.math.TAN;
import io.warp10.script.functions.math.TANH;
import io.warp10.script.functions.math.TODEGREES;
import io.warp10.script.functions.math.TOINTEXACT;
import io.warp10.script.functions.math.TORADIANS;
import io.warp10.script.functions.math.ULP;
import io.warp10.script.functions.shape.CHECKSHAPE;
import io.warp10.script.functions.shape.HULLSHAPE;
import io.warp10.script.functions.shape.PERMUTE;
import io.warp10.script.functions.shape.RESHAPE;
import io.warp10.script.functions.shape.SHAPE;
import io.warp10.script.mapper.MapperAbs;
import io.warp10.script.mapper.MapperAdd;
import io.warp10.script.mapper.MapperCeil;
import io.warp10.script.mapper.MapperCompareTo;
import io.warp10.script.mapper.MapperDayOfMonth;
import io.warp10.script.mapper.MapperDayOfWeek;
import io.warp10.script.mapper.MapperDotProduct;
import io.warp10.script.mapper.MapperDotProductPositive;
import io.warp10.script.mapper.MapperDotProductSigmoid;
import io.warp10.script.mapper.MapperDotProductTanh;
import io.warp10.script.mapper.MapperExp;
import io.warp10.script.mapper.MapperFinite;
import io.warp10.script.mapper.MapperFloor;
import io.warp10.script.mapper.MapperGeoApproximate;
import io.warp10.script.mapper.MapperGeoClearPosition;
import io.warp10.script.mapper.MapperGeoOutside;
import io.warp10.script.mapper.MapperGeoWithin;
import io.warp10.script.mapper.MapperHourOfDay;
import io.warp10.script.mapper.MapperKernelCosine;
import io.warp10.script.mapper.MapperKernelEpanechnikov;
import io.warp10.script.mapper.MapperKernelGaussian;
import io.warp10.script.mapper.MapperKernelLogistic;
import io.warp10.script.mapper.MapperKernelQuartic;
import io.warp10.script.mapper.MapperKernelSilverman;
import io.warp10.script.mapper.MapperKernelTriangular;
import io.warp10.script.mapper.MapperKernelTricube;
import io.warp10.script.mapper.MapperKernelTriweight;
import io.warp10.script.mapper.MapperKernelUniform;
import io.warp10.script.mapper.MapperLog;
import io.warp10.script.mapper.MapperMaxX;
import io.warp10.script.mapper.MapperMinX;
import io.warp10.script.mapper.MapperMinuteOfHour;
import io.warp10.script.mapper.MapperMod;
import io.warp10.script.mapper.MapperMonthOfYear;
import io.warp10.script.mapper.MapperMul;
import io.warp10.script.mapper.MapperNPDF;
import io.warp10.script.mapper.MapperParseDouble;
import io.warp10.script.mapper.MapperPow;
import io.warp10.script.mapper.MapperProduct;
import io.warp10.script.mapper.MapperReplace;
import io.warp10.script.mapper.MapperRound;
import io.warp10.script.mapper.MapperSecondOfMinute;
import io.warp10.script.mapper.MapperSigmoid;
import io.warp10.script.mapper.MapperTanh;
import io.warp10.script.mapper.MapperTick;
import io.warp10.script.mapper.MapperToBoolean;
import io.warp10.script.mapper.MapperToDouble;
import io.warp10.script.mapper.MapperToLong;
import io.warp10.script.mapper.MapperToString;
import io.warp10.script.mapper.MapperYear;
import io.warp10.script.mapper.STRICTMAPPER;
import io.warp10.script.op.OpAND;
import io.warp10.script.op.OpAdd;
import io.warp10.script.op.OpDiv;
import io.warp10.script.op.OpEQ;
import io.warp10.script.op.OpGE;
import io.warp10.script.op.OpGT;
import io.warp10.script.op.OpLE;
import io.warp10.script.op.OpLT;
import io.warp10.script.op.OpMask;
import io.warp10.script.op.OpMul;
import io.warp10.script.op.OpNE;
import io.warp10.script.op.OpOR;
import io.warp10.script.op.OpSub;
import io.warp10.script.processing.Pencode;
import io.warp10.script.processing.color.Palpha;
import io.warp10.script.processing.color.Pbackground;
import io.warp10.script.processing.color.Pblue;
import io.warp10.script.processing.color.Pbrightness;
import io.warp10.script.processing.color.Pclear;
import io.warp10.script.processing.color.Pcolor;
import io.warp10.script.processing.color.PcolorMode;
import io.warp10.script.processing.color.Pfill;
import io.warp10.script.processing.color.Pgreen;
import io.warp10.script.processing.color.Phue;
import io.warp10.script.processing.color.PlerpColor;
import io.warp10.script.processing.color.PnoFill;
import io.warp10.script.processing.color.PnoStroke;
import io.warp10.script.processing.color.Pred;
import io.warp10.script.processing.color.Psaturation;
import io.warp10.script.processing.color.Pstroke;
import io.warp10.script.processing.image.Pblend;
import io.warp10.script.processing.image.Pcopy;
import io.warp10.script.processing.image.Pdecode;
import io.warp10.script.processing.image.Pfilter;
import io.warp10.script.processing.image.Pget;
import io.warp10.script.processing.image.Pimage;
import io.warp10.script.processing.image.PimageMode;
import io.warp10.script.processing.image.PnoTint;
import io.warp10.script.processing.image.Ppixels;
import io.warp10.script.processing.image.Pset;
import io.warp10.script.processing.image.Ptint;
import io.warp10.script.processing.image.PtoImage;
import io.warp10.script.processing.image.PupdatePixels;
import io.warp10.script.processing.math.Pconstrain;
import io.warp10.script.processing.math.Pdist;
import io.warp10.script.processing.math.Plerp;
import io.warp10.script.processing.math.Pmag;
import io.warp10.script.processing.math.Pmap;
import io.warp10.script.processing.math.Pnorm;
import io.warp10.script.processing.rendering.PGraphics;
import io.warp10.script.processing.rendering.PblendMode;
import io.warp10.script.processing.rendering.Pclip;
import io.warp10.script.processing.rendering.PnoClip;
import io.warp10.script.processing.shape.Parc;
import io.warp10.script.processing.shape.PbeginContour;
import io.warp10.script.processing.shape.PbeginShape;
import io.warp10.script.processing.shape.Pbezier;
import io.warp10.script.processing.shape.PbezierDetail;
import io.warp10.script.processing.shape.PbezierPoint;
import io.warp10.script.processing.shape.PbezierTangent;
import io.warp10.script.processing.shape.PbezierVertex;
import io.warp10.script.processing.shape.Pbox;
import io.warp10.script.processing.shape.Pcurve;
import io.warp10.script.processing.shape.PcurveDetail;
import io.warp10.script.processing.shape.PcurvePoint;
import io.warp10.script.processing.shape.PcurveTangent;
import io.warp10.script.processing.shape.PcurveTightness;
import io.warp10.script.processing.shape.PcurveVertex;
import io.warp10.script.processing.shape.Pellipse;
import io.warp10.script.processing.shape.PellipseMode;
import io.warp10.script.processing.shape.PendContour;
import io.warp10.script.processing.shape.PendShape;
import io.warp10.script.processing.shape.Pline;
import io.warp10.script.processing.shape.PloadShape;
import io.warp10.script.processing.shape.Ppoint;
import io.warp10.script.processing.shape.Pquad;
import io.warp10.script.processing.shape.PquadraticVertex;
import io.warp10.script.processing.shape.Prect;
import io.warp10.script.processing.shape.PrectMode;
import io.warp10.script.processing.shape.Pshape;
import io.warp10.script.processing.shape.PshapeMode;
import io.warp10.script.processing.shape.Psphere;
import io.warp10.script.processing.shape.PsphereDetail;
import io.warp10.script.processing.shape.PstrokeCap;
import io.warp10.script.processing.shape.PstrokeJoin;
import io.warp10.script.processing.shape.PstrokeWeight;
import io.warp10.script.processing.shape.Ptriangle;
import io.warp10.script.processing.shape.Pvertex;
import io.warp10.script.processing.structure.PpopStyle;
import io.warp10.script.processing.structure.PpushStyle;
import io.warp10.script.processing.transform.PpopMatrix;
import io.warp10.script.processing.transform.PpushMatrix;
import io.warp10.script.processing.transform.PresetMatrix;
import io.warp10.script.processing.transform.Protate;
import io.warp10.script.processing.transform.ProtateX;
import io.warp10.script.processing.transform.ProtateY;
import io.warp10.script.processing.transform.ProtateZ;
import io.warp10.script.processing.transform.Pscale;
import io.warp10.script.processing.transform.PshearX;
import io.warp10.script.processing.transform.PshearY;
import io.warp10.script.processing.transform.Ptranslate;
import io.warp10.script.processing.typography.PcreateFont;
import io.warp10.script.processing.typography.Ptext;
import io.warp10.script.processing.typography.PtextAlign;
import io.warp10.script.processing.typography.PtextAscent;
import io.warp10.script.processing.typography.PtextDescent;
import io.warp10.script.processing.typography.PtextFont;
import io.warp10.script.processing.typography.PtextLeading;
import io.warp10.script.processing.typography.PtextMode;
import io.warp10.script.processing.typography.PtextSize;
import io.warp10.script.processing.typography.PtextWidth;
import io.warp10.script.unary.ABS;
import io.warp10.script.unary.COMPLEMENT;
import io.warp10.script.unary.FROMBIN;
import io.warp10.script.unary.FROMBITS;
import io.warp10.script.unary.FROMHEX;
import io.warp10.script.unary.NOT;
import io.warp10.script.unary.REVERSEBITS;
import io.warp10.script.unary.TOBIN;
import io.warp10.script.unary.TOBITS;
import io.warp10.script.unary.TOBOOLEAN;
import io.warp10.script.unary.TODOUBLE;
import io.warp10.script.unary.TOHEX;
import io.warp10.script.unary.TOLONG;
import io.warp10.script.unary.TOSTRING;
import io.warp10.script.unary.TOTIMESTAMP;
import io.warp10.script.unary.UNIT;
import io.warp10.warp.sdk.WarpScriptExtension;

/**
 * Library of functions used to manipulate Geo Time Series
 * and more generally interact with a WarpScriptStack
 */
public class WarpScriptLib {
  
  private static final Logger LOG = LoggerFactory.getLogger(WarpScriptLib.class);
  
  private static Map<String,Object> functions = new HashMap<String, Object>();
  
  private static Set<String> extloaded = new LinkedHashSet<String>();
  
  /**
   * Static definition of name so it can be reused outside of WarpScriptLib
   */
  
  public static final String NULL = "NULL";

  public static final String COUNTER = "COUNTER";
  public static final String COUNTERSET = "COUNTERSET";
  
  
  public static final String REF = "REF";
  public static final String COMPILE = "COMPILE";
  public static final String SAFECOMPILE = "SAFECOMPILE";
  public static final String COMPILED = "COMPILED";
  
  public static final String EVAL = "EVAL";
  public static final String EVALSECURE = "EVALSECURE";
  public static final String SNAPSHOT = "SNAPSHOT";
  public static final String SNAPSHOTALL = "SNAPSHOTALL";
  public static final String DEREF = "DEREF";
  public static final String LOAD = "LOAD";
  public static final String POPR = "POPR";
  public static final String CPOPR = "CPOPR";
  public static final String PUSHR = "PUSHR";
  public static final String CLEARREGS = "CLEARREGS";
  public static final String RUN = "RUN";
  public static final String BOOTSTRAP = "BOOTSTRAP";
  public static final String NOOP = "NOOP";
  public static final String JSONTO = "JSON->";
  
  public static final String MAP_START = "{";
  public static final String MAP_END = "}";

  public static final String LIST_START = "[";
  public static final String LIST_END = "]";

  public static final String SET_START = "(";
  public static final String SET_END = ")";
  
  public static final String VECTOR_START = "[[";
  public static final String VECTOR_END = "]]";
  
  public static final String TO_VECTOR = "->V";
  public static final String TO_SET = "->SET";
  
  public static final String NEWGTS = "NEWGTS";
  public static final String SWAP = "SWAP";
  public static final String RELABEL = "RELABEL";
  public static final String RENAME = "RENAME";
  public static final String PARSESELECTOR = "PARSESELECTOR";
  
  public static final String GEO_WKT = "GEO.WKT";
  public static final String GEO_WKT_UNIFORM = "GEO.WKT.UNIFORM";
  public static final String GEO_WKB = "GEO.WKB";
  public static final String GEO_WKB_UNIFORM = "GEO.WKB.UNIFORM";
  
  public static final String TOGEOJSON = "->GEOJSON";
  public static final String GEO_JSON = "GEO.JSON";
  public static final String GEO_JSON_UNIFORM = "GEO.JSON.UNIFORM";
  public static final String GEO_INTERSECTION = "GEO.INTERSECTION";
  public static final String GEO_DIFFERENCE = "GEO.DIFFERENCE";
  public static final String GEO_UNION = "GEO.UNION";
  public static final String GEOPACK = "GEOPACK";
  public static final String GEOUNPACK = "GEOUNPACK";
  
  public static final String SECTION = "SECTION";
  public static final String UNWRAP = "UNWRAP";
  public static final String UNWRAPENCODER = "UNWRAPENCODER";
  public static final String OPB64TO = "OPB64->";
  public static final String TOOPB64 = "->OPB64";
  public static final String BYTESTO = "BYTES->";
  public static final String BYTESTOBITS = "BYTESTOBITS";
  public static final String MARK = "MARK";
  public static final String STORE = "STORE";
  
  public static final String MAPPER_HIGHEST = "mapper.highest";
  public static final String MAPPER_LOWEST = "mapper.lowest";
  public static final String MAPPER_MAX = "mapper.max";
  public static final String MAPPER_MIN = "mapper.min";
  
  public static final String RSAPUBLIC = "RSAPUBLIC";
  public static final String RSAPRIVATE = "RSAPRIVATE";
  
  public static final String MSGFAIL = "MSGFAIL";
  
  public static final String INPLACEADD = "+!";
  public static final String PUT = "PUT";

  public static final String SAVE = "SAVE";
  public static final String RESTORE = "RESTORE";

  public static final String CHRONOSTART = "CHRONOSTART";
  public static final String CHRONOEND = "CHRONOEND";

  public static final String TRY = "TRY";
  public static final String RETHROW = "RETHROW";

  public static final String TOGTS = "->GTS";

  public static final String REV = "REV";
  public static final String REPORT = "REPORT";
  public static final String MINREV = "MINREV";
  public static final String UPDATEON = "UPDATEON";
  public static final String UPDATEOFF = "UPDATEOFF";
  public static final String METAON = "METAON";
  public static final String METAOFF = "METAOFF";
  public static final String DELETEON = "DELETEON";
  public static final String DELETEOFF = "DELETEOFF";
  public static final String RTFM = "RTFM";
  public static final String MAN = "MAN";
  public static final String PIGSCHEMA = "PIGSCHEMA";
  public static final String CLEARTOMARK = "CLEARTOMARK";
  public static final String COUNTTOMARK = "COUNTTOMARK";
  public static final String AUTHENTICATE = "AUTHENTICATE";
  public static final String ISAUTHENTICATED = "ISAUTHENTICATED";
  public static final String STACKATTRIBUTE = "STACKATTRIBUTE";
  public static final String EXPORT = "EXPORT";
  public static final String TIMINGS = "TIMINGS";
  public static final String NOTIMINGS = "NOTIMINGS";
  public static final String ELAPSED = "ELAPSED";
  public static final String TIMED = "TIMED";
  public static final String CHRONOSTATS = "CHRONOSTATS";
  public static final String UNLIST = "UNLIST";
  public static final String UNION = "UNION";
  public static final String INTERSECTION = "INTERSECTION";
  public static final String DIFFERENCE = "DIFFERENCE";
  public static final String UNMAP = "UNMAP";
  public static final String MAPID = "MAPID";
  public static final String GET = "GET";
  public static final String SET = "SET";
  public static final String SUBMAP = "SUBMAP";
  public static final String SUBLIST = "SUBLIST";
  public static final String KEYLIST = "KEYLIST";
  public static final String VALUELIST = "VALUELIST";
  public static final String SIZE = "SIZE";
  public static final String SHRINK = "SHRINK";
  public static final String REMOVE = "REMOVE";
  public static final String UNIQUE = "UNIQUE";
  public static final String CONTAINS = "CONTAINS";
  public static final String CONTAINSKEY = "CONTAINSKEY";
  public static final String CONTAINSVALUE = "CONTAINSVALUE";
  public static final String REVERSE = "REVERSE";
  public static final String CLONEREVERSE = "CLONEREVERSE";
  public static final String DUP = "DUP";
  public static final String DUPN = "DUPN";
  public static final String DROP = "DROP";
  public static final String CLEAR = "CLEAR";
  public static final String CLEARDEFS = "CLEARDEFS";
  public static final String CLEARSYMBOLS = "CLEARSYMBOLS";
  public static final String DROPN = "DROPN";
  public static final String ROT = "ROT";
  public static final String ROLL = "ROLL";
  public static final String ROLLD = "ROLLD";
  public static final String PICK = "PICK";
  public static final String DEPTH = "DEPTH";
  public static final String MAXDEPTH = "MAXDEPTH";
  public static final String RESET = "RESET";
  public static final String MAXOPS = "MAXOPS";
  public static final String MAXLOOP = "MAXLOOP";
  public static final String MAXBUCKETS = "MAXBUCKETS";
  public static final String MAXGEOCELLS = "MAXGEOCELLS";
  public static final String MAXPIXELS = "MAXPIXELS";
  public static final String MAXRECURSION = "MAXRECURSION";
  public static final String OPS = "OPS";
  public static final String MAXSYMBOLS = "MAXSYMBOLS";
  public static final String SYMBOLS = "SYMBOLS";
  public static final String MAXJSON = "MAXJSON";
  public static final String NOW = "NOW";
  public static final String AGO = "AGO";
  public static final String MSTU = "MSTU";
  public static final String STU = "STU";
  public static final String APPEND = "APPEND";
  public static final String CSTORE = "CSTORE";
  public static final String IMPORT = "IMPORT";
  public static final String DEF = "DEF";
  public static final String UDF = "UDF";
  public static final String CUDF = "CUDF";
  public static final String CALL = "CALL";
  public static final String FORGET = "FORGET";
  public static final String DEFINED = "DEFINED";
  public static final String REDEFS = "REDEFS";
  public static final String DEFINEDMACRO = "DEFINEDMACRO";
  public static final String CHECKMACRO = "CHECKMACRO";
  public static final String NAN = "NaN";
  public static final String ISNAN = "ISNaN";
  public static final String TYPEOF = "TYPEOF";
  public static final String EXTLOADED = "EXTLOADED";
  public static final String ASSERT = "ASSERT";
  public static final String ASSERTMSG = "ASSERTMSG";
  public static final String FAIL = "FAIL";
  public static final String STOP = "STOP";
  public static final String ERROR = "ERROR";
  public static final String TIMEBOX = "TIMEBOX";
  public static final String JSONSTRICT = "JSONSTRICT";
  public static final String JSONLOOSE = "JSONLOOSE";
  public static final String DEBUGON = "DEBUGON";
  public static final String NDEBUGON = "NDEBUGON";
  public static final String DEBUGOFF = "DEBUGOFF";
  public static final String LINEON = "LINEON";
  public static final String LINEOFF = "LINEOFF";
  public static final String LMAP = "LMAP";
  public static final String NONNULL = "NONNULL";
  public static final String LFLATMAP = "LFLATMAP";
  public static final String STACKTOLIST = "STACKTOLIST";
  public static final String IMMUTABLE = "IMMUTABLE";
  public static final String SECUREKEY = "SECUREKEY";
  public static final String SECURE = "SECURE";
  public static final String UNSECURE = "UNSECURE";
  public static final String DOC = "DOC";
  public static final String DOCMODE = "DOCMODE";
  public static final String INFO = "INFO";
  public static final String INFOMODE = "INFOMODE";
  public static final String GETSECTION = "GETSECTION";
  public static final String SNAPSHOTTOMARK = "SNAPSHOTTOMARK";
  public static final String SNAPSHOTALLTOMARK = "SNAPSHOTALLTOMARK";
  public static final String SNAPSHOTCOPY = "SNAPSHOTCOPY";
  public static final String SNAPSHOTCOPYALL = "SNAPSHOTCOPYALL";
  public static final String SNAPSHOTCOPYTOMARK = "SNAPSHOTCOPYTOMARK";
  public static final String SNAPSHOTCOPYALLTOMARK = "SNAPSHOTCOPYALLTOMARK";
  public static final String SNAPSHOTN = "SNAPSHOTN";
  public static final String SNAPSHOTCOPYN = "SNAPSHOTCOPYN";
  public static final String HEADER = "HEADER";
  public static final String ECHOON = "ECHOON";
  public static final String ECHOOFF = "ECHOOFF";
  public static final String JSONSTACK = "JSONSTACK";
  public static final String WSSTACK = "WSSTACK";
  public static final String PEEK = "PEEK";
  public static final String PEEKN = "PEEKN";
  public static final String NPEEK = "NPEEK";
  public static final String PSTACK = "PSTACK";
  public static final String TIMEON = "TIMEON";
  public static final String TIMEOFF = "TIMEOFF";
  public static final String MACROTTL = "MACROTTL";
  public static final String WFON = "WFON";
  public static final String WFOFF = "WFOFF";
  public static final String SETMACROCONFIG = "SETMACROCONFIG";
  public static final String MACROCONFIGSECRET = "MACROCONFIGSECRET";
  public static final String MACROCONFIG = "MACROCONFIG";
  public static final String MACROCONFIGDEFAULT = "MACROCONFIGDEFAULT";
  public static final String MACROMAPPER = "MACROMAPPER";
  public static final String MACROREDUCER = "MACROREDUCER";
  public static final String MACROBUCKETIZER = "MACROBUCKETIZER";
  public static final String MACROFILTER = "MACROFILTER";
  public static final String MACROFILLER = "MACROFILLER";
  public static final String STRICTMAPPER = "STRICTMAPPER";
  public static final String STRICTREDUCER = "STRICTREDUCER";
  public static final String TOSELECTOR = "TOSELECTOR";
  public static final String PARSE = "PARSE";
  public static final String SMARTPARSE = "SMARTPARSE";
  public static final String DUMP = "DUMP";
  public static final String AND = "AND";
  public static final String OR = "OR";
  public static final String BITGET = "BITGET";
  public static final String BITCOUNT = "BITCOUNT";
  public static final String BITSTOBYTES = "BITSTOBYTES";
  public static final String REVBITS = "REVBITS";
  public static final String NOT = "NOT";
  public static final String ABS = "ABS";
  public static final String TODOUBLE = "TODOUBLE";
  public static final String TOBOOLEAN = "TOBOOLEAN";
  public static final String TOLONG = "TOLONG";
  public static final String TOSTRING = "TOSTRING";
  public static final String TOHEX = "TOHEX";
  public static final String TOBIN = "TOBIN";
  public static final String FROMHEX = "FROMHEX";
  public static final String FROMBIN = "FROMBIN";
  public static final String TOBITS = "TOBITS";
  public static final String FROMBITS = "FROMBITS";
  public static final String TOKENINFO = "TOKENINFO";
  public static final String GETHOOK = "GETHOOK";
  public static final String W = "w";
  public static final String D = "d";
  public static final String H = "h";
  public static final String M = "m";
  public static final String S = "s";
  public static final String MS = "ms";
  public static final String US = "us";
  public static final String NS = "ns";
  public static final String PS = "ps";
  public static final String HASH = "HASH";
  public static final String MD5 = "MD5";
  public static final String SHA1 = "SHA1";
  public static final String SHA256 = "SHA256";
  public static final String SHA256HMAC = "SHA256HMAC";
  public static final String SHA1HMAC = "SHA1HMAC";
  public static final String AESWRAP = "AESWRAP";
  public static final String AESUNWRAP = "AESUNWRAP";
  public static final String RUNNERNONCE = "RUNNERNONCE";
  public static final String GZIP = "GZIP";
  public static final String UNGZIP = "UNGZIP";
  public static final String DEFLATE = "DEFLATE";
  public static final String INFLATE = "INFLATE";
  public static final String RSAGEN = "RSAGEN";
  public static final String RSAENCRYPT = "RSAENCRYPT";
  public static final String RSADECRYPT = "RSADECRYPT";
  public static final String RSASIGN = "RSASIGN";
  public static final String RSAVERIFY = "RSAVERIFY";
  public static final String URLDECODE = "URLDECODE";
  public static final String URLENCODE = "URLENCODE";
  public static final String SPLIT = "SPLIT";
  public static final String UUID = "UUID";
  public static final String JOIN = "JOIN";
  public static final String SUBSTRING = "SUBSTRING";
  public static final String TOUPPER = "TOUPPER";
  public static final String TOLOWER = "TOLOWER";
  public static final String TRIM = "TRIM";
  public static final String B64TOHEX = "B64TOHEX";
  public static final String HEXTOB64 = "HEXTOB64";
  public static final String BINTOHEX = "BINTOHEX";
  public static final String HEXTOBIN = "HEXTOBIN";
  public static final String OPB64TOHEX = "OPB64TOHEX";
  public static final String IFT = "IFT";
  public static final String IFTE = "IFTE";
  public static final String SWITCH = "SWITCH";
  public static final String WHILE = "WHILE";
  public static final String UNTIL = "UNTIL";
  public static final String FOR = "FOR";
  public static final String FORSTEP = "FORSTEP";
  public static final String FOREACH = "FOREACH";
  public static final String BREAK = "BREAK";
  public static final String CONTINUE = "CONTINUE";
  public static final String EVERY = "EVERY";
  public static final String RANGE = "RANGE";
  public static final String RETURN = "RETURN";
  public static final String NRETURN = "NRETURN";
  public static final String NEWENCODER = "NEWENCODER";
  public static final String CHUNKENCODER = "CHUNKENCODER";
  public static final String OPTIMIZE = "OPTIMIZE";
  public static final String MAKEGTS = "MAKEGTS";
  public static final String ADDVALUE = "ADDVALUE";
  public static final String SETVALUE = "SETVALUE";
  public static final String REMOVETICK = "REMOVETICK";
  public static final String FETCH = "FETCH";
  public static final String FETCHLONG = "FETCHLONG";
  public static final String FETCHDOUBLE = "FETCHDOUBLE";
  public static final String FETCHSTRING = "FETCHSTRING";
  public static final String FETCHBOOLEAN = "FETCHBOOLEAN";
  public static final String LIMIT = "LIMIT";
  public static final String MAXGTS = "MAXGTS";
  public static final String FIND = "FIND";
  public static final String FINDSETS = "FINDSETS";
  public static final String METASET = "METASET";
  public static final String FINDSTATS = "FINDSTATS";
  public static final String DEDUP = "DEDUP";
  public static final String ONLYBUCKETS = "ONLYBUCKETS";
  public static final String VALUEDEDUP = "VALUEDEDUP";
  public static final String CLONEEMPTY = "CLONEEMPTY";
  public static final String COMPACT = "COMPACT";
  public static final String RANGECOMPACT = "RANGECOMPACT";
  public static final String STANDARDIZE = "STANDARDIZE";
  public static final String NORMALIZE = "NORMALIZE";
  public static final String ISONORMALIZE = "ISONORMALIZE";
  public static final String ZSCORE = "ZSCORE";
  public static final String FILL = "FILL";
  public static final String FILLPREVIOUS = "FILLPREVIOUS";
  public static final String FILLNEXT = "FILLNEXT";
  public static final String FILLVALUE = "FILLVALUE";
  public static final String FILLTICKS = "FILLTICKS";
  public static final String INTERPOLATE = "INTERPOLATE";
  public static final String FIRSTTICK = "FIRSTTICK";
  public static final String LASTTICK = "LASTTICK";
  public static final String MERGE = "MERGE";
  public static final String RESETS = "RESETS";
  public static final String MONOTONIC = "MONOTONIC";
  public static final String TIMESPLIT = "TIMESPLIT";
  public static final String TIMECLIP = "TIMECLIP";
  public static final String CLIP = "CLIP";
  public static final String TIMEMODULO = "TIMEMODULO";
  public static final String CHUNK = "CHUNK";
  public static final String FUSE = "FUSE";
  public static final String SETATTRIBUTES = "SETATTRIBUTES";
  public static final String CROP = "CROP";
  public static final String TIMESHIFT = "TIMESHIFT";
  public static final String TIMESCALE = "TIMESCALE";
  public static final String TICKINDEX = "TICKINDEX";
  public static final String FFT = "FFT";
  public static final String FFTAP = "FFTAP";
  public static final String IFFT = "IFFT";
  public static final String FFTWINDOW = "FFTWINDOW";
  public static final String FDWT = "FDWT";
  public static final String IDWT = "IDWT";
  public static final String DWTSPLIT = "DWTSPLIT";
  public static final String EMPTY = "EMPTY";
  public static final String NONEMPTY = "NONEMPTY";
  public static final String PARTITION = "PARTITION";
  public static final String STRICTPARTITION = "STRICTPARTITION";
  public static final String ZIP = "ZIP";
  public static final String PATTERNS = "PATTERNS";
  public static final String PATTERNDETECTION = "PATTERNDETECTION";
  public static final String ZPATTERNS = "ZPATTERNS";
  public static final String ZPATTERNDETECTION = "ZPATTERNDETECTION";
  public static final String DTW = "DTW";
  public static final String OPTDTW = "OPTDTW";
  public static final String ZDTW = "ZDTW";
  public static final String RAWDTW = "RAWDTW";
  public static final String VALUEHISTOGRAM = "VALUEHISTOGRAM";
  public static final String PROBABILITY = "PROBABILITY";
  public static final String PROB = "PROB";
  public static final String CPROB = "CPROB";
  public static final String RANDPDF = "RANDPDF";
  public static final String SRANDPDF = "SRANDPDF";
  public static final String SINGLEEXPONENTIALSMOOTHING = "SINGLEEXPONENTIALSMOOTHING";
  public static final String DOUBLEEXPONENTIALSMOOTHING = "DOUBLEEXPONENTIALSMOOTHING";
  public static final String LOWESS = "LOWESS";
  public static final String RLOWESS = "RLOWESS";
  public static final String STL = "STL";
  public static final String LTTB = "LTTB";
  public static final String TLTTB = "TLTTB";
  public static final String LOCATIONOFFSET = "LOCATIONOFFSET";
  public static final String MOTIONSPLIT = "MOTIONSPLIT";
  public static final String FLATTEN = "FLATTEN";
  public static final String RESHAPE = "RESHAPE";
  public static final String PERMUTE = "PERMUTE";
  public static final String CHECKSHAPE = "CHECKSHAPE";
  public static final String SHAPE = "SHAPE";
  public static final String HULLSHAPE = "HULLSHAPE";
  public static final String CORRELATE = "CORRELATE";
  public static final String SORT = "SORT";
  public static final String SORTBY = "SORTBY";
  public static final String SORTWITH = "SORTWITH";
  public static final String RSORT = "RSORT";
  public static final String LASTSORT = "LASTSORT";
  public static final String METASORT = "METASORT";
  public static final String VALUESORT = "VALUESORT";
  public static final String RVALUESORT = "RVALUESORT";
  public static final String LSORT = "LSORT";
  public static final String SHUFFLE = "SHUFFLE";
  public static final String MSORT = "MSORT";
  public static final String GROUPBY = "GROUPBY";
  public static final String FILTERBY = "FILTERBY";
  public static final String ACCEL_NOCACHE = "ACCEL.NOCACHE";
  public static final String ACCEL_CACHE = "ACCEL.CACHE";
  public static final String ACCEL_NOPERSIST = "ACCEL.NOPERSIST";
  public static final String ACCEL_PERSIST = "ACCEL.PERSIST";
  public static final String ACCEL_REPORT = "ACCEL.REPORT";
  public static final String UPDATE = "UPDATE";
  public static final String META = "META";
  public static final String METADIFF = "METADIFF";
  public static final String DELETE = "DELETE";
  public static final String WEBCALL = "WEBCALL";
  public static final String MATCH = "MATCH";
  public static final String MATCHER = "MATCHER";
  public static final String REPLACE = "REPLACE";
  public static final String REPLACEALL = "REPLACEALL";
  public static final String REOPTALT = "REOPTALT";
  public static final String TEMPLATE = "TEMPLATE";
  public static final String TOTIMESTAMP = "TOTIMESTAMP";
  public static final String STRINGFORMAT = "STRINGFORMAT";
  public static final String DISCORDS = "DISCORDS";
  public static final String ZDISCORDS = "ZDISCORDS";
  public static final String INTEGRATE = "INTEGRATE";
  public static final String BUCKETSPAN = "BUCKETSPAN";
  public static final String BUCKETCOUNT = "BUCKETCOUNT";
  public static final String UNBUCKETIZE = "UNBUCKETIZE";
  public static final String UNBUCKETIZE_CALENDAR = "UNBUCKETIZE.CALENDAR";
  public static final String LASTBUCKET = "LASTBUCKET";
  public static final String NAME = "NAME";
  public static final String LABELS = "LABELS";
  public static final String ATTRIBUTES = "ATTRIBUTES";
  public static final String LASTACTIVITY = "LASTACTIVITY";
  public static final String TICKS = "TICKS";
  public static final String LOCATIONS = "LOCATIONS";
  public static final String LOCSTRINGS = "LOCSTRINGS";
  public static final String ELEVATIONS = "ELEVATIONS";
  public static final String VALUES = "VALUES";
  public static final String VALUESPLIT = "VALUESPLIT";
  public static final String TICKLIST = "TICKLIST";
  public static final String COMMONTICKS = "COMMONTICKS";
  public static final String GOLDWRAP = "GOLDWRAP";
  public static final String WRAP = "WRAP";
  public static final String WRAPRAW = "WRAPRAW";
  public static final String WRAPFAST = "WRAPFAST";
  public static final String WRAPOPT = "WRAPOPT";
  public static final String WRAPRAWOPT = "WRAPRAWOPT";
  public static final String UNWRAPEMPTY = "UNWRAPEMPTY";
  public static final String UNWRAPSIZE = "UNWRAPSIZE";
  public static final String WRAPMV = "WRAPMV";
  public static final String MVTICKSPLIT = "MVTICKSPLIT";
  public static final String MVINDEXSPLIT = "MVINDEXSPLIT";
  public static final String MVVALUES = "MVVALUES";
  public static final String MVLOCATIONS = "MVLOCATIONS";
  public static final String MVELEVATIONS = "MVELEVATIONS";
  public static final String MVTICKS = "MVTICKS";
  public static final String MVHHCODES = "MVHHCODES";
  public static final String PARSEVALUE = "PARSEVALUE";
  public static final String THRESHOLDTEST = "THRESHOLDTEST";
  public static final String ZSCORETEST = "ZSCORETEST";
  public static final String GRUBBSTEST = "GRUBBSTEST";
  public static final String ESDTEST = "ESDTEST";
  public static final String STLESDTEST = "STLESDTEST";
  public static final String HYBRIDTEST = "HYBRIDTEST";
  public static final String HYBRIDTEST2 = "HYBRIDTEST2";
  public static final String QCONJUGATE = "QCONJUGATE";
  public static final String QDIVIDE = "QDIVIDE";
  public static final String QMULTIPLY = "QMULTIPLY";
  public static final String QROTATE = "QROTATE";
  public static final String QROTATION = "QROTATION";
  public static final String ROTATIONQ = "ROTATIONQ";
  public static final String ATINDEX = "ATINDEX";
  public static final String ATTICK = "ATTICK";
  public static final String ATBUCKET = "ATBUCKET";
  public static final String CLONE = "CLONE";
  public static final String DURATION = "DURATION";
  public static final String HUMANDURATION = "HUMANDURATION";
  public static final String ISODURATION = "ISODURATION";
  public static final String ISO8601 = "ISO8601";
  public static final String NOTBEFORE = "NOTBEFORE";
  public static final String NOTAFTER = "NOTAFTER";
  public static final String TSELEMENTS = "TSELEMENTS";
  public static final String ADDDAYS = "ADDDAYS";
  public static final String ADDDURATION = "ADDDURATION";
  public static final String ADDMONTHS = "ADDMONTHS";
  public static final String ADDYEARS = "ADDYEARS";
  public static final String QUANTIZE = "QUANTIZE";
  public static final String NBOUNDS = "NBOUNDS";
  public static final String LBOUNDS = "LBOUNDS";
  public static final String BUCKETIZE = "BUCKETIZE";
  public static final String BUCKETIZE_CALENDAR = "BUCKETIZE.CALENDAR";
  public static final String MAP = "MAP";
  public static final String FILTER = "FILTER";
  public static final String APPLY = "APPLY";
  public static final String PFILTER = "PFILTER";
  public static final String PAPPLY = "PAPPLY";
  public static final String REDUCE = "REDUCE";
  public static final String PREDUCE = "PREDUCE";
  public static final String PIVOT = "PIVOT";
  public static final String PIVOTSTRICT = "PIVOTSTRICT";
  public static final String ISNULL = "ISNULL";
  public static final String HAVERSINE = "HAVERSINE";
  public static final String COPYGEO = "COPYGEO";
  public static final String BBOX = "BBOX";
  public static final String COUNTERVALUE = "COUNTERVALUE";
  public static final String COUNTERDELTA = "COUNTERDELTA";
  public static final String PI_LOWERCASE = "pi";
  public static final String PI = "PI";
  public static final String E_LOWERCASE = "e";
  public static final String E = "E";
  public static final String MINLONG = "MINLONG";
  public static final String MAXLONG = "MAXLONG";
  public static final String RAND = "RAND";
  public static final String PRNG = "PRNG";
  public static final String SRAND = "SRAND";
  public static final String NPDF = "NPDF";
  public static final String MUSIGMA = "MUSIGMA";
  public static final String KURTOSIS = "KURTOSIS";
  public static final String SKEWNESS = "SKEWNESS";
  public static final String NSUMSUMSQ = "NSUMSUMSQ";
  public static final String LR = "LR";
  public static final String MODE = "MODE";
  public static final String PACK = "PACK";
  public static final String UNPACK = "UNPACK";
  public static final String TR = "TR";
  public static final String TRANSPOSE = "TRANSPOSE";
  public static final String DET = "DET";
  public static final String INV = "INV";
  public static final String COS = "COS";
  public static final String COSH = "COSH";
  public static final String ACOS = "ACOS";
  public static final String SIN = "SIN";
  public static final String SINH = "SINH";
  public static final String ASIN = "ASIN";
  public static final String TAN = "TAN";
  public static final String TANH = "TANH";
  public static final String ATAN = "ATAN";
  public static final String SIGNUM = "SIGNUM";
  public static final String FLOOR = "FLOOR";
  public static final String CEIL = "CEIL";
  public static final String ROUND = "ROUND";
  public static final String RINT = "RINT";
  public static final String NEXTUP = "NEXTUP";
  public static final String ULP = "ULP";
  public static final String SQRT = "SQRT";
  public static final String CBRT = "CBRT";
  public static final String EXP = "EXP";
  public static final String EXPM1 = "EXPM1";
  public static final String LOG_ = "LOG";
  public static final String LOG10 = "LOG10";
  public static final String LOG1P = "LOG1P";
  public static final String TORADIANS = "TORADIANS";
  public static final String TODEGREES = "TODEGREES";
  public static final String MAX = "MAX";
  public static final String MIN = "MIN";
  public static final String COPYSIGN = "COPYSIGN";
  public static final String HYPOT = "HYPOT";
  public static final String IEEEREMAINDER = "IEEEREMAINDER";
  public static final String NEXTAFTER = "NEXTAFTER";
  public static final String ATAN2 = "ATAN2";
  public static final String FLOORDIV = "FLOORDIV";
  public static final String FLOORMOD = "FLOORMOD";
  public static final String ADDEXACT = "ADDEXACT";
  public static final String SUBTRACTEXACT = "SUBTRACTEXACT";
  public static final String MULTIPLYEXACT = "MULTIPLYEXACT";
  public static final String INCREMENTEXACT = "INCREMENTEXACT";
  public static final String DECREMENTEXACT = "DECREMENTEXACT";
  public static final String NEGATEEXACT = "NEGATEEXACT";
  public static final String TOINTEXACT = "TOINTEXACT";
  public static final String SCALB = "SCALB";
  public static final String RANDOM = "RANDOM";
  public static final String NEXTDOWN = "NEXTDOWN";
  public static final String GETEXPONENT = "GETEXPONENT";
  public static final String IDENT = "IDENT";
  public static final String PENCODE = "Pencode";
  public static final String PPUSHSTYLE = "PpushStyle";
  public static final String PPOPSTYLE = "PpopStyle";
  public static final String PARC = "Parc";
  public static final String PELLIPSE = "Pellipse";
  public static final String PPOINT = "Ppoint";
  public static final String PLINE = "Pline";
  public static final String PTRIANGLE = "Ptriangle";
  public static final String PRECT = "Prect";
  public static final String PQUAD = "Pquad";
  public static final String PBEZIER = "Pbezier";
  public static final String PBEZIERPOINT = "PbezierPoint";
  public static final String PBEZIERTANGENT = "PbezierTangent";
  public static final String PBEZIERDETAIL = "PbezierDetail";
  public static final String PCURVE = "Pcurve";
  public static final String PCURVEPOINT = "PcurvePoint";
  public static final String PCURVETANGENT = "PcurveTangent";
  public static final String PCURVEDETAIL = "PcurveDetail";
  public static final String PCURVETIGHTNESS = "PcurveTightness";
  public static final String PBOX = "Pbox";
  public static final String PSPHERE = "Psphere";
  public static final String PSPHEREDETAIL = "PsphereDetail";
  public static final String PELLIPSEMODE = "PellipseMode";
  public static final String PRECTMODE = "PrectMode";
  public static final String PSTROKECAP = "PstrokeCap";
  public static final String PSTROKEJOIN = "PstrokeJoin";
  public static final String PSTROKEWEIGHT = "PstrokeWeight";
  public static final String PBEGINSHAPE = "PbeginShape";
  public static final String PENDSHAPE = "PendShape";
  public static final String PLOADSHAPE = "PloadShape";
  public static final String PBEGINCONTOUR = "PbeginContour";
  public static final String PENDCONTOUR = "PendContour";
  public static final String PVERTEX = "Pvertex";
  public static final String PCURVEVERTEX = "PcurveVertex";
  public static final String PBEZIERVERTEX = "PbezierVertex";
  public static final String PQUADRATICVERTEX = "PquadraticVertex";
  public static final String PSHAPEMODE = "PshapeMode";
  public static final String PSHAPE = "Pshape";
  public static final String PPUSHMATRIX = "PpushMatrix";
  public static final String PPOPMATRIX = "PpopMatrix";
  public static final String PRESETMATRIX = "PresetMatrix";
  public static final String PROTATE = "Protate";
  public static final String PROTATEX = "ProtateX";
  public static final String PROTATEY = "ProtateY";
  public static final String PROTATEZ = "ProtateZ";
  public static final String PSCALE = "Pscale";
  public static final String PSHEARX = "PshearX";
  public static final String PSHEARY = "PshearY";
  public static final String PTRANSLATE = "Ptranslate";
  public static final String PBACKGROUND = "Pbackground";
  public static final String PCOLORMODE = "PcolorMode";
  public static final String PCLEAR = "Pclear";
  public static final String PFILL = "Pfill";
  public static final String PNOFILL = "PnoFill";
  public static final String PSTROKE = "Pstroke";
  public static final String PNOSTROKE = "PnoStroke";
  public static final String PALPHA = "Palpha";
  public static final String PBLUE = "Pblue";
  public static final String PBRIGHTNESS = "Pbrightness";
  public static final String PCOLOR = "Pcolor";
  public static final String PGREEN = "Pgreen";
  public static final String PHUE = "Phue";
  public static final String PLERPCOLOR = "PlerpColor";
  public static final String PRED = "Pred";
  public static final String PSATURATION = "Psaturation";
  public static final String PDECODE = "Pdecode";
  public static final String PIMAGE = "Pimage";
  public static final String PIMAGEMODE = "PimageMode";
  public static final String PTINT = "Ptint";
  public static final String PNOTINT = "PnoTint";
  public static final String PPIXELS = "Ppixels";
  public static final String PUPDATEPIXELS = "PupdatePixels";
  public static final String PTOIMAGE = "PtoImage";
  public static final String PBLEND = "Pblend";
  public static final String PCOPY = "Pcopy";
  public static final String PGET = "Pget";
  public static final String PSET = "Pset";
  public static final String PFILTER_ = "Pfilter";
  public static final String PBLENDMODE = "PblendMode";
  public static final String PCLIP = "Pclip";
  public static final String PNOCLIP = "PnoClip";
  public static final String PGRAPHICS = "PGraphics";
  public static final String PCREATEFONT = "PcreateFont";
  public static final String PTEXT = "Ptext";
  public static final String PTEXTALIGN = "PtextAlign";
  public static final String PTEXTASCENT = "PtextAscent";
  public static final String PTEXTDESCENT = "PtextDescent";
  public static final String PTEXTFONT = "PtextFont";
  public static final String PTEXTLEADING = "PtextLeading";
  public static final String PTEXTMODE = "PtextMode";
  public static final String PTEXTSIZE = "PtextSize";
  public static final String PTEXTWIDTH = "PtextWidth";
  public static final String PCONSTRAIN = "Pconstrain";
  public static final String PDIST = "Pdist";
  public static final String PLERP = "Plerp";
  public static final String PMAG = "Pmag";
  public static final String PMAP = "Pmap";
  public static final String PNORM = "Pnorm";
  public static final String VARS = "VARS";
  public static final String ASREGS = "ASREGS";
  public static final String ASENCODERS = "ASENCODERS";

  public static final String TOLIST = "->LIST";
  public static final String TOMAP = "->MAP";
  public static final String TOJSON = "->JSON";
  public static final String TOPICKLE = "->PICKLE";
  public static final String TOLONGBYTES = "->LONGBYTES";
  public static final String TODOUBLEBITS = "->DOUBLEBITS";
  public static final String TOFLOATBITS = "->FLOATBITS";
  public static final String TOBYTES = "->BYTES";
  public static final String TOBIN_ = "->BIN";
  public static final String TOHEX_ = "->HEX";
  public static final String TOB64 = "->B64";
  public static final String TOB64URL = "->B64URL";
  public static final String TOENCODER = "->ENCODER";
  public static final String TOENCODERS = "->ENCODERS";
  public static final String TOMVSTRING = "->MVSTRING";
  public static final String TOQ = "->Q";
  public static final String TOTSELEMENTS = "->TSELEMENTS";
  public static final String TOHHCODE = "->HHCODE";
  public static final String TOHHCODELONG = "->HHCODELONG";
  public static final String TOGTSHHCODE = "->GTSHHCODE";
  public static final String TOGTSHHCODELONG = "->GTSHHCODELONG";
  public static final String TOGEOHASH = "->GEOHASH";
  public static final String TOZ = "->Z";
  public static final String TOMAT = "->MAT";
  public static final String TOVEC = "->VEC";

  public static final String LISTTO = "LIST->";
  public static final String SETTO = "SET->";
  public static final String VTO = "V->";
  public static final String MAPTO = "MAP->";
  public static final String PICKLETO = "PICKLE->";
  public static final String DOUBLEBITSTO = "DOUBLEBITS->";
  public static final String FLOATBITSTO = "FLOATBITS->";
  public static final String BINTO = "BIN->";
  public static final String HEXTO = "HEX->";
  public static final String B64TO = "B64->";
  public static final String B64URLTO = "B64URL->";
  public static final String ENCODERTO = "ENCODER->";
  public static final String QTO = "Q->";
  public static final String TSELEMENTSTO = "TSELEMENTS->";
  public static final String HHCODETO = "HHCODE->";
  public static final String GTSHHCODETO = "GTSHHCODE->";
  public static final String GEOHASHTO = "GEOHASH->";
  public static final String GEOSPLIT = "GEOSPLIT";
  public static final String ZTO = "Z->";
  public static final String MATTO = "MAT->";
  public static final String VECTO = "VEC->";

  public static final String GEOSHIFT = "GEOSHIFT";
  public static final String GEO_REGEXP = "GEO.REGEXP";
  public static final String GEO_OPTIMIZE = "GEO.OPTIMIZE";
  public static final String GEO_NORMALIZE = "GEO.NORMALIZE";
  public static final String GEO_WITHIN = "GEO.WITHIN";
  public static final String GEO_INTERSECTS = "GEO.INTERSECTS";
  public static final String GEO_COVER = "GEO.COVER";
  public static final String GEO_COVER_RL = "GEO.COVER.RL";
  public static final String HHCODE_CENTER = "HHCODE.CENTER";
  public static final String HHCODE_BBOX = "HHCODE.BBOX";
  public static final String HHCODE_NORTH = "HHCODE.NORTH";
  public static final String HHCODE_SOUTH = "HHCODE.SOUTH";
  public static final String HHCODE_EAST = "HHCODE.EAST";
  public static final String HHCODE_WEST = "HHCODE.WEST";
  public static final String HHCODE_NORTH_EAST = "HHCODE.NORTH.EAST";
  public static final String HHCODE_NORTH_WEST = "HHCODE.NORTH.WEST";
  public static final String HHCODE_SOUTH_EAST = "HHCODE.SOUTH.EAST";
  public static final String HHCODE_SOUTH_WEST = "HHCODE.SOUTH.WEST";

  public static final String MAPPER_GT = "mapper.gt";
  public static final String MAPPER_GE = "mapper.ge";
  public static final String MAPPER_EQ = "mapper.eq";
  public static final String MAPPER_NE = "mapper.ne";
  public static final String MAPPER_LE = "mapper.le";
  public static final String MAPPER_LT = "mapper.lt";
  public static final String MAPPER_GT_TICK = "mapper.gt.tick";
  public static final String MAPPER_GE_TICK = "mapper.ge.tick";
  public static final String MAPPER_EQ_TICK = "mapper.eq.tick";
  public static final String MAPPER_NE_TICK = "mapper.ne.tick";
  public static final String MAPPER_LE_TICK = "mapper.le.tick";
  public static final String MAPPER_LT_TICK = "mapper.lt.tick";
  public static final String MAPPER_GT_LAT = "mapper.gt.lat";
  public static final String MAPPER_GE_LAT = "mapper.ge.lat";
  public static final String MAPPER_EQ_LAT = "mapper.eq.lat";
  public static final String MAPPER_NE_LAT = "mapper.ne.lat";
  public static final String MAPPER_LE_LAT = "mapper.le.lat";
  public static final String MAPPER_LT_LAT = "mapper.lt.lat";
  public static final String MAPPER_GT_LON = "mapper.gt.lon";
  public static final String MAPPER_GE_LON = "mapper.ge.lon";
  public static final String MAPPER_EQ_LON = "mapper.eq.lon";
  public static final String MAPPER_NE_LON = "mapper.ne.lon";
  public static final String MAPPER_LE_LON = "mapper.le.lon";
  public static final String MAPPER_LT_LON = "mapper.lt.lon";
  public static final String MAPPER_GT_HHCODE = "mapper.gt.hhcode";
  public static final String MAPPER_GE_HHCODE = "mapper.ge.hhcode";
  public static final String MAPPER_EQ_HHCODE = "mapper.eq.hhcode";
  public static final String MAPPER_NE_HHCODE = "mapper.ne.hhcode";
  public static final String MAPPER_LE_HHCODE = "mapper.le.hhcode";
  public static final String MAPPER_LT_HHCODE = "mapper.lt.hhcode";
  public static final String MAPPER_GT_ELEV = "mapper.gt.elev";
  public static final String MAPPER_GE_ELEV = "mapper.ge.elev";
  public static final String MAPPER_EQ_ELEV = "mapper.eq.elev";
  public static final String MAPPER_NE_ELEV = "mapper.ne.elev";
  public static final String MAPPER_LE_ELEV = "mapper.le.elev";
  public static final String MAPPER_LT_ELEV = "mapper.lt.elev";

  public static final String EQ = "==";

  
  static {

    addNamedWarpScriptFunction(new REV(REV));
    addNamedWarpScriptFunction(new REPORT(REPORT));
    addNamedWarpScriptFunction(new MINREV(MINREV));

    addNamedWarpScriptFunction(new MANAGERONOFF(UPDATEON, WarpManager.UPDATE_DISABLED, true));   
    addNamedWarpScriptFunction(new MANAGERONOFF(UPDATEOFF, WarpManager.UPDATE_DISABLED, false));   
    addNamedWarpScriptFunction(new MANAGERONOFF(METAON, WarpManager.META_DISABLED, true));   
    addNamedWarpScriptFunction(new MANAGERONOFF(METAOFF, WarpManager.META_DISABLED, false));   
    addNamedWarpScriptFunction(new MANAGERONOFF(DELETEON, WarpManager.DELETE_DISABLED, true));   
    addNamedWarpScriptFunction(new MANAGERONOFF(DELETEOFF, WarpManager.DELETE_DISABLED, false));
    
    addNamedWarpScriptFunction(new NOOP(BOOTSTRAP));

    addNamedWarpScriptFunction(new RTFM(RTFM));
    addNamedWarpScriptFunction(new MAN(MAN));

    //
    // Stack manipulation functions
    //
    
    addNamedWarpScriptFunction(new PIGSCHEMA(PIGSCHEMA));
    addNamedWarpScriptFunction(new MARK(MARK));
    addNamedWarpScriptFunction(new CLEARTOMARK(CLEARTOMARK));
    addNamedWarpScriptFunction(new COUNTTOMARK(COUNTTOMARK));
    addNamedWarpScriptFunction(new AUTHENTICATE(AUTHENTICATE));
    addNamedWarpScriptFunction(new ISAUTHENTICATED(ISAUTHENTICATED));
    addNamedWarpScriptFunction(new STACKATTRIBUTE(STACKATTRIBUTE)); // NOT TO BE DOCUMENTED
    addNamedWarpScriptFunction(new EXPORT(EXPORT));
    addNamedWarpScriptFunction(new TIMINGS(TIMINGS)); // NOT TO BE DOCUMENTED (YET)
    addNamedWarpScriptFunction(new NOTIMINGS(NOTIMINGS)); // NOT TO BE DOCUMENTED (YET)
    addNamedWarpScriptFunction(new ELAPSED(ELAPSED)); // NOT TO BE DOCUMENTED (YET)
    addNamedWarpScriptFunction(new TIMED(TIMED));
    addNamedWarpScriptFunction(new CHRONOSTART(CHRONOSTART));
    addNamedWarpScriptFunction(new CHRONOEND(CHRONOEND));
    addNamedWarpScriptFunction(new CHRONOSTATS(CHRONOSTATS));
    addNamedWarpScriptFunction(new TOLIST(TOLIST));
    addNamedWarpScriptFunction(new LISTTO(LISTTO));
    addNamedWarpScriptFunction(new UNLIST(UNLIST));
    addNamedWarpScriptFunction(new TOSET(TO_SET));
    addNamedWarpScriptFunction(new SETTO(SETTO));
    addNamedWarpScriptFunction(new TOVECTOR(TO_VECTOR));
    addNamedWarpScriptFunction(new VECTORTO(VTO));
    addNamedWarpScriptFunction(new UNION(UNION));
    addNamedWarpScriptFunction(new INTERSECTION(INTERSECTION));
    addNamedWarpScriptFunction(new DIFFERENCE(DIFFERENCE));
    addNamedWarpScriptFunction(new TOMAP(TOMAP));
    addNamedWarpScriptFunction(new MAPTO(MAPTO));
    addNamedWarpScriptFunction(new UNMAP(UNMAP));
    addNamedWarpScriptFunction(new MAPID(MAPID));
    addNamedWarpScriptFunction(new TOJSON(TOJSON));
    addNamedWarpScriptFunction(new JSONTO(JSONTO));
    addNamedWarpScriptFunction(new TOPICKLE(TOPICKLE));
    addNamedWarpScriptFunction(new PICKLETO(PICKLETO));
    addNamedWarpScriptFunction(new GET(GET));
    addNamedWarpScriptFunction(new SET(SET));
    addNamedWarpScriptFunction(new PUT(PUT));
    addNamedWarpScriptFunction(new SUBMAP(SUBMAP));
    addNamedWarpScriptFunction(new SUBLIST(SUBLIST));
    addNamedWarpScriptFunction(new KEYLIST(KEYLIST));
    addNamedWarpScriptFunction(new VALUELIST(VALUELIST));
    addNamedWarpScriptFunction(new SIZE(SIZE));
    addNamedWarpScriptFunction(new SHRINK(SHRINK));
    addNamedWarpScriptFunction(new REMOVE(REMOVE));
    addNamedWarpScriptFunction(new UNIQUE(UNIQUE));
    addNamedWarpScriptFunction(new CONTAINS(CONTAINS));
    addNamedWarpScriptFunction(new CONTAINSKEY(CONTAINSKEY));
    addNamedWarpScriptFunction(new CONTAINSVALUE(CONTAINSVALUE));
    addNamedWarpScriptFunction(new REVERSE(REVERSE, true));
    addNamedWarpScriptFunction(new REVERSE(CLONEREVERSE, false));
    addNamedWarpScriptFunction(new DUP(DUP));
    addNamedWarpScriptFunction(new DUPN(DUPN));
    addNamedWarpScriptFunction(new SWAP(SWAP));
    addNamedWarpScriptFunction(new DROP(DROP));
    addNamedWarpScriptFunction(new SAVE(SAVE));
    addNamedWarpScriptFunction(new RESTORE(RESTORE));
    addNamedWarpScriptFunction(new CLEAR(CLEAR));
    addNamedWarpScriptFunction(new CLEARDEFS(CLEARDEFS));
    addNamedWarpScriptFunction(new CLEARSYMBOLS(CLEARSYMBOLS));
    addNamedWarpScriptFunction(new DROPN(DROPN));
    addNamedWarpScriptFunction(new ROT(ROT));
    addNamedWarpScriptFunction(new ROLL(ROLL));
    addNamedWarpScriptFunction(new ROLLD(ROLLD));
    addNamedWarpScriptFunction(new PICK(PICK));
    addNamedWarpScriptFunction(new DEPTH(DEPTH));
    addNamedWarpScriptFunction(new MAXDEPTH(MAXDEPTH));
    addNamedWarpScriptFunction(new RESET(RESET));
    addNamedWarpScriptFunction(new MAXOPS(MAXOPS));
    addNamedWarpScriptFunction(new MAXLOOP(MAXLOOP));
    addNamedWarpScriptFunction(new MAXBUCKETS(MAXBUCKETS));
    addNamedWarpScriptFunction(new MAXGEOCELLS(MAXGEOCELLS));
    addNamedWarpScriptFunction(new MAXPIXELS(MAXPIXELS));
    addNamedWarpScriptFunction(new MAXRECURSION(MAXRECURSION));
    addNamedWarpScriptFunction(new OPS(OPS));
    addNamedWarpScriptFunction(new MAXSYMBOLS(MAXSYMBOLS));
    addNamedWarpScriptFunction(new SYMBOLS(SYMBOLS));
    addNamedWarpScriptFunction(new MAXJSON(MAXJSON));
    addNamedWarpScriptFunction(new EVAL(EVAL));
    addNamedWarpScriptFunction(new NOW(NOW));
    addNamedWarpScriptFunction(new AGO(AGO));
    addNamedWarpScriptFunction(new MSTU(MSTU));
    addNamedWarpScriptFunction(new STU(STU));
    addNamedWarpScriptFunction(new APPEND(APPEND));
    addNamedWarpScriptFunction(new STORE(STORE));
    addNamedWarpScriptFunction(new CSTORE(CSTORE));
    addNamedWarpScriptFunction(new LOAD(LOAD));
    addNamedWarpScriptFunction(new DEREF(DEREF));
    addNamedWarpScriptFunction(new IMPORT(IMPORT));
    addNamedWarpScriptFunction(new RUN(RUN));
    addNamedWarpScriptFunction(new DEF(DEF));
    addNamedWarpScriptFunction(new UDF(UDF, false));
    addNamedWarpScriptFunction(new UDF(CUDF, true));
    addNamedWarpScriptFunction(new CALL(CALL));
    addNamedWarpScriptFunction(new FORGET(FORGET));
    addNamedWarpScriptFunction(new DEFINED(DEFINED));
    addNamedWarpScriptFunction(new REDEFS(REDEFS));
    addNamedWarpScriptFunction(new DEFINEDMACRO(DEFINEDMACRO));
    addNamedWarpScriptFunction(new DEFINEDMACRO(CHECKMACRO, true));
    addNamedWarpScriptFunction(new NaN(NAN));
    addNamedWarpScriptFunction(new ISNaN(ISNAN));
    addNamedWarpScriptFunction(new TYPEOF(TYPEOF));
    addNamedWarpScriptFunction(new EXTLOADED(EXTLOADED));
    addNamedWarpScriptFunction(new ASSERT(ASSERT));
    addNamedWarpScriptFunction(new ASSERTMSG(ASSERTMSG));
    addNamedWarpScriptFunction(new FAIL(FAIL));
    addNamedWarpScriptFunction(new MSGFAIL(MSGFAIL));
    addNamedWarpScriptFunction(new STOP(STOP));
    addNamedWarpScriptFunction(new TRY(TRY));
    addNamedWarpScriptFunction(new RETHROW(RETHROW));
    addNamedWarpScriptFunction(new ERROR(ERROR));
    addNamedWarpScriptFunction(new TIMEBOX(TIMEBOX));
    addNamedWarpScriptFunction(new JSONSTRICT(JSONSTRICT));
    addNamedWarpScriptFunction(new JSONLOOSE(JSONLOOSE));
    addNamedWarpScriptFunction(new DEBUGON(DEBUGON));
    addNamedWarpScriptFunction(new NDEBUGON(NDEBUGON));
    addNamedWarpScriptFunction(new DEBUGOFF(DEBUGOFF));
    addNamedWarpScriptFunction(new LINEON(LINEON));
    addNamedWarpScriptFunction(new LINEOFF(LINEOFF));
    addNamedWarpScriptFunction(new LMAP(LMAP));
    addNamedWarpScriptFunction(new NONNULL(NONNULL));
    addNamedWarpScriptFunction(new LMAP(LFLATMAP, true));
    addNamedWarpScriptFunction(new EMPTYLIST("[]"));
    addNamedWarpScriptFunction(new MARK(LIST_START));
    addNamedWarpScriptFunction(new ENDLIST(LIST_END));
    addNamedWarpScriptFunction(new STACKTOLIST(STACKTOLIST));
    addNamedWarpScriptFunction(new MARK(SET_START));
    addNamedWarpScriptFunction(new ENDSET(SET_END));
    addNamedWarpScriptFunction(new EMPTYSET("()"));
    addNamedWarpScriptFunction(new MARK(VECTOR_START));
    addNamedWarpScriptFunction(new ENDVECTOR(VECTOR_END));
    addNamedWarpScriptFunction(new EMPTYVECTOR("[[]]"));
    addNamedWarpScriptFunction(new EMPTYMAP("{}"));
    addNamedWarpScriptFunction(new IMMUTABLE(IMMUTABLE));
    addNamedWarpScriptFunction(new MARK(MAP_START));
    addNamedWarpScriptFunction(new ENDMAP(MAP_END));
    addNamedWarpScriptFunction(new SECUREKEY(SECUREKEY));
    addNamedWarpScriptFunction(new SECURE(SECURE));
    addNamedWarpScriptFunction(new UNSECURE(UNSECURE, true));
    addNamedWarpScriptFunction(new EVALSECURE(EVALSECURE));
    addNamedWarpScriptFunction(new NOOP(NOOP));
    addNamedWarpScriptFunction(new DOC(DOC));
    addNamedWarpScriptFunction(new DOCMODE(DOCMODE));
    addNamedWarpScriptFunction(new INFO(INFO));
    addNamedWarpScriptFunction(new INFOMODE(INFOMODE));
    addNamedWarpScriptFunction(new SECTION(SECTION));
    addNamedWarpScriptFunction(new GETSECTION(GETSECTION));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOT, false, false, true, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTALL, true, false, true, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTTOMARK, false, true, true, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTALLTOMARK, true, true, true, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTCOPY, false, false, false, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTCOPYALL, true, false, false, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTCOPYTOMARK, false, true, false, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTCOPYALLTOMARK, true, true, false, false));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTN, false, false, true, true));
    addNamedWarpScriptFunction(new SNAPSHOT(SNAPSHOTCOPYN, false, false, false, true));
    addNamedWarpScriptFunction(new HEADER(HEADER));
    
    addNamedWarpScriptFunction(new ECHOON(ECHOON));
    addNamedWarpScriptFunction(new ECHOOFF(ECHOOFF));
    addNamedWarpScriptFunction(new JSONSTACK(JSONSTACK));
    addNamedWarpScriptFunction(new WSSTACK(WSSTACK));
    addNamedWarpScriptFunction(new PEEK(PEEK));
    addNamedWarpScriptFunction(new PEEKN(PEEKN));
    addNamedWarpScriptFunction(new NPEEK(NPEEK));
    addNamedWarpScriptFunction(new PSTACK(PSTACK));
    addNamedWarpScriptFunction(new TIMEON(TIMEON));
    addNamedWarpScriptFunction(new TIMEOFF(TIMEOFF));

    //
    // Compilation related dummy functions
    //
    addNamedWarpScriptFunction(new FAIL(COMPILE, "Not supported"));
    addNamedWarpScriptFunction(new NOOP(SAFECOMPILE));
    addNamedWarpScriptFunction(new FAIL(COMPILED, "Not supported"));
    addNamedWarpScriptFunction(new REF(REF));

    addNamedWarpScriptFunction(new MACROTTL(MACROTTL));
    addNamedWarpScriptFunction(new WFON(WFON));
    addNamedWarpScriptFunction(new WFOFF(WFOFF));
    addNamedWarpScriptFunction(new SETMACROCONFIG(SETMACROCONFIG));
    addNamedWarpScriptFunction(new MACROCONFIGSECRET(MACROCONFIGSECRET));
    addNamedWarpScriptFunction(new MACROCONFIG(MACROCONFIG, false));
    addNamedWarpScriptFunction(new MACROCONFIG(MACROCONFIGDEFAULT, true));
    addNamedWarpScriptFunction(new MACROMAPPER(MACROMAPPER));
    addNamedWarpScriptFunction(new MACROMAPPER(MACROREDUCER));
    addNamedWarpScriptFunction(new MACROMAPPER(MACROBUCKETIZER));
    addNamedWarpScriptFunction(new MACROFILTER(MACROFILTER));
    addNamedWarpScriptFunction(new MACROFILLER(MACROFILLER));
    addNamedWarpScriptFunction(new STRICTMAPPER(STRICTMAPPER));
    addNamedWarpScriptFunction(new STRICTREDUCER(STRICTREDUCER));
    
    addNamedWarpScriptFunction(new PARSESELECTOR(PARSESELECTOR));
    addNamedWarpScriptFunction(new TOSELECTOR(TOSELECTOR));
    addNamedWarpScriptFunction(new PARSE(PARSE));
    addNamedWarpScriptFunction(new SMARTPARSE(SMARTPARSE));
        
    // We do not expose DUMP, it might allocate too much memory
    //addNamedWarpScriptFunction(new DUMP(DUMP));
    
    // Binary ops
    addNamedWarpScriptFunction(new ADD("+"));
    addNamedWarpScriptFunction(new INPLACEADD(INPLACEADD));
    addNamedWarpScriptFunction(new SUB("-"));
    addNamedWarpScriptFunction(new DIV("/"));
    addNamedWarpScriptFunction(new MUL("*"));
    addNamedWarpScriptFunction(new POW("**"));
    addNamedWarpScriptFunction(new MOD("%"));
    addNamedWarpScriptFunction(new EQ(EQ));
    addNamedWarpScriptFunction(new NE("!="));
    addNamedWarpScriptFunction(new LT("<"));
    addNamedWarpScriptFunction(new GT(">"));
    addNamedWarpScriptFunction(new LE("<="));
    addNamedWarpScriptFunction(new GE(">="));
    addNamedWarpScriptFunction(new CondAND("&&"));
    addNamedWarpScriptFunction(new CondAND(AND));
    addNamedWarpScriptFunction(new CondOR("||"));
    addNamedWarpScriptFunction(new CondOR(OR));
    addNamedWarpScriptFunction(new BitwiseAND("&"));
    addNamedWarpScriptFunction(new SHIFTRIGHT(">>", true));
    addNamedWarpScriptFunction(new SHIFTRIGHT(">>>", false));
    addNamedWarpScriptFunction(new SHIFTLEFT("<<"));
    addNamedWarpScriptFunction(new BitwiseOR("|"));
    addNamedWarpScriptFunction(new BitwiseXOR("^"));
    addNamedWarpScriptFunction(new ALMOSTEQ("~="));

    // Bitset ops
    addNamedWarpScriptFunction(new BITGET(BITGET));
    addNamedWarpScriptFunction(new BITCOUNT(BITCOUNT));
    addNamedWarpScriptFunction(new BITSTOBYTES(BITSTOBYTES));
    addNamedWarpScriptFunction(new BYTESTOBITS(BYTESTOBITS));
    
    // Unary ops    
    addNamedWarpScriptFunction(new NOT("!"));
    addNamedWarpScriptFunction(new COMPLEMENT("~"));
    addNamedWarpScriptFunction(new REVERSEBITS(REVBITS));
    addNamedWarpScriptFunction(new NOT(NOT));
    addNamedWarpScriptFunction(new ABS(ABS));
    addNamedWarpScriptFunction(new TODOUBLE(TODOUBLE));
    addNamedWarpScriptFunction(new TOBOOLEAN(TOBOOLEAN));
    addNamedWarpScriptFunction(new TOLONG(TOLONG));
    addNamedWarpScriptFunction(new TOSTRING(TOSTRING));
    addNamedWarpScriptFunction(new TOHEX(TOHEX));
    addNamedWarpScriptFunction(new TOBIN(TOBIN));
    addNamedWarpScriptFunction(new FROMHEX(FROMHEX));
    addNamedWarpScriptFunction(new FROMBIN(FROMBIN));
    addNamedWarpScriptFunction(new TOBITS(TOBITS, false));
    addNamedWarpScriptFunction(new FROMBITS(FROMBITS, false));
    addNamedWarpScriptFunction(new TOLONGBYTES(TOLONGBYTES));
    addNamedWarpScriptFunction(new TOBITS(TODOUBLEBITS, false));
    addNamedWarpScriptFunction(new FROMBITS(DOUBLEBITSTO, false));
    addNamedWarpScriptFunction(new TOBITS(TOFLOATBITS, true));
    addNamedWarpScriptFunction(new FROMBITS(FLOATBITSTO, true));
    addNamedWarpScriptFunction(new TOKENINFO(TOKENINFO));
    addNamedWarpScriptFunction(new GETHOOK(GETHOOK));
    
    // Unit converters
    addNamedWarpScriptFunction(new UNIT(W, 7 * 24 * 60 * 60 * 1000));
    addNamedWarpScriptFunction(new UNIT(D, 24 * 60 * 60 * 1000));
    addNamedWarpScriptFunction(new UNIT(H, 60 * 60 * 1000));
    addNamedWarpScriptFunction(new UNIT(M, 60 * 1000));
    addNamedWarpScriptFunction(new UNIT(S,  1000));
    addNamedWarpScriptFunction(new UNIT(MS, 1));
    addNamedWarpScriptFunction(new UNIT(US, 0.001));
    addNamedWarpScriptFunction(new UNIT(NS, 0.000001));
    addNamedWarpScriptFunction(new UNIT(PS, 0.000000001));
    
    // Crypto functions
    addNamedWarpScriptFunction(new HASH(HASH));
    addNamedWarpScriptFunction(new DIGEST(MD5, MD5Digest.class));
    addNamedWarpScriptFunction(new DIGEST(SHA1, SHA1Digest.class));
    addNamedWarpScriptFunction(new DIGEST(SHA256, SHA256Digest.class));
    addNamedWarpScriptFunction(new HMAC(SHA256HMAC, SHA256Digest.class));
    addNamedWarpScriptFunction(new HMAC(SHA1HMAC, SHA1Digest.class));
    addNamedWarpScriptFunction(new AESWRAP(AESWRAP));
    addNamedWarpScriptFunction(new AESUNWRAP(AESUNWRAP));
    addNamedWarpScriptFunction(new RUNNERNONCE(RUNNERNONCE));
    addNamedWarpScriptFunction(new GZIP(GZIP));
    addNamedWarpScriptFunction(new UNGZIP(UNGZIP));
    addNamedWarpScriptFunction(new DEFLATE(DEFLATE));
    addNamedWarpScriptFunction(new INFLATE(INFLATE));
    addNamedWarpScriptFunction(new RSAGEN(RSAGEN));
    addNamedWarpScriptFunction(new RSAPUBLIC(RSAPUBLIC));
    addNamedWarpScriptFunction(new RSAPRIVATE(RSAPRIVATE));
    addNamedWarpScriptFunction(new RSAENCRYPT(RSAENCRYPT));
    addNamedWarpScriptFunction(new RSADECRYPT(RSADECRYPT));
    addNamedWarpScriptFunction(new RSASIGN(RSASIGN));
    addNamedWarpScriptFunction(new RSAVERIFY(RSAVERIFY));

    //
    // String functions
    //
    
    addNamedWarpScriptFunction(new URLDECODE(URLDECODE));
    addNamedWarpScriptFunction(new URLENCODE(URLENCODE));
    addNamedWarpScriptFunction(new SPLIT(SPLIT));
    addNamedWarpScriptFunction(new UUID(UUID));
    addNamedWarpScriptFunction(new JOIN(JOIN));
    addNamedWarpScriptFunction(new SUBSTRING(SUBSTRING));
    addNamedWarpScriptFunction(new TOUPPER(TOUPPER));
    addNamedWarpScriptFunction(new TOLOWER(TOLOWER));
    addNamedWarpScriptFunction(new TRIM(TRIM));
    
    addNamedWarpScriptFunction(new B64TOHEX(B64TOHEX));
    addNamedWarpScriptFunction(new HEXTOB64(HEXTOB64));
    addNamedWarpScriptFunction(new BINTOHEX(BINTOHEX));
    addNamedWarpScriptFunction(new HEXTOBIN(HEXTOBIN));
    
    addNamedWarpScriptFunction(new BINTO(BINTO));
    addNamedWarpScriptFunction(new HEXTO(HEXTO));
    addNamedWarpScriptFunction(new B64TO(B64TO));
    addNamedWarpScriptFunction(new B64URLTO(B64URLTO));
    addNamedWarpScriptFunction(new BYTESTO(BYTESTO));

    addNamedWarpScriptFunction(new TOBYTES(TOBYTES));
    addNamedWarpScriptFunction(new io.warp10.script.functions.TOBIN(TOBIN_));
    addNamedWarpScriptFunction(new io.warp10.script.functions.TOHEX(TOHEX_));
    addNamedWarpScriptFunction(new TOB64(TOB64));
    addNamedWarpScriptFunction(new TOB64URL(TOB64URL));
    addNamedWarpScriptFunction(new TOOPB64(TOOPB64));
    addNamedWarpScriptFunction(new OPB64TO(OPB64TO));
    addNamedWarpScriptFunction(new OPB64TOHEX(OPB64TOHEX));
    
    //
    // Conditionals
    //
    
    addNamedWarpScriptFunction(new IFT(IFT));
    addNamedWarpScriptFunction(new IFTE(IFTE));
    addNamedWarpScriptFunction(new SWITCH(SWITCH));
    
    //
    // Loops
    //
    
    addNamedWarpScriptFunction(new WHILE(WHILE));
    addNamedWarpScriptFunction(new UNTIL(UNTIL));
    addNamedWarpScriptFunction(new FOR(FOR));
    addNamedWarpScriptFunction(new FORSTEP(FORSTEP));
    addNamedWarpScriptFunction(new FOREACH(FOREACH));
    addNamedWarpScriptFunction(new BREAK(BREAK));
    addNamedWarpScriptFunction(new CONTINUE(CONTINUE));
    addNamedWarpScriptFunction(new EVERY(EVERY));
    addNamedWarpScriptFunction(new RANGE(RANGE));
    
    //
    // Macro end
    //
    
    addNamedWarpScriptFunction(new RETURN(RETURN));
    addNamedWarpScriptFunction(new NRETURN(NRETURN));
    
    //
    // GTS standalone functions
    //
    
    addNamedWarpScriptFunction(new NEWENCODER(NEWENCODER));
    addNamedWarpScriptFunction(new CHUNKENCODER(CHUNKENCODER, true));
    addNamedWarpScriptFunction(new TOENCODER(TOENCODER));
    addNamedWarpScriptFunction(new ENCODERTO(ENCODERTO));
    addNamedWarpScriptFunction(new TOGTS(TOGTS));
    addNamedWarpScriptFunction(new ASENCODERS(ASENCODERS));
    addNamedWarpScriptFunction(new TOENCODERS(TOENCODERS));
    addNamedWarpScriptFunction(new OPTIMIZE(OPTIMIZE));
    addNamedWarpScriptFunction(new NEWGTS(NEWGTS));
    addNamedWarpScriptFunction(new MAKEGTS(MAKEGTS));
    addNamedWarpScriptFunction(new ADDVALUE(ADDVALUE, false));
    addNamedWarpScriptFunction(new ADDVALUE(SETVALUE, true));
    addNamedWarpScriptFunction(new REMOVETICK(REMOVETICK));
    addNamedWarpScriptFunction(new FETCH(FETCH, null));
    addNamedWarpScriptFunction(new FETCH(FETCHLONG, TYPE.LONG));
    addNamedWarpScriptFunction(new FETCH(FETCHDOUBLE, TYPE.DOUBLE));
    addNamedWarpScriptFunction(new FETCH(FETCHSTRING, TYPE.STRING));
    addNamedWarpScriptFunction(new FETCH(FETCHBOOLEAN, TYPE.BOOLEAN));
    addNamedWarpScriptFunction(new LIMIT(LIMIT));
    addNamedWarpScriptFunction(new MAXGTS(MAXGTS));
    addNamedWarpScriptFunction(new FIND(FIND, false));
    addNamedWarpScriptFunction(new FIND(FINDSETS, true));
    addNamedWarpScriptFunction(new FIND(METASET, false, true));
    addNamedWarpScriptFunction(new FINDSTATS(FINDSTATS));
    addNamedWarpScriptFunction(new DEDUP(DEDUP));
    addNamedWarpScriptFunction(new ONLYBUCKETS(ONLYBUCKETS));
    addNamedWarpScriptFunction(new VALUEDEDUP(VALUEDEDUP));
    addNamedWarpScriptFunction(new CLONEEMPTY(CLONEEMPTY));
    addNamedWarpScriptFunction(new COMPACT(COMPACT));
    addNamedWarpScriptFunction(new RANGECOMPACT(RANGECOMPACT));
    addNamedWarpScriptFunction(new STANDARDIZE(STANDARDIZE));
    addNamedWarpScriptFunction(new NORMALIZE(NORMALIZE));
    addNamedWarpScriptFunction(new ISONORMALIZE(ISONORMALIZE));
    addNamedWarpScriptFunction(new ZSCORE(ZSCORE));
    addNamedWarpScriptFunction(new FILL(FILL));
    addNamedWarpScriptFunction(new FILLPREVIOUS(FILLPREVIOUS));
    addNamedWarpScriptFunction(new FILLNEXT(FILLNEXT));
    addNamedWarpScriptFunction(new FILLVALUE(FILLVALUE));
    addNamedWarpScriptFunction(new FILLTICKS(FILLTICKS));
    addNamedWarpScriptFunction(new INTERPOLATE(INTERPOLATE));
    addNamedWarpScriptFunction(new FIRSTTICK(FIRSTTICK));
    addNamedWarpScriptFunction(new LASTTICK(LASTTICK));
    addNamedWarpScriptFunction(new MERGE(MERGE));
    addNamedWarpScriptFunction(new RESETS(RESETS));
    addNamedWarpScriptFunction(new MONOTONIC(MONOTONIC));
    addNamedWarpScriptFunction(new TIMESPLIT(TIMESPLIT));
    addNamedWarpScriptFunction(new TIMECLIP(TIMECLIP));
    addNamedWarpScriptFunction(new CLIP(CLIP));
    addNamedWarpScriptFunction(new TIMEMODULO(TIMEMODULO));
    addNamedWarpScriptFunction(new CHUNK(CHUNK, true));
    addNamedWarpScriptFunction(new FUSE(FUSE));
    addNamedWarpScriptFunction(new RENAME(RENAME));
    addNamedWarpScriptFunction(new RELABEL(RELABEL));
    addNamedWarpScriptFunction(new SETATTRIBUTES(SETATTRIBUTES));
    addNamedWarpScriptFunction(new CROP(CROP));
    addNamedWarpScriptFunction(new TIMESHIFT(TIMESHIFT));
    addNamedWarpScriptFunction(new TIMESCALE(TIMESCALE));
    addNamedWarpScriptFunction(new TICKINDEX(TICKINDEX));
    addNamedWarpScriptFunction(new FFT.Builder(FFT, true));
    addNamedWarpScriptFunction(new FFT.Builder(FFTAP, false));
    addNamedWarpScriptFunction(new IFFT.Builder(IFFT));
    addNamedWarpScriptFunction(new FFTWINDOW(FFTWINDOW));
    addNamedWarpScriptFunction(new FDWT(FDWT));
    addNamedWarpScriptFunction(new IDWT(IDWT));
    addNamedWarpScriptFunction(new DWTSPLIT(DWTSPLIT));
    addNamedWarpScriptFunction(new EMPTY(EMPTY));
    addNamedWarpScriptFunction(new NONEMPTY(NONEMPTY));
    addNamedWarpScriptFunction(new PARTITION(PARTITION));
    addNamedWarpScriptFunction(new PARTITION(STRICTPARTITION, true));
    addNamedWarpScriptFunction(new ZIP(ZIP));
    addNamedWarpScriptFunction(new PATTERNS(PATTERNS, true));
    addNamedWarpScriptFunction(new PATTERNDETECTION(PATTERNDETECTION, true));
    addNamedWarpScriptFunction(new PATTERNS(ZPATTERNS, false));
    addNamedWarpScriptFunction(new PATTERNDETECTION(ZPATTERNDETECTION, false));
    addNamedWarpScriptFunction(new DTW(DTW, true, false));
    addNamedWarpScriptFunction(new OPTDTW(OPTDTW));
    addNamedWarpScriptFunction(new DTW(ZDTW, true, true));
    addNamedWarpScriptFunction(new DTW(RAWDTW, false, false));
    addNamedWarpScriptFunction(new VALUEHISTOGRAM(VALUEHISTOGRAM));
    addNamedWarpScriptFunction(new PROBABILITY.Builder(PROBABILITY));
    addNamedWarpScriptFunction(new PROB(PROB));
    addNamedWarpScriptFunction(new CPROB(CPROB));
    addNamedWarpScriptFunction(new RANDPDF.Builder(RANDPDF, false));
    addNamedWarpScriptFunction(new RANDPDF.Builder(SRANDPDF, true));
    addNamedWarpScriptFunction(new SINGLEEXPONENTIALSMOOTHING(SINGLEEXPONENTIALSMOOTHING));
    addNamedWarpScriptFunction(new DOUBLEEXPONENTIALSMOOTHING(DOUBLEEXPONENTIALSMOOTHING));
    addNamedWarpScriptFunction(new LOWESS(LOWESS));
    addNamedWarpScriptFunction(new RLOWESS(RLOWESS));
    addNamedWarpScriptFunction(new STL(STL));
    addNamedWarpScriptFunction(new LTTB(LTTB, false));
    addNamedWarpScriptFunction(new LTTB(TLTTB, true));
    addNamedWarpScriptFunction(new LOCATIONOFFSET(LOCATIONOFFSET));
    addNamedWarpScriptFunction(new MOTIONSPLIT(MOTIONSPLIT));
    addNamedWarpScriptFunction(new FLATTEN(FLATTEN));
    addNamedWarpScriptFunction(new RESHAPE(RESHAPE));
    addNamedWarpScriptFunction(new PERMUTE(PERMUTE));
    addNamedWarpScriptFunction(new CHECKSHAPE(CHECKSHAPE));
    addNamedWarpScriptFunction(new SHAPE(SHAPE));
    addNamedWarpScriptFunction(new HULLSHAPE(HULLSHAPE));
    addNamedWarpScriptFunction(new CORRELATE.Builder(CORRELATE));
    addNamedWarpScriptFunction(new SORT(SORT));
    addNamedWarpScriptFunction(new SORTBY(SORTBY));
    addNamedWarpScriptFunction(new SORTWITH(SORTWITH));
    addNamedWarpScriptFunction(new RSORT(RSORT));
    addNamedWarpScriptFunction(new LASTSORT(LASTSORT));
    addNamedWarpScriptFunction(new METASORT(METASORT));
    addNamedWarpScriptFunction(new VALUESORT(VALUESORT));
    addNamedWarpScriptFunction(new RVALUESORT(RVALUESORT));
    addNamedWarpScriptFunction(new LSORT(LSORT));
    addNamedWarpScriptFunction(new SHUFFLE(SHUFFLE));
    addNamedWarpScriptFunction(new MSORT(MSORT));
    addNamedWarpScriptFunction(new GROUPBY(GROUPBY));
    addNamedWarpScriptFunction(new FILTERBY(FILTERBY));
    addNamedWarpScriptFunction(new ACCELCACHE(ACCEL_CACHE, false));
    addNamedWarpScriptFunction(new ACCELCACHE(ACCEL_NOCACHE, true));
    addNamedWarpScriptFunction(new ACCELPERSIST(ACCEL_PERSIST, false));
    addNamedWarpScriptFunction(new ACCELPERSIST(ACCEL_NOPERSIST, true));
    addNamedWarpScriptFunction(new ACCELREPORT(ACCEL_REPORT));
    addNamedWarpScriptFunction(new UPDATE(UPDATE));
    addNamedWarpScriptFunction(new META(META));
    addNamedWarpScriptFunction(new META(METADIFF, true));    
    addNamedWarpScriptFunction(new DELETE(DELETE));
    addNamedWarpScriptFunction(new WEBCALL(WEBCALL));
    addNamedWarpScriptFunction(new MATCH(MATCH));
    addNamedWarpScriptFunction(new MATCHER(MATCHER));
    addNamedWarpScriptFunction(new REPLACE(REPLACE, false));
    addNamedWarpScriptFunction(new REPLACE(REPLACEALL, true));
    addNamedWarpScriptFunction(new REOPTALT(REOPTALT));
    
    addNamedWarpScriptFunction(new TEMPLATE(TEMPLATE));
    addNamedWarpScriptFunction(new TOTIMESTAMP(TOTIMESTAMP));

    addNamedWarpScriptFunction(new STRINGFORMAT(STRINGFORMAT));

    addNamedWarpScriptFunction(new DISCORDS(DISCORDS, true));
    addNamedWarpScriptFunction(new DISCORDS(ZDISCORDS, false));
    addNamedWarpScriptFunction(new INTEGRATE(INTEGRATE));
    
    addNamedWarpScriptFunction(new BUCKETSPAN(BUCKETSPAN));
    addNamedWarpScriptFunction(new BUCKETCOUNT(BUCKETCOUNT));
    addNamedWarpScriptFunction(new UNBUCKETIZE(UNBUCKETIZE));
    addNamedWarpScriptFunction(new UNBUCKETIZECALENDAR(UNBUCKETIZE_CALENDAR));
    addNamedWarpScriptFunction(new LASTBUCKET(LASTBUCKET));
    addNamedWarpScriptFunction(new NAME(NAME));
    addNamedWarpScriptFunction(new LABELS(LABELS));
    addNamedWarpScriptFunction(new ATTRIBUTES(ATTRIBUTES));
    addNamedWarpScriptFunction(new LASTACTIVITY(LASTACTIVITY));
    addNamedWarpScriptFunction(new TICKS(TICKS));
    addNamedWarpScriptFunction(new LOCATIONS(LOCATIONS));
    addNamedWarpScriptFunction(new LOCSTRINGS(LOCSTRINGS));
    addNamedWarpScriptFunction(new ELEVATIONS(ELEVATIONS));
    addNamedWarpScriptFunction(new VALUES(VALUES));
    addNamedWarpScriptFunction(new VALUESPLIT(VALUESPLIT));
    addNamedWarpScriptFunction(new TICKLIST(TICKLIST));
    addNamedWarpScriptFunction(new COMMONTICKS(COMMONTICKS));
    addNamedWarpScriptFunction(new GOLDWRAP(GOLDWRAP));
    addNamedWarpScriptFunction(new WRAP(WRAP));
    addNamedWarpScriptFunction(new WRAP(WRAPRAW, false, true, true));
    addNamedWarpScriptFunction(new WRAP(WRAPFAST, false, false, true));
    addNamedWarpScriptFunction(new WRAP(WRAPOPT, true));
    addNamedWarpScriptFunction(new WRAP(WRAPRAWOPT, true, true, true));
    addNamedWarpScriptFunction(new UNWRAP(UNWRAP));
    addNamedWarpScriptFunction(new UNWRAP(UNWRAPEMPTY, true));
    addNamedWarpScriptFunction(new UNWRAPSIZE(UNWRAPSIZE));
    addNamedWarpScriptFunction(new UNWRAPENCODER(UNWRAPENCODER));
    addNamedWarpScriptFunction(new WRAP(WRAPMV, true, true, true, true));
    addNamedWarpScriptFunction(new TOMVSTRING(TOMVSTRING));
    addNamedWarpScriptFunction(new MVSPLIT(MVTICKSPLIT, true));
    addNamedWarpScriptFunction(new MVSPLIT(MVINDEXSPLIT, false));
    addNamedWarpScriptFunction(new MVEXTRACT(MVVALUES, MVEXTRACT.ELEMENT.VALUE));
    addNamedWarpScriptFunction(new MVEXTRACT(MVLOCATIONS, MVEXTRACT.ELEMENT.LATLON));
    addNamedWarpScriptFunction(new MVEXTRACT(MVELEVATIONS, MVEXTRACT.ELEMENT.ELEVATION));
    addNamedWarpScriptFunction(new MVEXTRACT(MVTICKS, MVEXTRACT.ELEMENT.TICK));
    addNamedWarpScriptFunction(new MVEXTRACT(MVHHCODES, MVEXTRACT.ELEMENT.LOCATION));
    addNamedWarpScriptFunction(new PARSEVALUE(PARSEVALUE));
    
    //
    // Outlier detection
    //
    
    addNamedWarpScriptFunction(new THRESHOLDTEST(THRESHOLDTEST));
    addNamedWarpScriptFunction(new ZSCORETEST(ZSCORETEST));
    addNamedWarpScriptFunction(new GRUBBSTEST(GRUBBSTEST));
    addNamedWarpScriptFunction(new ESDTEST(ESDTEST));
    addNamedWarpScriptFunction(new STLESDTEST(STLESDTEST));
    addNamedWarpScriptFunction(new HYBRIDTEST(HYBRIDTEST));
    addNamedWarpScriptFunction(new HYBRIDTEST2(HYBRIDTEST2));
    
    //
    // Quaternion related functions
    //
    
    addNamedWarpScriptFunction(new TOQUATERNION(TOQ));
    addNamedWarpScriptFunction(new QUATERNIONTO(QTO));
    addNamedWarpScriptFunction(new QCONJUGATE(QCONJUGATE));
    addNamedWarpScriptFunction(new QDIVIDE(QDIVIDE));
    addNamedWarpScriptFunction(new QMULTIPLY(QMULTIPLY));
    addNamedWarpScriptFunction(new QROTATE(QROTATE));
    addNamedWarpScriptFunction(new QROTATION(QROTATION));
    addNamedWarpScriptFunction(new ROTATIONQ(ROTATIONQ));
    
    addNamedWarpScriptFunction(new ATINDEX(ATINDEX));
    addNamedWarpScriptFunction(new ATTICK(ATTICK));
    addNamedWarpScriptFunction(new ATBUCKET(ATBUCKET));
    
    addNamedWarpScriptFunction(new CLONE(CLONE));
    addNamedWarpScriptFunction(new DURATION(DURATION));
    addNamedWarpScriptFunction(new HUMANDURATION(HUMANDURATION));
    addNamedWarpScriptFunction(new ISODURATION(ISODURATION));
    addNamedWarpScriptFunction(new ISO8601(ISO8601));
    addNamedWarpScriptFunction(new NOTBEFORE(NOTBEFORE));
    addNamedWarpScriptFunction(new NOTAFTER(NOTAFTER));
    addNamedWarpScriptFunction(new TSELEMENTS(TSELEMENTS));
    addNamedWarpScriptFunction(new TSELEMENTS(TOTSELEMENTS));
    addNamedWarpScriptFunction(new FROMTSELEMENTS(TSELEMENTSTO));
    addNamedWarpScriptFunction(new ADDDAYS(ADDDAYS));
    addNamedWarpScriptFunction(new ADDDURATION(ADDDURATION));
    addNamedWarpScriptFunction(new ADDMONTHS(ADDMONTHS));
    addNamedWarpScriptFunction(new ADDYEARS(ADDYEARS));
    
    addNamedWarpScriptFunction(new QUANTIZE(QUANTIZE));
    addNamedWarpScriptFunction(new NBOUNDS(NBOUNDS));
    addNamedWarpScriptFunction(new LBOUNDS(LBOUNDS));
    
    //NFIRST -> Retain at most the N first values
    //NLAST -> Retain at most the N last values
    
    //
    // GTS manipulation frameworks
    //
    
    addNamedWarpScriptFunction(new BUCKETIZE(BUCKETIZE));
    addNamedWarpScriptFunction(new BUCKETIZECALENDAR(BUCKETIZE_CALENDAR));
    addNamedWarpScriptFunction(new MAP(MAP));
    addNamedWarpScriptFunction(new FILTER(FILTER, true));
    addNamedWarpScriptFunction(new APPLY(APPLY, true));
    addNamedWarpScriptFunction(new FILTER(PFILTER, false));
    addNamedWarpScriptFunction(new APPLY(PAPPLY, false));
    addNamedWarpScriptFunction(new REDUCE(REDUCE, true));
    addNamedWarpScriptFunction(new REDUCE(PREDUCE, false));
    addNamedWarpScriptFunction(new PIVOT(PIVOT, false));
    addNamedWarpScriptFunction(new PIVOT(PIVOTSTRICT, true));    
    addNamedWarpScriptFunction(new MaxTickSlidingWindow("max.tick.sliding.window"));
    addNamedWarpScriptFunction(new MaxTimeSlidingWindow("max.time.sliding.window"));
    addNamedWarpScriptFunction(new NULL(NULL));
    addNamedWarpScriptFunction(new ISNULL(ISNULL));
    addNamedWarpScriptFunction(new MapperReplace.Builder("mapper.replace"));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GT, CompareTo.Compared.VALUE, CompareTo.Comparison.GT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GE, CompareTo.Compared.VALUE, CompareTo.Comparison.GE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_EQ, CompareTo.Compared.VALUE, CompareTo.Comparison.EQ));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_NE, CompareTo.Compared.VALUE, CompareTo.Comparison.NE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LE, CompareTo.Compared.VALUE, CompareTo.Comparison.LE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LT, CompareTo.Compared.VALUE, CompareTo.Comparison.LT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GT_TICK, CompareTo.Compared.TICK, CompareTo.Comparison.GT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GE_TICK, CompareTo.Compared.TICK, CompareTo.Comparison.GE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_EQ_TICK, CompareTo.Compared.TICK, CompareTo.Comparison.EQ));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_NE_TICK, CompareTo.Compared.TICK, CompareTo.Comparison.NE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LE_TICK, CompareTo.Compared.TICK, CompareTo.Comparison.LE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LT_TICK, CompareTo.Compared.TICK, CompareTo.Comparison.LT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GT_LAT, CompareTo.Compared.LAT, CompareTo.Comparison.GT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GE_LAT, CompareTo.Compared.LAT, CompareTo.Comparison.GE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_EQ_LAT, CompareTo.Compared.LAT, CompareTo.Comparison.EQ));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_NE_LAT, CompareTo.Compared.LAT, CompareTo.Comparison.NE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LE_LAT, CompareTo.Compared.LAT, CompareTo.Comparison.LE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LT_LAT, CompareTo.Compared.LAT, CompareTo.Comparison.LT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GT_LON, CompareTo.Compared.LON, CompareTo.Comparison.GT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GE_LON, CompareTo.Compared.LON, CompareTo.Comparison.GE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_EQ_LON, CompareTo.Compared.LON, CompareTo.Comparison.EQ));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_NE_LON, CompareTo.Compared.LON, CompareTo.Comparison.NE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LE_LON, CompareTo.Compared.LON, CompareTo.Comparison.LE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LT_LON, CompareTo.Compared.LON, CompareTo.Comparison.LT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GT_HHCODE, CompareTo.Compared.HHCODE, CompareTo.Comparison.GT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GE_HHCODE, CompareTo.Compared.HHCODE, CompareTo.Comparison.GE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_EQ_HHCODE, CompareTo.Compared.HHCODE, CompareTo.Comparison.EQ));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_NE_HHCODE, CompareTo.Compared.HHCODE, CompareTo.Comparison.NE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LE_HHCODE, CompareTo.Compared.HHCODE, CompareTo.Comparison.LE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LT_HHCODE, CompareTo.Compared.HHCODE, CompareTo.Comparison.LT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GT_ELEV, CompareTo.Compared.ELEV, CompareTo.Comparison.GT));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_GE_ELEV, CompareTo.Compared.ELEV, CompareTo.Comparison.GE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_EQ_ELEV, CompareTo.Compared.ELEV, CompareTo.Comparison.EQ));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_NE_ELEV, CompareTo.Compared.ELEV, CompareTo.Comparison.NE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LE_ELEV, CompareTo.Compared.ELEV, CompareTo.Comparison.LE));
    addNamedWarpScriptFunction(new MapperCompareTo(MAPPER_LT_ELEV, CompareTo.Compared.ELEV, CompareTo.Comparison.LT));
    addNamedWarpScriptFunction(new MapperAdd.Builder("mapper.add"));
    addNamedWarpScriptFunction(new MapperMul.Builder("mapper.mul"));
    addNamedWarpScriptFunction(new MapperPow.Builder("mapper.pow"));
    try {
      addNamedWarpScriptFunction(new MapperPow("mapper.sqrt", 0.5D));
    } catch (WarpScriptException wse) {
      throw new RuntimeException(wse);
    }
    addNamedWarpScriptFunction(new MapperExp.Builder("mapper.exp"));
    addNamedWarpScriptFunction(new MapperLog.Builder("mapper.log"));
    addNamedWarpScriptFunction(new MapperMinX.Builder("mapper.min.x"));
    addNamedWarpScriptFunction(new MapperMaxX.Builder("mapper.max.x"));
    addNamedWarpScriptFunction(new MapperParseDouble.Builder("mapper.parsedouble"));
    
    addNamedWarpScriptFunction(new MapperTick.Builder("mapper.tick"));
    addNamedWarpScriptFunction(new MapperYear.Builder("mapper.year"));
    addNamedWarpScriptFunction(new MapperMonthOfYear.Builder("mapper.month"));
    addNamedWarpScriptFunction(new MapperDayOfMonth.Builder("mapper.day"));
    addNamedWarpScriptFunction(new MapperDayOfWeek.Builder("mapper.weekday"));
    addNamedWarpScriptFunction(new MapperHourOfDay.Builder("mapper.hour"));
    addNamedWarpScriptFunction(new MapperMinuteOfHour.Builder("mapper.minute"));
    addNamedWarpScriptFunction(new MapperSecondOfMinute.Builder("mapper.second"));

    addNamedWarpScriptFunction(new MapperNPDF.Builder("mapper.npdf"));
    addNamedWarpScriptFunction(new MapperDotProduct.Builder("mapper.dotproduct"));

    addNamedWarpScriptFunction(new MapperDotProductTanh.Builder("mapper.dotproduct.tanh"));
    addNamedWarpScriptFunction(new MapperDotProductSigmoid.Builder("mapper.dotproduct.sigmoid"));
    addNamedWarpScriptFunction(new MapperDotProductPositive.Builder("mapper.dotproduct.positive"));

    // Kernel mappers
    addNamedWarpScriptFunction(new MapperKernelCosine("mapper.kernel.cosine"));
    addNamedWarpScriptFunction(new MapperKernelEpanechnikov("mapper.kernel.epanechnikov"));
    addNamedWarpScriptFunction(new MapperKernelGaussian("mapper.kernel.gaussian"));
    addNamedWarpScriptFunction(new MapperKernelLogistic("mapper.kernel.logistic"));
    addNamedWarpScriptFunction(new MapperKernelQuartic("mapper.kernel.quartic"));
    addNamedWarpScriptFunction(new MapperKernelSilverman("mapper.kernel.silverman"));
    addNamedWarpScriptFunction(new MapperKernelTriangular("mapper.kernel.triangular"));
    addNamedWarpScriptFunction(new MapperKernelTricube("mapper.kernel.tricube"));
    addNamedWarpScriptFunction(new MapperKernelTriweight("mapper.kernel.triweight"));
    addNamedWarpScriptFunction(new MapperKernelUniform("mapper.kernel.uniform"));

    addNamedWarpScriptFunction(new Percentile.Builder("mapper.percentile", false));
    addNamedWarpScriptFunction(new Percentile.Builder("mapper.percentile.forbid-nulls", true));

    //functions.put("mapper.abscissa", new MapperSAX.Builder());
    
    addNamedWarpScriptFunction(new FilterByClass.Builder("filter.byclass"));
    addNamedWarpScriptFunction(new FilterByLabels.Builder("filter.bylabels", true, false));
    addNamedWarpScriptFunction(new FilterByLabels.Builder("filter.byattr", false, true));
    addNamedWarpScriptFunction(new FilterByLabels.Builder("filter.bylabelsattr", true, true));
    addNamedWarpScriptFunction(new FilterByMetadata.Builder("filter.bymetadata"));
    addNamedWarpScriptFunction(new FilterBySelector.Builder("filter.byselector"));
    addNamedWarpScriptFunction(new FilterBySize.Builder("filter.bysize"));

    addNamedWarpScriptFunction(new FilterLastEQ.Builder("filter.last.eq"));
    addNamedWarpScriptFunction(new FilterLastGE.Builder("filter.last.ge"));
    addNamedWarpScriptFunction(new FilterLastGT.Builder("filter.last.gt"));
    addNamedWarpScriptFunction(new FilterLastLE.Builder("filter.last.le"));
    addNamedWarpScriptFunction(new FilterLastLT.Builder("filter.last.lt"));
    addNamedWarpScriptFunction(new FilterLastNE.Builder("filter.last.ne"));

    addNamedWarpScriptFunction(new FilterAny.Builder("filter.any.eq", FilterAny.Comparator.EQ));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.any.ge", FilterAny.Comparator.GE));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.any.gt", FilterAny.Comparator.GT));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.any.le", FilterAny.Comparator.LE));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.any.lt", FilterAny.Comparator.LT));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.any.ne", FilterAny.Comparator.NE));

    addNamedWarpScriptFunction(new FilterAny.Builder("filter.all.ne", FilterAny.Comparator.EQ, true));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.all.lt", FilterAny.Comparator.GE, true));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.all.le", FilterAny.Comparator.GT, true));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.all.gt", FilterAny.Comparator.LE, true));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.all.ge", FilterAny.Comparator.LT, true));
    addNamedWarpScriptFunction(new FilterAny.Builder("filter.all.eq", FilterAny.Comparator.NE, true));

    addNamedWarpScriptFunction(new LatencyFilter.Builder("filter.latencies"));
    
    //
    // Fillers
    //
    
    addNamedWarpScriptFunction(new FillerNext("filler.next"));
    addNamedWarpScriptFunction(new FillerPrevious("filler.previous"));
    addNamedWarpScriptFunction(new FillerInterpolate("filler.interpolate"));
    addNamedWarpScriptFunction(new FillerTrend("filler.trend"));
 
    //
    // Geo Manipulation functions
    //
    
    addNamedWarpScriptFunction(new TOHHCODE(TOHHCODE, true));
    addNamedWarpScriptFunction(new TOHHCODE(TOHHCODELONG, false));
    addNamedWarpScriptFunction(new TOHHCODE(TOGTSHHCODE, true, true));
    addNamedWarpScriptFunction(new TOHHCODE(TOGTSHHCODELONG, false, true));
    addNamedWarpScriptFunction(new HHCODETO(HHCODETO));
    addNamedWarpScriptFunction(new HHCODETO(GTSHHCODETO, true));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_BBOX, HHCODEFUNC.HHCodeAction.BBOX));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_CENTER, HHCODEFUNC.HHCodeAction.CENTER));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_NORTH, HHCODEFUNC.HHCodeAction.NORTH));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_SOUTH, HHCODEFUNC.HHCodeAction.SOUTH));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_EAST, HHCODEFUNC.HHCodeAction.EAST));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_WEST, HHCODEFUNC.HHCodeAction.WEST));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_NORTH_EAST, HHCODEFUNC.HHCodeAction.NORTH_EAST));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_NORTH_WEST, HHCODEFUNC.HHCodeAction.NORTH_WEST));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_SOUTH_EAST, HHCODEFUNC.HHCodeAction.SOUTH_EAST));
    addNamedWarpScriptFunction(new HHCODEFUNC(HHCODE_SOUTH_WEST, HHCODEFUNC.HHCodeAction.SOUTH_WEST));
    addNamedWarpScriptFunction(new GEOREGEXP(GEO_REGEXP));
    addNamedWarpScriptFunction(new GeoWKT(GEO_WKT, false));
    addNamedWarpScriptFunction(new GeoWKT(GEO_WKT_UNIFORM, true));
    addNamedWarpScriptFunction(new GeoWKB(GEO_WKB, false));
    addNamedWarpScriptFunction(new GeoWKB(GEO_WKB_UNIFORM, true));
    addNamedWarpScriptFunction(new GeoJSON(GEO_JSON, false));
    addNamedWarpScriptFunction(new GeoJSON(GEO_JSON_UNIFORM, true));
    addNamedWarpScriptFunction(new TOGEOJSON(TOGEOJSON));
    addNamedWarpScriptFunction(new GEOOPTIMIZE(GEO_OPTIMIZE));
    addNamedWarpScriptFunction(new GEONORMALIZE(GEO_NORMALIZE));
    addNamedWarpScriptFunction(new GEOSHIFT(GEOSHIFT));
    addNamedWarpScriptFunction(new GeoIntersection(GEO_INTERSECTION));
    addNamedWarpScriptFunction(new GeoUnion(GEO_UNION));
    addNamedWarpScriptFunction(new GeoSubtraction(GEO_DIFFERENCE));
    addNamedWarpScriptFunction(new GEOWITHIN(GEO_WITHIN));
    addNamedWarpScriptFunction(new GEOINTERSECTS(GEO_INTERSECTS));
    addNamedWarpScriptFunction(new HAVERSINE(HAVERSINE));
    addNamedWarpScriptFunction(new GEOPACK(GEOPACK));
    addNamedWarpScriptFunction(new GEOUNPACK(GEOUNPACK));
    addNamedWarpScriptFunction(new MapperGeoWithin.Builder("mapper.geo.within"));
    addNamedWarpScriptFunction(new MapperGeoOutside.Builder("mapper.geo.outside"));
    addNamedWarpScriptFunction(new MapperGeoApproximate.Builder("mapper.geo.approximate"));
    addNamedWarpScriptFunction(new COPYGEO(COPYGEO));
    addNamedWarpScriptFunction(new BBOX(BBOX));
    addNamedWarpScriptFunction(new TOGEOHASH(TOGEOHASH));
    addNamedWarpScriptFunction(new GEOHASHTO(GEOHASHTO));
    addNamedWarpScriptFunction(new GEOCOVER(GEO_COVER, false));
    addNamedWarpScriptFunction(new GEOCOVER(GEO_COVER_RL, true));
    addNamedWarpScriptFunction(new GEOSPLIT(GEOSPLIT));
    
    //
    // Counters
    //
    
    addNamedWarpScriptFunction(new COUNTER(COUNTER));
    addNamedWarpScriptFunction(new COUNTERVALUE(COUNTERVALUE));
    addNamedWarpScriptFunction(new COUNTERDELTA(COUNTERDELTA));
    addNamedWarpScriptFunction(new COUNTERSET(COUNTERSET));

    //
    // Math functions
    //
    
    addNamedWarpScriptFunction(new Pi(PI_LOWERCASE));
    addNamedWarpScriptFunction(new Pi(PI));
    addNamedWarpScriptFunction(new E(E_LOWERCASE));
    addNamedWarpScriptFunction(new E(E));
    addNamedWarpScriptFunction(new MINLONG(MINLONG));
    addNamedWarpScriptFunction(new MAXLONG(MAXLONG));
    addNamedWarpScriptFunction(new RAND(RAND));
    addNamedWarpScriptFunction(new PRNG(PRNG));
    addNamedWarpScriptFunction(new SRAND(SRAND));

    addNamedWarpScriptFunction(new NPDF.Builder(NPDF));
    addNamedWarpScriptFunction(new MUSIGMA(MUSIGMA));
    addNamedWarpScriptFunction(new KURTOSIS(KURTOSIS));
    addNamedWarpScriptFunction(new SKEWNESS(SKEWNESS));
    addNamedWarpScriptFunction(new NSUMSUMSQ(NSUMSUMSQ));
    addNamedWarpScriptFunction(new LR(LR));
    addNamedWarpScriptFunction(new MODE(MODE));
    
    addNamedWarpScriptFunction(new TOZ(TOZ));
    addNamedWarpScriptFunction(new ZTO(ZTO));
    addNamedWarpScriptFunction(new PACK(PACK));
    addNamedWarpScriptFunction(new UNPACK(UNPACK));
    
    //
    // Linear Algebra
    //
    
    addNamedWarpScriptFunction(new TOMAT(TOMAT));
    addNamedWarpScriptFunction(new MATTO(MATTO));
    addNamedWarpScriptFunction(new TR(TR));
    addNamedWarpScriptFunction(new TRANSPOSE(TRANSPOSE));
    addNamedWarpScriptFunction(new DET(DET));
    addNamedWarpScriptFunction(new INV(INV));
    addNamedWarpScriptFunction(new TOVEC(TOVEC));
    addNamedWarpScriptFunction(new VECTO(VECTO));

    addNamedWarpScriptFunction(new COS(COS));
    addNamedWarpScriptFunction(new COSH(COSH));
    addNamedWarpScriptFunction(new ACOS(ACOS));

    addNamedWarpScriptFunction(new SIN(SIN));
    addNamedWarpScriptFunction(new SINH(SINH));
    addNamedWarpScriptFunction(new ASIN(ASIN));

    addNamedWarpScriptFunction(new TAN(TAN));
    addNamedWarpScriptFunction(new TANH(TANH));
    addNamedWarpScriptFunction(new ATAN(ATAN));

    addNamedWarpScriptFunction(new SIGNUM(SIGNUM));
    addNamedWarpScriptFunction(new FLOOR(FLOOR));
    addNamedWarpScriptFunction(new CEIL(CEIL));
    addNamedWarpScriptFunction(new ROUND(ROUND));

    addNamedWarpScriptFunction(new RINT(RINT));
    addNamedWarpScriptFunction(new NEXTUP(NEXTUP));
    addNamedWarpScriptFunction(new ULP(ULP));

    addNamedWarpScriptFunction(new SQRT(SQRT));
    addNamedWarpScriptFunction(new CBRT(CBRT));
    addNamedWarpScriptFunction(new EXP(EXP));
    addNamedWarpScriptFunction(new EXPM1(EXPM1));
    addNamedWarpScriptFunction(new LOG(LOG_));
    addNamedWarpScriptFunction(new LOG10(LOG10));
    addNamedWarpScriptFunction(new LOG1P(LOG1P));

    addNamedWarpScriptFunction(new TORADIANS(TORADIANS));
    addNamedWarpScriptFunction(new TODEGREES(TODEGREES));

    addNamedWarpScriptFunction(new MAX(MAX));
    addNamedWarpScriptFunction(new MIN(MIN));

    addNamedWarpScriptFunction(new COPYSIGN(COPYSIGN));
    addNamedWarpScriptFunction(new HYPOT(HYPOT));
    addNamedWarpScriptFunction(new IEEEREMAINDER(IEEEREMAINDER));
    addNamedWarpScriptFunction(new NEXTAFTER(NEXTAFTER));
    addNamedWarpScriptFunction(new ATAN2(ATAN2));

    addNamedWarpScriptFunction(new FLOORDIV(FLOORDIV));
    addNamedWarpScriptFunction(new FLOORMOD(FLOORMOD));

    addNamedWarpScriptFunction(new ADDEXACT(ADDEXACT));
    addNamedWarpScriptFunction(new SUBTRACTEXACT(SUBTRACTEXACT));
    addNamedWarpScriptFunction(new MULTIPLYEXACT(MULTIPLYEXACT));
    addNamedWarpScriptFunction(new INCREMENTEXACT(INCREMENTEXACT));
    addNamedWarpScriptFunction(new DECREMENTEXACT(DECREMENTEXACT));
    addNamedWarpScriptFunction(new NEGATEEXACT(NEGATEEXACT));
    addNamedWarpScriptFunction(new TOINTEXACT(TOINTEXACT));

    addNamedWarpScriptFunction(new SCALB(SCALB));
    addNamedWarpScriptFunction(new RANDOM(RANDOM));
    addNamedWarpScriptFunction(new NEXTDOWN(NEXTDOWN));
    addNamedWarpScriptFunction(new GETEXPONENT(GETEXPONENT));
    
    addNamedWarpScriptFunction(new IDENT(IDENT));
    
    //
    // Processing
    //

    addNamedWarpScriptFunction(new Pencode(PENCODE));

    // Structure
    
    addNamedWarpScriptFunction(new PpushStyle(PPUSHSTYLE));
    addNamedWarpScriptFunction(new PpopStyle(PPOPSTYLE));

    // Environment
    
    
    // Shape
    
    addNamedWarpScriptFunction(new Parc(PARC));
    addNamedWarpScriptFunction(new Pellipse(PELLIPSE));
    addNamedWarpScriptFunction(new Ppoint(PPOINT));
    addNamedWarpScriptFunction(new Pline(PLINE));
    addNamedWarpScriptFunction(new Ptriangle(PTRIANGLE));
    addNamedWarpScriptFunction(new Prect(PRECT));
    addNamedWarpScriptFunction(new Pquad(PQUAD));
    
    addNamedWarpScriptFunction(new Pbezier(PBEZIER));
    addNamedWarpScriptFunction(new PbezierPoint(PBEZIERPOINT));
    addNamedWarpScriptFunction(new PbezierTangent(PBEZIERTANGENT));
    addNamedWarpScriptFunction(new PbezierDetail(PBEZIERDETAIL));
    
    addNamedWarpScriptFunction(new Pcurve(PCURVE));
    addNamedWarpScriptFunction(new PcurvePoint(PCURVEPOINT));
    addNamedWarpScriptFunction(new PcurveTangent(PCURVETANGENT));
    addNamedWarpScriptFunction(new PcurveDetail(PCURVEDETAIL));
    addNamedWarpScriptFunction(new PcurveTightness(PCURVETIGHTNESS));

    addNamedWarpScriptFunction(new Pbox(PBOX));
    addNamedWarpScriptFunction(new Psphere(PSPHERE));
    addNamedWarpScriptFunction(new PsphereDetail(PSPHEREDETAIL));
    
    addNamedWarpScriptFunction(new PellipseMode(PELLIPSEMODE));
    addNamedWarpScriptFunction(new PrectMode(PRECTMODE));
    addNamedWarpScriptFunction(new PstrokeCap(PSTROKECAP));
    addNamedWarpScriptFunction(new PstrokeJoin(PSTROKEJOIN));
    addNamedWarpScriptFunction(new PstrokeWeight(PSTROKEWEIGHT));
    
    addNamedWarpScriptFunction(new PbeginShape(PBEGINSHAPE));
    addNamedWarpScriptFunction(new PendShape(PENDSHAPE));
    addNamedWarpScriptFunction(new PloadShape(PLOADSHAPE));
    addNamedWarpScriptFunction(new PbeginContour(PBEGINCONTOUR));
    addNamedWarpScriptFunction(new PendContour(PENDCONTOUR));
    addNamedWarpScriptFunction(new Pvertex(PVERTEX));
    addNamedWarpScriptFunction(new PcurveVertex(PCURVEVERTEX));
    addNamedWarpScriptFunction(new PbezierVertex(PBEZIERVERTEX));
    addNamedWarpScriptFunction(new PquadraticVertex(PQUADRATICVERTEX));
    
    // TODO(hbs): support PShape (need to support PbeginShape etc applied to PShape instances)
    addNamedWarpScriptFunction(new PshapeMode(PSHAPEMODE));
    addNamedWarpScriptFunction(new Pshape(PSHAPE));
    
    // Transform
    
    addNamedWarpScriptFunction(new PpushMatrix(PPUSHMATRIX));
    addNamedWarpScriptFunction(new PpopMatrix(PPOPMATRIX));
    addNamedWarpScriptFunction(new PresetMatrix(PRESETMATRIX));
    addNamedWarpScriptFunction(new Protate(PROTATE));
    addNamedWarpScriptFunction(new ProtateX(PROTATEX));
    addNamedWarpScriptFunction(new ProtateY(PROTATEY));
    addNamedWarpScriptFunction(new ProtateZ(PROTATEZ));
    addNamedWarpScriptFunction(new Pscale(PSCALE));
    addNamedWarpScriptFunction(new PshearX(PSHEARX));
    addNamedWarpScriptFunction(new PshearY(PSHEARY));
    addNamedWarpScriptFunction(new Ptranslate(PTRANSLATE));
    
    // Color
    
    addNamedWarpScriptFunction(new Pbackground(PBACKGROUND));
    addNamedWarpScriptFunction(new PcolorMode(PCOLORMODE));
    addNamedWarpScriptFunction(new Pclear(PCLEAR));
    addNamedWarpScriptFunction(new Pfill(PFILL));
    addNamedWarpScriptFunction(new PnoFill(PNOFILL));
    addNamedWarpScriptFunction(new Pstroke(PSTROKE));
    addNamedWarpScriptFunction(new PnoStroke(PNOSTROKE));
    
    addNamedWarpScriptFunction(new Palpha(PALPHA));
    addNamedWarpScriptFunction(new Pblue(PBLUE));
    addNamedWarpScriptFunction(new Pbrightness(PBRIGHTNESS));
    addNamedWarpScriptFunction(new Pcolor(PCOLOR));
    addNamedWarpScriptFunction(new Pgreen(PGREEN));
    addNamedWarpScriptFunction(new Phue(PHUE));
    addNamedWarpScriptFunction(new PlerpColor(PLERPCOLOR));
    addNamedWarpScriptFunction(new Pred(PRED));
    addNamedWarpScriptFunction(new Psaturation(PSATURATION));
    
    // Image
    
    addNamedWarpScriptFunction(new Pdecode(PDECODE));
    addNamedWarpScriptFunction(new Pimage(PIMAGE));
    addNamedWarpScriptFunction(new PimageMode(PIMAGEMODE));
    addNamedWarpScriptFunction(new Ptint(PTINT));
    addNamedWarpScriptFunction(new PnoTint(PNOTINT));
    addNamedWarpScriptFunction(new Ppixels(PPIXELS));
    addNamedWarpScriptFunction(new PupdatePixels(PUPDATEPIXELS));
    addNamedWarpScriptFunction(new PtoImage(PTOIMAGE));
    
    // TODO(hbs): support texture related functions?
    
    addNamedWarpScriptFunction(new Pblend(PBLEND));
    addNamedWarpScriptFunction(new Pcopy(PCOPY));
    addNamedWarpScriptFunction(new Pget(PGET));
    addNamedWarpScriptFunction(new Pset(PSET));
    addNamedWarpScriptFunction(new Pfilter(PFILTER_));

    // Rendering
    
    addNamedWarpScriptFunction(new PblendMode(PBLENDMODE));
    addNamedWarpScriptFunction(new Pclip(PCLIP));
    addNamedWarpScriptFunction(new PnoClip(PNOCLIP));
    addNamedWarpScriptFunction(new PGraphics(PGRAPHICS));

    // TODO(hbs): support shaders?
    
    // Typography
    
    addNamedWarpScriptFunction(new PcreateFont(PCREATEFONT));
    addNamedWarpScriptFunction(new Ptext(PTEXT));
    addNamedWarpScriptFunction(new PtextAlign(PTEXTALIGN));
    addNamedWarpScriptFunction(new PtextAscent(PTEXTASCENT));
    addNamedWarpScriptFunction(new PtextDescent(PTEXTDESCENT));
    addNamedWarpScriptFunction(new PtextFont(PTEXTFONT));
    addNamedWarpScriptFunction(new PtextLeading(PTEXTLEADING));
    addNamedWarpScriptFunction(new PtextMode(PTEXTMODE));
    addNamedWarpScriptFunction(new PtextSize(PTEXTSIZE));
    addNamedWarpScriptFunction(new PtextWidth(PTEXTWIDTH));
    
    // Math
    
    addNamedWarpScriptFunction(new Pconstrain(PCONSTRAIN));
    addNamedWarpScriptFunction(new Pdist(PDIST));
    addNamedWarpScriptFunction(new Plerp(PLERP));
    addNamedWarpScriptFunction(new Pmag(PMAG));
    addNamedWarpScriptFunction(new Pmap(PMAP));
    addNamedWarpScriptFunction(new Pnorm(PNORM));
    
    ////////////////////////////////////////////////////////////////////////////
    
    //
    // Moved from JavaLibrary
    //
    /////////////////////////
    
    //
    // Bucketizers
    //

    addNamedWarpScriptFunction(new And("bucketizer.and", false));
    addNamedWarpScriptFunction(new First("bucketizer.first"));
    addNamedWarpScriptFunction(new Last("bucketizer.last"));
    addNamedWarpScriptFunction(new Min("bucketizer.min", true));
    addNamedWarpScriptFunction(new Max("bucketizer.max", true));
    addNamedWarpScriptFunction(new Mean("bucketizer.mean", false));
    addNamedWarpScriptFunction(new Median("bucketizer.median", false));
    addNamedWarpScriptFunction(new Median("bucketizer.median.forbid-nulls", true));
    addNamedWarpScriptFunction(new MAD("bucketizer.mad"));
    addNamedWarpScriptFunction(new Or("bucketizer.or", false));
    addNamedWarpScriptFunction(new Sum("bucketizer.sum", true));
    addNamedWarpScriptFunction(new Join.Builder("bucketizer.join", true, false, null));
    addNamedWarpScriptFunction(new Count("bucketizer.count", false));
    addNamedWarpScriptFunction(new Percentile.Builder("bucketizer.percentile", false));
    addNamedWarpScriptFunction(new Percentile.Builder("bucketizer.percentile.forbid-nulls", true));

    addNamedWarpScriptFunction(new Min("bucketizer.min.forbid-nulls", false));
    addNamedWarpScriptFunction(new Max("bucketizer.max.forbid-nulls", false));
    addNamedWarpScriptFunction(new Mean("bucketizer.mean.exclude-nulls", true));
    addNamedWarpScriptFunction(new Sum("bucketizer.sum.forbid-nulls", false));
    addNamedWarpScriptFunction(new Join.Builder("bucketizer.join.forbid-nulls", false, false, null));
    addNamedWarpScriptFunction(new Count("bucketizer.count.exclude-nulls", true));
    addNamedWarpScriptFunction(new Count("bucketizer.count.include-nulls", false));
    addNamedWarpScriptFunction(new Count("bucketizer.count.nonnull", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("bucketizer.mean.circular", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("bucketizer.mean.circular.exclude-nulls", false));
    addNamedWarpScriptFunction(new RMS("bucketizer.rms", false));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("bucketizer.sd", false));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("bucketizer.sd.forbid-nulls", true));

    //
    // Mappers
    //

    addNamedWarpScriptFunction(new And("mapper.and", false));
    addNamedWarpScriptFunction(new Count("mapper.count", false));
    addNamedWarpScriptFunction(new First("mapper.first"));
    addNamedWarpScriptFunction(new Last("mapper.last"));
    addNamedWarpScriptFunction(new Min(MAPPER_MIN, true));
    addNamedWarpScriptFunction(new Max(MAPPER_MAX, true));
    addNamedWarpScriptFunction(new Mean("mapper.mean", false));
    addNamedWarpScriptFunction(new Median("mapper.median", false));
    addNamedWarpScriptFunction(new Median("mapper.median.forbid-nulls", true));
    addNamedWarpScriptFunction(new MAD("mapper.mad"));
    addNamedWarpScriptFunction(new Or("mapper.or", false));
    addNamedWarpScriptFunction(new Highest(MAPPER_HIGHEST));
    addNamedWarpScriptFunction(new Lowest(MAPPER_LOWEST));
    addNamedWarpScriptFunction(new Sum("mapper.sum", true));
    addNamedWarpScriptFunction(new Join.Builder("mapper.join", true, false, null));
    addNamedWarpScriptFunction(new Delta("mapper.delta"));
    addNamedWarpScriptFunction(new Rate("mapper.rate"));
    addNamedWarpScriptFunction(new HSpeed("mapper.hspeed"));
    addNamedWarpScriptFunction(new HDist("mapper.hdist"));
    addNamedWarpScriptFunction(new TrueCourse("mapper.truecourse"));
    addNamedWarpScriptFunction(new VSpeed("mapper.vspeed"));
    addNamedWarpScriptFunction(new VDist("mapper.vdist"));
    addNamedWarpScriptFunction(new Variance.Builder("mapper.var", false));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("mapper.sd", false));
    addNamedWarpScriptFunction(new MapperAbs("mapper.abs"));
    addNamedWarpScriptFunction(new MapperCeil("mapper.ceil"));
    addNamedWarpScriptFunction(new MapperFloor("mapper.floor"));
    addNamedWarpScriptFunction(new MapperFinite("mapper.finite"));
    addNamedWarpScriptFunction(new MapperRound("mapper.round"));
    addNamedWarpScriptFunction(new MapperToBoolean("mapper.toboolean"));
    addNamedWarpScriptFunction(new MapperToLong("mapper.tolong"));
    addNamedWarpScriptFunction(new MapperToDouble("mapper.todouble"));
    addNamedWarpScriptFunction(new MapperToString("mapper.tostring"));
    addNamedWarpScriptFunction(new MapperTanh("mapper.tanh"));
    addNamedWarpScriptFunction(new MapperSigmoid("mapper.sigmoid"));
    addNamedWarpScriptFunction(new MapperProduct("mapper.product"));
    addNamedWarpScriptFunction(new MapperGeoClearPosition("mapper.geo.clear"));
    addNamedWarpScriptFunction(new Count("mapper.count.exclude-nulls", true));
    addNamedWarpScriptFunction(new Count("mapper.count.include-nulls", false));
    addNamedWarpScriptFunction(new Count("mapper.count.nonnull", true));
    addNamedWarpScriptFunction(new Min("mapper.min.forbid-nulls", false));
    addNamedWarpScriptFunction(new Max("mapper.max.forbid-nulls", false));
    addNamedWarpScriptFunction(new Mean("mapper.mean.exclude-nulls", true));
    addNamedWarpScriptFunction(new Sum("mapper.sum.forbid-nulls", false));
    addNamedWarpScriptFunction(new Join.Builder("mapper.join.forbid-nulls", false, false, null));
    addNamedWarpScriptFunction(new Variance.Builder("mapper.var.forbid-nulls", true));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("mapper.sd.forbid-nulls", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("mapper.mean.circular", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("mapper.mean.circular.exclude-nulls", false));
    addNamedWarpScriptFunction(new MapperMod.Builder("mapper.mod"));
    addNamedWarpScriptFunction(new RMS("mapper.rms", false));

    //
    // Reducers
    //

    addNamedWarpScriptFunction(new And("reducer.and", false));
    addNamedWarpScriptFunction(new And("reducer.and.exclude-nulls", true));
    addNamedWarpScriptFunction(new Min("reducer.min", true));
    addNamedWarpScriptFunction(new Min("reducer.min.forbid-nulls", false));
    addNamedWarpScriptFunction(new Min("reducer.min.nonnull", false));
    addNamedWarpScriptFunction(new Max("reducer.max", true));
    addNamedWarpScriptFunction(new Max("reducer.max.forbid-nulls", false));
    addNamedWarpScriptFunction(new Max("reducer.max.nonnull", false));
    addNamedWarpScriptFunction(new Mean("reducer.mean", false));
    addNamedWarpScriptFunction(new Mean("reducer.mean.exclude-nulls", true));
    addNamedWarpScriptFunction(new Median("reducer.median", false));
    addNamedWarpScriptFunction(new Median("reducer.median.forbid-nulls", true));
    addNamedWarpScriptFunction(new MAD("reducer.mad"));
    addNamedWarpScriptFunction(new Or("reducer.or", false));
    addNamedWarpScriptFunction(new Or("reducer.or.exclude-nulls", true));
    addNamedWarpScriptFunction(new Sum("reducer.sum", true));
    addNamedWarpScriptFunction(new Sum("reducer.sum.forbid-nulls", false));
    addNamedWarpScriptFunction(new Sum("reducer.sum.nonnull", false));
    addNamedWarpScriptFunction(new Join.Builder("reducer.join", true, false, null));
    addNamedWarpScriptFunction(new Join.Builder("reducer.join.forbid-nulls", false, false, null));
    addNamedWarpScriptFunction(new Join.Builder("reducer.join.nonnull", false, false, null));
    addNamedWarpScriptFunction(new Join.Builder("reducer.join.urlencoded", false, true, ""));
    addNamedWarpScriptFunction(new Variance.Builder("reducer.var", false));
    addNamedWarpScriptFunction(new Variance.Builder("reducer.var.forbid-nulls", false));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("reducer.sd", false));
    addNamedWarpScriptFunction(new StandardDeviation.Builder("reducer.sd.forbid-nulls", false));
    addNamedWarpScriptFunction(new Argminmax.Builder("reducer.argmin", true));
    addNamedWarpScriptFunction(new Argminmax.Builder("reducer.argmax", false));
    addNamedWarpScriptFunction(new MapperProduct("reducer.product"));
    addNamedWarpScriptFunction(new Count("reducer.count", false));
    addNamedWarpScriptFunction(new Count("reducer.count.include-nulls", false));
    addNamedWarpScriptFunction(new Count("reducer.count.exclude-nulls", true));
    addNamedWarpScriptFunction(new Count("reducer.count.nonnull", true));
    addNamedWarpScriptFunction(new ShannonEntropy("reducer.shannonentropy.0", false));
    addNamedWarpScriptFunction(new ShannonEntropy("reducer.shannonentropy.1", true));
    addNamedWarpScriptFunction(new Percentile.Builder("reducer.percentile", false));
    addNamedWarpScriptFunction(new Percentile.Builder("reducer.percentile.forbid-nulls", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("reducer.mean.circular", true));
    addNamedWarpScriptFunction(new CircularMean.Builder("reducer.mean.circular.exclude-nulls", false));
    addNamedWarpScriptFunction(new RMS("reducer.rms", false));
    addNamedWarpScriptFunction(new RMS("reducer.rms.exclude-nulls", true));

    //
    // Filters
    //
    
    //
    // N-ary ops
    //
    
    addNamedWarpScriptFunction(new OpAdd("op.add", true));
    addNamedWarpScriptFunction(new OpAdd("op.add.ignore-nulls", false));
    addNamedWarpScriptFunction(new OpSub("op.sub"));
    addNamedWarpScriptFunction(new OpMul("op.mul", true));
    addNamedWarpScriptFunction(new OpMul("op.mul.ignore-nulls", false));
    addNamedWarpScriptFunction(new OpDiv("op.div"));
    addNamedWarpScriptFunction(new OpMask("op.mask", false));
    addNamedWarpScriptFunction(new OpMask("op.negmask", true));
    addNamedWarpScriptFunction(new OpNE("op.ne"));
    addNamedWarpScriptFunction(new OpEQ("op.eq"));
    addNamedWarpScriptFunction(new OpLT("op.lt"));
    addNamedWarpScriptFunction(new OpGT("op.gt"));
    addNamedWarpScriptFunction(new OpLE("op.le"));
    addNamedWarpScriptFunction(new OpGE("op.ge"));
    addNamedWarpScriptFunction(new OpAND("op.and.ignore-nulls", false));
    addNamedWarpScriptFunction(new OpAND("op.and", true));
    addNamedWarpScriptFunction(new OpOR("op.or.ignore-nulls", false));
    addNamedWarpScriptFunction(new OpOR("op.or", true));

    /////////////////////////

    int nregs = Integer.parseInt(WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_REGISTERS, String.valueOf(WarpScriptStack.DEFAULT_REGISTERS)));

    addNamedWarpScriptFunction(new CLEARREGS(CLEARREGS));
    addNamedWarpScriptFunction(new VARS(VARS));
    addNamedWarpScriptFunction(new ASREGS(ASREGS));
    for (int i = 0; i < nregs; i++) {
      addNamedWarpScriptFunction(new POPR(POPR + i, i));
      addNamedWarpScriptFunction(new POPR(CPOPR + i, i, true));
      addNamedWarpScriptFunction(new PUSHR(PUSHR + i, i));
    }
  }

  public static void addNamedWarpScriptFunction(NamedWarpScriptFunction namedFunction) {
    functions.put(namedFunction.getName(), namedFunction);
  }
  
  public static Object getFunction(String name) {
    return functions.get(name);
  }
  
  public static void registerExtensions() { 
    Properties props = WarpConfig.getProperties();
    
    if (null == props) {
      return;
    }
    
    //
    // Extract the list of extensions
    //
    
    Set<String> ext = new LinkedHashSet<String>();
    
    if (props.containsKey(Configuration.CONFIG_WARPSCRIPT_EXTENSIONS)) {
      String[] extensions = props.getProperty(Configuration.CONFIG_WARPSCRIPT_EXTENSIONS).split(",");
      
      for (String extension: extensions) {
        ext.add(extension.trim());
      }
    }
    
    for (String key: props.stringPropertyNames()) {
      if (!key.startsWith(Configuration.CONFIG_WARPSCRIPT_EXTENSION_PREFIX)) {
        continue;
      }
      
      ext.add(props.getProperty(key).trim());
    }
    
    // Sort the extensions
    List<String> sortedext = new ArrayList<String>(ext);
    sortedext.sort(null);
    
    List<String> failedExt = new ArrayList<String>();
      
    //
    // Determine the possible jar from which WarpScriptLib was loaded
    //
      
    String wsljar = null;
    URL wslurl = WarpScriptLib.class.getResource('/' + WarpScriptLib.class.getCanonicalName().replace('.',  '/') + ".class");
    if (null != wslurl && "jar".equals(wslurl.getProtocol())) {
      wsljar = wslurl.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");
    }
      
    for (String extension: sortedext) {
      
      // If the extension name contains '#', remove everything up to the last '#', this was used as a sorting prefix
            
      if (extension.contains("#")) {
        extension = extension.replaceAll("^.*#", "");
      }
      
      try {
        //
        // Locate the class using the current class loader
        //
        
        URL url = WarpScriptLib.class.getResource('/' + extension.replace('.', '/') + ".class");
        
        if (null == url) {
          LOG.error("Unable to load extension '" + extension + "', make sure it is in the class path.");
          failedExt.add(extension);
          continue;
        }
        
        Class cls = null;

        //
        // If the class was located in a jar, load it using a specific class loader
        // so we can have fat jars with specific deps, unless the jar is the same as
        // the one from which WarpScriptLib was loaded, in which case we use the same
        // class loader.
        //
        
        if ("jar".equals(url.getProtocol())) {
          String jarfile = url.toString().replaceAll("!/.*", "").replaceAll("jar:file:", "");

          ClassLoader cl = WarpScriptLib.class.getClassLoader();
          
          // If the jar differs from that from which WarpScriptLib was loaded, create a dedicated class loader
          if (!jarfile.equals(wsljar) && !"true".equals(props.getProperty(Configuration.CONFIG_WARPSCRIPT_DEFAULTCL_PREFIX + extension))) {
            cl = new WarpClassLoader(jarfile, WarpScriptLib.class.getClassLoader());
          }
        
          cls = Class.forName(extension, true, cl);
        } else {
          cls = Class.forName(extension, true, WarpScriptLib.class.getClassLoader());
        }

        //Class cls = Class.forName(extension);
        WarpScriptExtension wse = (WarpScriptExtension) cls.newInstance();          
        wse.register();
        
        String namespace = props.getProperty(Configuration.CONFIG_WARPSCRIPT_NAMESPACE_PREFIX + wse.getClass().getName(), "").trim(); 
        if (null != namespace && !"".equals(namespace)) {
          namespace = WarpURLDecoder.decode(namespace, StandardCharsets.UTF_8);
          LOG.info("LOADED extension '" + extension + "'" + " under namespace '" + namespace + "'.");
        } else {
          LOG.info("LOADED extension '" + extension + "'");
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    if (!failedExt.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("The following WarpScript extensions could not be loaded, aborting:");
      for (String extension: failedExt) {
        sb.append(" '");
        sb.append(extension);
        sb.append("'");
      }
      LOG.error(sb.toString());
      throw new RuntimeException(sb.toString());
    }
  }
  
  public static void register(WarpScriptExtension extension) {
    String namespace = WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_NAMESPACE_PREFIX + extension.getClass().getName(), "").trim();
        
    try {
      namespace = WarpURLDecoder.decode(namespace, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    register(namespace, extension);
  }
  
  public static void register(String namespace, WarpScriptExtension extension) {
    
    extloaded.add(extension.getClass().getCanonicalName());
    
    Map<String,Object> extfuncs = extension.getFunctions();
    
    if (null == extfuncs) {
      return;
    }
    
    for (Entry<String,Object> entry: extfuncs.entrySet()) {
      if (null == entry.getValue()) {
        functions.remove(namespace + entry.getKey());
      } else {
        functions.put(namespace + entry.getKey(), entry.getValue());
      }
    }          
  }
  
  public static boolean extloaded(String name) {
    return extloaded.contains(name);
  }
  
  public static List<String> extensions() {
    return new ArrayList<String>(extloaded);
  }

  public static ArrayList getFunctionNames() {

    List<Object> list = new ArrayList<Object>();

    list.addAll(functions.keySet());

    return (ArrayList)list;

  }
}
