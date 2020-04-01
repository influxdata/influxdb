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

package io.warp10.script.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.GTSStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Parse string values of a GTS to extract multiple GTS based on regexp groups
 */
public class SMARTPARSE  extends GTSStackFunction {
  
  // Groups containing values
  private static final String VGROUPS = "vgroups";
  // Groups containing labels
  private static final String LGROUPS = "lgroups";
  // Groups containing timestamps
  private static final String TGROUPS = "tgroups";  
  private static final String MATCHER = "matcher";
  private static final String LAT = "lat";
  private static final String LON = "lon";
  private static final String ELEV = "elev";
  
  public SMARTPARSE(String name) {
    super(name);
  }
    
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    if (stack.get(0) instanceof String && stack.get(1) instanceof String) {
      Map<String,Object> params = retrieveParameters(stack);
      
      List<String> vgroups = (List<String>) params.get(VGROUPS);
      List<String> lgroups = (List<String>) params.get(LGROUPS);
      List<String> tgroups = (List<String>) params.get(TGROUPS);
      Matcher matcher = (Matcher) params.get(MATCHER);
      
      String lat = (String) params.get(LAT);
      String lon = (String) params.get(LON);
      String elev = (String) params.get(ELEV);
      
      Map<Metadata,GeoTimeSerie> results = new HashMap<Metadata, GeoTimeSerie>();
          
      Map<String,String> labels = new HashMap<String,String>();
      
      String val = stack.pop().toString();
        
      long timestamp = 0L;
        
      doParse(val, timestamp, vgroups,lgroups,tgroups,matcher,lat,lon,elev,labels,results);
        
      stack.push(new ArrayList<GeoTimeSerie>(results.values()));
      return stack;
    } else {
      return super.apply(stack);
    }
  }
  
  @Override
  protected Map<String, Object> retrieveParameters(WarpScriptStack stack) throws WarpScriptException {
    
    Map<String,Object> params = new HashMap<String,Object>();
    
    String regexp = stack.pop().toString();
    
    Pattern pattern = Pattern.compile(regexp);
    
    //
    // Extract the named fields
    //
    
    Pattern namedgrouppattern = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    
    Matcher namedgroup = namedgrouppattern.matcher(regexp);
    
    List<String> vgroups = new ArrayList<String>();
    List<String> lgroups = new ArrayList<String>();
    List<String> tgroups = new ArrayList<String>();
    
    while(namedgroup.find()) {
      String group = namedgroup.group(1);
      
      if (group.startsWith("V")) {
        vgroups.add(group);
      } else if (group.startsWith("L")) {
        lgroups.add(group);
      } else if (group.startsWith("T")) {
        tgroups.add(group);
      } else if (group.equals("lat")) {
        params.put(LAT, group);
      } else if (group.equals("lon")) {
        params.put(LON, group);
      } else if (group.equals("elev")) {
        params.put(ELEV, group);
      }      
    }
    
    Matcher matcher = pattern.matcher("");
    
    params.put(MATCHER, matcher);
    params.put(VGROUPS, vgroups);
    params.put(LGROUPS, lgroups);
    params.put(TGROUPS, tgroups);
    
    return params;
  }

  @Override
  protected Object gtsOp(Map<String, Object> params, GeoTimeSerie gts) throws WarpScriptException {
    List<String> vgroups = (List<String>) params.get(VGROUPS);
    List<String> lgroups = (List<String>) params.get(LGROUPS);
    List<String> tgroups = (List<String>) params.get(TGROUPS);
    Matcher matcher = (Matcher) params.get(MATCHER);
    
    String lat = (String) params.get(LAT);
    String lon = (String) params.get(LON);
    String elev = (String) params.get(ELEV);
    
    Map<Metadata,GeoTimeSerie> results = new HashMap<Metadata, GeoTimeSerie>();
        
    int idx = 0;
    int n = GTSHelper.nvalues(gts);
    
    Map<String,String> labels = new HashMap<String,String>();
    
    while (idx < n) {
      String val = GTSHelper.valueAtIndex(gts, idx).toString();
      
      long timestamp = GTSHelper.tickAtIndex(gts, idx);
      
      doParse(val, timestamp, vgroups,lgroups,tgroups,matcher,lat,lon,elev,labels,results);
      
      idx++;
    }

    return new ArrayList<GeoTimeSerie>(results.values());
  }
  
  private static void doParse(String val, long timestamp, List<String> vgroups, List<String> lgroups, List<String> tgroups, Matcher matcher, String lat, String lon, String elev, Map<String,String> labels, Map<Metadata,GeoTimeSerie> results) {
    
    matcher.reset(val);
    
    while(matcher.find()) {
      
      //
      // Build labels
      //
      
      labels.clear();
      
      for (String group: lgroups) {
        String groupval = matcher.group(group);
        
        if (null != groupval) {
          labels.put(group.substring(1), groupval);
        }
      }
      
      long location = GeoTimeSerie.NO_LOCATION;
      long elevation = GeoTimeSerie.NO_ELEVATION;

      if (null != lat && null != lon) {
        String latval = matcher.group(lat);
        String lonval = matcher.group(lon);
        
        if (null != latval && null != lonval) {
          location = GeoXPLib.toGeoXPPoint(Double.parseDouble(latval), Double.parseDouble(lonval));
        }
      }
      
      if (null != elev) {
        String elevval = matcher.group(elev);
        
        if (null != elevval) {          
          elevation = (long) Math.round(Double.parseDouble(elevval));
        }
        
        if (elev.equals("elevm")) {
          elevation = elevation * 1000L;
        } else if (elev.equals("elevft")) {
          elevation = elevation * (254L*12L);
        } else if (elev.equals("elevkm")) {
          elevation = elevation * 1000000L;
        } else if (elev.equals("elevmi")) {
          elevation = elevation * 1609340L;
        } else if (elev.equals("elevnm")) {
          elevation = elevation * 1852000L;
        } else if (elev.equals("elevcm")) {
          elevation = elevation * 10L;
        }
      }
      
      //
      // Parse timestamp groups, stopping at the first successfully parsed group
      //
      
      for (String group: tgroups) {
        String groupval = matcher.group(group);
        
        if (null == groupval) {
          continue;
        }
        
        Double ts = null;
        
        if (group.startsWith("Ts")) {            
          ts = Double.parseDouble(groupval) * Constants.TIME_UNITS_PER_S;
        } else if (group.startsWith("Tms")) {
          ts = Double.parseDouble(groupval) * Constants.TIME_UNITS_PER_MS;
        } else if (group.startsWith("Tus")) {            
          ts = Double.parseDouble(groupval);
          ts = ts / (1000000.0D / Constants.TIME_UNITS_PER_S);
        } else if (group.startsWith("Tns")) {
          ts = Double.parseDouble(groupval);
          ts = ts / (1000000000.0D / Constants.TIME_UNITS_PER_S);
        }

        if (null != ts) {
          timestamp = ts.longValue();
          break;
        }
      }
      
      for (String group: vgroups) {
        String groupval = matcher.group(group);
        
        if (null != groupval) {            
          Object value = null;
          
          if ('L' == group.charAt(1)) {
            value = Long.parseLong(groupval);
          } else if ('D' == group.charAt(1)) {
            value = Double.parseDouble(groupval);
          } else if ('B' == group.charAt(1)) {
            value = Boolean.parseBoolean(groupval);
          } else if ('S' == group.charAt(1)) {
            value = groupval;
          }
          
          if (null != value) {
            Metadata meta = new Metadata();
            meta.setLabels(new HashMap<String,String>(labels));
            meta.setName(group.substring(2));
            
            GeoTimeSerie g = results.get(meta);
            
            if (null == g) {
              g = new GeoTimeSerie();
              g.setMetadata(meta);
              results.put(meta, g);
            }
            
            GTSHelper.setValue(g, timestamp, location, elevation, value, false);
          }
        }
      }        
    }    
  }
}
