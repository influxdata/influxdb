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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class MVSPLIT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean bytick;
  
  public MVSPLIT(String name, boolean bytick) {
    super(name);
    this.bytick = bytick;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    Map<Object,Object> renamingMap = new HashMap<Object,Object>();
    
    if (top instanceof Map) {
      renamingMap = (Map<Object,Object>) top;
      top = stack.pop();
    }
    
    Set<Long> ticks = new HashSet<Long>();
    List<Pair<Long, Long>> ranges = new ArrayList<Pair<Long,Long>>();
    
    boolean check = false;
    boolean includeZero = false;
    long maxindex = Long.MIN_VALUE;
    
    if (top instanceof List) {
      
      boolean gtslist = true;
      
      // Check of it is a list of GTS or GTSEncoders
      for (Object elt: (List) top) {
        if (!(elt instanceof GeoTimeSerie) && !(elt instanceof GTSEncoder)) {
          gtslist = false;
          break;
        }
      }
      
      if (!gtslist) {
        for (Object elt: (List) top) {
          if (elt instanceof Long) {
            ticks.add((Long) elt);
            if (0L == (Long) elt) {
              includeZero = true;
            }
            if (((Long) elt).longValue() > maxindex) {
              maxindex = ((Long) elt).longValue();
            }
          } else if (elt instanceof List) {
            if (2 != ((List) elt).size()) {
              throw new WarpScriptException(getName() + " expects ticks or ranges of ticks (LONGs).");
            }
            if (!(((List) elt).get(0) instanceof Long)
                || !(((List) elt).get(1) instanceof Long)) {
              throw new WarpScriptException(getName() + " expects ticks or ranges of ticks (LONGs).");
            }
            Pair<Long,Long> range = Pair.of((Long) ((List) elt).get(0), (Long) ((List) elt).get(1));
            
            if (range.getLeft() > range.getRight()) {
              range = Pair.of(range.getRight(), range.getLeft());
            }
            
            ranges.add(range);
            
            if (range.getRight() > maxindex) {
              maxindex = range.getRight();
            }
          } else {
            throw new WarpScriptException(getName() + " expects ticks or ranges of ticks (LONGs).");
          }
        }
        check = true;
        top = stack.pop();        
      }
    } else {
      includeZero = true;
    }
    
    List<Object> inputs = null;
    boolean listinput = false;
    
    if (top instanceof GTSEncoder || top instanceof GeoTimeSerie) {
      inputs = new ArrayList<Object>(1);
      inputs.add(top);
    } else if (top instanceof List) {
      inputs = (List) top;
      listinput = true;
    } else {
      throw new WarpScriptException(getName() + " operates on Geo Time Series™ or ENCODER or a list thereof.");
    }
    
    TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());
    GTSWrapper wrapper = new GTSWrapper();

    Map<Long,GTSEncoder> encoders = new HashMap<Long, GTSEncoder>();

    List<List<GTSEncoder>> outputs = new ArrayList<List<GTSEncoder>>(inputs.size());
    
    for (Object input: inputs) {
      boolean isencoder = false;
      
      GTSDecoder decoder = null;
      GeoTimeSerie gts = null;

      if (input instanceof GTSEncoder) {
        isencoder = true;
        decoder = ((GTSEncoder) input).getDecoder(true);
      } else if (input instanceof GeoTimeSerie) {
        isencoder = false;
        gts = ((GeoTimeSerie) input);
      } else {
        throw new WarpScriptException(getName() + " operates on Geo Time Series™ or ENCODER or a list thereof.");
      }
      
      encoders.clear();
      
      int idx = 0;
            
      try {
        // Iterate over the datapoints of the input GTS or encoder
        int nvalues = !isencoder ? GTSHelper.nvalues(gts) : 0;
        while((isencoder && decoder.next()) || (!isencoder && idx < nvalues)) {
          long ts;
          long location;
          long elevation;
          Object value;
          
          if (isencoder) {
            ts = decoder.getTimestamp();
            location = decoder.getLocation();
            elevation = decoder.getElevation();
            value = decoder.getBinaryValue();
            
            // If the value is a byte array or STRING, attempt to deserialize it
            if (value instanceof byte[]) {
              try {
                wrapper.clear();
                deser.deserialize(wrapper, (byte[]) value);
                value = wrapper;
              } catch (TException te) {            
              }
            } else if (value instanceof String) {
              try {
                byte[] bytes = value.toString().getBytes(StandardCharsets.ISO_8859_1);
                wrapper.clear();
                deser.deserialize(wrapper, bytes);
                value = wrapper;
              } catch (TException te) {   
              }          
            }
          } else {
            ts = GTSHelper.tickAtIndex(gts, idx);
            location = GTSHelper.locationAtIndex(gts, idx);
            elevation = GTSHelper.elevationAtIndex(gts, idx);
            value = GTSHelper.valueAtIndex(gts, idx);
            
            // Check if a STRING is a wrapper
            if (value instanceof String) {
              try {
                byte[] bytes = value.toString().getBytes(StandardCharsets.ISO_8859_1);
                wrapper.clear();
                deser.deserialize(wrapper, bytes);
                value = wrapper;
              } catch (TException te) {   
              }          
            }
          }

          if (value instanceof GTSWrapper) {
            GTSDecoder deco = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
            
            long index = 0;
            
            while(deco.next()) {
              long dts = deco.getTimestamp();
              long dlocation = deco.getLocation();
              long delevation = deco.getElevation();
              Object dvalue = deco.getBinaryValue();
              
              if (bytick) {
                index = dts;
              }
              
              boolean skip = false;

              // Check if we should include the element
              if (check) {
                skip = true;
                if (!ticks.isEmpty() && ticks.contains(index)) {
                  skip = false;
                }
                if (skip && !ranges.isEmpty()) {
                  for (Pair<Long,Long> range: ranges) {
                    if (index >= range.getLeft() && index <= range.getRight()) {
                      skip = false;
                      break;
                    }
                  }
                }
              }
              
              if (!skip) {              
                GTSEncoder encoder = encoders.get(index);
                if (null == encoder) {
                  encoder = new GTSEncoder(0L);
                  encoder.setMetadata(null != decoder ? decoder.getMetadata() : gts.getMetadata());
                  encoders.put(index, encoder);              
                }
                encoder.addValue(ts,
                  GeoTimeSerie.NO_LOCATION != dlocation ? dlocation : location,
                  GeoTimeSerie.NO_ELEVATION != delevation ? delevation : elevation,
                  dvalue);
              }
              
              index++;
              
              // Early exit if we are past the last index we were interested in
              if (!bytick && check && index > maxindex) {
                break;
              }
            }
          } else if (includeZero) {
            // We only have a single value, assume timestamp is 0 with no location/elevation
            GTSEncoder encoder = encoders.get(0L);
            if (null == encoder) {
              encoder = new GTSEncoder(0L);
              encoder.setMetadata(null != decoder ? decoder.getMetadata() : gts.getMetadata());
              encoders.put(0L, encoder);
            }        
            encoder.addValue(ts, location, elevation, value);
          }
          
          idx++;
        }      
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " encountered an error while splitting input.");
      }
      
      // Now rename the encoders
      GeoTimeSerie g = new GeoTimeSerie();
      for (Entry<Long,GTSEncoder> entry: encoders.entrySet()) {
        Object name = renamingMap.get(entry.getKey());
        
        // If there was no name with the Long as the key, try with the String representation
        if (null == name) {
          name = renamingMap.get(entry.getKey().toString());
        }
        
        if (null == name) {
          entry.getValue().setName(entry.getValue().getName() + ":" + entry.getKey());
        } else {
          g.safeSetMetadata(entry.getValue().getMetadata());
          GTSHelper.rename(g, name.toString());
        }
      }
      
      // Now build a list of extracted encoders
      List<GTSEncoder> results = new ArrayList<GTSEncoder>(encoders.size());
      results.addAll(encoders.values());
      outputs.add(results);
    }  
    
    if (listinput) {
      stack.push(outputs);
    } else {
      stack.push(outputs.get(0));
    }
    
    return stack;
  }

}
