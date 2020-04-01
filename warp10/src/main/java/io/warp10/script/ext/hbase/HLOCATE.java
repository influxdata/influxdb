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

package io.warp10.script.ext.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;

import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.egress.HBaseStoreClient;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class HLOCATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public HLOCATE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a list of Geo Time Series on top of the stack.");
    }
    
    StoreClient sc = stack.getStoreClient();
    
    if (!(sc instanceof HBaseStoreClient)) {
      throw new WarpScriptException(getName() + " only works with an HBase storage backend.");
    }
    
    HBaseStoreClient hbsc = (HBaseStoreClient) sc;
    
    RegionLocator locator = null;
    List<HRegionLocation>  regions = null;
    
    try {
      locator = hbsc.getRegionLocator();
      regions = locator.getAllRegionLocations();
    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    }

    //
    // Sort regions by start key
    //
    
    regions.sort(new Comparator<HRegionLocation>() {
      @Override
      public int compare(HRegionLocation o1, HRegionLocation o2) {
        return Bytes.compareTo(o1.getRegionInfo().getStartKey(), o2.getRegionInfo().getStartKey());
      }
      
    });
    
    //
    // Sort GTS by class/labels Id
    // We assume class/labels Ids are present in the GTS
    //
    
    List<GeoTimeSerie> lgts = (List<GeoTimeSerie>) top;
    
    lgts.sort(new Comparator<GeoTimeSerie>() {
      @Override
      public int compare(GeoTimeSerie o1, GeoTimeSerie o2) {
        return MetadataUtils.compare(o1.getMetadata(), o2.getMetadata());
      }
    });
    
    //
    // Now loop over the GeoTimeSeries list, checking in which region they fall
    //
    
    int gtsidx = 0;
    int regionidx = 0;
    
    List<List<String>> locations = new ArrayList<List<String>>();
    
    if (0 == lgts.size()) {
      stack.push(locations);
      return stack;
    }
    
    byte[] startrow = regions.get(regionidx).getRegionInfo().getStartKey();
    byte[] endrow = regions.get(regionidx).getRegionInfo().getEndKey();
    byte[] rowprefix = MetadataUtils.HBaseRowKeyPrefix(lgts.get(gtsidx).getMetadata());
    String selector = GTSHelper.buildSelector(lgts.get(gtsidx), false);
    
    while (gtsidx < lgts.size()) {
      
      //
      // Advance region index while rowprefix is after the current region end
      //
            
      while(regionidx < regions.size() && Bytes.compareTo(rowprefix, 0, rowprefix.length, endrow, 0, rowprefix.length) > 0) {
        regionidx++;
        if (regionidx < regions.size()) {
          startrow = regions.get(regionidx).getRegionInfo().getStartKey();
          endrow = regions.get(regionidx).getRegionInfo().getEndKey();
        }
      }
      
      // We're done scanning regions
      if (regions.size() == regionidx) {
        break;
      }

      //
      // If the current GTS is within the current region, output its location,
      // otherwise advance the GTS index as the current GTS cannot be located
      //
      
      if (Bytes.compareTo(rowprefix, 0, rowprefix.length, startrow, 0, rowprefix.length) >= 0) {
        List<String> location = new ArrayList<String>();
        
        location.add(selector);
        location.add(regions.get(regionidx).getHostnamePort().toString());
        location.add(regions.get(regionidx).getRegionInfo().getEncodedName());
        
        locations.add(location);
        
        // If the current end of region prefix is > rowprefix, advance GTS index as the GTS does not span regions
        
        if (Bytes.compareTo(rowprefix, 0, rowprefix.length, endrow, 0, rowprefix.length) < 0) {
          gtsidx++;
          
          if (gtsidx < lgts.size()) {
            rowprefix = MetadataUtils.HBaseRowKeyPrefix(lgts.get(gtsidx).getMetadata());
            selector = GTSHelper.buildSelector(lgts.get(gtsidx), false);            
          }
          continue;
        }
        
        //
        // The GTS spans regions, increase regionidx
        //
        
        
        regionidx++;
        if (regionidx < regions.size()) {
          startrow = regions.get(regionidx).getRegionInfo().getStartKey();
          endrow = regions.get(regionidx).getRegionInfo().getEndKey();          
        }
        continue;
      } else {
        // rowprefix is <= the end of the region but not within the region, advance gtsidx, there is no location info
        // for the current GTS

        gtsidx++;
        
        if (gtsidx < lgts.size()) {
          rowprefix = MetadataUtils.HBaseRowKeyPrefix(lgts.get(gtsidx).getMetadata());
          selector = GTSHelper.buildSelector(lgts.get(gtsidx), false);          
        }
        continue;
      }      
    }
    
    stack.push(locations);
    
    return stack;
  }
}
