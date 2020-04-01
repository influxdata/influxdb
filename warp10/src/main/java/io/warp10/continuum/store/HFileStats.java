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

package io.warp10.continuum.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;

public class HFileStats {
  
  public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();
    CacheConfig cacheConf = new CacheConfig(conf);
    
    FileSystem fs = FileSystem.newInstance(conf);
    
    FileStatus[] pathes = fs.globStatus(new Path(args[0]));
    
    long bytes = 0L;
    long cells = 0L;

    for (FileStatus status: pathes) {    
      try {
        HFile.Reader reader = HFile.createReader(fs, status.getPath(), cacheConf, conf);
        bytes += reader.length();
        cells += reader.getEntries();

        System.out.println(status.getPath() + " >>> " + reader.length() + " bytes " + reader.getEntries() + " cells");
      
        reader.close();      
      } catch (Exception e) {
        continue;
      }      
    }

    System.out.println("TOTAL: " + cells + " cells " + bytes + " bytes " + (bytes/(double) cells) + " bytes/cell");
 
    long ts = System.currentTimeMillis();

    System.out.println(ts * 1000 + "// hbase.bytes{} " + bytes);
    System.out.println(ts * 1000 + "// hbase.datapoints{} " + cells);
  }
}

