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

package io.warp10.continuum;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

public class Dump extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    
    String dumpurl = args[0];
    String seqfile = args[1];
    
    //
    // Open output SequenceFile
    //
    
    Configuration conf = getConf();
    
    //
    // Open output file
    //
    
    FSDataOutputStream out = null;
    
    if ("-".equals(args[args.length - 1])) {
      out = new FSDataOutputStream(System.out, null);
    }

    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.compression(CompressionType.BLOCK, new DefaultCodec()),
      SequenceFile.Writer.keyClass(BytesWritable.class),
      SequenceFile.Writer.valueClass(BytesWritable.class),
      null == out ? SequenceFile.Writer.file(new Path(args[args.length - 1])) : SequenceFile.Writer.stream(out));

    InputStream is = null;
    
    if (dumpurl.startsWith("http://") || dumpurl.startsWith("https://")) {
      URLConnection conn = new URL(dumpurl).openConnection();
      conn.setDoInput(true);
      conn.connect();
      is = conn.getInputStream();      
    } else if ("-".equals(dumpurl)) {
      is = System.in;
    } else {
      is = new FileInputStream(dumpurl);
    }
          
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    while(true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      //
      // Extract ts// class{labels}
      //
      
      String meta = line.substring(0, line.indexOf('}') + 1);
      
      //
      // Parse a dummy line 'ts// class{labels} T' to retrieve the Metadata
      //
      
      GTSEncoder encoder = GTSHelper.parse(null, meta + " T");
      
      Metadata metadata = encoder.getMetadata();
      
      // Retrieve potential dummy elevation which will encode the number of datapoints encoded
      
      GTSDecoder decoder = encoder.getDecoder();
      decoder.next();
      
      long count = decoder.getElevation();
      
      //
      // Create a GTSWrapper
      //
      
      GTSWrapper wrapper = new GTSWrapper();
      wrapper.setMetadata(metadata);
      wrapper.setBase(encoder.getBaseTimestamp());
      
      if (GeoTimeSerie.NO_ELEVATION != count) {
        wrapper.setCount(count);
      } else {
        wrapper.setCount(0L);
      }

      //
      // Retrieve encoded datapoints
      //
      
      byte[] datapoints = OrderPreservingBase64.decode(line.substring(line.indexOf('}') + 2).getBytes(StandardCharsets.UTF_8));
      
      writer.append(new BytesWritable(serializer.serialize(wrapper)), new BytesWritable(datapoints));
    }
    
    writer.close();
    br.close();
    is.close();
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    
    if (args.length < 2) {
      throw new IOException("Usage: Dump FETCH_URL output");
    }
    
    //args = new String[] { "/var/tmp/debs.encoders", "file:///var/tmp/debs.seq" };
    int exitCode = ToolRunner.run(new Dump(), args);
    System.exit(exitCode);
  }
}
