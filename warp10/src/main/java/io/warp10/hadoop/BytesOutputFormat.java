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

package io.warp10.hadoop;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class BytesOutputFormat extends FileOutputFormat<Writable,BytesWritable> {
  
  public static class BytesRecordWriter extends RecordWriter<Writable, BytesWritable> {    
    private final DataOutputStream out;
    
    public BytesRecordWriter(DataOutputStream out) {
      this.out = out;
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      this.out.close();
    }
    
    @Override
    public void write(Writable key, BytesWritable value) throws IOException, InterruptedException {
      out.write(value.getBytes(), 0, value.getLength());
    }
  }
  
  @Override
  public RecordWriter<Writable, BytesWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    String extension = "";
    boolean isCompressed = getCompressOutput(context);
    Configuration conf = context.getConfiguration();
    CompressionCodec codec = null;
    
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    
    Path file = getDefaultWorkFile(context, extension);
    FileSystem fs = file.getFileSystem(context.getConfiguration());
    FSDataOutputStream fileOut = fs.create(file, false);
    DataOutputStream out = fileOut;
    
    if (isCompressed) {
      out = new DataOutputStream(codec.createOutputStream(fileOut));
    }
    
    return new BytesRecordWriter(out);
  }
}
