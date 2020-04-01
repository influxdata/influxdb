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

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class Warp10OutputFormat extends OutputFormat<Writable, Writable> {

  private final String suffix;
  
  public Warp10OutputFormat() {
    this(null);
  }
  
  public Warp10OutputFormat(String suffix) {
    if (null != suffix) {
      this.suffix = "." + suffix;
    } else {
      this.suffix = "";
    }
  }
  
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {}

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new OutputCommitter() {
      
      @Override
      public void setupTask(TaskAttemptContext taskContext) throws IOException {}
      
      @Override
      public void setupJob(JobContext jobContext) throws IOException {}
      
      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
      }
      
      @Override
      public void commitTask(TaskAttemptContext taskContext) throws IOException {}
      
      @Override
      public void abortTask(TaskAttemptContext taskContext) throws IOException {}
    };
  }
  
  @Override
  public RecordWriter<Writable, Writable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    
    Properties props = new Properties();
    
    Configuration conf = context.getConfiguration();
    
    props.setProperty(Warp10RecordWriter.WARP10_GZIP, Warp10InputFormat.getProperty(conf, this.suffix, Warp10RecordWriter.WARP10_GZIP, "false"));
    props.setProperty(Warp10RecordWriter.WARP10_ENDPOINT, Warp10InputFormat.getProperty(conf, this.suffix, Warp10RecordWriter.WARP10_ENDPOINT, ""));
    props.setProperty(Warp10RecordWriter.WARP10_TOKEN, Warp10InputFormat.getProperty(conf, this.suffix, Warp10RecordWriter.WARP10_TOKEN, ""));
    props.setProperty(Warp10RecordWriter.WARP10_MAXRATE, Warp10InputFormat.getProperty(conf, this.suffix, Warp10RecordWriter.WARP10_MAXRATE, Long.toString(Long.MAX_VALUE)));
    
    return new Warp10RecordWriter(props);
  }
}
