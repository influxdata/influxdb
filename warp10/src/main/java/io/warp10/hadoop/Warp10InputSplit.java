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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

public class Warp10InputSplit extends InputSplit implements Writable {

  /**
   * Address fetchers, ideal one first
   */
  private String[] fetchers;
  
  /**
   * Byte array containing gzipped GTSSplits content
   */
  private byte[] splits;
  
  private boolean complete = false;
  
  private Set<String> fetcherSet = new LinkedHashSet<String>();
  private ByteArrayOutputStream baos = null;
  private OutputStream out = null;
  
  public Warp10InputSplit() {}
  
  public void addEntry(String fetcher, String entry) throws IOException {
    if (this.complete) {
      throw new RuntimeException("InputSplit already completed.");
    }
    
    if (null != fetcher) {
      this.fetcherSet.add(fetcher);
    }
  
    if (null == out) {
      baos = new ByteArrayOutputStream();
      out = new GZIPOutputStream(baos);
    }
    
    out.write(entry.getBytes(StandardCharsets.US_ASCII));
    out.write('\r');
    out.write('\n');
  }
  
  public void addFetcher(String fetcher) {
    if (this.complete) {
      throw new RuntimeException("InputSplit already completed.");
    }
    
    this.fetcherSet.add(fetcher);
  }
  
  public Warp10InputSplit build() throws IOException {
    if (this.complete) {
      throw new RuntimeException("InputSplit already completed.");
    }

    out.close();
    this.splits = baos.toByteArray();
    baos.close();
    baos = null;
    
    this.fetchers = this.fetcherSet.toArray(new String[0]);
    
    this.complete = true;
    
    return this;
  }
  
  @Override
  public long getLength() throws IOException {
    return 0;
  }
  
  @Override
  public String[] getLocations() throws IOException {
    return Arrays.copyOf(fetchers, fetchers.length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    //
    // Read fetchers
    //
    
    int nfetchers = WritableUtils.readVInt(in);

    this.fetchers = new String[nfetchers];
    
    for (int i = 0; i < nfetchers; i++) {
      String currentFetcher = WritableUtils.readString(in);
      this.fetchers[i] = currentFetcher;
    }
    
    //
    // Read splits
    //
    
    int splitsize = WritableUtils.readVInt(in);

    this.splits = WritableUtils.readCompressedByteArray(in);
    this.complete = true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    
    if (!this.complete) {
      throw new IOException("InputSplit is not completed.");      
    }

    WritableUtils.writeVInt(out,this.fetchers.length);
    
    for (int i = 0; i < this.fetchers.length; i++) {
      WritableUtils.writeString(out, this.fetchers[i]);
    }

    WritableUtils.writeVInt(out, this.splits.length);
    WritableUtils.writeCompressedByteArray(out, this.splits);
  }

  public byte[] getBytes() {
    return this.splits;
  }

}
