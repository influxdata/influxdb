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
package io.warp10;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.std.TextFileSorter;
public class SortedPathIterator implements Iterator<Path> {
  
  private final Iterator<byte[]> sortedIter;
  
  public SortedPathIterator(final Iterator<Path> iter) throws IOException {
    SortConfig config = new SortConfig().withMaxMemoryUsage(1000000);
    Sorter sorter = new TextFileSorter(config);

    DataReader<byte[]> reader = new DataReader<byte[]>() {
      @Override
      public byte[] readNext() throws IOException {
        if (!iter.hasNext()) {
          return null;
        }
        return iter.next().toString().getBytes(StandardCharsets.UTF_8);
      }
      @Override
      public void close() throws IOException {}
      @Override
      public int estimateSizeInBytes(byte[] item) { return item.length; }
    };

    this.sortedIter = sorter.sort(reader);
  }
  
  @Override
  public boolean hasNext() {
    return this.sortedIter.hasNext();
  }
  
  @Override
  public Path next() {
    File f = new File(new String(this.sortedIter.next(), StandardCharsets.UTF_8));
    return f.toPath();
  }  
 }
