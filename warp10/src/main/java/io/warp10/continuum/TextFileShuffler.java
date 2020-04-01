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

import io.warp10.crypto.SipHashInline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

import com.fasterxml.sort.DataReader;
import com.fasterxml.sort.DataReaderFactory;
import com.fasterxml.sort.SortConfig;
import com.fasterxml.sort.Sorter;
import com.fasterxml.sort.std.RawTextLineWriter;
public class TextFileShuffler extends Sorter<byte[]> {
  
  public static class CustomReader<T> extends DataReader<T> {
    
    private final BufferedReader reader;
    
    public CustomReader(InputStream in) {
      this.reader = new BufferedReader(new InputStreamReader(in));
    }
    
    @Override
    public T readNext() throws IOException {
      String line = this.reader.readLine();
      
      if (null == line) {
        return null;
      } else {
        return (T) line.getBytes(StandardCharsets.UTF_8);
      }
    }
    
    @Override
    public int estimateSizeInBytes(T item) {
      if (item instanceof byte[]) {
        return 24 + ((byte[]) item).length;
      } else {
        return 24;
      }
    }
    
    @Override
    public void close() throws IOException {
      this.reader.close();
    }
  }
  
  private static class CustomReaderFactory<T> extends DataReaderFactory<T> {
    @Override
    public DataReader<T> constructReader(InputStream in) throws IOException {
      return new CustomReader<T>(in);
    }
  }
  
  private static final class ShufflingComparator implements Comparator<byte[]> {
    
    /**
     * Random keys for SipHash
     */
    
    private final long k0 = System.currentTimeMillis();
    private final long k1 = System.nanoTime();
       
    @Override
    public int compare(byte[] o1, byte[] o2) {
      long h1 = SipHashInline.hash24(k0, k1, o1, 0, o1.length);
      long h2 = SipHashInline.hash24(k0, k1, o2, 0, o2.length);
      
      return Long.compare(h1, h2);
    }
  }
  
  public TextFileShuffler() {
    this(new SortConfig());
  }

  public TextFileShuffler(SortConfig config) {
    //super(config, RawTextLineReader.factory(), RawTextLineWriter.factory(), new ShufflingComparator());        
    super(config, new CustomReaderFactory<byte[]>(), RawTextLineWriter.factory(), new ShufflingComparator());        
  }
}
