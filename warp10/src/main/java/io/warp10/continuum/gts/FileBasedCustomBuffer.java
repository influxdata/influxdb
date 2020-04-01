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

package io.warp10.continuum.gts;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * ByteBuffer implementation with data backed by a file
 */
public class FileBasedCustomBuffer implements CustomBuffer {
  
  /**
   * Minimum number of bytes we prefetch from the file
   */
  private final int bufsize;
  
  /**
   * Underlying buffer, to be filled from file
   */
  private ByteBuffer buffer = null;
  
  /**
   * Offset of the start of the buffer 
   */
  private long offset = 0L;
  
  private long position = 0L;
  
  private final long filelen;
  
  private final FileInputStream stream;
  
  public FileBasedCustomBuffer(String path, int bufsize) throws IOException {
    this.filelen = new File(path).length();
    this.stream = new FileInputStream(path);
    this.bufsize = bufsize;
    ensure(1);
  }
  
  public long getPosition() {
    if (null == this.buffer) {
      return 0L;
    }
    
    return this.offset + this.buffer.position();
  }
  
  public void setPosition(long position) {
    
    if (position >= filelen) {
      throw new RuntimeException("Requested position is past the end of the file.");
    }
    
    // Reset the buffer when setting the position
    this.buffer = null;
    
    // Skip to the requested file position
    try {
      this.stream.skip(position - this.offset);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    this.offset = position;
  }
  
  
  @Override
  public byte get() {
    ensure(1);
    return this.buffer.get();
  }
  
  @Override
  public CustomBuffer get(byte[] dst) {
    ensure(dst.length);
    this.buffer.get(dst);
    return this;
  }
  
  @Override
  public CustomBuffer get(byte[] dst, int offset, int length) {
    ensure(length);
    this.buffer.get(dst, offset, length);
    return this;
  }
  
  @Override
  public double getDouble() {
    ensure(8);
    return this.buffer.getDouble();
  }

  @Override
  public long getLong() {
    ensure(8);
    return this.buffer.getLong();
  }
  
  @Override
  public void insert(byte[] data, int offset, int len) {
    ByteBuffer bb = ByteBuffer.allocate(len + this.buffer.remaining());
    
    bb.put(data, offset, len);
    bb.put(this.buffer);
    bb.flip();
    
    this.buffer = bb;          
  }
  
  /**
   * Make sure we have at least 'capacity' bytes available in 'buffer'
   * @param capacity
   */
  private final void ensure(int capacity) {
    if (null != this.buffer && this.buffer.remaining() >= capacity) {
      return;
    }
    
    byte[] buf = null;
    int off = 0;
    
    if (null == this.buffer) {
      buf = new byte[Math.max(bufsize, capacity)];
      this.buffer = ByteBuffer.wrap(buf);
    } else {
      buf = new byte[Math.max(this.buffer.remaining() + bufsize, capacity)];
      ByteBuffer bb = ByteBuffer.wrap(buf);
      // Copy remaining data
      off = this.buffer.remaining();
      this.buffer.get(buf, 0, this.buffer.remaining());
      this.buffer = bb;
    }
    
    // Set buffer's position to 0 as we just created a new buffer
    this.buffer.position(0);
    
    // Read from the stream
    try {
      int len = this.stream.read(buf, off, this.buffer.limit());
    
      if (len > 0) {
        this.offset += len;
      }
      
      this.buffer.limit(off + (len > 0 ? len : 0));

    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  @Override
  public ByteBuffer asByteBuffer() {
    return this.buffer;
  }
  
  @Override
  public boolean hasRemaining() {
    return remaining() > 0;
  }
  
  @Override
  public ByteOrder order() {
    return this.buffer.order();
  }
  
  @Override
  public void order(ByteOrder order) {
    this.buffer.order(order);    
  }
  
  @Override
  public int position() {
    return this.buffer.position();
  }
  
  @Override
  public void position(int position) {
    throw new RuntimeException("Unimplemented");
  }  
  
  @Override
  protected void finalize() throws Throwable {
    this.stream.close();
  }
  
  @Override
  public long remaining() {
    return (this.filelen - this.offset) + this.buffer.remaining();
  }
  
  public void close() throws IOException {
    this.stream.close();
  }
}
