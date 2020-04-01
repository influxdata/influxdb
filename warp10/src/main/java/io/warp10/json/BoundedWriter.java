//
//   Copyright 2020  SenX S.A.S.
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

package io.warp10.json;

import java.io.IOException;
import java.io.Writer;

/**
 * A wrapper for Writers to limit the number of written chars.
 * When the number of chars that should be written exceeds the given limit, a WriterBoundReachedException is thrown.
 */
public class BoundedWriter extends Writer {

  public static class WriterBoundReachedException extends IOException {

    public WriterBoundReachedException(String message) {
      super(message);
    }

  }

  protected final Writer writer;
  protected final long maxWrittenChars;
  protected long currentWrittenChars;

  public BoundedWriter(Writer writer, long maxAppendedChars) {
    this.writer = writer;
    this.maxWrittenChars = maxAppendedChars;
    this.currentWrittenChars = 0;
  }

  /**
   * Writes a portion of an array of characters.
   * @param cbuf Array of characters
   * @param off Offset from which to start writing characters
   * @param len Number of characters to write
   * @throws WriterBoundReachedException if this write would exceed the maximum number of written characters to this writer.
   * @throws IOException if this the underlying writer throws an IOException.
   */
  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    if (len + currentWrittenChars - maxWrittenChars > 0) {
      throw new WriterBoundReachedException("Cannot write, maximum number of characters written :" + this.maxWrittenChars);
    }
    this.currentWrittenChars += len;
    writer.write(cbuf, off, len);
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
