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

package io.warp10.crypto;

import io.warp10.crypto.OrderPreservingBase64;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class OrderPreservingBase64Test {
  @Test
  public void testEncode() {
    byte[] data = new byte[] { -1 };
    byte[] encoded = OrderPreservingBase64.encode(data);
    
    Assert.assertEquals(2, encoded.length);
    Assert.assertEquals("zk", new String(encoded));
    
    data = new byte[] { -1, -1 };
    encoded = OrderPreservingBase64.encode(data);
    Assert.assertEquals(3, encoded.length);
    Assert.assertEquals("zzw", new String(encoded));
    
    data = new byte[] { -1, -1, -1 };
    encoded = OrderPreservingBase64.encode(data);
    Assert.assertEquals(4, encoded.length);
    Assert.assertEquals("zzzz", new String(encoded));
    
    data = new byte[] { -1, -1, -1, -1 };
    encoded = OrderPreservingBase64.encode(data);
    Assert.assertEquals(6, encoded.length);
    Assert.assertEquals("zzzzzk", new String(encoded));

    data = new byte[] { -1, -1, -1, -1, -1 };
    encoded = OrderPreservingBase64.encode(data);
    Assert.assertEquals(7, encoded.length);
    Assert.assertEquals("zzzzzzw", new String(encoded));
  }
  
  @Test
  public void testDecode() {
    byte[] data = "zk".getBytes();
    byte[] decoded = OrderPreservingBase64.decode(data);
    Assert.assertEquals(1, decoded.length);
    Assert.assertEquals(-1, decoded[0]);

    data = "zzw".getBytes();
    decoded = OrderPreservingBase64.decode(data);
    Assert.assertEquals(2, decoded.length);
    Assert.assertEquals(-1, decoded[0]);
    Assert.assertEquals(-1, decoded[1]);

    data = "zzzz".getBytes();
    decoded = OrderPreservingBase64.decode(data);
    Assert.assertEquals(3, decoded.length);
    Assert.assertEquals(-1, decoded[0]);
    Assert.assertEquals(-1, decoded[1]);
    Assert.assertEquals(-1, decoded[2]);

    data = "zzzzzk".getBytes();
    decoded = OrderPreservingBase64.decode(data);
    Assert.assertEquals(4, decoded.length);
    Assert.assertEquals(-1, decoded[0]);
    Assert.assertEquals(-1, decoded[1]);
    Assert.assertEquals(-1, decoded[2]);
    Assert.assertEquals(-1, decoded[3]);
  }
  
  @Test
  public void testOrder() {
    Random rand = new Random();
    long nano = System.nanoTime();
    int n = 1000000;
    
    for (int i = 0; i < n; i++) {
      byte[] a = new byte[(int) (Math.random() * 16)];
      byte[] b = new byte[(int) (Math.random() * 16)];
      rand.nextBytes(a);
      rand.nextBytes(b);
      
      int bytecomp = compareTo(a, 0, a.length, b, 0, b.length);
      int b64comp = new String(OrderPreservingBase64.encode(a), StandardCharsets.US_ASCII).compareTo(new String(OrderPreservingBase64.encode(b), StandardCharsets.US_ASCII));
      
      Assert.assertTrue((bytecomp == 0 && b64comp == 0) || (bytecomp * b64comp > 0));
    }
    
    System.out.println((System.nanoTime() - nano) / (double) n);
  }
  
  @Test
  public void testPerf() {
    byte[] bytes = new byte[100000];
    SecureRandom sr = new SecureRandom();
    sr.nextBytes(bytes);
    
    int n = 100000;
    
    for (int i = 0; i < n; i++) {
      byte[] enc = OrderPreservingBase64.encode(bytes);
    }
    
  }
  
  @Test
  public void testStream() throws Exception {
    SecureRandom sr = new SecureRandom();
    for (int i = 1; i < 100; i++) {
      byte[] bytes = new byte[i];
      //sr.nextBytes(bytes);
      byte[] raw = OrderPreservingBase64.encode(bytes);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(raw.length);
      OrderPreservingBase64.encodeToStream(bytes, baos);
      Assert.assertArrayEquals(raw, baos.toByteArray());
      StringWriter sw = new StringWriter();
      OrderPreservingBase64.encodeToWriter(bytes, sw);
      byte[] writer = sw.toString().getBytes(StandardCharsets.US_ASCII);
      Assert.assertArrayEquals(raw, writer);
    }
  }
  // Bytes comparator
  private int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
    int end1 = offset1 + length1;
    int end2 = offset2 + length2;
    for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
      int a = (buffer1[i] & 0xff);
      int b = (buffer2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }
}
