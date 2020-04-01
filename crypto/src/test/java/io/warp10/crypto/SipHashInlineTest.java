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

import io.warp10.crypto.SipHashInline;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test class
 * 
 * @see https://github.com/nahi/siphash-java-inline/blob/master/src/test/java/SipHashInlineTest.java
 *
 */
public class SipHashInlineTest {
  
  private long[] EXPECTED = new long[] { 0x726fdb47dd0e0e31L,
      0x74f839c593dc67fdL, 0x0d6c8009d9a94f5aL, 0x85676696d7fb7e2dL,
      0xcf2794e0277187b7L, 0x18765564cd99a68dL, 0xcbc9466e58fee3ceL,
      0xab0200f58b01d137L, 0x93f5f5799a932462L, 0x9e0082df0ba9e4b0L,
      0x7a5dbbc594ddb9f3L, 0xf4b32f46226bada7L, 0x751e8fbc860ee5fbL,
      0x14ea5627c0843d90L, 0xf723ca908e7af2eeL, 0xa129ca6149be45e5L,
      0x3f2acc7f57c29bdbL, 0x699ae9f52cbe4794L, 0x4bc1b3f0968dd39cL,
      0xbb6dc91da77961bdL, 0xbed65cf21aa2ee98L, 0xd0f2cbb02e3b67c7L,
      0x93536795e3a33e88L, 0xa80c038ccd5ccec8L, 0xb8ad50c6f649af94L,
      0xbce192de8a85b8eaL, 0x17d835b85bbb15f3L, 0x2f2e6163076bcfadL,
      0xde4daaaca71dc9a5L, 0xa6a2506687956571L, 0xad87a3535c49ef28L,
      0x32d892fad841c342L, 0x7127512f72f27cceL, 0xa7f32346f95978e3L,
      0x12e0b01abb051238L, 0x15e034d40fa197aeL, 0x314dffbe0815a3b4L,
      0x027990f029623981L, 0xcadcd4e59ef40c4dL, 0x9abfd8766a33735cL,
      0x0e3ea96b5304a7d0L, 0xad0c42d6fc585992L, 0x187306c89bc215a9L,
      0xd4a60abcf3792b95L, 0xf935451de4f21df2L, 0xa9538f0419755787L,
      0xdb9acddff56ca510L, 0xd06c98cd5c0975ebL, 0xe612a3cb9ecba951L,
      0xc766e62cfcadaf96L, 0xee64435a9752fe72L, 0xa192d576b245165aL,
      0x0a8787bf8ecb74b2L, 0x81b3e73d20b49b6fL, 0x7fa8220ba3b2eceaL,
      0x245731c13ca42499L, 0xb78dbfaf3a8d83bdL, 0xea1ad565322a1a0bL,
      0x60e61c23a3795013L, 0x6606d7e446282b93L, 0x6ca4ecb15c5f91e1L,
      0x9f626da15c9625f3L, 0xe51b38608ef25f57L, 0x958a324ceb064572L };

  // Ported from test vectors in siphash24.c at
  // https://www.131002.net/siphash/siphash24.c
  @Test
  public void testVectors() {
    long k0 = 0x0706050403020100L;
    long k1 = 0x0f0e0d0c0b0a0908L;
    for (int i = 0; i < EXPECTED.length; ++i) {
      byte[] msg = new byte[i];
      for (int j = 0; j < i; ++j) {
        msg[j] = (byte) j;
      }
      Assert.assertEquals(EXPECTED[i], SipHashInline.hash24(k0, k1, msg, 0, msg.length));
    }
  }

  @Test
  public void testReversedHashes() {
    long k0 = 0x0706050403020100L;
    long k1 = 0x0f0e0d0c0b0a0908L;
    
    byte[] msg = "Too many secrets, Marty!".getBytes(StandardCharsets.UTF_8);
    byte[] rmsg = "!ytraM ,sterces ynam ooT".getBytes(StandardCharsets.UTF_8);
        
    long hash24 = SipHashInline.hash24(k0,k1,msg,0,msg.length,false);
    
    double ms = 0D;
    
    byte[] key = new byte[16];
    for (int i = 0; i < 1000000; i++) {
    long nano = System.nanoTime();
    //GTSHelper.classId(key, "Too many secrets, Marty!");
    //SipHashInline.hash24(k0,k1,msg,0,msg.length,true);
    //SipHashInline.hash24(k0,k1,msg,0,msg.length,true);
    //SipHashInline.hash24(k0,k1,msg,0,msg.length,true);
    Assert.assertEquals(SipHashInline.hash24(k0,k1,msg,0,msg.length,true),SipHashInline.hash24(k0,k1,rmsg,0,rmsg.length,false));
    Assert.assertEquals(SipHashInline.hash24(k0,k1,msg,0,msg.length,false),SipHashInline.hash24(k0,k1,rmsg,0,rmsg.length,true));
    ms += (System.nanoTime() - nano);
    }
    System.out.println(ms/1000000.0D);
  }
  
  /**
   * This is a test to make sure we use a corrected version of SipHashInline which is not
   * subject to negative values errors...
   */
  @Test
  public void testCollission() {
    
    byte[] b1 = new byte[] { 109, -45, -99, -85, -72, 37, -51, 120, -56, -10, -17, -53, -83, 84, -127, 67 };
    byte[] b2 = new byte[] { 109, -45, -99, -85, -72, 37, -51, 120, -56, 80, 111, 67, -59, 92, 100, 2 }; 
    
    long h1 = SipHashInline.hash24(new byte[16], b1);
    long h2 = SipHashInline.hash24(new byte[16], b2);
    
    Assert.assertTrue(h1 != h2);
  }
  
  @Test
  public void testHash24_palindromic() {
    // Test for content length varying from 1 to 16 bytes

    Random r = new Random();
    
    int from = 1;
    int n = 100;
    int offset = 10;
    
    byte[] data = new byte[n + offset];
    r.nextBytes(data);
    
    byte[] dblbuf = new byte[2 * n];
    
    // Allocate random key
    long k0 = r.nextLong();
    long k1 = r.nextLong();
    
    for (int i = from; i <= n; i++) {
      r.nextBytes(data);
      long palindromic = SipHashInline.hash24_palindromic(k0, k1, data, offset, i);
      // Create concatenated buffer
      for (int j = 0; j < i; j++) {
        dblbuf[j] = data[offset + j];
        dblbuf[i + j] = data[offset + (i - 1) - j];
      }
      long hash = SipHashInline.hash24(k0, k1, dblbuf, 0, i * 2);
      
      Assert.assertEquals(palindromic, hash);
    }
  }    
    
  @Test
  public void testHash24_palindromic_perf() {
    Random r = new Random();
    byte[] data = new byte[100];
    
    r.nextBytes(data);

    // Allocate random key
    long k0 = r.nextLong();
    long k1 = r.nextLong();
        
    long nano = System.nanoTime();
    
    for (int i = 0; i < 10000000; i++) {
      long hash = SipHashInline.hash24_palindromic(k0, k1, data, 0, data.length);
    }
    
    System.out.println((System.nanoTime() - nano) / 1000000.0D);
  }
  
  @Test
  public void testHash24_dbl_perf() {
    Random r = new Random();
    byte[] data = new byte[1000000];
    
    r.nextBytes(data);

    // Allocate random key
    long k0 = r.nextLong();
    long k1 = r.nextLong();
        
    long nano = System.nanoTime();

    for (int i = 0; i < 1000; i++) {
      byte[] dbl = new byte[data.length * 2];
      for (int j = 0; j < data.length; j++) {
        dbl[j] = data[j];
        dbl[data.length + j] = data[(data.length - 1) - j];
      }
      long hash = SipHashInline.hash24(k0, k1, dbl, 0, dbl.length);
    }
    
    System.out.println((System.nanoTime() - nano) / 1000000.0D);
  }
  
  @Test
  public void testGetKey_perf() {
    byte[] key = new byte[16];
    
    int n = 2000000000;
    
    long nano = System.nanoTime();
    
    for (int i = 0; i < n; i++) {
      long[] sipkey = SipHashInline.getKey(key);
    }
    
    System.out.println((System.nanoTime() - nano) / 1000000.0D);
  }
  
  @Test
  public void testPerf() {
    byte[] buf = new byte[256];
    
    SecureRandom sr = new SecureRandom();
    
    sr.nextBytes(buf);
    
    long nano = System.nanoTime();
    
    for (int i = 0; i < 1000; i++) {
      long hash = SipHashInline.hash24(0L, 1L, buf, 0, 256);
    }
    
    System.out.println((System.nanoTime() - nano) / 1000000.0D);
  }
}