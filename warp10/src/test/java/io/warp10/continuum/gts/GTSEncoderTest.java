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

import io.warp10.WarpConfig;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.geoxp.GeoXPLib;
public class GTSEncoderTest {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.safeSetProperties(new StringReader(props.toString()));
  }
  
  @Test
  public void testAddValue_encrypted() throws Exception {
    long now = System.currentTimeMillis() * 1000L;

    byte[] key = new byte[32];
    
    GTSEncoder encoder = new GTSEncoder(now - 1000000L, key);
    
    encoder.addValue(now, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1L);
    encoder.addValue(now + 1000000L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 2L);
    
    byte[] encrypted = encoder.getBytes();
    Assert.assertEquals(GTSEncoder.FLAGS_ENCRYPTED, encrypted[0] & GTSEncoder.FLAGS_MASK_ENCRYPTED);
    Assert.assertEquals(26, encrypted.length);

    //
    // Now check that we can decrypt the payload
    // We can't use n offset different than 0 in unwrap due to BJA-461
    // so we have to copy the data prior to decrypting it.
    //
        
    AESWrapEngine engine = new AESWrapEngine();
    KeyParameter params = new KeyParameter(key);
    engine.init(false, params);
    byte[] enc = new byte[24];
    System.arraycopy(encrypted, 2, enc, 0, 24);
    byte[] decrypted = engine.unwrap(enc, 0, 24);    
    
    //
    // Now decode the decrypted data
    //
    
    PKCS7Padding padding = new PKCS7Padding();    
    GTSDecoder decoder = new GTSDecoder(now - 1000000L, ByteBuffer.wrap(decrypted, 0, decrypted.length - padding.padCount(decrypted)));
    
    decoder.next();
    Assert.assertEquals(now, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertEquals(1L, decoder.getValue());

    decoder.next();
    Assert.assertEquals(now + 1000000L, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertEquals(2L, decoder.getValue());
  }
  
  @Test
  public void testGetDecoder() throws Exception {
    long now = System.currentTimeMillis() * 1000L;
    byte[] key = new byte[32];
    GTSEncoder encoder = new GTSEncoder(now - 1000000L, key);
    
    encoder.addValue(now, 111L, 11L, new BigDecimal("1.11"));
    encoder.addValue(now + 1000000L, 222L, 22L, new BigDecimal("2.22"));
    
    GTSDecoder decoder = encoder.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now, decoder.getTimestamp());
    Assert.assertEquals(111L, decoder.getLocation());
    Assert.assertEquals(11L, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("1.11"), decoder.getValue());
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now + 1000000L, decoder.getTimestamp());
    Assert.assertEquals(222L, decoder.getLocation());
    Assert.assertEquals(22L, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("2.22"), decoder.getValue());
    
    Assert.assertFalse(decoder.next());
  }
  
  @Test
  public void testMerge() throws Exception {
    long now = System.currentTimeMillis() * 1000L;
    byte[] key = new byte[32];
    GTSEncoder encoder1 = new GTSEncoder(now - 1000000L, key);
    
    encoder1.addValue(now, 111L, 11L, new BigDecimal("1.11"));
    encoder1.addValue(now + 1000000L, 222L, 22L, new BigDecimal("2.22"));
    
    GTSEncoder encoder2 = new GTSEncoder(now - 500000L);
    
    encoder2.addValue(now, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, new BigDecimal("3.33"));
    encoder2.addValue(now + 500000L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, new BigDecimal("4.44"));
    
    encoder1.merge(encoder2);
    
    GTSDecoder decoder = encoder1.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now, decoder.getTimestamp());
    Assert.assertEquals(111L, decoder.getLocation());
    Assert.assertEquals(11L, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("1.11"), decoder.getValue());
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now + 1000000L, decoder.getTimestamp());
    Assert.assertEquals(222L, decoder.getLocation());
    Assert.assertEquals(22L, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("2.22"), decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("3.33"), decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now + 500000L, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("4.44"), decoder.getValue());

    Assert.assertFalse(decoder.next());
    
    //
    // Now do the same with encoders which have the same base, triggering the use of the fastpath
    //
    
    encoder1 = new GTSEncoder(now - 1000000L, key);
    encoder1.addValue(now, 111L, 11L, new BigDecimal("1.11"));
    encoder1.addValue(now + 1000000L, 222L, 22L, new BigDecimal("2.22"));

    encoder2 = new GTSEncoder(now - 1000000L, key);
    encoder2.addValue(now, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, new BigDecimal("3.33"));
    encoder2.addValue(now + 500000L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, new BigDecimal("4.44"));
    
    encoder1.merge(encoder2);

    decoder = encoder1.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now, decoder.getTimestamp());
    Assert.assertEquals(111L, decoder.getLocation());
    Assert.assertEquals(11L, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("1.11"), decoder.getValue());
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now + 1000000L, decoder.getTimestamp());
    Assert.assertEquals(222L, decoder.getLocation());
    Assert.assertEquals(22L, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("2.22"), decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("3.33"), decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(now + 500000L, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertEquals(new BigDecimal("4.44"), decoder.getValue());

    Assert.assertFalse(decoder.next());
    
    
  }
  
  @Test
  public void testMerge_FastPath() throws Exception {
    //
    // This test is here to ensure we correctly copy the last value/ts/loc/elev when merging using
    // the fast path, if we don't, the next value entered might get delta encoded with the wrong
    // reference thus leading to an incorrect value when decoding.
    //
    
    GTSEncoder encoder1 = new GTSEncoder(0L);
    GTSEncoder encoder2 = new GTSEncoder(0L);
    
    encoder1.addValue(1L, 1L, 1L, 100);
    encoder1.addValue(2L, 2L, 2L, 101);
    
    encoder2.addValue(3L, 3L, 3L, 102);
    
    // Merge encoder2 into encoder1
    encoder1.merge(encoder2);
    
    // Now add a value to encoder1
    encoder1.addValue(4L, 4L, 4L, 103);
    
    // Decode values, ensuring they are all correct
    GTSDecoder decoder = encoder1.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(1L, decoder.getTimestamp());
    Assert.assertEquals(1L, decoder.getLocation());
    Assert.assertEquals(1L, decoder.getElevation());
    Assert.assertEquals(100L, decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(2L, decoder.getTimestamp());
    Assert.assertEquals(2L, decoder.getLocation());
    Assert.assertEquals(2L, decoder.getElevation());
    Assert.assertEquals(101L, decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(3L, decoder.getTimestamp());
    Assert.assertEquals(3L, decoder.getLocation());
    Assert.assertEquals(3L, decoder.getElevation());
    Assert.assertEquals(102L, decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(4L, decoder.getTimestamp());
    Assert.assertEquals(4L, decoder.getLocation());
    Assert.assertEquals(4L, decoder.getElevation());
    Assert.assertEquals(103L, decoder.getValue());

  }
  
  
  @Test
  public void testDelete() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(1L, 2L, 3L, null);

    GTSDecoder decoder = encoder.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(1L, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertNull(decoder.getValue());
    Assert.assertFalse(decoder.next());
  }
  
  @Test
  public void testAddValue_IntermittentLocation() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(1L, 1L, 10L, 1L);
    encoder.addValue(2L, 2L, 20L, 2L);
    encoder.addValue(3L, GeoTimeSerie.NO_LOCATION, 30L, 3L);
    encoder.addValue(4L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 4L);
    encoder.addValue(5L, 5L, 50L, 5L);
    
    GTSDecoder decoder = encoder.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(1L, decoder.getLocation());
    Assert.assertEquals(10L, decoder.getElevation());
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(2L, decoder.getLocation());
    Assert.assertEquals(20L, decoder.getElevation());
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(30L, decoder.getElevation());
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(5L, decoder.getLocation());
    Assert.assertEquals(50L, decoder.getElevation());
  }
  
  @Test
  public void testEncoding() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(0, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1);
    
    Assert.assertEquals("2c02", new String(Hex.encode(encoder.getBytes())));
    
    //
    // Timestamp which will be raw encoded
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0x0123456789abcdefL, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1);
    Assert.assertEquals("6c0123456789abcdef02", new String(Hex.encode(encoder.getBytes())));
    
    //
    // Timestamp which will be delta encoded from base
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue((1L << 48) - 1L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1);
    Assert.assertEquals("4cfeffffffffff7f02", new String(Hex.encode(encoder.getBytes())));
    
    //
    // Two values, delta encoding of ts and values
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1);
    encoder.addValue(1L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 2);
    Assert.assertEquals("2c024e0202", new String(Hex.encode(encoder.getBytes())));

    //
    // Two values, delta encoding of ts, identical values
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1);
    encoder.addValue(1L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1);
    Assert.assertEquals("2c024902", new String(Hex.encode(encoder.getBytes())));

    //
    // Double value, IEEE754
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, Double.NaN);
    Assert.assertEquals("347ff8000000000000", new String(Hex.encode(encoder.getBytes())));

    //
    // BigDecimal value, custom encoding
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, new BigDecimal("1.0"));
    Assert.assertEquals("300002", new String(Hex.encode(encoder.getBytes())));

    //
    // Location and delta location
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0L, 0xb000000000000001L, GeoTimeSerie.NO_ELEVATION, 0);
    encoder.addValue(1L, 0xb000000000000002L, GeoTimeSerie.NO_ELEVATION, 1);
    Assert.assertEquals("ac40b00000000000000100cc60020202", new String(Hex.encode(encoder.getBytes())));

    //
    // Elevation and delta elevation
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, 0x7000000000000001L, 0);
    encoder.addValue(1L, GeoTimeSerie.NO_LOCATION, 0x7000000000000002L, 1);
    Assert.assertEquals("ac08700000000000000100cc0e020202", new String(Hex.encode(encoder.getBytes())));
    
    //
    // Location + Elevation
    //
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0L, 0xb000000000000001L, 0x7000000000000001L, 0);
    encoder.addValue(1L, 0xb000000000000002L, 0x7000000000000002L, 1);
    Assert.assertEquals("ac48b000000000000001700000000000000100cc6e02020202", new String(Hex.encode(encoder.getBytes())));

}
  
  @Test
  public void testEncodingConsistency() throws Exception {
    GTSEncoder encoder = null;
    
    for (long base = 0; base < 2; base++) {
      encoder = new GTSEncoder(base);
      
      for (int i = 0; i < 100000; i++) {
        encoder.addValue(i, i * 10, i * 100, i * 1000);        
      }
      
      GTSDecoder decoder = encoder.getDecoder();
      
      for (int i = 0; i < encoder.getCount(); i++) {
        Assert.assertTrue(decoder.next());
        Assert.assertEquals(i, decoder.getTimestamp());
        Assert.assertEquals(i * 10L, decoder.getLocation());
        Assert.assertEquals(i * 100L, decoder.getElevation());
        Assert.assertEquals(i * 1000L, decoder.getValue());
      }
      
      Assert.assertFalse(decoder.next());
    }
  }
  
  @Test
  public void testSafeDelta() throws Exception {
    // Create an encoder with a single value
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(1L, 2L, 3L, 4L);
    
    byte[] bytes = encoder.getBytes();
    
    // Reallocate encoder
    encoder = new GTSEncoder(0L, null, bytes);
    
    Assert.assertEquals(13, encoder.size());
    
    // Add values, adding location and elevation progressively
    
    encoder.addValue(2L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 4L);
    Assert.assertEquals(23, encoder.size());    
    encoder.addValue(3L, 4L, GeoTimeSerie.NO_ELEVATION, 4L);
    Assert.assertEquals(34, encoder.size());
    encoder.addValue(4L, GeoTimeSerie.NO_LOCATION, 4L, 4L);
    Assert.assertEquals(38, encoder.size());
    encoder.addValue(5L, 6L, 4L, 9L);
    Assert.assertEquals(50, encoder.size());
    encoder.addValue(6L, 4L, 4L, 9L);
    Assert.assertEquals(54, encoder.size());
    encoder.addValue(7L, 4L, 4L, 9L);
    Assert.assertEquals(57, encoder.size());
    
    GTSDecoder decoder = encoder.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(1L, decoder.getTimestamp());
    Assert.assertEquals(2L, decoder.getLocation());
    Assert.assertEquals(3L, decoder.getElevation());
    Assert.assertEquals(4L, decoder.getValue());
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(2L, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertEquals(4L, decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(3L, decoder.getTimestamp());
    Assert.assertEquals(4L, decoder.getLocation());
    Assert.assertEquals(GeoTimeSerie.NO_ELEVATION, decoder.getElevation());
    Assert.assertEquals(4L, decoder.getValue());
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(4L, decoder.getTimestamp());
    Assert.assertEquals(GeoTimeSerie.NO_LOCATION, decoder.getLocation());
    Assert.assertEquals(4L, decoder.getElevation());
    Assert.assertEquals(4L, decoder.getValue());
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(5L, decoder.getTimestamp());
    Assert.assertEquals(6L, decoder.getLocation());
    Assert.assertEquals(4L, decoder.getElevation());
    Assert.assertEquals(9L, decoder.getValue());

    Assert.assertTrue(decoder.next());
    Assert.assertEquals(6L, decoder.getTimestamp());
    Assert.assertEquals(4L, decoder.getLocation());
    Assert.assertEquals(4L, decoder.getElevation());
    Assert.assertEquals(9L, decoder.getValue());
    
    Assert.assertTrue(decoder.next());
    Assert.assertEquals(7L, decoder.getTimestamp());
    Assert.assertEquals(4L, decoder.getLocation());
    Assert.assertEquals(4L, decoder.getElevation());
    Assert.assertEquals(9L, decoder.getValue());
  }
    
  @Test
  public void testToBlock() throws IOException {
    GTSEncoder encoder = new GTSEncoder(12345678);
    
    int n = 2;
    
    for (int i = 0; i < n; i++) {
      encoder.addValue(i * 1000, i * 12345678, i * 87654321, i * Math.PI);
    }
    
    System.out.println(encoder.size());
    
    long nano = System.nanoTime();
    byte[] block = encoder.toBlock(true);
    System.out.println((System.nanoTime() - nano) / 1000000.0D);
    System.out.println(block.length);
    
    nano = System.nanoTime();
    
    GTSDecoder decoder = GTSDecoder.fromBlock(block, null);
    System.out.println((System.nanoTime() - nano) / 1000000.0D);
    GTSDecoder orig = encoder.getDecoder();
    
    while(true) {
      boolean copy = decoder.next();
      boolean org = orig.next();
      
      Assert.assertEquals(copy,org);
      
      if (!copy) {
        break;
      }
      Assert.assertEquals(orig.getTimestamp(), decoder.getTimestamp());
      Assert.assertEquals(orig.getLocation(), decoder.getLocation());
      Assert.assertEquals(orig.getElevation(), decoder.getElevation());
      Assert.assertEquals(orig.getValue(), decoder.getValue());
    }
  }
  
  @Test
  public void testWARP50() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder = GTSHelper.parse(encoder, "0// gts{} 10000000.000000");
    encoder = GTSHelper.parse(encoder, "0// gts{} -20000000.000000");
    encoder = GTSHelper.parse(encoder, "0// gts{} 10000000.000000");
    encoder = GTSHelper.parse(encoder, "0// gts{} -20000000.000000");

    GTSDecoder decoder = encoder.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertTrue(decoder.getValue() instanceof BigDecimal);
    Assert.assertEquals(10000000.0D, ((BigDecimal) decoder.getValue()).doubleValue(), 0.01D);

    Assert.assertTrue(decoder.next());
    Assert.assertTrue(decoder.getValue() instanceof Double);
    Assert.assertEquals(-20000000.0D, ((Double) decoder.getValue()).doubleValue(), 0.01D);

    Assert.assertTrue(decoder.next());
    Assert.assertTrue(decoder.getValue() instanceof BigDecimal);
    Assert.assertEquals(10000000.0D, ((BigDecimal) decoder.getValue()).doubleValue(), 0.01D);

    Assert.assertTrue(decoder.next());
    Assert.assertTrue(decoder.getValue() instanceof Double);
    Assert.assertEquals(-20000000.0D, ((Double) decoder.getValue()).doubleValue(), 0.01D);
  }
  
  @Test
  public void testResetLONG() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1L);
    encoder.addValue(1L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1L);
    
    GTSDecoder decoder = encoder.getDecoder();
    decoder.next();
    decoder.next();
    
    encoder.reset(decoder.getEncoder());
    
    decoder = encoder.getDecoder();
    
    decoder.next();
    
    Assert.assertEquals(1L, decoder.getTimestamp());
    Assert.assertEquals(1L, decoder.getValue());
  }

  @Test
  public void testResetDOUBLE() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1.0D);
    encoder.addValue(1L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1.0D);
    
    GTSDecoder decoder = encoder.getDecoder();
    decoder.next();
    decoder.next();
    
    encoder.reset(decoder.getEncoder());
    
    decoder = encoder.getDecoder();
    
    decoder.next();
    
    Assert.assertEquals(1L, decoder.getTimestamp());
    Assert.assertEquals(1.0D, (double) decoder.getValue(), 0.000000000001D);
  }

  @Test
  public void testResetSTRING() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "1");
    encoder.addValue(1L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "1");
    
    GTSDecoder decoder = encoder.getDecoder();
    decoder.next();
    decoder.next();
    
    encoder.reset(decoder.getEncoder());
    
    decoder = encoder.getDecoder();
    
    decoder.next();
    
    Assert.assertEquals(1.0D, decoder.getTimestamp(), 0.000000000001D);
    Assert.assertEquals("1", decoder.getValue().toString());
  }
  
  @Test
  public void testBINARY() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "é".getBytes(StandardCharsets.ISO_8859_1));
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "é".getBytes(StandardCharsets.ISO_8859_1));
    
    byte[] bytes = encoder.getBytes();
    Assert.assertEquals(4, bytes.length);
    
    encoder.reset(0L);
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "é".getBytes(StandardCharsets.ISO_8859_1));

    bytes = encoder.getBytes();
    Assert.assertEquals(3, bytes.length);
    
    GTSEncoder enc2 = new GTSEncoder(123L);
    enc2.reset(encoder);
    
    bytes = encoder.getBytes();
    Assert.assertEquals(3, bytes.length);

    GTSDecoder decoder = enc2.getDecoder();
    
    Assert.assertTrue(decoder.next());
    Assert.assertTrue(0L == decoder.getTimestamp());
    Assert.assertTrue(decoder.getBinaryValue() instanceof byte[]);
    Assert.assertEquals("é", decoder.getValue());        
    
    encoder = new GTSEncoder(0L);
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "@".getBytes(StandardCharsets.ISO_8859_1));
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "@");
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "@".getBytes(StandardCharsets.ISO_8859_1));
    
    bytes = encoder.getBytes();
    Assert.assertEquals(5, bytes.length);
    
    //
    // Ensure that dedup does not remove artificial duplicates between byte[] and String values
    //
    
    decoder = encoder.getDecoder();
    decoder = decoder.dedup();
    decoder.next();
    encoder = decoder.getEncoder();
    
    bytes = encoder.getBytes();
    Assert.assertEquals(5, bytes.length);    
  }
  
  @Test
  public void testResetBINARY() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    
    encoder.addValue(0L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "é".getBytes(StandardCharsets.ISO_8859_1));
    encoder.addValue(1L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, "è".getBytes(StandardCharsets.ISO_8859_1));
    
    GTSDecoder decoder = encoder.getDecoder();
    decoder.next();
    decoder.next();
    
    encoder.reset(decoder.getEncoder());
    
    decoder = encoder.getDecoder();
    
    decoder.next();
    
    Assert.assertEquals(1.0D, decoder.getTimestamp(), 0.000000000001D);
    Object value = decoder.getBinaryValue();
    Assert.assertTrue(value instanceof byte[]);    
    Assert.assertEquals(1, ((byte[]) value).length);
    Assert.assertEquals((byte) 0xE8, ((byte[]) value)[0]);    
  }
  
  @Test
  public void testParse() throws Exception {
    String GTS = "0/-90.0:-180.0/0 {} F\r\n=1/-90.0:-180.0/0 1\r\n=2/-90.0:-180.0/0 2.0\r\n=3/-90.0:-180.0/0 '3'\r\n=4/-90.0:-180.0/0 b64:6Q\r\n=5// hex:404142\r\n";
    
    BufferedReader br = new BufferedReader(new StringReader(GTS));
    
    GTSEncoder encoder = null;
    
    while(true) {
      String line = br.readLine();
      
      if (null == line) {
        break;
      }
      
      encoder = GTSHelper.parse(encoder, line);
    }
    
    br.close();
    
    GTSDecoder decoder = encoder.getDecoder();
    
    decoder.next();
    Assert.assertEquals(false, decoder.getBinaryValue());
    decoder.next();
    Assert.assertEquals(1L, decoder.getBinaryValue());
    decoder.next();
    Assert.assertTrue(decoder.getBinaryValue() instanceof BigDecimal);
    Assert.assertEquals(new BigDecimal(2.0D), decoder.getBinaryValue());
    decoder.next();
    Assert.assertEquals("3", decoder.getBinaryValue());
    decoder.next();
    Assert.assertTrue(decoder.getBinaryValue() instanceof byte[]);
    Assert.assertTrue(decoder.isBinary());
    Assert.assertArrayEquals("é".getBytes(StandardCharsets.ISO_8859_1), (byte[]) decoder.getBinaryValue());
    Assert.assertEquals("é", decoder.getValue());
    decoder.next();
    Assert.assertTrue(decoder.getBinaryValue() instanceof byte[]);
    Assert.assertTrue(decoder.isBinary());
    Assert.assertArrayEquals("@AB".getBytes(StandardCharsets.ISO_8859_1), (byte[]) decoder.getBinaryValue());
    Assert.assertEquals("@AB", decoder.getValue());

  }
}
