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

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.geoxp.oss.jarjar.org.bouncycastle.util.encoders.Hex;
public class GTSDecoderTest {
  
  @Test
  public void testDecoder_encrypted() throws Exception {
    long now = System.currentTimeMillis() * 1000L;

    byte[] key = new byte[32];
    
    GTSEncoder encoder = new GTSEncoder(now - 1000000L, key);
    
    encoder.addValue(now, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 1L);
    encoder.addValue(now + 1000000L, GeoTimeSerie.NO_LOCATION, GeoTimeSerie.NO_ELEVATION, 2L);
    
    byte[] encrypted = encoder.getBytes();

    //
    // Test decoding encrypted values without the key
    //
    
    GTSDecoder decoder = new GTSDecoder(now - 1000000L, ByteBuffer.wrap(encrypted));
    
    // There should be no values as we can't decrypt them
    Assert.assertFalse(decoder.next());
    
    //
    // Test decoding encrypted values with the key
    //
    
    decoder = new GTSDecoder(now - 1000000L, key, ByteBuffer.wrap(encrypted));
   
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
  public void testDecoder_random_boolean() throws Exception {
    
    //
    // Create an encoder
    //
    
    GTSEncoder encoder = new GTSEncoder();
    
    //
    // Generate N values with associated timestamp/location/elevation
    //
    
    int N = 1000000;
    
    boolean[] values = new boolean[N];
    long[] locations = new long[N];
    long[] elevations = new long[N];
    long[] timestamps = new long[N];
    
    long lastlocation = (long) (Math.random() * Long.MAX_VALUE);
    for (int i = 0; i < N; i++) {
      values[i] = Math.random() > 0.5;
      locations[i] = lastlocation + (long) (Math.random() * Long.MAX_VALUE);
      elevations[i] = (long) (Math.random() * 1000000L);
      timestamps[i] = System.currentTimeMillis() * 1000L + i;
      encoder.addValue(timestamps[i], locations[i], elevations[i], values[i]);
    }
    
    byte[] bytes = encoder.getBytes();
    System.out.println(bytes.length);

    int i = 0;
    
    ByteBuffer bb = ByteBuffer.wrap(encoder.getBytes());
    //bb.position(bytes.length);
    
    GTSDecoder decoder = new GTSDecoder(0, bb);
    
    while(decoder.next()) {
      Assert.assertEquals(timestamps[i], decoder.getTimestamp());
      Assert.assertEquals(locations[i], decoder.getLocation());
      Assert.assertEquals(elevations[i], decoder.getElevation());
      Assert.assertEquals(values[i], decoder.getValue());
      i++;
    } 
    
    Assert.assertEquals(N, i);
  }

  @Test
  public void testDecoder_random_long() throws Exception {
    
    //
    // Create an encoder
    //
    
    GTSEncoder encoder = new GTSEncoder();
    
    //
    // Generate N values with associated timestamp/location/elevation
    //
    
    int N = 1000000;
    
    long[] values = new long[N];
    long[] locations = new long[N];
    long[] elevations = new long[N];
    long[] timestamps = new long[N];
    
    long lastlocation = (long) (Math.random() * Long.MAX_VALUE);
    for (int i = 0; i < N; i++) {
      values[i] = (long) (Math.random() * 2 * Long.MAX_VALUE + Long.MIN_VALUE);
      locations[i] = lastlocation + (long) (Math.random() * Long.MAX_VALUE);
      elevations[i] = (long) (Math.random() * 1000000L);
      timestamps[i] = System.currentTimeMillis() * 1000L + i;
      encoder.addValue(timestamps[i], locations[i], elevations[i], values[i]);
    }
    
    byte[] bytes = encoder.getBytes();
    System.out.println(bytes.length);

    int i = 0;
    
    ByteBuffer bb = ByteBuffer.wrap(encoder.getBytes());
    //bb.position(bytes.length);
    
    GTSDecoder decoder = new GTSDecoder(0, bb);
    
    while(decoder.next()) {
      Assert.assertEquals(timestamps[i], decoder.getTimestamp());
      Assert.assertEquals(locations[i], decoder.getLocation());
      Assert.assertEquals(elevations[i], decoder.getElevation());
      Assert.assertEquals(values[i], decoder.getValue());
      i++;
    } 
    
    Assert.assertEquals(N, i);
  }
  
  @Test
  public void testDecoder_random_double() throws Exception {
    
    //
    // Create an encoder
    //
    
    GTSEncoder encoder = new GTSEncoder();
    
    //
    // Generate N values with associated timestamp/location/elevation
    //
    
    int N = 1000000;
    
    double[] values = new double[N];
    long[] locations = new long[N];
    long[] elevations = new long[N];
    long[] timestamps = new long[N];
    
    long lastlocation = (long) (Math.random() * Long.MAX_VALUE);
    for (int i = 0; i < N; i++) {
      values[i] = Math.random() * Double.MAX_VALUE;
      locations[i] = lastlocation + (long) (Math.random() * Long.MAX_VALUE);
      elevations[i] = (long) (Math.random() * 1000000L);
      timestamps[i] = System.currentTimeMillis() * 1000L + i;
      encoder.addValue(timestamps[i], locations[i], elevations[i], values[i]);
    }
    
    byte[] bytes = encoder.getBytes();

    System.out.println(bytes.length);
    
    for (int k = 0; k < 1; k++){

    int i = 0;
    
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    //bb.position(bytes.length);
    
    GTSDecoder decoder = new GTSDecoder(0, bb);
    
    while(decoder.next()) {
      Assert.assertEquals(timestamps[i], decoder.getTimestamp());
      Assert.assertEquals(locations[i], decoder.getLocation());
      Assert.assertEquals(elevations[i], decoder.getElevation());
      Assert.assertEquals(values[i], decoder.getValue());
      i++;
    } 
    
    Assert.assertEquals(N, i);
    }
  }

  @Test
  public void testDecoder_random_string() throws Exception {
    
    //
    // Create an encoder
    //
    
    GTSEncoder encoder = new GTSEncoder();
    
    //
    // Generate N values with associated timestamp/location/elevation
    //
    
    int N = 1000;
    
    String[] values = new String[N];
    long[] locations = new long[N];
    long[] elevations = new long[N];
    long[] timestamps = new long[N];
    
    long lastlocation = (long) (Math.random() * Long.MAX_VALUE);
    for (int i = 0; i < N; i++) {
      values[i] = Double.toString(Math.random() * Double.MAX_VALUE);
      locations[i] = lastlocation + (long) (Math.random() * Long.MAX_VALUE);
      elevations[i] = (long) (Math.random() * 1000000L);
      timestamps[i] = System.currentTimeMillis() * 1000L + i;
      encoder.addValue(timestamps[i], locations[i], elevations[i], values[i]);
    }
    
    byte[] bytes = encoder.getBytes();

    System.out.println(bytes.length);
    
    for (int k = 0; k < 1; k++){
    int i = 0;
    
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    //bb.position(bytes.length);
    
    GTSDecoder decoder = new GTSDecoder(0, bb);
    
    while(decoder.next()) {
      Assert.assertEquals(timestamps[i], decoder.getTimestamp());
      Assert.assertEquals(locations[i], decoder.getLocation());
      Assert.assertEquals(elevations[i], decoder.getElevation());
      Assert.assertEquals(values[i], decoder.getValue());
      i++;
    }
    
    Assert.assertEquals(N, i);
    }
  }

  @Test
  public void testDecoder_ConsecutiveEncodes() {
    //
    // The goal of this test is to ensure that we do not
    // incorrectly decode a buffer built by concatenating
    // independently encoded buffers which may have use
    // some delta encoding from the previous value.
    //
  }
  
  @Test
  public void testDecoder_getEncoder() throws IOException {
    GTSEncoder encoder = new GTSEncoder(2L);
    encoder.setName("name");
    encoder.setLabel("label0", "value0");
    encoder.setLabel("label1", "value1");
    
    long ts = System.currentTimeMillis() * 1000L;
    
    int n = 1000;
    
    for (int i = 1; i < n; i++) {
      encoder.addValue(i * 1L, i * 10L, i * 100L, i * 1000L);
    }
    
    GTSDecoder decoder = encoder.getDecoder(true);
    decoder.next();
    Assert.assertEquals((long) 1L, decoder.getTimestamp());
    Assert.assertEquals((long) 10L, decoder.getLocation());
    Assert.assertEquals((long) 100L, decoder.getElevation());
    Assert.assertEquals((long) 1000L, decoder.getValue());
    
    long nano = System.nanoTime();

    //
    // Add 'add' values at the end of the encoder to check if
    // the initial values are correctly handled
    //
    
    int add = 10;
    
    for (int i = 1; i < n - 1 + add; i++) {
      decoder.next();      
      encoder = decoder.getEncoder(true);
      
      if (add > 0) {
        encoder.addValue(i + n - 1, (i+n-1)*10L, (i+n-1)*100L, (i+n-1)*1000L);
      }
      
      //System.out.println(encoder.size());
      decoder = encoder.getDecoder(true);
      decoder.next();
      Assert.assertEquals((long) (i + 1), decoder.getTimestamp());
      Assert.assertEquals((long) 10L * (i + 1), decoder.getLocation());
      Assert.assertEquals((long) 100L * (i + 1), decoder.getElevation());
      Assert.assertEquals((long) 1000L * (i + 1), decoder.getValue());
    }
  }
  
  @Test
  public void testDecoder_getEncoder2() throws Exception {
    GTSEncoder encoder = new GTSEncoder();
    
    int n = 1000;
    
    for (int i = 1; i < n; i++) {
      encoder.addValue(i * 1L, i * 10L, i * 100L, i * 1000L);      
    }
    
    for (int i = 0; i < n >> 1; i++) {
    GTSDecoder decoder = encoder.getDecoder();
    Assert.assertTrue(decoder.next());
    GTSEncoder enc = decoder.getEncoder();
    // The encoder we retrieve after calling 'next' should have the same size as
    // the one that gave birth to 'decoder'
    Assert.assertEquals(encoder.size(), enc.size());
    GTSDecoder dec = enc.getDecoder();
    // Advance 'dec' by 2
    dec.next(); dec.next();
    // Advance decoder by 1
    decoder.next();
    Assert.assertEquals(decoder.getTimestamp(), dec.getTimestamp());
    Assert.assertEquals(decoder.getLocation(), dec.getLocation());
    Assert.assertEquals(decoder.getElevation(), dec.getElevation());
    Assert.assertEquals(decoder.getValue(), dec.getValue());
    encoder = decoder.getEncoder();
    }
  }
  
  @Test
  public void testDecoder_getEncoderWithEncryptedContent() throws Exception {
    //
    // Check that getEncoder functions propertly when next encountered some encrypted content
    //
  }
  
  @Test
  public void testDecoder_getEncoder3() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    encoder.addValue(1L, 1L, 1L, 1L);
    encoder.addValue(2L, 2L, 2L, 2L);
    
    GTSDecoder decoder = encoder.getDecoder(true);
    
    int encsize = encoder.size();
    
    Assert.assertEquals(encsize, decoder.getRemainingSize());
    
    decoder.next();
    Assert.assertEquals(6, decoder.getRemainingSize());
    
    encoder = decoder.getEncoder(true);
    
    //
    // Check that getting an encoder after calling next returns the data including the
    // one retrieved by the call to 'next'
    //
    
    Assert.assertEquals(19, encoder.size());    
  }  
  
  @Test
  public void testDecoder_duplicateBinary() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    encoder.addValue(0L, 0L, 0L, "@".getBytes(StandardCharsets.ISO_8859_1));
    encoder.addValue(0L, 0L, 0L, "@");
    
    GTSDecoder decoder = encoder.getDecoder();
    decoder.next();
    
    Assert.assertTrue(decoder.getBinaryValue() instanceof byte[]);
    Assert.assertTrue(decoder.isBinary());
    
    decoder = decoder.duplicate();    
    Assert.assertTrue(decoder.getBinaryValue() instanceof byte[]);
    Assert.assertTrue(decoder.isBinary());
    
    decoder.next();
    Assert.assertTrue(decoder.getBinaryValue() instanceof String);
    Assert.assertFalse(decoder.isBinary());
    
    decoder = decoder.duplicate();
    Assert.assertTrue(decoder.getBinaryValue() instanceof String);
    Assert.assertFalse(decoder.isBinary());
  }
  
  @Test
  public void testDecoder_dump() throws Exception {
    GTSEncoder encoder = new GTSEncoder(0L);
    encoder.addValue(0L, 0L, 0L, false);
    encoder.addValue(1L, 0L, 0L, 1L);
    encoder.addValue(2L, 0L, 0L, 2.0D);
    encoder.addValue(3L, 0L, 0L, "3");
    encoder.addValue(4L, 0L, 0L, "é".getBytes(StandardCharsets.ISO_8859_1));
    
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    
    GTSDecoder decoder = encoder.getDecoder();
    decoder.dump(pw);
    pw.close();
    
    Assert.assertEquals("0/-90.0:-180.0/0 {} F\r\n=1/-90.0:-180.0/0 1\r\n=2/-90.0:-180.0/0 2.0\r\n=3/-90.0:-180.0/0 '3'\r\n=4/-90.0:-180.0/0 b64:6Q\r\n", sw.toString());
  }
}
