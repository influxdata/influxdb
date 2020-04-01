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

import io.warp10.continuum.gts.GeoTimeSerie.TYPE;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESWrapEngine;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.KeyParameter;

import com.geoxp.GeoXPLib;
/**
 * Class for decoding an encoded time serie.
 * 
 * WARNING: this class is NOT ThreadSafe as the underlying ByteBuffer could
 * be advanced by another thread while in a decoding function.
 * 
 * This is a known fact, we don't intend to make the methods synchronized so as
 * not to undermine the performances.
 */
public class GTSDecoder {
  
  /**
   * Buffer this decoder will decode
   */
  private ByteBuffer buffer;
  
  private TYPE lastType = null;
    
  /**
   * Base timestamp to use for this decoder
   */
  final long baseTimestamp;
  
  /**
   * Last timestamp retrieved from decoder (post call to 'next')
   */
  private long lastTimestamp = 0L;
  
  /**
   * Last location retrieved from decoder (post call to 'next')
   */
  private long lastGeoXPPoint = GeoTimeSerie.NO_LOCATION;
  
  /**
   * Last elevation retrieved from decoder (post call to 'next')
   */
  private long lastElevation = GeoTimeSerie.NO_ELEVATION;
  
  /**
   * Last long value retrieved from decoder (post call to 'next')
   */
  private long lastLongValue = Long.MAX_VALUE;
  
  /**
   * Last boolean value retrieved from decoder (post call to 'next')
   */
  private boolean lastBooleanValue = false;
  
  /**
   * Last double value retrieved from decoder (post call to 'next')
   */
  private double lastDoubleValue = Double.NaN;
  
  /**
   * Last BigDecimal value retrieved from decoder (post call to 'next')
   */
  private BigDecimal lastBDValue = null;
  
  /**
   * Last String retrieved from decoder (post call to 'next')
   */
  private String lastStringValue = null;
  private boolean lastStringBinary = false;
  
  private long previousLastTimestamp = lastTimestamp;
  private long previousLastGeoXPPoint = lastGeoXPPoint;
  private long previousLastElevation = lastElevation;
  private long previousLastLongValue = lastLongValue;
  private double previousLastDoubleValue = lastDoubleValue;
  private BigDecimal previousLastBDValue = lastBDValue;
  private String previousLastStringValue = lastStringValue;

  /**
   * Flag indicating whether or not 'next' was called at least once
   */
  private boolean nextCalled = false;
  
  /**
   * Flag indicating whether or not we decoded some encrypted data
   */
  private boolean decodedEncrypted = false;
  
  /**
   * Number of calls to next() which consumed datapoints
   */
  private int consumingNextCalls = 0;
  
  /**
   * AES key for encrypting content
   */
  private final byte[] wrappingKey;
  
  /**
   * Metadata associated with this decoder
   */
  private Metadata metadata;
  
  /**
   * Position of the current (post call to next()) reading in the buffer
   */
  private int position;

  /**
   * Estimation of the number of elements in the decoder
   */
  private long count = 0;
  
  /**
   * @param baseTimestamp Base timestamp for computing deltas.
   * @param bb ByteBuffer containing the encoded GTS. Only remaining data will be read.
   *                      Encrypted data will silently be skipped.
   */
  public GTSDecoder(long baseTimestamp, ByteBuffer bb) {
    this.baseTimestamp = baseTimestamp;
    this.buffer = bb;
    this.wrappingKey = null;
  }

  /**
   * @param baseTimestamp Base timestamp for computing deltas.
   * @param key AES Wrapping key to use for unwrapping encrypted data
   * @param bb ByteBuffer containing the encoded GTS. Only remaining data will be read.
   *                      Encrypted data that cannot be decrypted will be silently ignored.
   *                      If the buffer contains encrypted data which could be decrypted,
   *                      reallocation will take place therefore 'bb' and the internal buffer
   *                      used by this instance of GTSDecoder will
   *                      differ after the first encrypted chunk is encountered.
   */
  public GTSDecoder(long baseTimestamp, byte[] key, ByteBuffer bb) {
    this.baseTimestamp = baseTimestamp;
    this.buffer = bb;
    if (null != key) {
      this.wrappingKey = Arrays.copyOfRange(key, 0, key.length);
    } else {
      this.wrappingKey = null;
    }
    this.position = bb.position();
  }

  /**
   * Attempt to read the next measurement and associated metadata (timestamp, location, elevation)
   * @return true if a measurement was successfully read, false if none were left in the buffer.
   */
  public boolean next() {
    
    //
    // Update position prior to reading the next value, etc so we can 
    //
    
    this.position = this.buffer.position();

    if (!buffer.hasRemaining()) {
      return false;
    }

    this.nextCalled = true;

    //
    // Read timestamp/type flag
    //
    
    byte tsTypeFlag = buffer.get();

    //
    // Check if we encountered encrypted data
    //
    
    if (GTSEncoder.FLAGS_ENCRYPTED == (tsTypeFlag & GTSEncoder.FLAGS_MASK_ENCRYPTED)) {
      //
      // Extract encrypted length
      //
      
      int enclen = (int) Varint.decodeUnsignedLong(buffer);

      //
      // If there is no decryption key, simply skip the encrypted data
      // and call next recursively.
      //
      
      if (null == wrappingKey) {
        buffer.position(buffer.position() + enclen);
        
        // WARNING(hbs): if there are many encrypted chunks this may lead to a stack overflow
        return next();
      }
      
      byte[] encrypted = new byte[enclen];
      buffer.get(encrypted);
             
      //
      // Decrypt the encrypted data
      //
      
      AESWrapEngine engine = new AESWrapEngine();
      CipherParameters params = new KeyParameter(this.wrappingKey);
      engine.init(false, params);
      
      try {
        byte[] decrypted = engine.unwrap(encrypted, 0, encrypted.length);
        //
        // Unpad the decrypted data
        //

        PKCS7Padding padding = new PKCS7Padding();
        int padcount = padding.padCount(decrypted);
        
        //
        // Replace the current buffer with a new one containing the
        // decrypted data followed by any remaining data in the original
        // buffer.
        //
        
        ByteBuffer bb = ByteBuffer.allocate(decrypted.length - padcount + this.buffer.remaining());
        
        bb.put(decrypted, 0, decrypted.length - padcount);
        bb.put(this.buffer);
        bb.flip();
        
        this.buffer = bb;
        decodedEncrypted = true;
      } catch (InvalidCipherTextException icte) {
        // FIXME(hbs): log this somewhere...
        //
        // Skip the encrypted chunk we failed to decrypt
        //
      }
      
      //
      // Call next recursively
      //
      // WARNING(hbs): we may hit StackOverflow in some cases
      
      return next();
    }

    //
    // Read location/elevation flag if needed
    //
    
    byte locElevFlag = 0x0;
    
    if (GTSEncoder.FLAGS_CONTINUATION == (tsTypeFlag & GTSEncoder.FLAGS_CONTINUATION)) {
      if (!buffer.hasRemaining()) {
        return false;
      }
      
      locElevFlag = buffer.get();
    }
    
    //
    // Read timestamp
    //
        
    switch (tsTypeFlag & GTSEncoder.FLAGS_MASK_TIMESTAMP) {
      case GTSEncoder.FLAGS_TIMESTAMP_RAW_ABSOLUTE: {
          ByteOrder order = buffer.order();
          buffer.order(ByteOrder.BIG_ENDIAN);
          previousLastTimestamp = lastTimestamp;
          lastTimestamp = buffer.getLong();
          buffer.order(order);
        }
        break;
      //case GTSEncoder.FLAGS_TIMESTAMP_ZIGZAG_ABSOLUTE:
      //  previousLastTimestamp = lastTimestamp;
      //  lastTimestamp = Varint.decodeSignedLong(buffer);
      //  break;
      case GTSEncoder.FLAGS_TIMESTAMP_EQUALS_BASE:
        previousLastTimestamp = lastTimestamp;
        lastTimestamp = baseTimestamp;
        break;
      case GTSEncoder.FLAGS_TIMESTAMP_ZIGZAG_DELTA_BASE: {
          long delta = Varint.decodeSignedLong(buffer);
          previousLastTimestamp = lastTimestamp;
          lastTimestamp = baseTimestamp + delta;
        }
        break;
      case GTSEncoder.FLAGS_TIMESTAMP_ZIGZAG_DELTA_PREVIOUS: {
          long delta = Varint.decodeSignedLong(buffer);
          previousLastTimestamp = lastTimestamp;
          lastTimestamp = lastTimestamp + delta;
        }
        break;
      default:
        throw new RuntimeException("Invalid timestamp format.");
    }

    //
    // Read location/elevation
    //
    
    if (GTSEncoder.FLAGS_LOCATION == (locElevFlag & GTSEncoder.FLAGS_LOCATION)) {
      if (GTSEncoder.FLAGS_LOCATION_IDENTICAL != (locElevFlag & GTSEncoder.FLAGS_LOCATION_IDENTICAL)) {
        if (GTSEncoder.FLAGS_LOCATION_GEOXPPOINT_ZIGZAG_DELTA == (locElevFlag & GTSEncoder.FLAGS_LOCATION_GEOXPPOINT_ZIGZAG_DELTA)) {
          long delta = Varint.decodeSignedLong(buffer);
          previousLastGeoXPPoint = lastGeoXPPoint;
          lastGeoXPPoint = lastGeoXPPoint + delta;
        } else {
          ByteOrder order = buffer.order();
          buffer.order(ByteOrder.BIG_ENDIAN);
          previousLastGeoXPPoint = lastGeoXPPoint;
          lastGeoXPPoint = buffer.getLong();
          buffer.order(order);
        } 
      }
    } else {
      previousLastGeoXPPoint = lastGeoXPPoint;
      lastGeoXPPoint = GeoTimeSerie.NO_LOCATION;
    }
    
    if (GTSEncoder.FLAGS_ELEVATION == (locElevFlag & GTSEncoder.FLAGS_ELEVATION)) {
      if (GTSEncoder.FLAGS_ELEVATION_IDENTICAL != (locElevFlag & GTSEncoder.FLAGS_ELEVATION_IDENTICAL)) {
        boolean zigzag = GTSEncoder.FLAGS_ELEVATION_ZIGZAG == (locElevFlag & GTSEncoder.FLAGS_ELEVATION_ZIGZAG);
        
        long encoded;
        
        if (zigzag) {
          encoded = Varint.decodeSignedLong(buffer);
        } else {
          ByteOrder order = buffer.order();
          buffer.order(ByteOrder.BIG_ENDIAN);
          encoded = buffer.getLong();
          buffer.order(order);          
        }
        
        if (GTSEncoder.FLAGS_ELEVATION_DELTA_PREVIOUS == (locElevFlag & GTSEncoder.FLAGS_ELEVATION_DELTA_PREVIOUS)) {
          previousLastElevation = lastElevation;
          lastElevation = lastElevation + encoded;
        } else {
          previousLastElevation = lastElevation;
          lastElevation = encoded;
        }
      }      
    } else {
      previousLastElevation = lastElevation;
      lastElevation = GeoTimeSerie.NO_ELEVATION;
    }
    
    //
    // Extract value
    //
    
    switch (tsTypeFlag & GTSEncoder.FLAGS_MASK_TYPE) {
      case GTSEncoder.FLAGS_TYPE_LONG:
        lastType = TYPE.LONG;
        if (GTSEncoder.FLAGS_VALUE_IDENTICAL != (tsTypeFlag & GTSEncoder.FLAGS_VALUE_IDENTICAL)) {
          long encoded;
          
          if (GTSEncoder.FLAGS_LONG_ZIGZAG == (tsTypeFlag & GTSEncoder.FLAGS_LONG_ZIGZAG)) {
            encoded = Varint.decodeSignedLong(buffer);
          } else {
            ByteOrder order = buffer.order();
            buffer.order(ByteOrder.BIG_ENDIAN);
            encoded = buffer.getLong();
            buffer.order(order);          
          }

          if (GTSEncoder.FLAGS_LONG_DELTA_PREVIOUS == (tsTypeFlag & GTSEncoder.FLAGS_LONG_DELTA_PREVIOUS)) {
            previousLastLongValue = lastLongValue;
            lastLongValue = lastLongValue + encoded;
          } else {
            previousLastLongValue = lastLongValue;
            lastLongValue = encoded;
          }
        } else {
          previousLastLongValue = lastLongValue;
        }
        break;
        
      case GTSEncoder.FLAGS_TYPE_DOUBLE:
        lastType = TYPE.DOUBLE;
        if (GTSEncoder.FLAGS_VALUE_IDENTICAL != (tsTypeFlag & GTSEncoder.FLAGS_VALUE_IDENTICAL)) {
          if (GTSEncoder.FLAGS_DOUBLE_IEEE754 == (tsTypeFlag & GTSEncoder.FLAGS_DOUBLE_IEEE754)) {
            ByteOrder order = buffer.order();
            buffer.order(ByteOrder.BIG_ENDIAN);
            previousLastDoubleValue = lastDoubleValue;
            lastDoubleValue = buffer.getDouble();
            previousLastBDValue = lastBDValue;
            lastBDValue = null;
            buffer.order(order);          
          } else {
            int scale = buffer.get();
            long unscaled = Varint.decodeSignedLong(buffer);
            previousLastBDValue = lastBDValue;
            //lastBDValue = new BigDecimal(BigInteger.valueOf(unscaled), scale);
            lastBDValue = BigDecimal.valueOf(unscaled, scale);
          }
        } else {
          previousLastDoubleValue = lastDoubleValue;
          previousLastBDValue = lastBDValue;
        }
        break;
        
      case GTSEncoder.FLAGS_TYPE_STRING:
        lastType = TYPE.STRING;
        if (GTSEncoder.FLAGS_VALUE_IDENTICAL != (tsTypeFlag & GTSEncoder.FLAGS_VALUE_IDENTICAL)) {
          // Decode String length
          long len = Varint.decodeUnsignedLong(buffer);
          
          // Prevent excessive allocation
          if (len > buffer.remaining()) {
            throw new RuntimeException("Invalid string length.");
          }
          
          byte[] bytes = new byte[(int) len];
          // Read String bytes
          buffer.get(bytes);
          previousLastStringValue = lastStringValue;
          boolean binary = GTSEncoder.FLAGS_STRING_BINARY == (tsTypeFlag & GTSEncoder.FLAGS_STRING_BINARY);
          lastStringValue = new String(bytes, binary ? StandardCharsets.ISO_8859_1 : StandardCharsets.UTF_8);
          lastStringBinary = binary;
        } else {
          previousLastStringValue = lastStringValue;
          lastStringBinary = GTSEncoder.FLAGS_STRING_BINARY == (tsTypeFlag & GTSEncoder.FLAGS_STRING_BINARY);
        }
        break;
        
      case GTSEncoder.FLAGS_TYPE_BOOLEAN:
        if (GTSEncoder.FLAGS_DELETE_MARKER == (tsTypeFlag & GTSEncoder.FLAGS_MASK_TYPE_FLAGS)) {
          lastType = TYPE.UNDEFINED;
        } else {
          lastType = TYPE.BOOLEAN;
          
          if (GTSEncoder.FLAGS_BOOLEAN_VALUE_TRUE == (tsTypeFlag & GTSEncoder.FLAGS_MASK_TYPE_FLAGS)) {
            lastBooleanValue = true;
          } else if (GTSEncoder.FLAGS_BOOLEAN_VALUE_FALSE == (tsTypeFlag & GTSEncoder.FLAGS_MASK_TYPE_FLAGS)) {
            lastBooleanValue = false;
          } else {
            throw new RuntimeException("Invalid boolean value.");
          }
          //lastBooleanValue = GTSEncoder.FLAGS_BOOLEAN_VALUE == (tsTypeFlag & GTSEncoder.FLAGS_BOOLEAN_VALUE);
        }
        break;
        
      default:
        throw new RuntimeException("Invalid type encountered!");
    }

    this.consumingNextCalls++;
    return true;
  }
  
  public long getTimestamp() {
    return lastTimestamp;
  }
  
  public long getLocation() {
    return lastGeoXPPoint;
  }
  
  public long getElevation() {
    return lastElevation;
  }
  
  public Object getValue() {
    switch (lastType) {
      case BOOLEAN:
        return lastBooleanValue;
      case LONG:
        return lastLongValue;
      case DOUBLE:
        return null == lastBDValue ? lastDoubleValue : lastBDValue;
      case STRING:
        return lastStringValue;
      default:
        return null;
    }
  }
  
  public boolean isBinary() {
    return TYPE.STRING.equals(lastType) && lastStringBinary;
  }
  
  public Object getBinaryValue() {
    Object val = getValue();
    if (val instanceof String && lastStringBinary) {
      return ((String) val).getBytes(StandardCharsets.ISO_8859_1);
    } else {
      return val;
    }
  }
  
  /**
   * Decode any remaining values into a GTS instance.
   * 
   * @param type TYPE to force for the resulting GTS
   * @param strict Set to true to force values to be of uniform types, will throw RuntimeException if not
   * 
   * @return A GTS instance containing the remaining values.
   */
  public GeoTimeSerie decode(TYPE type, boolean strict) {
    GeoTimeSerie gts = new GeoTimeSerie(this.count > 0 ? (int) Math.min(Integer.MAX_VALUE, this.count) : Math.max(16, this.buffer.remaining() / 10));
    
    if (null != type) {
      gts.setType(type);
    }
    
    gts.setMetadata(this.getMetadata());
  
    if (strict) {
      Class lastClass = null;
      
      while(next()) {        
        // TODO(hbs): may differentiate STRING and binary values if the use case ever arises
        Object value = getValue();
        Class valClass = value.getClass();
        // getValue could return a BigDecimal, we need to smooth the comparison so
        // they are considered as Double
        if (value instanceof BigDecimal) {
          valClass = Double.class;
        }
        if (null != lastClass && !(valClass.equals(lastClass))) {
          throw new RuntimeException("Non homogeneous GTS Encoder.");
        }
        lastClass = valClass;
        GTSHelper.setValue(gts, getTimestamp(), getLocation(), getElevation(), value, false);
      }
    } else {
      while(next()) {
        GTSHelper.setValue(gts, getTimestamp(), getLocation(), getElevation(), getValue(), false);
      }      
    }
    
    return gts;
  }
  
  public GeoTimeSerie decode(TYPE type) {
    return decode(type, false);
  }
  
  public GeoTimeSerie decode() {
    return decode(null, false);
  }
  
  public GTSEncoder getCompatibleEncoder(long basets) {
    GTSEncoder encoder = new GTSEncoder(basets, this.wrappingKey);
    encoder.setMetadata(this.getMetadata());
    return encoder;
  }

  public long getBaseTimestamp() {
    return this.baseTimestamp;
  }

  public long getClassId() {
    return this.getMetadata().getClassId();
  }

  public void setClassId(long classId) {
    this.getMetadata().setClassId(classId);
  }

  public long getLabelsId() {
    return this.getMetadata().getLabelsId();
  }

  public void setLabelsId(long labelsId) {
    this.getMetadata().setLabelsId(labelsId);
  }
  
  public String getName() {
    return this.getMetadata().getName();
  }

  public void setName(String name) {
    this.getMetadata().setName(name);
  }

  public Map<String, String> getLabels() {
    return Collections.unmodifiableMap(this.getMetadata().getLabels());
  }

  public void setLabels(Map<String, String> labels) {
    this.getMetadata().setLabels(new HashMap<String,String>(labels));
  }

  public void setLabel(String key, String value) {
    this.getMetadata().getLabels().put(key, value);
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = new Metadata(metadata);
  }

  void safeSetMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public Metadata getMetadata() {
    if (null == this.metadata) {
      this.metadata = new Metadata();
    }
    
    if (null == this.metadata.getLabels()) {
      this.metadata.setLabels(new HashMap<String,String>());
    }
    
    if (null == this.metadata.getAttributes()) {
      this.metadata.setAttributes(new HashMap<String,String>());
    }
    
    return this.metadata;
  }
  
  /**
   * Return an encoder with all data from the last value retrieved (post call to next())
   * onwards
   * 
   * @param safeMetadata Is it safe to reuse the Metadata?
   */
  public GTSEncoder getEncoder(boolean safeMetadata) throws IOException {

    if (!nextCalled) {
      throw new IOException("Can only get an encoder for a decoder on which 'next' was called at least once.");
    }
    
    //
    // Copy the remaining data into a new ByteBuffer
    //
    
    ByteBuffer bb = this.buffer.duplicate();
    bb.position(this.position);
    
    int offset = 0;
    int len = bb.remaining();
    byte[] bytes = null;
    
    if (bb.hasArray()) {
      bytes = bb.array();
      offset = bb.arrayOffset() + bb.position();
    } else {
      bytes = new byte[bb.remaining()];
      bb.get(bytes);
    }

    //
    // Create an encoder with the same base timestamp and wrapping key, providing a sizing hint
    //
        
    GTSEncoder encoder = new GTSEncoder(this.baseTimestamp, this.wrappingKey, bb.remaining());
    
    if (safeMetadata) {
      encoder.safeSetMetadata(this.getMetadata());
    } else {
      encoder.setMetadata(this.getMetadata());
    }

    //
    // Set initial values
    //
    
    encoder.initialize(
      this.previousLastTimestamp,
      this.previousLastGeoXPPoint,
      this.previousLastElevation,
      this.previousLastLongValue,
      this.previousLastDoubleValue,
      this.previousLastBDValue,
      this.previousLastStringValue);
        
    //
    // Copy the encoded data
    //
    
    encoder.stream.write(bytes, offset, len);

    //
    // Put the encoder into 'safe delta' mode, because we don't know what the last
    // value/ts/elevation/location were, we can't use delta encoding for now
    //
    
    encoder.safeDelta();
    
    // Only set the count if we did not decode encrypted chunks otherwise the value would be wrong
    if (!this.decodedEncrypted) {
      encoder.setCount(this.count - this.consumingNextCalls + 1);
    }
    
    return encoder;
  }

  public GTSEncoder getEncoder() throws IOException {
    return getEncoder(false);
  }
  
  public int getRemainingSize() {
    return this.buffer.remaining();
  }
  
  void initialize(long initialTimestamp, long initialGeoXPPoint, long initialElevation, long initialLongValue, double initialDoubleValue, BigDecimal initialBDValue, String initialStringValue) {
    this.lastTimestamp = initialTimestamp;
    this.lastGeoXPPoint = initialGeoXPPoint;
    this.lastElevation = initialElevation;
    this.lastLongValue = initialLongValue;
    this.lastDoubleValue = initialDoubleValue;
    this.lastBDValue = initialBDValue;
    this.lastStringValue = initialStringValue;
  } 
  
  public long getCount() {
    return this.count;
  }
  
  void setCount(long count) {
    this.count = count;
  }
  
  /**
   * Returns a new instance of GTSDecoder with duplicates removed
   * 
   * WARNING: the duplicates removal is done in the order in which the values are found in the decoder. If timestamps
   * are not in chronological or reverse chronological order then you might remove values you won't be able to
   * reconstruct using FILLPREVIOUS/FILLNEXT/FILLVALUE
   * 
   * @return A GTSDecoder instance with duplicates removed
   */
  public GTSDecoder dedup() throws IOException {

    //
    // If next has already been called, bail out
    //
    
    if (nextCalled) {
      throw new IOException("Unable to dedup a decoder for which next has been called.");
    }
    
    GTSEncoder dedupped = new GTSEncoder(0L);
    dedupped.setMetadata(this.getMetadata());
    
    boolean first = true;
    
    long timestamp = 0L;
    long location = GeoTimeSerie.NO_LOCATION;
    long elevation = GeoTimeSerie.NO_ELEVATION;
    Object value = null;
    
    boolean dup = true;

    while(this.next()) {
      
      dup = true;
      
      if (first) {
        first = false;
        dup = false;
        
        location = this.getLocation();
        elevation = this.getElevation();
        value = this.getBinaryValue();
        
        dedupped.addValue(timestamp, location, elevation, value);
        continue;
      }
      
      long newTimestamp = this.getTimestamp();
      long newloc = this.getLocation();
      long newelev = this.getElevation();
      Object newValue = this.getBinaryValue();
            
      if (location != newloc || elevation != newelev) {
        dup = false;
      }
      
      if (dup) {
        if (null == value) {
          // Consider null to be duplicates! This should not happen though...
          dup = false;
        } else if (value instanceof Number) {
          if (!((Number) value).equals(newValue)) {
            dup = false;
          }
        } else if (value instanceof String) {
          if (!((String) value).equals(newValue)) {
            dup = false;
          }
        } else if (value instanceof Boolean) {
          if (!((Boolean) value).equals(newValue)) {
            dup = false;
          }
        } else if (value instanceof byte[]) {
          if (newValue instanceof byte[]) {
            if (0 != Bytes.compareTo((byte[]) value, (byte[]) newValue)) {
              dup = false;
            }
          } else {
            dup = false;
          }
        }
      }
      
      timestamp = newTimestamp;
      location = newloc;
      elevation = newelev;
      value = newValue;

      if (!dup) {
        dedupped.addValue(timestamp, location, elevation, value);
        continue;
      }
    }
    
    //
    // Add the last datapoint if it was skipped
    //
    
    if (dup) {
      dedupped.addValue(timestamp, location, elevation, value);
    }
    
    return dedupped.getDecoder();
  }
  
  public ByteBuffer getBuffer() {
    return this.buffer.asReadOnlyBuffer();
  }
  
  /**
   * Duplicate the current GTSDecoder. The copy is backed by the same
   * buffer.
   */
  public GTSDecoder duplicate() {
    GTSDecoder decoder = new GTSDecoder(this.baseTimestamp, this.wrappingKey, this.buffer.asReadOnlyBuffer());
    
    decoder.safeSetMetadata(new Metadata(this.getMetadata()));

    decoder.consumingNextCalls = this.consumingNextCalls;
    decoder.count = this.count;
    decoder.decodedEncrypted = this.decodedEncrypted;
    decoder.nextCalled = this.nextCalled;
    decoder.position = this.position;

    decoder.previousLastBDValue = this.previousLastBDValue;
    decoder.previousLastDoubleValue = this.previousLastDoubleValue;
    decoder.previousLastElevation = this.previousLastElevation;
    decoder.previousLastGeoXPPoint = this.previousLastGeoXPPoint;
    decoder.previousLastLongValue = this.previousLastLongValue;
    decoder.previousLastStringValue = this.previousLastStringValue;
    decoder.previousLastTimestamp = this.previousLastTimestamp;

    decoder.lastTimestamp = this.lastTimestamp;
    decoder.lastGeoXPPoint = this.lastGeoXPPoint;
    decoder.lastElevation = this.lastElevation;
    decoder.lastLongValue = this.lastLongValue;
    decoder.lastDoubleValue = this.lastDoubleValue;
    decoder.lastBDValue = this.lastBDValue;
    decoder.lastStringValue = this.lastStringValue;
    decoder.lastBooleanValue = this.lastBooleanValue;
    decoder.lastType = this.lastType;
    decoder.lastStringBinary = this.lastStringBinary;

    return decoder;
  }
  
  public void dump(PrintWriter pw) {
    StringBuilder sb = new StringBuilder(" ");
    
    GTSHelper.encodeName(sb, this.getName());
    sb.append("{");
    boolean first = true;
    for (Entry<String,String> entry: this.getLabels().entrySet()) {
      if (!first) {
        sb.append(",");
      }
      GTSHelper.encodeName(sb, entry.getKey());
      sb.append("=");
      GTSHelper.encodeName(sb, entry.getValue());
      first = false;
    }
    sb.append("} ");
    
    String clslbs = sb.toString();
    
    first = true;
    
    long l;
    
    while(this.next()) {      
      if (!first) {
        pw.print("=");
      }
      pw.print(this.getTimestamp());
      pw.print("/");
      l = getLocation();
      if (GeoTimeSerie.NO_LOCATION != l) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(l);
        pw.print(latlon[0]);
        pw.print(":");
        pw.print(latlon[1]);
      }
      pw.print("/");
      l = getElevation();
      if (GeoTimeSerie.NO_ELEVATION != l) {
        pw.print(l);
      }
      if (first) {
        pw.print(clslbs);
        first = false;
      } else {
        pw.print(" ");
      }
      sb.setLength(0);
      GTSHelper.encodeValue(sb, getBinaryValue());
      pw.print(sb.toString());
      pw.print("\r\n");
    }
  }

  public static GTSDecoder fromBlock(byte[] block, byte[] key) throws IOException {
    
    if (block.length < 6) {
      throw new IOException("Invalid block.");
    }
    
    ByteBuffer buffer = ByteBuffer.wrap(block);

    //
    // Extract size
    //
    
    buffer.order(ByteOrder.BIG_ENDIAN);
    int size = buffer.getInt();
        
    // Check size
    
    if (block.length != size) {
      throw new IOException("Invalid block size, expected " + size + ", block is " + block.length);
    }
    
    // Extract compression
    
    byte comp = buffer.get();
    
    boolean compress = false;
    
    if (0 == comp) {
      compress = false;
    } else if (1 == comp) {
      compress = true;
    } else {
      throw new IOException("Invalid compression flag");
    }
    
    // Extract base timestamp
    
    long base = Varint.decodeSignedLong(buffer);

    InputStream in;
    
    ByteArrayInputStream bain = new ByteArrayInputStream(block, buffer.position(), buffer.remaining());
    
    if (compress) {
      in = new GZIPInputStream(bain);
    } else {
      in = bain;
    }
    
    byte[] buf = new byte[1024];
    
    ByteArrayOutputStream out = new ByteArrayOutputStream(buffer.remaining());
    
    while(true) {
      int len = in.read(buf);
      
      if (len <= 0) {
        break;
      }
      out.write(buf, 0, len);
    }
    
    GTSDecoder decoder = new GTSDecoder(base, key, ByteBuffer.wrap(out.toByteArray()));
    
    return decoder;
  }
}
