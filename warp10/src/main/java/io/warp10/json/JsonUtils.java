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

import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.impl.UnknownSerializer;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStack;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class JsonUtils {

  /**
   * A serializer for null keys.
   * Outputs "null" because most javascript engines coerce null to "null" when using it as a key.
   */
  private static class NullKeySerializer extends JsonSerializer<Object> {
    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeFieldName("null");
    }
  }

  /**
   * Used to swap UnknownSerializer and BeanSerializer for CustomEncodersSerializer.
   */
  public static class NotSerializedToCustomSerializedModifier extends BeanSerializerModifier {
    @Override
    public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
      if (serializer instanceof UnknownSerializer || serializer instanceof BeanSerializer) {
        return customEncodersSerializer;
      } else {
        return serializer;
      }
    }
  }

  public static class CustomEncodersSerializer extends JsonSerializer<Object> {
    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      if (null != encoders && !encoders.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        boolean encoded = false;
        for (JsonEncoder encoder: encoders) {
          encoded = encoder.addElement(sb, value);
          if (encoded) {
            break;
          }
        }
        if (encoded) {
          gen.writeRaw(sb.toString());
        } else {
          // No custom encoders able to encode this object, write null.
          gen.writeNull();
        }
      } else {
        // No custom encoders defined, write null.
        gen.writeNull();
      }
    }
  }

  public static final NullKeySerializer nullKeySerializer = new NullKeySerializer();
  public static final CustomEncodersSerializer customEncodersSerializer = new CustomEncodersSerializer();

  //
  // ObjectMapper instances are thread-safe, so we can safely use a single static instance.
  //
  private static final ObjectMapper strictMapper;
  private static final ObjectMapper looseMapper;

  public interface JsonEncoder {
    boolean addElement(StringBuilder sb, Object o);
  }

  private static List<JsonEncoder> encoders;

  static {
    //
    // Configure a module to handle the serialization of non-base classes.
    //
    SimpleModule module = new SimpleModule();
    // Add the NotSerializedToCustomSerializedModifier instance
    module.setSerializerModifier(new NotSerializedToCustomSerializedModifier());
    // Add core custom serializers
    module.addSerializer(new GeoTimeSerieSerializer());
    module.addSerializer(new GTSEncoderSerializer());
    module.addSerializer(new MetadataSerializer());
    module.addSerializer(new NamedWarpScriptFunctionSerializer());
    module.addSerializer(new MacroSerializer());
    module.addSerializer(new BytesSerializer());
    module.addSerializer(new RealVectorSerializer());
    module.addSerializer(new RealMatrixSerializer());

    //
    // Common configuration for both strict and loose mappers.
    //
    JsonFactoryBuilder builder = new JsonFactoryBuilder();
    builder.enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS);
    builder.enable(JsonReadFeature.ALLOW_MISSING_VALUES);
    builder.enable(JsonWriteFeature.ESCAPE_NON_ASCII);
    builder.disable(JsonWriteFeature.WRITE_NUMBERS_AS_STRINGS);
    builder.disable(StreamWriteFeature.AUTO_CLOSE_TARGET);

    //
    // Configure strict mapper
    //
    builder.enable(JsonWriteFeature.WRITE_NAN_AS_STRINGS);
    strictMapper = new ObjectMapper(builder.build());
    strictMapper.getSerializerProvider().setNullKeySerializer(nullKeySerializer);
    strictMapper.registerModule(module);

    //
    // Configure loose mapper
    //
    builder.disable(JsonWriteFeature.WRITE_NAN_AS_STRINGS);
    looseMapper = new ObjectMapper(builder.build());
    looseMapper.getSerializerProvider().setNullKeySerializer(nullKeySerializer);
    looseMapper.registerModule(module);
  }

  //
  // Method to deserialize JSON to Objects.
  //

  public static Object jsonToObject(String json) throws JsonProcessingException {
    return strictMapper.readValue(json, Object.class);
  }

  //
  // Methods to serialize objects to JSON
  //

  public static String objectToJson(Object o) throws IOException {
    return objectToJson(o, Long.MAX_VALUE);
  }

  public static String objectToJson(Object o, long maxJsonSize) throws IOException {
    return objectToJson(o, false, maxJsonSize);
  }

  public static String objectToJson(Object o, boolean isStrict) throws IOException {
    return objectToJson(o, isStrict, Long.MAX_VALUE);
  }

  public static String objectToJson(Object o, boolean isStrict, long maxJsonSize) throws IOException {
    StringWriter writer = new StringWriter();
    objectToJson(writer, o, isStrict, maxJsonSize);
    return writer.toString();
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict) throws IOException {
    objectToJson(writer, o, isStrict, Long.MAX_VALUE);
  }

  public static void objectToJson(Writer writer, Object o, boolean isStrict, long maxJsonSize) throws IOException {
    if (Long.MAX_VALUE != maxJsonSize) {
      writer = new BoundedWriter(writer, maxJsonSize);
    }

    try {
      if (isStrict) {
        strictMapper.writeValue(writer, o);
      } else {
        looseMapper.writeValue(writer, o);
      }
    } catch (BoundedWriter.WriterBoundReachedException wbre) {
      throw new IOException("Resulting JSON is too big.", wbre);
    }
  }

  public static synchronized void addEncoder(JsonEncoder encoder) {
    if (null == encoders) {
      encoders = new ArrayList<JsonEncoder>();
    }
    encoders.add(encoder);
  }

}
