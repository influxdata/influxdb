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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.IOException;

public class GTSEncoderSerializer extends StdSerializer<GTSEncoder> {

  protected GTSEncoderSerializer() {
    super(GTSEncoder.class);
  }

  @Override
  public void serialize(GTSEncoder encoder, JsonGenerator gen, SerializerProvider provider) throws IOException {
    Metadata metadata = encoder.getMetadata();

    gen.writeStartObject();
    MetadataSerializer.serializeMetadataFields(metadata, gen);
    gen.writeFieldName(GeoTimeSerieSerializer.FIELD_VALUES);
    gen.writeStartArray();

    GTSDecoder decoder = encoder.getUnsafeDecoder(false);
    while (decoder.next()) {
      long ts = decoder.getTimestamp();
      long location = decoder.getLocation();
      long elevation = decoder.getElevation();
      // We do not call getBinaryValue because JSON will also encode byte[] as IS0-8859-1
      Object v = decoder.getValue();

      gen.writeStartArray();
      gen.writeNumber(ts);
      if (GeoTimeSerie.NO_LOCATION != location) {
        double[] latlon = GeoXPLib.fromGeoXPPoint(location);
        gen.writeNumber(latlon[0]);
        gen.writeNumber(latlon[1]);
      }
      if (GeoTimeSerie.NO_ELEVATION != elevation) {
        gen.writeNumber(elevation);
      }

      // Do not use directly gen.writeObject() because it is VERY slow.
      if (v instanceof Boolean) {
        gen.writeBoolean((boolean) v);
      } else if (v instanceof Long) {
        gen.writeNumber((long) v);
      } else if (v instanceof Double) {
        gen.writeNumber((double) v);
      } else if (v instanceof String) {
        gen.writeString((String) v);
      } else {
        gen.writeObject(v);
      }

      gen.writeEndArray();
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }
}
