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
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.geoxp.GeoXPLib;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.IOException;

public class GeoTimeSerieSerializer extends StdSerializer<GeoTimeSerie> {

  public static final String FIELD_VALUES = "v";

  protected GeoTimeSerieSerializer() {
    super(GeoTimeSerie.class);
  }

  @Override
  public void serialize(GeoTimeSerie gts, JsonGenerator gen, SerializerProvider provider) throws IOException {
    Metadata metadata = gts.getMetadata();

    gen.writeStartObject();
    MetadataSerializer.serializeMetadataFields(metadata, gen);
    gen.writeFieldName(FIELD_VALUES);
    gen.writeStartArray(gts.size());

    for (int i = 0; i < gts.size(); i++) {
      long ts = GTSHelper.tickAtIndex(gts, i);
      long location = GTSHelper.locationAtIndex(gts, i);
      long elevation = GTSHelper.elevationAtIndex(gts, i);
      Object v = GTSHelper.valueAtIndex(gts, i);

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
      switch (gts.getType()) {
        case UNDEFINED:
          gen.writeObject(v);
          break;
        case LONG:
          gen.writeNumber((long) v);
          break;
        case DOUBLE:
          gen.writeNumber((double) v);
          break;
        case BOOLEAN:
          gen.writeBoolean((boolean) v);
          break;
        case STRING:
          gen.writeString((String) v);
          break;
      }
      gen.writeEndArray();
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }
}
