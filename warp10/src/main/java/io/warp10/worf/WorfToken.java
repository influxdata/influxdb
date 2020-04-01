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

package io.warp10.worf;

import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import org.apache.thrift.TBase;

import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

public class WorfToken {

  public static void printTokenInfos(TBase<?,?> token, PrintWriter out) {
    final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
    final SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    sdf.setTimeZone(utc);

    if (token == null) {
      out.println("Unable to print null token");
      return;
    }

    out.println("TOKEN INFO BEGIN");
    if (token instanceof ReadToken) {
      ReadToken readToken = (ReadToken) token;

      out.println("issuance date=" + sdf.format(new Date(readToken.getIssuanceTimestamp())));
      out.println("expiry date=" + sdf.format(new Date(readToken.getExpiryTimestamp())));
      out.println("type=" + readToken.getTokenType().name());
      out.println("application name=" + readToken.getAppName());
      out.println("application scope=" + readToken.getApps().toString());
      out.println("owners=" + uuidToString(readToken.getOwners()));
      out.println("producers=" + uuidToString(readToken.getProducers()));

    }else if (token instanceof WriteToken) {
      WriteToken writeToken = (WriteToken) token;
      out.println("issuance date=" + sdf.format(new Date(writeToken.getIssuanceTimestamp())));
      out.println("expiry date=" + sdf.format(new Date(writeToken.getExpiryTimestamp())));
      out.println("type=" + writeToken.getTokenType().name());
      out.println("application name=" + writeToken.getAppName());
      out.println("owner=" + getUUID(writeToken.getOwnerId()));
      out.println("producer=" + getUUID(writeToken.getProducerId()));
    }
    out.println("TOKEN INFO END");
  }

  private static  String uuidToString(List<ByteBuffer> uuids) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = false;
    for(ByteBuffer uuid : uuids) {
      if (first) {
        sb.append(",");
      }
      sb.append(getUUID(uuid.array()));
      first = true;
    }
    sb.append("]");

    return sb.toString();
  }

  private static String getUUID(byte[] binaryUUID) {
    long msb = 0;
    long lsb = 0;
    assert binaryUUID.length == 16 : "data must be 16 bytes in length";
    for (int i=0; i<8; i++)
      msb = (msb << 8) | (binaryUUID[i] & 0xff);
    for (int i=8; i<16; i++)
      lsb = (lsb << 8) | (binaryUUID[i] & 0xff);

    UUID uid = new UUID(msb, lsb);
    return uid.toString();
  }


}
