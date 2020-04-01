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

import com.google.common.base.Strings;
import io.warp10.quasar.token.thrift.data.WriteToken;
import org.apache.thrift.TBase;

import java.io.PrintWriter;

public class DecodeTokenCommand extends TokenCommand {

  public DecodeTokenCommand() {
    this.commandName = "decodeToken";
  }

  public String token = null;

  public WriteToken writeToken = null;
  /*
  public TokenType tokenType = null;
  public String application = null;
  public String owner = null;
  public String producer = null;
  public long ttl = 0L;
    */
  @Override
  public boolean isReady() {
    return !Strings.isNullOrEmpty(token);
  }

  @Override
  public boolean execute(String function, WorfKeyMaster worfKeyMaster, PrintWriter out) throws WorfException {
    boolean returnValue = false;
    switch (function) {
      case "decode":
        TBase<?, ?> thriftToken = worfKeyMaster.decodeToken(token);
        WorfToken.printTokenInfos(thriftToken, out);

        if (thriftToken!= null && thriftToken instanceof WriteToken) {
          writeToken = (WriteToken) thriftToken;
        } else {
          returnValue = true;
        }
        break;

      case "yes":
        if (writeToken != null) {
          String readToken = worfKeyMaster.convertToken(writeToken);

          out.println("read token=" + readToken);
          returnValue = true;
        }
        break;

      case "no":
        returnValue = true;
        break;
    }

    return returnValue;
  }
}
