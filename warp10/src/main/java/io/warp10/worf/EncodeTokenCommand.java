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
import io.warp10.quasar.token.thrift.data.TokenType;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class EncodeTokenCommand extends TokenCommand {

  public EncodeTokenCommand() {
    this.commandName = "encodeToken";
  }

  public TokenType tokenType = null;
  public String application = null;
  public List<String> applications = null;
  public String owner = null;
  public List<String> owners = null;
  public String producer = null;
  public List<String> producers = null;
  public long ttl = 0L;
  public Map<String,String> labels = null;

  public String toString() {
    return "type=" + tokenType + " application=" + application + " applications=" + applications + " owner=" + owner + " owners=" + owners + " producer=" + producer + " ttl=" + ttl + " labels=" + labels;
  }

  @Override
  public boolean isReady() {
    return !Strings.isNullOrEmpty(application) && (!Strings.isNullOrEmpty(owner) || owners!=null ) && !Strings.isNullOrEmpty(producer) && ttl != 0 && tokenType != null;
  }

  @Override
  public boolean execute(String function, WorfKeyMaster worfKeyMaster, PrintWriter out) throws WorfException {
    boolean returnValue = false;
    switch (function) {
      case "generate":
        String token = null;
        switch (tokenType) {
          case READ:
            token = worfKeyMaster.deliverReadToken(application, applications, producer, producers, owners, labels, ttl);
            break;
          case WRITE:
            token = worfKeyMaster.deliverWriteToken(application, producer, owner, labels, ttl);
            break;
        }

        out.println("token=" + token);
        out.println("tokenIdent=" + worfKeyMaster.getTokenIdent(token));
        out.println("application name=" + application);
        out.println("producer=" + producer);

        switch (tokenType) {
          case READ:
            out.println("authorizations (applications)=" + applications);
            out.println("authorizations (owners)=" + owners);
            out.println("authorizations (producers)=" + producers);
            break;

          case WRITE:
            out.println("owner=" + owner);
            break;
        }

        if (null != labels && labels.size() > 0) {
          out.println("labels=" + labels.toString());
        }
        out.println("ttl=" + ttl);

        // reset current command
        returnValue = true;
        break;
    }

    return returnValue;
  }
}
