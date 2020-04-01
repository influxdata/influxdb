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

package io.warp10.plugins.authexample;

import java.util.Properties;

import io.warp10.continuum.AuthenticationPlugin;
import io.warp10.continuum.Tokens;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.warp.sdk.AbstractWarp10Plugin;

/**
 * Example Warp 10 plugin which adds an AuthenticationPlugin to support a dummy
 * token type prefixed by 'dummy:'
 * 
 * The plugin is added by adding the following configuration:
 * 
 * warp10.plugin.authexample = io.warp10.plugins.authexample.AuthExampleWarp10Plugin
 * 
 */
public class AuthExampleWarp10Plugin extends AbstractWarp10Plugin implements AuthenticationPlugin {
  
  private static final String PREFIX = "dummy:";
  
  @Override
  public ReadToken extractReadToken(String token) throws WarpScriptException {
    if (!token.startsWith(PREFIX)) {
      return null;
    }

    ReadToken rtoken = new ReadToken();
    
    // .... populate the ReadToken
    
    return rtoken;
  }
  
  @Override
  public WriteToken extractWriteToken(String token) throws WarpScriptException {
    if (!token.startsWith(PREFIX)) {
      return null;
    }
    
    WriteToken wtoken = new WriteToken();
    
    // .... populate the WriteToken
    
    return wtoken;
  }
  
  @Override
  public void init(Properties properties) {
    Tokens.register(this);
  }
}
