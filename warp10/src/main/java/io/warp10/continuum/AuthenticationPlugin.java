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

package io.warp10.continuum;

import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;

/**
 * Interface for authentication plugins to support alternate token types.
 */
public interface AuthenticationPlugin {  
  /**
   * This method attempts to extract a ReadToken from a provided token string.
   * If the token string is not one the plugin can handle, this method MUST return null.
   * If the token string can be handled by the plugin but is an invalid token, the method MUST throw a WarpScriptException
   * 
   * @param token Token string to parse
   * @return The extracted ReadToken instance or null if the token type is not supported by the plugin.
   * @throws WarpScriptException If the provided token string is of the correct type but invalid.
   */
  public ReadToken extractReadToken(String token) throws WarpScriptException;
  
  
  /**
   * This method attempts to extract a WriteToken from a provided token string.
   * If the token string is not one the plugin can handle, this method MUST return null.
   * If the token string can be handled by the plugin but is an invalid token, the method MUST throw a WarpScriptException
   * 
   * @param token Token string to parse
   * @return The extracted ReadToken instance or null if the token type is not supported by the plugin.
   * @throws WarpScriptException If the provided token string is of the correct type but invalid.
   */
  public WriteToken extractWriteToken(String token) throws WarpScriptException;
}
