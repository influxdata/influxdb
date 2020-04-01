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

package io.warp10.script.functions;

import io.warp10.continuum.TimeSource;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Checks that the current time is not before the provided instant.
 */
public class NOTBEFORE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  public NOTBEFORE(String name) {
    super(name);    
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    long instant;
    
    if (top instanceof String) {
      instant = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(top.toString());
    } else if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a timestamp or ISO8601 datetime string on top of the stack.");
    } else {
      instant = ((Number) top).longValue();
    }
    
    long now = TimeSource.getTime();
    
    if (now < instant) {
      throw new WarpScriptException("Current time is before '" + top + "'");
    }
    
    return stack;
  }
}
