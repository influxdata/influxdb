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

import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import java.util.Locale;

import org.joda.time.MutablePeriod;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.ISOPeriodFormat;

/**
 * Convert a duration in time units into one suitable for humans...
 */
public class ISODURATION extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public ISODURATION(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of time units (LONG) on top of the stack.");
    }
    
    long duration = ((Number) top).longValue();
    
    StringBuffer buf = new StringBuffer();
    ReadablePeriod period = new MutablePeriod(duration / Constants.TIME_UNITS_PER_MS);
    ISOPeriodFormat.standard().getPrinter().printTo(buf, period, Locale.US);

    stack.push(buf.toString());
    
    return stack;
  }
}
