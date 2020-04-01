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

package io.warp10.script.functions;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import org.apache.commons.lang3.LocaleUtils;

import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;

public class STRINGFORMAT extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public STRINGFORMAT(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();

    // STRINGFORMAT defaults to default locale when none is given.
    Locale locale = Locale.getDefault();

    List args = null;

    if (o instanceof List) {
      // Case with no given locale.
      args = (List) o;
    } else if (o instanceof String) {
      locale = new Locale((String) o);

      // Locale instantiation does not check the validity of the given String, check that.
      if (!LocaleUtils.isAvailableLocale(locale)) {
        throw new WarpScriptException(getName() + " expects a valid locale.");
      }

      // Locale has been retrieved and checked, now get the list of args.
      o = stack.pop();

      if (o instanceof List) {
        args = (List) o;
      } else {
        // Not a list as second parameter, throw.
        throw new WarpScriptException(getName() + " expects list of Objects under the locale.");
      }
    } else {
      // First parameter not a String or a List, throw.
      throw new WarpScriptException(getName() + " expects a locale name or a list of Objects as the first parameter.");
    }

    o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " expects format under the list of Objects.");
    }

    try {
      // Push the formatted String.
      stack.push(String.format(locale, (String) o, args.toArray()));
    } catch (IllegalFormatException ife) {
      throw new WarpScriptException(getName() + " expects a valid format.", ife);
    }

    return stack;
  }

}
