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

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSPickler;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptLib;


import java.io.IOException;

import net.razorvine.pickle.Pickler;
import net.razorvine.pyro.serializer.PickleSerializer;

/**
 * Encode an object as Pickle
 */
public class TOPICKLE extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final TOGTS TOGTS = new TOGTS(WarpScriptLib.TOGTS);

  // register pickler for custom classes
  static {
    Pickler.registerCustomPickler(GeoTimeSerie.class, new GTSPickler());
  }

  public TOPICKLE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.peek();

    if (o instanceof GTSEncoder) {
      TOGTS.apply(stack);
    }

    o = stack.pop();
    
    try {

      // pickle
      PickleSerializer serializer = new PickleSerializer();
      byte[] data = serializer.serializeData(o);
      stack.push(data);

    } catch (IllegalArgumentException iae) {
      throw new WarpScriptException(iae);
    } catch (IOException ioe) {
      throw new WarpScriptException(ioe);
    }
    
    return stack;
  }
}
