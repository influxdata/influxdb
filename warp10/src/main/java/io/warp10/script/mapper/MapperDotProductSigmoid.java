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

package io.warp10.script.mapper;

import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;

/**
 * Mapper which operates a dot product between an input vector and the sliding window and
 * returns the sigmoid of the result
 */
public class MapperDotProductSigmoid extends MapperDotProduct {
  
  public static class Builder extends MapperDotProduct.Builder {
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      super.apply(stack);
      MapperDotProduct mdp = (MapperDotProduct) stack.pop();
      stack.push(new MapperDotProductSigmoid(getName(), mdp.getOmega()));
      return stack;
    }
  }

  public MapperDotProductSigmoid(String name, double[] omega) {
    super(name, omega);
  }
  
  @Override
  public Object apply(Object[] args) throws WarpScriptException {
    Object[] result = (Object[]) super.apply(args);
    
    if (null != result[3]) {
      result[3] = 1.0D / (1.0D + Math.exp(-(double) result[3]));
    }
    
    return result;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(WarpScriptLib.LIST_START);
    sb.append(" ");
    for (double d: this.getOmega()) {
      sb.append(StackUtils.toString(d));
      sb.append(" ");
    }
    sb.append(WarpScriptLib.LIST_END);
    sb.append(" ");
    sb.append(this.getName());
    
    return sb.toString();
  }
}
