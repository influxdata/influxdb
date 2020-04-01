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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Normal (Gaussian) distribution Probability Density Function.
 * 
 * @see http://en.wikipedia.org/wiki/Normal_distribution
 */
public class NPDF extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private static final double TWOPISQRT = 1.0D / Math.sqrt(Math.PI * 2.0D);
  
  private final double mu;
  private final double sigma;
  
  public static class Builder extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
    public Builder(String name) {
      super(name);
    }
    
    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {
      Object value = stack.pop();
      
      if (!(value instanceof Number) || (0 >= ((Number) value).doubleValue())) {
        throw new WarpScriptException(getName() + " expects a standard deviation (sigma) on top of the stack.");
      }
      
      double sigma = (double) value;
      
      value = stack.pop();
      
      if (!(value instanceof Number)) {
        throw new WarpScriptException(getName() + " expects a mean (mu) below the standard deviation.");
      }
      
      double mu = ((Number) value).doubleValue();
      
      stack.push(new NPDF(getName(), mu, sigma));
      
      return stack;
    }
  }

  public NPDF(String name, double mu, double sigma) {
    super(name);
    this.mu = mu;
    this.sigma = sigma;
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof Number)) {
      throw new WarpScriptException(getName() + " expects a numeric value on top of the stack.");
    }
    
    double x = ((Number) o).doubleValue();
    
    double p = (TWOPISQRT / sigma) * Math.exp((-1.0D / (2 * sigma * sigma)) * Math.pow(x - mu, 2.0D));

    stack.push(p);

    return stack;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.sigma);
    sb.append(" ");
    sb.append(this.mu);
    sb.append(" ");
    sb.append(this.getName());
    return sb.toString();
  }
}
