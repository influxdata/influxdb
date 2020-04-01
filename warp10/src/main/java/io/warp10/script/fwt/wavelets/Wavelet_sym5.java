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

package io.warp10.script.fwt.wavelets;

import io.warp10.script.fwt.Wavelet;

public class Wavelet_sym5 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 0.027333068345077982, 0.029519490925774643, -0.039134249302383094, 0.1993975339773936, 0.7234076904024206, 0.6339789634582119, 0.01660210576452232, -0.17532808990845047, -0.021101834024758855, 0.019538882735286728,  };
  private static final double[] waveletDeComposition = new double[] { -0.019538882735286728, -0.021101834024758855, 0.17532808990845047, 0.01660210576452232, -0.6339789634582119, 0.7234076904024206, -0.1993975339773936, -0.039134249302383094, -0.029519490925774643, 0.027333068345077982,  };

  private static final double[] scalingReConstruction = new double[] { 0.019538882735286728, -0.021101834024758855, -0.17532808990845047, 0.01660210576452232, 0.6339789634582119, 0.7234076904024206, 0.1993975339773936, -0.039134249302383094, 0.029519490925774643, 0.027333068345077982,  };
  private static final double[] waveletReConstruction = new double[] { 0.027333068345077982, -0.029519490925774643, -0.039134249302383094, -0.1993975339773936, 0.7234076904024206, -0.6339789634582119, 0.01660210576452232, 0.17532808990845047, -0.021101834024758855, -0.019538882735286728,  };

  static {
    //
    // Reverse the arrays as we do convolutions
    //
    reverse(scalingDeComposition);
    reverse(waveletDeComposition);
  }

  private static final void reverse(double[] array) {
    int i = 0;
    int j = array.length - 1;
    
    while (i < j) {
      double tmp = array[i];
      array[i] = array[j];
      array[j] = tmp;
      i++;
      j--;
    }
  }

  public int getTransformWavelength() {
    return transformWavelength;
  }

  public int getMotherWavelength() {
    return waveletReConstruction.length;
  }

  public double[] getScalingDeComposition() {
    return scalingDeComposition;
  }

  public double[] getWaveletDeComposition() {
    return waveletDeComposition;
  }

  public double[] getScalingReConstruction() {
    return scalingReConstruction;
  }

  public double[] getWaveletReConstruction() {
    return waveletReConstruction;
  }
}

