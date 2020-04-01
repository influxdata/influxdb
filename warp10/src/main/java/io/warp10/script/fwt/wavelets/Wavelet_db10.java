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

public class Wavelet_db10 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { -1.326420300235487e-05, 9.358867000108985e-05, -0.0001164668549943862, -0.0006858566950046825, 0.00199240529499085, 0.0013953517469940798, -0.010733175482979604, 0.0036065535669883944, 0.03321267405893324, -0.02945753682194567, -0.07139414716586077, 0.09305736460380659, 0.12736934033574265, -0.19594627437659665, -0.24984642432648865, 0.2811723436604265, 0.6884590394525921, 0.5272011889309198, 0.18817680007762133, 0.026670057900950818,  };
  private static final double[] waveletDeComposition = new double[] { -0.026670057900950818, 0.18817680007762133, -0.5272011889309198, 0.6884590394525921, -0.2811723436604265, -0.24984642432648865, 0.19594627437659665, 0.12736934033574265, -0.09305736460380659, -0.07139414716586077, 0.02945753682194567, 0.03321267405893324, -0.0036065535669883944, -0.010733175482979604, -0.0013953517469940798, 0.00199240529499085, 0.0006858566950046825, -0.0001164668549943862, -9.358867000108985e-05, -1.326420300235487e-05,  };

  private static final double[] scalingReConstruction = new double[] { 0.026670057900950818, 0.18817680007762133, 0.5272011889309198, 0.6884590394525921, 0.2811723436604265, -0.24984642432648865, -0.19594627437659665, 0.12736934033574265, 0.09305736460380659, -0.07139414716586077, -0.02945753682194567, 0.03321267405893324, 0.0036065535669883944, -0.010733175482979604, 0.0013953517469940798, 0.00199240529499085, -0.0006858566950046825, -0.0001164668549943862, 9.358867000108985e-05, -1.326420300235487e-05,  };
  private static final double[] waveletReConstruction = new double[] { -1.326420300235487e-05, -9.358867000108985e-05, -0.0001164668549943862, 0.0006858566950046825, 0.00199240529499085, -0.0013953517469940798, -0.010733175482979604, -0.0036065535669883944, 0.03321267405893324, 0.02945753682194567, -0.07139414716586077, -0.09305736460380659, 0.12736934033574265, 0.19594627437659665, -0.24984642432648865, -0.2811723436604265, 0.6884590394525921, -0.5272011889309198, 0.18817680007762133, -0.026670057900950818,  };

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

