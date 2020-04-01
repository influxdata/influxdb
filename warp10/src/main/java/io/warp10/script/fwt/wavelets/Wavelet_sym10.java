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

public class Wavelet_sym10 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 0.0007701598091144901, 9.563267072289475e-05, -0.008641299277022422, -0.0014653825813050513, 0.0459272392310922, 0.011609893903711381, -0.15949427888491757, -0.07088053578324385, 0.47169066693843925, 0.7695100370211071, 0.38382676106708546, -0.03553674047381755, -0.0319900568824278, 0.04999497207737669, 0.005764912033581909, -0.02035493981231129, -0.0008043589320165449, 0.004593173585311828, 5.7036083618494284e-05, -0.0004593294210046588,  };
  private static final double[] waveletDeComposition = new double[] { 0.0004593294210046588, 5.7036083618494284e-05, -0.004593173585311828, -0.0008043589320165449, 0.02035493981231129, 0.005764912033581909, -0.04999497207737669, -0.0319900568824278, 0.03553674047381755, 0.38382676106708546, -0.7695100370211071, 0.47169066693843925, 0.07088053578324385, -0.15949427888491757, -0.011609893903711381, 0.0459272392310922, 0.0014653825813050513, -0.008641299277022422, -9.563267072289475e-05, 0.0007701598091144901,  };

  private static final double[] scalingReConstruction = new double[] { -0.0004593294210046588, 5.7036083618494284e-05, 0.004593173585311828, -0.0008043589320165449, -0.02035493981231129, 0.005764912033581909, 0.04999497207737669, -0.0319900568824278, -0.03553674047381755, 0.38382676106708546, 0.7695100370211071, 0.47169066693843925, -0.07088053578324385, -0.15949427888491757, 0.011609893903711381, 0.0459272392310922, -0.0014653825813050513, -0.008641299277022422, 9.563267072289475e-05, 0.0007701598091144901,  };
  private static final double[] waveletReConstruction = new double[] { 0.0007701598091144901, -9.563267072289475e-05, -0.008641299277022422, 0.0014653825813050513, 0.0459272392310922, -0.011609893903711381, -0.15949427888491757, 0.07088053578324385, 0.47169066693843925, -0.7695100370211071, 0.38382676106708546, 0.03553674047381755, -0.0319900568824278, -0.04999497207737669, 0.005764912033581909, 0.02035493981231129, -0.0008043589320165449, -0.004593173585311828, 5.7036083618494284e-05, 0.0004593294210046588,  };

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

