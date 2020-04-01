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

public class Wavelet_bior55 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 0.0, 0.0, 0.03968708834740544, 0.007948108637240322, -0.05446378846823691, 0.34560528195603346, 0.7366601814282105, 0.34560528195603346, -0.05446378846823691, 0.007948108637240322, 0.03968708834740544, 0.0,  };
  private static final double[] waveletDeComposition = new double[] { -0.013456709459118716, -0.002694966880111507, 0.13670658466432914, -0.09350469740093886, -0.47680326579848425, 0.8995061097486484, -0.47680326579848425, -0.09350469740093886, 0.13670658466432914, -0.002694966880111507, -0.013456709459118716, 0.0,  };

  private static final double[] scalingReConstruction = new double[] { 0.013456709459118716, -0.002694966880111507, -0.13670658466432914, -0.09350469740093886, 0.47680326579848425, 0.8995061097486484, 0.47680326579848425, -0.09350469740093886, -0.13670658466432914, -0.002694966880111507, 0.013456709459118716, 0.0,  };
  private static final double[] waveletReConstruction = new double[] { 0.0, 0.0, 0.03968708834740544, -0.007948108637240322, -0.05446378846823691, -0.34560528195603346, 0.7366601814282105, -0.34560528195603346, -0.05446378846823691, -0.007948108637240322, 0.03968708834740544, 0.0,  };

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

