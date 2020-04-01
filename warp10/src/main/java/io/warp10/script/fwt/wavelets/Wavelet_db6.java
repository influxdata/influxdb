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

public class Wavelet_db6 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { -0.00107730108499558, 0.004777257511010651, 0.0005538422009938016, -0.031582039318031156, 0.02752286553001629, 0.09750160558707936, -0.12976686756709563, -0.22626469396516913, 0.3152503517092432, 0.7511339080215775, 0.4946238903983854, 0.11154074335008017,  };
  private static final double[] waveletDeComposition = new double[] { -0.11154074335008017, 0.4946238903983854, -0.7511339080215775, 0.3152503517092432, 0.22626469396516913, -0.12976686756709563, -0.09750160558707936, 0.02752286553001629, 0.031582039318031156, 0.0005538422009938016, -0.004777257511010651, -0.00107730108499558,  };

  private static final double[] scalingReConstruction = new double[] { 0.11154074335008017, 0.4946238903983854, 0.7511339080215775, 0.3152503517092432, -0.22626469396516913, -0.12976686756709563, 0.09750160558707936, 0.02752286553001629, -0.031582039318031156, 0.0005538422009938016, 0.004777257511010651, -0.00107730108499558,  };
  private static final double[] waveletReConstruction = new double[] { -0.00107730108499558, -0.004777257511010651, 0.0005538422009938016, 0.031582039318031156, 0.02752286553001629, -0.09750160558707936, -0.12976686756709563, 0.22626469396516913, 0.3152503517092432, -0.7511339080215775, 0.4946238903983854, -0.11154074335008017,  };

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

