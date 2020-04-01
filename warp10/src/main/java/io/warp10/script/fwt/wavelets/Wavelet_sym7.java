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

public class Wavelet_sym7 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 0.002681814568257878, -0.0010473848886829163, -0.01263630340325193, 0.03051551316596357, 0.0678926935013727, -0.049552834937127255, 0.017441255086855827, 0.5361019170917628, 0.767764317003164, 0.2886296317515146, -0.14004724044296152, -0.10780823770381774, 0.004010244871533663, 0.010268176708511255,  };
  private static final double[] waveletDeComposition = new double[] { -0.010268176708511255, 0.004010244871533663, 0.10780823770381774, -0.14004724044296152, -0.2886296317515146, 0.767764317003164, -0.5361019170917628, 0.017441255086855827, 0.049552834937127255, 0.0678926935013727, -0.03051551316596357, -0.01263630340325193, 0.0010473848886829163, 0.002681814568257878,  };

  private static final double[] scalingReConstruction = new double[] { 0.010268176708511255, 0.004010244871533663, -0.10780823770381774, -0.14004724044296152, 0.2886296317515146, 0.767764317003164, 0.5361019170917628, 0.017441255086855827, -0.049552834937127255, 0.0678926935013727, 0.03051551316596357, -0.01263630340325193, -0.0010473848886829163, 0.002681814568257878,  };
  private static final double[] waveletReConstruction = new double[] { 0.002681814568257878, 0.0010473848886829163, -0.01263630340325193, -0.03051551316596357, 0.0678926935013727, 0.049552834937127255, 0.017441255086855827, -0.5361019170917628, 0.767764317003164, -0.2886296317515146, -0.14004724044296152, 0.10780823770381774, 0.004010244871533663, -0.010268176708511255,  };

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

