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

public class Wavelet_db8 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { -0.00011747678400228192, 0.0006754494059985568, -0.0003917403729959771, -0.00487035299301066, 0.008746094047015655, 0.013981027917015516, -0.04408825393106472, -0.01736930100202211, 0.128747426620186, 0.00047248457399797254, -0.2840155429624281, -0.015829105256023893, 0.5853546836548691, 0.6756307362980128, 0.3128715909144659, 0.05441584224308161,  };
  private static final double[] waveletDeComposition = new double[] { -0.05441584224308161, 0.3128715909144659, -0.6756307362980128, 0.5853546836548691, 0.015829105256023893, -0.2840155429624281, -0.00047248457399797254, 0.128747426620186, 0.01736930100202211, -0.04408825393106472, -0.013981027917015516, 0.008746094047015655, 0.00487035299301066, -0.0003917403729959771, -0.0006754494059985568, -0.00011747678400228192,  };

  private static final double[] scalingReConstruction = new double[] { 0.05441584224308161, 0.3128715909144659, 0.6756307362980128, 0.5853546836548691, -0.015829105256023893, -0.2840155429624281, 0.00047248457399797254, 0.128747426620186, -0.01736930100202211, -0.04408825393106472, 0.013981027917015516, 0.008746094047015655, -0.00487035299301066, -0.0003917403729959771, 0.0006754494059985568, -0.00011747678400228192,  };
  private static final double[] waveletReConstruction = new double[] { -0.00011747678400228192, -0.0006754494059985568, -0.0003917403729959771, 0.00487035299301066, 0.008746094047015655, -0.013981027917015516, -0.04408825393106472, 0.01736930100202211, 0.128747426620186, -0.00047248457399797254, -0.2840155429624281, 0.015829105256023893, 0.5853546836548691, -0.6756307362980128, 0.3128715909144659, -0.05441584224308161,  };

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

