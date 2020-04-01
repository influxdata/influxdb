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

public class Wavelet_db11 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 4.494274277236352e-06, -3.463498418698379e-05, 5.443907469936638e-05, 0.00024915252355281426, -0.0008930232506662366, -0.00030859285881515924, 0.004928417656058778, -0.0033408588730145018, -0.015364820906201324, 0.02084090436018004, 0.03133509021904531, -0.06643878569502022, -0.04647995511667613, 0.14981201246638268, 0.06604358819669089, -0.27423084681792875, -0.16227524502747828, 0.41196436894789695, 0.6856867749161785, 0.44989976435603013, 0.1440670211506196, 0.01869429776147044,  };
  private static final double[] waveletDeComposition = new double[] { -0.01869429776147044, 0.1440670211506196, -0.44989976435603013, 0.6856867749161785, -0.41196436894789695, -0.16227524502747828, 0.27423084681792875, 0.06604358819669089, -0.14981201246638268, -0.04647995511667613, 0.06643878569502022, 0.03133509021904531, -0.02084090436018004, -0.015364820906201324, 0.0033408588730145018, 0.004928417656058778, 0.00030859285881515924, -0.0008930232506662366, -0.00024915252355281426, 5.443907469936638e-05, 3.463498418698379e-05, 4.494274277236352e-06,  };

  private static final double[] scalingReConstruction = new double[] { 0.01869429776147044, 0.1440670211506196, 0.44989976435603013, 0.6856867749161785, 0.41196436894789695, -0.16227524502747828, -0.27423084681792875, 0.06604358819669089, 0.14981201246638268, -0.04647995511667613, -0.06643878569502022, 0.03133509021904531, 0.02084090436018004, -0.015364820906201324, -0.0033408588730145018, 0.004928417656058778, -0.00030859285881515924, -0.0008930232506662366, 0.00024915252355281426, 5.443907469936638e-05, -3.463498418698379e-05, 4.494274277236352e-06,  };
  private static final double[] waveletReConstruction = new double[] { 4.494274277236352e-06, 3.463498418698379e-05, 5.443907469936638e-05, -0.00024915252355281426, -0.0008930232506662366, 0.00030859285881515924, 0.004928417656058778, 0.0033408588730145018, -0.015364820906201324, -0.02084090436018004, 0.03133509021904531, 0.06643878569502022, -0.04647995511667613, -0.14981201246638268, 0.06604358819669089, 0.27423084681792875, -0.16227524502747828, -0.41196436894789695, 0.6856867749161785, -0.44989976435603013, 0.1440670211506196, -0.01869429776147044,  };

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

