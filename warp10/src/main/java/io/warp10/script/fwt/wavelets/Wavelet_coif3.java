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

public class Wavelet_coif3 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { -3.459977283621256e-05, -7.098330313814125e-05, 0.0004662169601128863, 0.0011175187708906016, -0.0025745176887502236, -0.00900797613666158, 0.015880544863615904, 0.03455502757306163, -0.08230192710688598, -0.07179982161931202, 0.42848347637761874, 0.7937772226256206, 0.4051769024096169, -0.06112339000267287, -0.0657719112818555, 0.023452696141836267, 0.007782596427325418, -0.003793512864491014,  };
  private static final double[] waveletDeComposition = new double[] { 0.003793512864491014, 0.007782596427325418, -0.023452696141836267, -0.0657719112818555, 0.06112339000267287, 0.4051769024096169, -0.7937772226256206, 0.42848347637761874, 0.07179982161931202, -0.08230192710688598, -0.03455502757306163, 0.015880544863615904, 0.00900797613666158, -0.0025745176887502236, -0.0011175187708906016, 0.0004662169601128863, 7.098330313814125e-05, -3.459977283621256e-05,  };

  private static final double[] scalingReConstruction = new double[] { -0.003793512864491014, 0.007782596427325418, 0.023452696141836267, -0.0657719112818555, -0.06112339000267287, 0.4051769024096169, 0.7937772226256206, 0.42848347637761874, -0.07179982161931202, -0.08230192710688598, 0.03455502757306163, 0.015880544863615904, -0.00900797613666158, -0.0025745176887502236, 0.0011175187708906016, 0.0004662169601128863, -7.098330313814125e-05, -3.459977283621256e-05,  };
  private static final double[] waveletReConstruction = new double[] { -3.459977283621256e-05, 7.098330313814125e-05, 0.0004662169601128863, -0.0011175187708906016, -0.0025745176887502236, 0.00900797613666158, 0.015880544863615904, -0.03455502757306163, -0.08230192710688598, 0.07179982161931202, 0.42848347637761874, -0.7937772226256206, 0.4051769024096169, 0.06112339000267287, -0.0657719112818555, -0.023452696141836267, 0.007782596427325418, 0.003793512864491014,  };

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

