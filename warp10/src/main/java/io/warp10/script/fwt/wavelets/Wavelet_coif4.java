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

public class Wavelet_coif4 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { -1.7849850030882614e-06, -3.2596802368833675e-06, 3.1229875865345646e-05, 6.233903446100713e-05, -0.00025997455248771324, -0.0005890207562443383, 0.0012665619292989445, 0.003751436157278457, -0.00565828668661072, -0.015211731527946259, 0.025082261844864097, 0.03933442712333749, -0.09622044203398798, -0.06662747426342504, 0.4343860564914685, 0.782238930920499, 0.41530840703043026, -0.05607731331675481, -0.08126669968087875, 0.026682300156053072, 0.016068943964776348, -0.0073461663276420935, -0.0016294920126017326, 0.0008923136685823146,  };
  private static final double[] waveletDeComposition = new double[] { -0.0008923136685823146, -0.0016294920126017326, 0.0073461663276420935, 0.016068943964776348, -0.026682300156053072, -0.08126669968087875, 0.05607731331675481, 0.41530840703043026, -0.782238930920499, 0.4343860564914685, 0.06662747426342504, -0.09622044203398798, -0.03933442712333749, 0.025082261844864097, 0.015211731527946259, -0.00565828668661072, -0.003751436157278457, 0.0012665619292989445, 0.0005890207562443383, -0.00025997455248771324, -6.233903446100713e-05, 3.1229875865345646e-05, 3.2596802368833675e-06, -1.7849850030882614e-06,  };

  private static final double[] scalingReConstruction = new double[] { 0.0008923136685823146, -0.0016294920126017326, -0.0073461663276420935, 0.016068943964776348, 0.026682300156053072, -0.08126669968087875, -0.05607731331675481, 0.41530840703043026, 0.782238930920499, 0.4343860564914685, -0.06662747426342504, -0.09622044203398798, 0.03933442712333749, 0.025082261844864097, -0.015211731527946259, -0.00565828668661072, 0.003751436157278457, 0.0012665619292989445, -0.0005890207562443383, -0.00025997455248771324, 6.233903446100713e-05, 3.1229875865345646e-05, -3.2596802368833675e-06, -1.7849850030882614e-06,  };
  private static final double[] waveletReConstruction = new double[] { -1.7849850030882614e-06, 3.2596802368833675e-06, 3.1229875865345646e-05, -6.233903446100713e-05, -0.00025997455248771324, 0.0005890207562443383, 0.0012665619292989445, -0.003751436157278457, -0.00565828668661072, 0.015211731527946259, 0.025082261844864097, -0.03933442712333749, -0.09622044203398798, 0.06662747426342504, 0.4343860564914685, -0.782238930920499, 0.41530840703043026, 0.05607731331675481, -0.08126669968087875, -0.026682300156053072, 0.016068943964776348, 0.0073461663276420935, -0.0016294920126017326, -0.0008923136685823146,  };

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

