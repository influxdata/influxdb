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

public class Wavelet_sym8 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { -0.0033824159510061256, -0.0005421323317911481, 0.03169508781149298, 0.007607487324917605, -0.1432942383508097, -0.061273359067658524, 0.4813596512583722, 0.7771857517005235, 0.3644418948353314, -0.05194583810770904, -0.027219029917056003, 0.049137179673607506, 0.003808752013890615, -0.01495225833704823, -0.0003029205147213668, 0.0018899503327594609,  };
  private static final double[] waveletDeComposition = new double[] { -0.0018899503327594609, -0.0003029205147213668, 0.01495225833704823, 0.003808752013890615, -0.049137179673607506, -0.027219029917056003, 0.05194583810770904, 0.3644418948353314, -0.7771857517005235, 0.4813596512583722, 0.061273359067658524, -0.1432942383508097, -0.007607487324917605, 0.03169508781149298, 0.0005421323317911481, -0.0033824159510061256,  };

  private static final double[] scalingReConstruction = new double[] { 0.0018899503327594609, -0.0003029205147213668, -0.01495225833704823, 0.003808752013890615, 0.049137179673607506, -0.027219029917056003, -0.05194583810770904, 0.3644418948353314, 0.7771857517005235, 0.4813596512583722, -0.061273359067658524, -0.1432942383508097, 0.007607487324917605, 0.03169508781149298, -0.0005421323317911481, -0.0033824159510061256,  };
  private static final double[] waveletReConstruction = new double[] { -0.0033824159510061256, 0.0005421323317911481, 0.03169508781149298, -0.007607487324917605, -0.1432942383508097, 0.061273359067658524, 0.4813596512583722, -0.7771857517005235, 0.3644418948353314, 0.05194583810770904, -0.027219029917056003, -0.049137179673607506, 0.003808752013890615, 0.01495225833704823, -0.0003029205147213668, -0.0018899503327594609,  };

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

