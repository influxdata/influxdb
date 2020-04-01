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

public class Wavelet_bior28 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 0.0, 0.0015105430506304422, -0.0030210861012608843, -0.012947511862546647, 0.02891610982635418, 0.052998481890690945, -0.13491307360773608, -0.16382918343409025, 0.4625714404759166, 0.9516421218971786, 0.4625714404759166, -0.16382918343409025, -0.13491307360773608, 0.052998481890690945, 0.02891610982635418, -0.012947511862546647, -0.0030210861012608843, 0.0015105430506304422,  };
  private static final double[] waveletDeComposition = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.3535533905932738, -0.7071067811865476, 0.3535533905932738, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  };

  private static final double[] scalingReConstruction = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.3535533905932738, 0.7071067811865476, 0.3535533905932738, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  };
  private static final double[] waveletReConstruction = new double[] { 0.0, -0.0015105430506304422, -0.0030210861012608843, 0.012947511862546647, 0.02891610982635418, -0.052998481890690945, -0.13491307360773608, 0.16382918343409025, 0.4625714404759166, -0.9516421218971786, 0.4625714404759166, 0.16382918343409025, -0.13491307360773608, -0.052998481890690945, 0.02891610982635418, 0.012947511862546647, -0.0030210861012608843, -0.0015105430506304422,  };

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

