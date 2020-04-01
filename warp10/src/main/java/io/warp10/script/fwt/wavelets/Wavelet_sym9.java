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

public class Wavelet_sym9 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 0.0014009155259146807, 0.0006197808889855868, -0.013271967781817119, -0.01152821020767923, 0.03022487885827568, 0.0005834627461258068, -0.05456895843083407, 0.238760914607303, 0.717897082764412, 0.6173384491409358, 0.035272488035271894, -0.19155083129728512, -0.018233770779395985, 0.06207778930288603, 0.008859267493400484, -0.010264064027633142, -0.0004731544986800831, 0.0010694900329086053,  };
  private static final double[] waveletDeComposition = new double[] { -0.0010694900329086053, -0.0004731544986800831, 0.010264064027633142, 0.008859267493400484, -0.06207778930288603, -0.018233770779395985, 0.19155083129728512, 0.035272488035271894, -0.6173384491409358, 0.717897082764412, -0.238760914607303, -0.05456895843083407, -0.0005834627461258068, 0.03022487885827568, 0.01152821020767923, -0.013271967781817119, -0.0006197808889855868, 0.0014009155259146807,  };

  private static final double[] scalingReConstruction = new double[] { 0.0010694900329086053, -0.0004731544986800831, -0.010264064027633142, 0.008859267493400484, 0.06207778930288603, -0.018233770779395985, -0.19155083129728512, 0.035272488035271894, 0.6173384491409358, 0.717897082764412, 0.238760914607303, -0.05456895843083407, 0.0005834627461258068, 0.03022487885827568, -0.01152821020767923, -0.013271967781817119, 0.0006197808889855868, 0.0014009155259146807,  };
  private static final double[] waveletReConstruction = new double[] { 0.0014009155259146807, -0.0006197808889855868, -0.013271967781817119, 0.01152821020767923, 0.03022487885827568, -0.0005834627461258068, -0.05456895843083407, -0.238760914607303, 0.717897082764412, -0.6173384491409358, 0.035272488035271894, 0.19155083129728512, -0.018233770779395985, -0.06207778930288603, 0.008859267493400484, 0.010264064027633142, -0.0004731544986800831, -0.0010694900329086053,  };

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

