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

public class Wavelet_db9 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 3.9347319995026124e-05, -0.0002519631889981789, 0.00023038576399541288, 0.0018476468829611268, -0.004281503681904723, -0.004723204757894831, 0.022361662123515244, 0.00025094711499193845, -0.06763282905952399, 0.030725681478322865, 0.14854074933476008, -0.09684078322087904, -0.29327378327258685, 0.13319738582208895, 0.6572880780366389, 0.6048231236767786, 0.24383467463766728, 0.03807794736316728,  };
  private static final double[] waveletDeComposition = new double[] { -0.03807794736316728, 0.24383467463766728, -0.6048231236767786, 0.6572880780366389, -0.13319738582208895, -0.29327378327258685, 0.09684078322087904, 0.14854074933476008, -0.030725681478322865, -0.06763282905952399, -0.00025094711499193845, 0.022361662123515244, 0.004723204757894831, -0.004281503681904723, -0.0018476468829611268, 0.00023038576399541288, 0.0002519631889981789, 3.9347319995026124e-05,  };

  private static final double[] scalingReConstruction = new double[] { 0.03807794736316728, 0.24383467463766728, 0.6048231236767786, 0.6572880780366389, 0.13319738582208895, -0.29327378327258685, -0.09684078322087904, 0.14854074933476008, 0.030725681478322865, -0.06763282905952399, 0.00025094711499193845, 0.022361662123515244, -0.004723204757894831, -0.004281503681904723, 0.0018476468829611268, 0.00023038576399541288, -0.0002519631889981789, 3.9347319995026124e-05,  };
  private static final double[] waveletReConstruction = new double[] { 3.9347319995026124e-05, 0.0002519631889981789, 0.00023038576399541288, -0.0018476468829611268, -0.004281503681904723, 0.004723204757894831, 0.022361662123515244, -0.00025094711499193845, -0.06763282905952399, -0.030725681478322865, 0.14854074933476008, 0.09684078322087904, -0.29327378327258685, -0.13319738582208895, 0.6572880780366389, -0.6048231236767786, 0.24383467463766728, -0.03807794736316728,  };

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

