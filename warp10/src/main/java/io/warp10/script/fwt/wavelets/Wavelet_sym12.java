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

public class Wavelet_sym12 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 0.00011196719424656033, -1.1353928041541452e-05, -0.0013497557555715387, 0.00018021409008538188, 0.007414965517654251, -0.0014089092443297553, -0.024220722675013445, 0.0075537806116804775, 0.04917931829966084, -0.03584883073695439, -0.022162306170337816, 0.39888597239022, 0.7634790977836572, 0.46274103121927235, -0.07833262231634322, -0.17037069723886492, 0.01530174062247884, 0.05780417944550566, -0.0026043910313322326, -0.014589836449234145, 0.00030764779631059454, 0.002350297614183465, -1.8158078862617515e-05, -0.0001790665869750869,  };
  private static final double[] waveletDeComposition = new double[] { 0.0001790665869750869, -1.8158078862617515e-05, -0.002350297614183465, 0.00030764779631059454, 0.014589836449234145, -0.0026043910313322326, -0.05780417944550566, 0.01530174062247884, 0.17037069723886492, -0.07833262231634322, -0.46274103121927235, 0.7634790977836572, -0.39888597239022, -0.022162306170337816, 0.03584883073695439, 0.04917931829966084, -0.0075537806116804775, -0.024220722675013445, 0.0014089092443297553, 0.007414965517654251, -0.00018021409008538188, -0.0013497557555715387, 1.1353928041541452e-05, 0.00011196719424656033,  };

  private static final double[] scalingReConstruction = new double[] { -0.0001790665869750869, -1.8158078862617515e-05, 0.002350297614183465, 0.00030764779631059454, -0.014589836449234145, -0.0026043910313322326, 0.05780417944550566, 0.01530174062247884, -0.17037069723886492, -0.07833262231634322, 0.46274103121927235, 0.7634790977836572, 0.39888597239022, -0.022162306170337816, -0.03584883073695439, 0.04917931829966084, 0.0075537806116804775, -0.024220722675013445, -0.0014089092443297553, 0.007414965517654251, 0.00018021409008538188, -0.0013497557555715387, -1.1353928041541452e-05, 0.00011196719424656033,  };
  private static final double[] waveletReConstruction = new double[] { 0.00011196719424656033, 1.1353928041541452e-05, -0.0013497557555715387, -0.00018021409008538188, 0.007414965517654251, 0.0014089092443297553, -0.024220722675013445, -0.0075537806116804775, 0.04917931829966084, 0.03584883073695439, -0.022162306170337816, -0.39888597239022, 0.7634790977836572, -0.46274103121927235, -0.07833262231634322, 0.17037069723886492, 0.01530174062247884, -0.05780417944550566, -0.0026043910313322326, 0.014589836449234145, 0.00030764779631059454, -0.002350297614183465, -1.8158078862617515e-05, 0.0001790665869750869,  };

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

