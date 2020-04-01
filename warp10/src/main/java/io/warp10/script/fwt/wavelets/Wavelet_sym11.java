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

public class Wavelet_sym11 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 0.00017172195069934854, -3.8795655736158566e-05, -0.0017343662672978692, 0.0005883527353969915, 0.00651249567477145, -0.009857934828789794, -0.024080841595864003, 0.0370374159788594, 0.06997679961073414, -0.022832651022562687, 0.09719839445890947, 0.5720229780100871, 0.7303435490883957, 0.23768990904924897, -0.2046547944958006, -0.1446023437053156, 0.03526675956446655, 0.04300019068155228, -0.0020034719001093887, -0.006389603666454892, 0.00011053509764272153, 0.0004892636102619239,  };
  private static final double[] waveletDeComposition = new double[] { -0.0004892636102619239, 0.00011053509764272153, 0.006389603666454892, -0.0020034719001093887, -0.04300019068155228, 0.03526675956446655, 0.1446023437053156, -0.2046547944958006, -0.23768990904924897, 0.7303435490883957, -0.5720229780100871, 0.09719839445890947, 0.022832651022562687, 0.06997679961073414, -0.0370374159788594, -0.024080841595864003, 0.009857934828789794, 0.00651249567477145, -0.0005883527353969915, -0.0017343662672978692, 3.8795655736158566e-05, 0.00017172195069934854,  };

  private static final double[] scalingReConstruction = new double[] { 0.0004892636102619239, 0.00011053509764272153, -0.006389603666454892, -0.0020034719001093887, 0.04300019068155228, 0.03526675956446655, -0.1446023437053156, -0.2046547944958006, 0.23768990904924897, 0.7303435490883957, 0.5720229780100871, 0.09719839445890947, -0.022832651022562687, 0.06997679961073414, 0.0370374159788594, -0.024080841595864003, -0.009857934828789794, 0.00651249567477145, 0.0005883527353969915, -0.0017343662672978692, -3.8795655736158566e-05, 0.00017172195069934854,  };
  private static final double[] waveletReConstruction = new double[] { 0.00017172195069934854, 3.8795655736158566e-05, -0.0017343662672978692, -0.0005883527353969915, 0.00651249567477145, 0.009857934828789794, -0.024080841595864003, -0.0370374159788594, 0.06997679961073414, 0.022832651022562687, 0.09719839445890947, -0.5720229780100871, 0.7303435490883957, -0.23768990904924897, -0.2046547944958006, 0.1446023437053156, 0.03526675956446655, -0.04300019068155228, -0.0020034719001093887, 0.006389603666454892, 0.00011053509764272153, -0.0004892636102619239,  };

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

