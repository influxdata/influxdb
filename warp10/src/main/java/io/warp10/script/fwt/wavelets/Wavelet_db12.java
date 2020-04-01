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

public class Wavelet_db12 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { -1.5290717580684923e-06, 1.2776952219379579e-05, -2.4241545757030318e-05, -8.850410920820318e-05, 0.0003886530628209267, 6.5451282125215034e-06, -0.0021795036186277044, 0.0022486072409952287, 0.006711499008795549, -0.012840825198299882, -0.01221864906974642, 0.04154627749508764, 0.010849130255828966, -0.09643212009649671, 0.0053595696743599965, 0.18247860592758275, -0.023779257256064865, -0.31617845375277914, -0.04476388565377762, 0.5158864784278007, 0.6571987225792911, 0.3773551352142041, 0.10956627282118277, 0.013112257957229239,  };
  private static final double[] waveletDeComposition = new double[] { -0.013112257957229239, 0.10956627282118277, -0.3773551352142041, 0.6571987225792911, -0.5158864784278007, -0.04476388565377762, 0.31617845375277914, -0.023779257256064865, -0.18247860592758275, 0.0053595696743599965, 0.09643212009649671, 0.010849130255828966, -0.04154627749508764, -0.01221864906974642, 0.012840825198299882, 0.006711499008795549, -0.0022486072409952287, -0.0021795036186277044, -6.5451282125215034e-06, 0.0003886530628209267, 8.850410920820318e-05, -2.4241545757030318e-05, -1.2776952219379579e-05, -1.5290717580684923e-06,  };

  private static final double[] scalingReConstruction = new double[] { 0.013112257957229239, 0.10956627282118277, 0.3773551352142041, 0.6571987225792911, 0.5158864784278007, -0.04476388565377762, -0.31617845375277914, -0.023779257256064865, 0.18247860592758275, 0.0053595696743599965, -0.09643212009649671, 0.010849130255828966, 0.04154627749508764, -0.01221864906974642, -0.012840825198299882, 0.006711499008795549, 0.0022486072409952287, -0.0021795036186277044, 6.5451282125215034e-06, 0.0003886530628209267, -8.850410920820318e-05, -2.4241545757030318e-05, 1.2776952219379579e-05, -1.5290717580684923e-06,  };
  private static final double[] waveletReConstruction = new double[] { -1.5290717580684923e-06, -1.2776952219379579e-05, -2.4241545757030318e-05, 8.850410920820318e-05, 0.0003886530628209267, -6.5451282125215034e-06, -0.0021795036186277044, -0.0022486072409952287, 0.006711499008795549, 0.012840825198299882, -0.01221864906974642, -0.04154627749508764, 0.010849130255828966, 0.09643212009649671, 0.0053595696743599965, -0.18247860592758275, -0.023779257256064865, 0.31617845375277914, -0.04476388565377762, -0.5158864784278007, 0.6571987225792911, -0.3773551352142041, 0.10956627282118277, -0.013112257957229239,  };

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

