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

public class Wavelet_db13 extends Wavelet {

  private static final int transformWavelength = 2;

  private static final double[] scalingDeComposition = new double[] { 5.2200350984548e-07, -4.700416479360808e-06, 1.0441930571407941e-05, 3.067853757932436e-05, -0.0001651289885565057, 4.9251525126285676e-05, 0.000932326130867249, -0.0013156739118922766, -0.002761911234656831, 0.007255589401617119, 0.003923941448795577, -0.02383142071032781, 0.002379972254052227, 0.056139477100276156, -0.026488406475345658, -0.10580761818792761, 0.07294893365678874, 0.17947607942935084, -0.12457673075080665, -0.31497290771138414, 0.086985726179645, 0.5888895704312119, 0.6110558511587811, 0.3119963221604349, 0.08286124387290195, 0.009202133538962279,  };
  private static final double[] waveletDeComposition = new double[] { -0.009202133538962279, 0.08286124387290195, -0.3119963221604349, 0.6110558511587811, -0.5888895704312119, 0.086985726179645, 0.31497290771138414, -0.12457673075080665, -0.17947607942935084, 0.07294893365678874, 0.10580761818792761, -0.026488406475345658, -0.056139477100276156, 0.002379972254052227, 0.02383142071032781, 0.003923941448795577, -0.007255589401617119, -0.002761911234656831, 0.0013156739118922766, 0.000932326130867249, -4.9251525126285676e-05, -0.0001651289885565057, -3.067853757932436e-05, 1.0441930571407941e-05, 4.700416479360808e-06, 5.2200350984548e-07,  };

  private static final double[] scalingReConstruction = new double[] { 0.009202133538962279, 0.08286124387290195, 0.3119963221604349, 0.6110558511587811, 0.5888895704312119, 0.086985726179645, -0.31497290771138414, -0.12457673075080665, 0.17947607942935084, 0.07294893365678874, -0.10580761818792761, -0.026488406475345658, 0.056139477100276156, 0.002379972254052227, -0.02383142071032781, 0.003923941448795577, 0.007255589401617119, -0.002761911234656831, -0.0013156739118922766, 0.000932326130867249, 4.9251525126285676e-05, -0.0001651289885565057, 3.067853757932436e-05, 1.0441930571407941e-05, -4.700416479360808e-06, 5.2200350984548e-07,  };
  private static final double[] waveletReConstruction = new double[] { 5.2200350984548e-07, 4.700416479360808e-06, 1.0441930571407941e-05, -3.067853757932436e-05, -0.0001651289885565057, -4.9251525126285676e-05, 0.000932326130867249, 0.0013156739118922766, -0.002761911234656831, -0.007255589401617119, 0.003923941448795577, 0.02383142071032781, 0.002379972254052227, -0.056139477100276156, -0.026488406475345658, 0.10580761818792761, 0.07294893365678874, -0.17947607942935084, -0.12457673075080665, 0.31497290771138414, 0.086985726179645, -0.5888895704312119, 0.6110558511587811, -0.3119963221604349, 0.08286124387290195, -0.009202133538962279,  };

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

