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

package io.warp10.script.fwt;

import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * Class which implements a fast wavelet transform. Inspired by the JWave code.
 * 
 * @see https://github.com/cscheiblich/JWave
 */
public class FWT {
  
  public static double[] forward(Wavelet wavelet, double[] timeBased) {
    Preconditions.checkArgument(0 == (timeBased.length & (timeBased.length - 1)), "Can only perform FWT on data whose cardinality is a power of 2.");
    
    double[] hilbert = Arrays.copyOf(timeBased, timeBased.length);

    int h = hilbert.length;
    int transformWavelength = wavelet.getTransformWavelength(); // 2, 4, 8, 16, 32, ...

    while( h >= transformWavelength ) {

      double[] temp = wavelet.forward(hilbert, h);

      System.arraycopy(temp, 0, hilbert, 0, h);

      h = h >> 1;
    } // levels

    return hilbert;
  }
  
  public static double[ ] reverse(Wavelet wavelet, double[ ] arrHilb ) {
    Preconditions.checkArgument(0 == (arrHilb.length & (arrHilb.length - 1)), "Can only perform FWT on data whose cardinality is a power of 2.");

    double[ ] arrTime = Arrays.copyOf(arrHilb, arrHilb.length);

    int transformWavelength = wavelet.getTransformWavelength(); // 2, 4, 8, 16, 32, ...

    int h = transformWavelength;

    while( h <= arrTime.length && h >= transformWavelength ) {

      double[] arrTempPart = wavelet.reverse( arrTime, h );

      System.arraycopy(arrTempPart, 0, arrTime, 0, h);
      
      h = h << 1;

    } // levels

    return arrTime;

  } // reverse
}
