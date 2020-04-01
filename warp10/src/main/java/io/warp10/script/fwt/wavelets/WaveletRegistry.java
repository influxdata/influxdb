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

import java.util.HashMap;
import java.util.Map;

public class WaveletRegistry {
  private static final Map<String,Wavelet> registry = new HashMap<String, Wavelet>();
  
  static {
    register("haar", new Wavelet_haar());
    register("db1", new Wavelet_db1());
    register("db2", new Wavelet_db2());
    register("db3", new Wavelet_db3());
    register("db4", new Wavelet_db4());
    register("db5", new Wavelet_db5());
    register("db6", new Wavelet_db6());
    register("db7", new Wavelet_db7());
    register("db8", new Wavelet_db8());
    register("db9", new Wavelet_db9());
    register("db10", new Wavelet_db10());
    register("db11", new Wavelet_db11());
    register("db12", new Wavelet_db12());
    register("db13", new Wavelet_db13());
    register("db14", new Wavelet_db14());
    register("db15", new Wavelet_db15());
    register("db16", new Wavelet_db16());
    register("db17", new Wavelet_db17());
    register("db18", new Wavelet_db18());
    register("db19", new Wavelet_db19());
    register("db20", new Wavelet_db20());
    register("sym2", new Wavelet_sym2());
    register("sym3", new Wavelet_sym3());
    register("sym4", new Wavelet_sym4());
    register("sym5", new Wavelet_sym5());
    register("sym6", new Wavelet_sym6());
    register("sym7", new Wavelet_sym7());
    register("sym8", new Wavelet_sym8());
    register("sym9", new Wavelet_sym9());
    register("sym10", new Wavelet_sym10());
    register("sym11", new Wavelet_sym11());
    register("sym12", new Wavelet_sym12());
    register("sym13", new Wavelet_sym13());
    register("sym14", new Wavelet_sym14());
    register("sym15", new Wavelet_sym15());
    register("sym16", new Wavelet_sym16());
    register("sym17", new Wavelet_sym17());
    register("sym18", new Wavelet_sym18());
    register("sym19", new Wavelet_sym19());
    register("sym20", new Wavelet_sym20());
    register("coif1", new Wavelet_coif1());
    register("coif2", new Wavelet_coif2());
    register("coif3", new Wavelet_coif3());
    register("coif4", new Wavelet_coif4());
    register("coif5", new Wavelet_coif5());
    register("bior1.1", new Wavelet_bior11());
    register("bior1.3", new Wavelet_bior13());
    register("bior1.5", new Wavelet_bior15());
    register("bior2.2", new Wavelet_bior22());
    register("bior2.4", new Wavelet_bior24());
    register("bior2.6", new Wavelet_bior26());
    register("bior2.8", new Wavelet_bior28());
    register("bior3.1", new Wavelet_bior31());
    register("bior3.3", new Wavelet_bior33());
    register("bior3.5", new Wavelet_bior35());
    register("bior3.7", new Wavelet_bior37());
    register("bior3.9", new Wavelet_bior39());
    register("bior4.4", new Wavelet_bior44());
    register("bior5.5", new Wavelet_bior55());
    register("bior6.8", new Wavelet_bior68());
    register("rbio1.1", new Wavelet_rbio11());
    register("rbio1.3", new Wavelet_rbio13());
    register("rbio1.5", new Wavelet_rbio15());
    register("rbio2.2", new Wavelet_rbio22());
    register("rbio2.4", new Wavelet_rbio24());
    register("rbio2.6", new Wavelet_rbio26());
    register("rbio2.8", new Wavelet_rbio28());
    register("rbio3.1", new Wavelet_rbio31());
    register("rbio3.3", new Wavelet_rbio33());
    register("rbio3.5", new Wavelet_rbio35());
    register("rbio3.7", new Wavelet_rbio37());
    register("rbio3.9", new Wavelet_rbio39());
    register("rbio4.4", new Wavelet_rbio44());
    register("rbio5.5", new Wavelet_rbio55());
    register("rbio6.8", new Wavelet_rbio68());
    register("dmey", new Wavelet_dmey());  
  }
  
  public static void register(String name, Wavelet wavelet) {
    registry.put(name, wavelet);
  }
  
  public static Wavelet find(String name) {
    return registry.get(name);
  }
}
