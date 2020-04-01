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

package io.warp10.continuum;

public class SortUtil {
  
  public static interface Sorter {
    /**
     * Exchange two indices
     */
    public void exch(int i, int j);
    
    /**
     * Compare two indices
     */
    public int compare(int i, int j);
    
    /**
     * 
     */
  }
  

  public static void naive_dual_pivot_quicksort(Sorter sorter, int lo, int hi) {
    if (hi <= lo) return;

    // make sure a[lo] <= a[hi]
    if (sorter.compare(hi, lo) < 0) {
      sorter.exch(hi,  lo);
    }

    int lt = lo + 1;
    int gt = hi - 1;
    int i = lo + 1;

    while (i <= gt) {
      if (sorter.compare(i, lo) < 0) {
        sorter.exch(lt++, i++);
      } else if (sorter.compare(hi, i) < 0) {
        sorter.exch(i, gt--);
      } else {
        i++;
      }
    }
    
    sorter.exch(lo, --lt);
    sorter.exch(hi, ++gt);

    // recursively sort three subarrays
    naive_dual_pivot_quicksort(sorter, lo, lt-1);
      
    if (sorter.compare(lt, gt) < 0) {
      naive_dual_pivot_quicksort(sorter, lt+1, gt-1);
    }

    naive_dual_pivot_quicksort(sorter, gt+1, hi);
  }
}
