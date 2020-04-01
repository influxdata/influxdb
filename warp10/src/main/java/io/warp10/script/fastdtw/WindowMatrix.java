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
/*
 * WindowMatrix.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

class WindowMatrix implements CostMatrix {
  // PRIVATE DATA
  private CostMatrix windowCells;

  // CONSTRUCTOR
  WindowMatrix(SearchWindow searchWindow) {
    try {
      windowCells = new MemoryResidentMatrix(searchWindow);
    } catch (OutOfMemoryError e) {
      System.err
          .println("Ran out of memory initializing window matrix, all cells in the window cannot fit into "
              + "main memory.  Will use a swap file instead (will run ~50% slower)");
      System.gc();
      windowCells = new SwapFileMatrix(searchWindow);
    } // end try
  } // end Constructor

  // PUBLIC FUNCTIONS
  public void put(int col, int row, double value) {
    windowCells.put(col, row, value);
  }

  public double get(int col, int row) {
    return windowCells.get(col, row);
  }

  public int size() {
    return windowCells.size();
  }

  public void freeMem() {
    // Resources only need to be freed for a SwapFileMatrix.
    if (windowCells instanceof SwapFileMatrix) {
      try {
        ((SwapFileMatrix) windowCells).freeMem();
      } catch (Throwable t) {
        // ignore the exception
      } // end try
    } // end if
  } // end freeMem()

} // end WindowMatrix
