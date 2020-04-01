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
 * PartialWindowMatrix.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

class PartialWindowMatrix implements CostMatrix {

  // PRIVATE DATA
  private static final double OUT_OF_WINDOW_VALUE = Double.POSITIVE_INFINITY;
  private double[] lastCol;
  private double[] currCol;
  private int currColIndex;
  private int minLastRow;
  private int minCurrRow;
  private final SearchWindow window;

  // CONSTRUCTOR
  PartialWindowMatrix(SearchWindow searchWindow) {
    window = searchWindow;

    if (window.maxI() > 0) {
      currCol = new double[window.maxJforI(1) - window.minJforI(1) + 1];
      currColIndex = 1;
      minLastRow = window.minJforI(currColIndex - 1);
    } else
      currColIndex = 0;

    minCurrRow = window.minJforI(currColIndex);
    lastCol = new double[window.maxJforI(0) - window.minJforI(0) + 1];
  } // end Constructor

  // PUBLIC FUNCTIONS
  public void put(int col, int row, double value) {
    if ((row < window.minJforI(col)) || (row > window.maxJforI(col))) {
      throw new RuntimeException("CostMatrix is filled in a cell (col=" + col
          + ", row=" + row + ") that is not in the " + "search window");
    } else {
      if (col == currColIndex)
        currCol[row - minCurrRow] = value;
      else if (col == currColIndex - 1)
        lastCol[row - minLastRow] = value;
      else if (col == currColIndex + 1) {
        lastCol = currCol;
        minLastRow = minCurrRow;
        currColIndex++;
        currCol = new double[window.maxJforI(col) - window.minJforI(col) + 1];
        minCurrRow = window.minJforI(col);

        currCol[row - minCurrRow] = value;
      } else
        throw new RuntimeException(
            "A PartialWindowMatrix can only fill in 2 adjacentcolumns at a time");
    } // end if
  } // end put(...)

  public double get(int col, int row) {
    if ((row < window.minJforI(col)) || (row > window.maxJforI(col)))
      return OUT_OF_WINDOW_VALUE;
    else {
      if (col == currColIndex)
        return currCol[row - minCurrRow];
      else if (col == currColIndex - 1)
        return lastCol[row - minLastRow];
      else
        return OUT_OF_WINDOW_VALUE;
    } // end if
  } // end get(..)

  public int size() {
    return lastCol.length + currCol.length;
  }

  public int windowSize() {
    return window.size();
  }

} // end WindowMatrix
