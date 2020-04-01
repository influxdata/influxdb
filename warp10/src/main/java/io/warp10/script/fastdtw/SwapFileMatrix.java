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
 * SwapFileMatrix.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Random;

class SwapFileMatrix implements CostMatrix {
  // CONSTANTS
  private static final double OUT_OF_WINDOW_VALUE = Double.POSITIVE_INFINITY;
  private static final Random RAND_GEN = new Random();

  // PRIVATE DATA
  private final SearchWindow window;

  // Private data needed to store the last 2 colums of the matrix.
  private double[] lastCol;
  private double[] currCol;
  private int currColIndex;
  private int minLastRow;
  private int minCurrRow;

  // Private data needed to read values from the swap file.
  private final File swapFile;
  private final RandomAccessFile cellValuesFile;
  private boolean isSwapFileFreed;
  private final long[] colOffsets;

  // CONSTRUCTOR
  SwapFileMatrix(SearchWindow searchWindow) {
    window = searchWindow;

    if (window.maxI() > 0) {
      currCol = new double[window.maxJforI(1) - window.minJforI(1) + 1];
      currColIndex = 1;
      minLastRow = window.minJforI(currColIndex - 1);
    } else
      // special case for a <=1 point time series, less than 2 columns to fill
      // in
      currColIndex = 0;

    minCurrRow = window.minJforI(currColIndex);
    lastCol = new double[window.maxJforI(0) - window.minJforI(0) + 1];

    swapFile = new File("swap" + RAND_GEN.nextLong());
    isSwapFileFreed = false;
    // swapFile.deleteOnExit();

    colOffsets = new long[window.maxI() + 1];

    try {
      cellValuesFile = new RandomAccessFile(swapFile, "rw");
    } catch (FileNotFoundException e) {
      throw new RuntimeException("ERROR:  Unable to create swap file: "
          + swapFile);
    } // end try
  } // end Constructor

  // PUBLIC FUNCTIONS
  public void put(int col, int row, double value) {
    if ((row < window.minJforI(col)) || (row > window.maxJforI(col))) {
      throw new RuntimeException("CostMatrix is filled in a cell (col=" + col
          + ", row=" + row + ") that is not in the " + "search window");
    } else {
      if (col == currColIndex)
        currCol[row - minCurrRow] = value;
      else if (col == currColIndex - 1) {
        lastCol[row - minLastRow] = value;
      } else if (col == currColIndex + 1) {
        // Write the last column to the swap file.
        try {
          if (isSwapFileFreed)
            throw new RuntimeException(
                "The SwapFileMatrix has been freeded by the freeMem() method");
          else {
            cellValuesFile.seek(cellValuesFile.length()); // move file pointer to
                                                          // end of file
            colOffsets[currColIndex - 1] = cellValuesFile.getFilePointer();

            // Write an entire column to the swap file.
            cellValuesFile.write(TypeConversions
                .doubleArrayToByteArray(lastCol));
          } // end if
        } catch (IOException e) {
          throw new RuntimeException(
              "Unable to fill the CostMatrix in the Swap file (IOException)");
        } // end try

        lastCol = currCol;
        minLastRow = minCurrRow;
        minCurrRow = window.minJforI(col);
        currColIndex++;
        currCol = new double[window.maxJforI(col) - window.minJforI(col) + 1];
        currCol[row - minCurrRow] = value;
      } else
        throw new RuntimeException(
            "A SwapFileMatrix can only fill in 2 adjacentcolumns at a time");
    } // end if
  } // end put(...)

  public double get(int col, int row) {
    if ((row < window.minJforI(col)) || (row > window.maxJforI(col)))
      return OUT_OF_WINDOW_VALUE;
    else if (col == currColIndex)
      return currCol[row - minCurrRow];
    else if (col == currColIndex - 1)
      return lastCol[row - minLastRow];
    else {
      try {
        if (isSwapFileFreed)
          throw new RuntimeException(
              "The SwapFileMatrix has been freeded by the freeMem() method");
        else {
          cellValuesFile.seek(colOffsets[col] + 8
              * (row - window.minJforI(col)));
          return cellValuesFile.readDouble();
        } // end if
      } catch (IOException e) {
        if (col > currColIndex)
          throw new RuntimeException(
              "The requested value is in the search window but has not been entered into "
                  + "the matrix: (col=" + col + "row=" + row + ").");
        else
          throw new RuntimeException(
              "Unable to read CostMatrix in the Swap file (IOException)");
      } // end try
    } // end if
  } // end get(..)

  // This method closes and deletes the swap file when the object's finalize()
  // method is called. This method will
  // ONLY be called by the JVM if the object is garbage collected while the
  // application is still running.
  // This method must be called explicitly to guarantee that the swap file is
  // deleted.
  protected void finalize() throws Throwable {
    // Close and Delete the (possibly VERY large) swap file.
    try {
      if (!isSwapFileFreed)
        cellValuesFile.close();
    } catch (Exception e) {
      System.err.println("unable to close swap file '"
          + this.swapFile.getPath() + "' during finialization");
    } finally {
      swapFile.delete(); // delete the swap file
      super.finalize(); // ensure that finalization continues for parent if an
                        // exception is thrown
    } // end try
  } // end finalize()

  public int size() {
    return window.size();
  }

  public void freeMem() {
    try {
      cellValuesFile.close();
    } catch (IOException e) {
      System.err.println("unable to close swap file '"
          + this.swapFile.getPath() + "'");
    } finally {
      if (!swapFile.delete())
        System.err.println("unable to delete swap file '"
            + this.swapFile.getPath() + "'");
    } // end try
  } // end freeMem

} // end class SwapFileMatrix

