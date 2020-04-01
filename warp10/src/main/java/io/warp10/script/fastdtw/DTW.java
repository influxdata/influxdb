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
 * DTW.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

import io.warp10.continuum.gts.GeoTimeSerie;

import java.util.Arrays;
import java.util.Iterator;

public class DTW {

  // FUNCTIONS
  public static double calcWarpCost(WarpPath path, GeoTimeSerie tsI, GeoTimeSerie tsJ, DistanceFunction distFn) {
    double totalCost = 0.0;

    for (int p = 0; p < path.size(); p++) {
      final ColMajorCell currWarp = path.get(p);
      totalCost += distFn.calcDistance(tsI, currWarp.getCol(), tsJ, currWarp.getRow());
    }

    return totalCost;
  }

  // Dynamic Time Warping where the warp path is not needed, an alternate
  // implementation can be used that does not
  // require the entire cost matrix to be filled and only needs 2 columns to be
  // stored at any one time.
  public static double getWarpDistBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ, DistanceFunction distFn) {
    // The space complexity is 2*tsJ.size(). Dynamic time warping is symmetric
    // so switching the two time series
    // parameters does not effect the final warp cost but can reduce the space
    // complexity by allowing tsJ to
    // be set as the shorter time series and only requiring 2 columns of size
    // |tsJ| rather than 2 larger columns of
    // size |tsI|.
    if (tsI.size() < tsJ.size())
      return getWarpDistBetween(tsJ, tsI, distFn);

    double[] lastCol = new double[tsJ.size()];
    double[] currCol = new double[tsJ.size()];
    final int maxI = tsI.size() - 1;
    final int maxJ = tsJ.size() - 1;

    // Calculate the values for the first column, from the bottom up.
    currCol[0] = distFn.calcDistance(tsI, 0, tsJ, 0); // first cell
    for (int j = 1; j <= maxJ; j++)
      // the rest of the first column
      currCol[j] = currCol[j - 1] + distFn.calcDistance(tsI, 0, tsJ, j);

    for (int i = 1; i <= maxI; i++) // i = columns
    {
      // Swap the references between the two arrays.
      final double[] temp = lastCol;
      lastCol = currCol;
      currCol = temp;

      // Calculate the value for the bottom row of the current column
      // (i,0) = LocalCost(i,0) + GlobalCost(i-1,0)
      currCol[0] = lastCol[0] + distFn.calcDistance(tsI, i, tsJ, 0);


      for (int j = 1; j <= maxJ; j++) // j = rows
      {
        // (i,j) = LocalCost(i,j) + minGlobalCost{(i-1,j),(i-1,j-1),(i,j-1)}
        final double minGlobalCost = Math.min(lastCol[j],
            Math.min(lastCol[j - 1], currCol[j - 1]));
        currCol[j] = minGlobalCost + distFn.calcDistance(tsI, i, tsJ, j);
      } // end for loop
    } // end for loop

    // Minimum Cost is at (maxI,maxJ)
    return currCol[maxJ];
  } // end getWarpDistBetween(..)

  public static WarpPath getWarpPathBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ,
      DistanceFunction distFn) {
    return DynamicTimeWarp(tsI, tsJ, distFn).getPath();
  }

  public static TimeWarpInfo getWarpInfoBetween(GeoTimeSerie tsI,
      GeoTimeSerie tsJ, DistanceFunction distFn) {
    return DynamicTimeWarp(tsI, tsJ, distFn);
  }

  private static TimeWarpInfo DynamicTimeWarp(GeoTimeSerie tsI,
      GeoTimeSerie tsJ, DistanceFunction distFn) {
    // COST MATRIX:
    // 5|_|_|_|_|_|_|E| E = min Global Cost
    // 4|_|_|_|_|_|_|_| S = Start point
    // 3|_|_|_|_|_|_|_| each cell = min global cost to get to that point
    // j 2|_|_|_|_|_|_|_|
    // 1|_|_|_|_|_|_|_|
    // 0|S|_|_|_|_|_|_|
    // 0 1 2 3 4 5 6
    // i
    // access is M(i,j)... column-row

    final double[][] costMatrix = new double[tsI.size()][tsJ.size()];
    final int maxI = tsI.size() - 1;
    final int maxJ = tsJ.size() - 1;

    // Calculate the values for the first column, from the bottom up.
    costMatrix[0][0] = distFn.calcDistance(tsI, 0, tsJ, 0);

    for (int j = 1; j <= maxJ; j++) {
      costMatrix[0][j] = costMatrix[0][j - 1] + distFn.calcDistance(tsI, 0, tsJ, j);
    }
    
    for (int i = 1; i <= maxI; i++) // i = columns
    {
      // Calculate the value for the bottom row of the current column
      // (i,0) = LocalCost(i,0) + GlobalCost(i-1,0)
      costMatrix[i][0] = costMatrix[i - 1][0]
          + distFn.calcDistance(tsI, i, tsJ, 0);

      for (int j = 1; j <= maxJ; j++) // j = rows
      {
        // (i,j) = LocalCost(i,j) + minGlobalCost{(i-1,j),(i-1,j-1),(i,j-1)}
        final double minGlobalCost = Math.min(costMatrix[i - 1][j],
            Math.min(costMatrix[i - 1][j - 1], costMatrix[i][j - 1]));
        costMatrix[i][j] = minGlobalCost
            + distFn.calcDistance(tsI, i, tsJ, j);

      } // end for loop
    } // end for loop

    /*
     * // writes a section of the cost matrix to a file
     * try
     * {
     * final PrintWriter out = new PrintWriter(new FileWriter("matrix1.csv"));
     * for (int j=maxJ; j>=0; j--)
     * {
     * for (int i=0; i<=maxI; i++)
     * {
     * out.print(costMatrix[i][j]);
     * if (i != maxI)
     * out.print(",");
     * }
     * out.println();
     * }
     * out.flush();
     * out.close();
     * }
     * catch (Exception e)
     * {
     * System.out.println(e);
     * e.printStackTrace();
     * }
     */
    // Minimum Cost is at (maxIi,maxJ)
    final double minimumCost = costMatrix[maxI][maxJ];

    // Find the Warp Path by searching the matrix from the solution at
    // (maxI, maxJ) to the beginning at (0,0). At each step move through
    // the matrix 1 step left, down, or diagonal, whichever has the
    // smallest cost. Favor diagonal moves and moves towards the i==j
    // axis to break ties.
    final WarpPath minCostPath = new WarpPath(maxI + maxJ - 1);
    int i = maxI;
    int j = maxJ;
    minCostPath.addFirst(i, j);
    while ((i > 0) || (j > 0)) {
      // Find the costs of moving in all three possible directions (left,
      // down, and diagonal (down and left at the same time).
      final double diagCost;
      final double leftCost;
      final double downCost;

      if ((i > 0) && (j > 0))
        diagCost = costMatrix[i - 1][j - 1];
      else
        diagCost = Double.POSITIVE_INFINITY;

      if (i > 0)
        leftCost = costMatrix[i - 1][j];
      else
        leftCost = Double.POSITIVE_INFINITY;

      if (j > 0)
        downCost = costMatrix[i][j - 1];
      else
        downCost = Double.POSITIVE_INFINITY;

      // Determine which direction to move in. Prefer moving diagonally and
      // moving towards the i==j axis of the matrix if there are ties.
      if ((diagCost <= leftCost) && (diagCost <= downCost)) {
        i--;
        j--;
      } else if ((leftCost < diagCost) && (leftCost < downCost))
        i--;
      else if ((downCost < diagCost) && (downCost < leftCost))
        j--;
      else if (i <= j) // leftCost==rightCost > diagCost
        j--;
      else
        // leftCost==rightCost > diagCost
        i--;

      // Add the current step to the warp path.
      minCostPath.addFirst(i, j);
    } // end while loop

    return new TimeWarpInfo(minimumCost, minCostPath);
  } // end DynamicTimeWarp(..)

  public static double getWarpDistBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ,
      SearchWindow window, DistanceFunction distFn) {
    // COST MATRIX:
    // 5|_|_|_|_|_|_|E| E = min Global Cost
    // 4|_|_|_|_|_|_|_| S = Start point
    // 3|_|_|_|_|_|_|_| each cell = min global cost to get to that point
    // j 2|_|_|_|_|_|_|_|
    // 1|_|_|_|_|_|_|_|
    // 0|S|_|_|_|_|_|_|
    // 0 1 2 3 4 5 6
    // i
    // access is M(i,j)... column-row
    final CostMatrix costMatrix = new PartialWindowMatrix(window);
    final int maxI = tsI.size() - 1;
    final int maxJ = tsJ.size() - 1;

    // Get an iterator that traverses the window cells in the order that the
    // cost matrix is filled.
    // (first to last row (1..maxI), bottom to top (1..MaxJ)
    final Iterator matrixIterator = window.iterator();

    while (matrixIterator.hasNext()) {
      final ColMajorCell currentCell = (ColMajorCell) matrixIterator.next(); // current
      // cell
      // being
      // filled
      final int i = currentCell.getCol();
      final int j = currentCell.getRow();

      if ((i == 0) && (j == 0)) // bottom left cell (first row AND first column)
        costMatrix.put(
            i,
            j,
            distFn.calcDistance(tsI, 0, tsJ, 0));
      else if (i == 0) // first column
      {
        costMatrix.put(
            i,
            j,
            distFn.calcDistance(tsI, 0, tsJ, j)
                + costMatrix.get(i, j - 1));
      } else if (j == 0) // first row
      {
        costMatrix.put(
            i,
            j,
            distFn.calcDistance(tsI, i, tsJ, 0)
                + costMatrix.get(i - 1, j));
      } else // not first column or first row
      {
        final double minGlobalCost = Math.min(costMatrix.get(i - 1, j),
            Math.min(costMatrix.get(i - 1, j - 1), costMatrix.get(i, j - 1)));
        costMatrix.put(
            i,
            j,
            minGlobalCost
            + distFn.calcDistance(tsI, i, tsJ, j));
      } // end if
    } // end while loop

    // Minimum Cost is at (maxI, maxJ)
    return costMatrix.get(maxI, maxJ);

  } // end getWarpDistBetween(...)

  public static WarpPath getWarpPathBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ,
      SearchWindow window, DistanceFunction distFn) {
    return constrainedTimeWarp(tsI, tsJ, window, distFn).getPath();
  }

  public static TimeWarpInfo getWarpInfoBetween(GeoTimeSerie tsI,
      GeoTimeSerie tsJ, SearchWindow window, DistanceFunction distFn) {
    return constrainedTimeWarp(tsI, tsJ, window, distFn);
  }

  private static TimeWarpInfo constrainedTimeWarp(GeoTimeSerie tsI,
      GeoTimeSerie tsJ, SearchWindow window, DistanceFunction distFn) {
    // COST MATRIX:
    // 5|_|_|_|_|_|_|E| E = min Global Cost
    // 4|_|_|_|_|_|_|_| S = Start point
    // 3|_|_|_|_|_|_|_| each cell = min global cost to get to that point
    // j 2|_|_|_|_|_|_|_|
    // 1|_|_|_|_|_|_|_|
    // 0|S|_|_|_|_|_|_|
    // 0 1 2 3 4 5 6
    // i
    // access is M(i,j)... column-row
    final WindowMatrix costMatrix = new WindowMatrix(window);
    final int maxI = tsI.size() - 1;
    final int maxJ = tsJ.size() - 1;

    // Get an iterator that traverses the window cells in the order that the
    // cost matrix is filled.
    // (first to last row (1..maxI), bottom to top (1..MaxJ)
    final Iterator matrixIterator = window.iterator();

    while (matrixIterator.hasNext()) {
      final ColMajorCell currentCell = (ColMajorCell) matrixIterator.next(); // current
      // cell
      // being
      // filled
      final int i = currentCell.getCol();
      final int j = currentCell.getRow();

      if ((i == 0) && (j == 0)) // bottom left cell (first row AND first column)
        costMatrix.put(
            i,
            j,
            distFn.calcDistance(tsI, 0, tsJ, 0));
      else if (i == 0) // first column
      {
        costMatrix.put(
            i,
            j,
            distFn.calcDistance(tsI, 0, tsJ, j)
                + costMatrix.get(i, j - 1));
      } else if (j == 0) // first row
      {
        costMatrix.put(
            i,
            j,
            distFn.calcDistance(tsI, i, tsJ, 0)
                + costMatrix.get(i - 1, j));
      } else // not first column or first row
      {
        final double minGlobalCost = Math.min(costMatrix.get(i - 1, j),
            Math.min(costMatrix.get(i - 1, j - 1), costMatrix.get(i, j - 1)));
        costMatrix.put(
            i,
            j,
            minGlobalCost
            + distFn.calcDistance(tsI, i, tsJ, j));
      } // end if
    } // end while loop

    // Minimum Cost is at (maxI, maxJ)
    final double minimumCost = costMatrix.get(maxI, maxJ);

    /*
     * try
     * {
     * final PrintWriter out = new PrintWriter(new FileWriter("matrix2.csv"));
     * for (int j=maxJ; j>=0; j--)
     * {
     * for (int i=0; i<=maxI; i++)
     * {
     * out.print(costMatrix.get(i, j));
     * if (i != maxI)
     * out.print(",");
     * }
     * out.println();
     * }
     * out.flush();
     * out.close();
     * }
     * catch (Exception e)
     * {
     * System.out.println(e);
     * e.printStackTrace();
     * }
     */
    // Find the Warp Path by searching the matrix from the solution at
    // (maxI, maxJ) to the beginning at (0,0). At each step move through
    // the matrix 1 step left, down, or diagonal, whichever has the
    // smallest cost. Favor diagonal moves and moves towards the i==j
    // axis to break ties.
    final WarpPath minCostPath = new WarpPath(maxI + maxJ - 1);
    int i = maxI;
    int j = maxJ;
    minCostPath.addFirst(i, j);
    while ((i > 0) || (j > 0)) {
      // Find the costs of moving in all three possible directions (left,
      // down, and diagonal (down and left at the same time).
      final double diagCost;
      final double leftCost;
      final double downCost;

      if ((i > 0) && (j > 0))
        diagCost = costMatrix.get(i - 1, j - 1);
      else
        diagCost = Double.POSITIVE_INFINITY;

      if (i > 0)
        leftCost = costMatrix.get(i - 1, j);
      else
        leftCost = Double.POSITIVE_INFINITY;

      if (j > 0)
        downCost = costMatrix.get(i, j - 1);
      else
        downCost = Double.POSITIVE_INFINITY;

      // Determine which direction to move in. Prefer moving diagonally and
      // moving towards the i==j axis of the matrix if there are ties.
      if ((diagCost <= leftCost) && (diagCost <= downCost)) {
        i--;
        j--;
      } else if ((leftCost < diagCost) && (leftCost < downCost))
        i--;
      else if ((downCost < diagCost) && (downCost < leftCost))
        j--;
      else if (i <= j) // leftCost==rightCost > diagCost
        j--;
      else
        // leftCost==rightCost > diagCost
        i--;

      // Add the current step to the warp path.
      minCostPath.addFirst(i, j);
    } // end while loop

    // Free any rescources associated with the costMatrix (a swap file may have
    // been created if the swa file did not
    // fit into main memory).
    costMatrix.freeMem();

    return new TimeWarpInfo(minimumCost, minCostPath);
  } // end ConstrainedTimeWarp

} // end class DtwTest
