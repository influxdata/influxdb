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
 * WarpPath.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class WarpPath {
  // DATA
  private final ArrayList tsIindexes; // ArrayList of Integer
  private final ArrayList tsJindexes; // ArrayList of Integer

  // CONSTRUCTORS
  public WarpPath() {
    tsIindexes = new ArrayList();
    tsJindexes = new ArrayList();
  }

  public WarpPath(int initialCapacity) {
    this();
    tsIindexes.ensureCapacity(initialCapacity);
    tsJindexes.ensureCapacity(initialCapacity);

  }

  public WarpPath(String inputFile) {
    this();

    try {
      // Record the Label names (from the top row.of the input file).
      final BufferedReader br = new BufferedReader(new FileReader(inputFile)); // open
                                                                               // the
                                                                               // input
                                                                               // file

      // Read Cluster assignments.
      String line;
      while ((line = br.readLine()) != null) // read lines until end of file
      {
        final StringTokenizer st = new StringTokenizer(line, ",", false);
        if (st.countTokens() == 2) {
          tsIindexes.add(new Integer(st.nextToken()));
          tsJindexes.add(new Integer(st.nextToken()));
        } else
          throw new RuntimeException("The Warp Path File '" + inputFile
              + "' has an incorrect format.  There must be\n"
              + "two numbers per line separated by commas");
      } // end while loop
    } catch (FileNotFoundException e) {
      throw new RuntimeException("ERROR:  The file '" + inputFile
          + "' was not found.");
    } catch (IOException e) {
      throw new RuntimeException("ERROR:  Problem reading the file '"
          + inputFile + "'.");
    } // end try block

  }

  // FUNCTIONS
  public int size() {
    return tsIindexes.size();
  }

  public int minI() {
    return ((Integer) tsIindexes.get(0)).intValue();
  }

  public int minJ() {
    return ((Integer) tsJindexes.get(0)).intValue();
  }

  public int maxI() {
    return ((Integer) tsIindexes.get(tsIindexes.size() - 1)).intValue();
  }

  public int maxJ() {
    return ((Integer) tsJindexes.get(tsJindexes.size() - 1)).intValue();
  }

  public void addFirst(int i, int j) {
    tsIindexes.add(0, new Integer(i));
    tsJindexes.add(0, new Integer(j));
  }

  public void addLast(int i, int j) {
    tsIindexes.add(new Integer(i));
    tsJindexes.add(new Integer(j));
  }

  public ArrayList getMatchingIndexesForI(int i) {
    int index = tsIindexes.indexOf(new Integer(i));
    if (index < 0)
      throw new RuntimeException("ERROR:  index '" + i + " is not in the "
          + "warp path.");
    final ArrayList matchingJs = new ArrayList();
    while (index < tsIindexes.size()
        && tsIindexes.get(index).equals(new Integer(i)))
      matchingJs.add(tsJindexes.get(index++));

    return matchingJs;
  } // end getMatchingIndexesForI(int i)

  public ArrayList getMatchingIndexesForJ(int j) {
    int index = tsJindexes.indexOf(new Integer(j));
    if (index < 0)
      throw new RuntimeException("ERROR:  index '" + j + " is not in the "
          + "warp path.");
    final ArrayList matchingIs = new ArrayList();
    while (index < tsJindexes.size()
        && tsJindexes.get(index).equals(new Integer(j)))
      matchingIs.add(tsIindexes.get(index++));

    return matchingIs;
  } // end getMatchingIndexesForI(int i)

  // Create a new WarpPath that is the same as THIS WarpPath, but J is the
  // reference template, rather than I.
  public WarpPath invertedCopy() {
    final WarpPath newWarpPath = new WarpPath();
    for (int x = 0; x < tsIindexes.size(); x++)
      newWarpPath.addLast(((Integer) tsJindexes.get(x)).intValue(),
          ((Integer) tsIindexes.get(x)).intValue());

    return newWarpPath;
  }

  // Swap I and J so that the warp path now indicates that J is the template
  // rather than I.
  public void invert() {
    for (int x = 0; x < tsIindexes.size(); x++) {
      final Object temp = tsIindexes.get(x);
      tsIindexes.set(x, tsJindexes.get(x));
      tsJindexes.set(x, temp);
    }
  } // end invert()

  public ColMajorCell get(int index) {
    if ((index > this.size()) || (index < 0))
      throw new NoSuchElementException();
    else
      return new ColMajorCell(((Integer) tsIindexes.get(index)).intValue(),
          ((Integer) tsJindexes.get(index)).intValue());
  }

  public String toString() {
    StringBuffer outStr = new StringBuffer("[");
    for (int x = 0; x < tsIindexes.size(); x++) {
      outStr.append("(" + tsIindexes.get(x) + "," + tsJindexes.get(x) + ")");
      if (x < tsIindexes.size() - 1)
        outStr.append(",");
    } // end for loop

    return new String(outStr.append("]"));
  } // end toString()

  public boolean equals(Object obj) {
    if ((obj instanceof WarpPath)) // trivial false test
    {
      final WarpPath p = (WarpPath) obj;
      if ((p.size() == this.size()) && (p.maxI() == this.maxI())
          && (p.maxJ() == this.maxJ())) // less trivial reject
      {
        // Compare each value in the warp path for equality
        for (int x = 0; x < this.size(); x++)
          if (!(this.get(x).equals(p.get(x))))
            return false;

        return true;
      } else
        return false;
    } else
      return false;
  } // end equals

  public int hashCode() {
    return tsIindexes.hashCode() * tsJindexes.hashCode();
  }

} // end class WarpPath
