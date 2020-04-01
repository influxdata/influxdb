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
 * ColMajorCell.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

public class ColMajorCell {
  private final int col;
  private final int row;

  public int getCol() {
    return col;
  }

  public int getRow() {
    return row;
  }

  public ColMajorCell(int column, int row) {
    this.col = column;
    this.row = row;
  } // end Constructor

  public boolean equals(Object o) {
    return (o instanceof ColMajorCell) && (((ColMajorCell) o).col == this.col)
        && (((ColMajorCell) o).row == this.row);
  }

  public int hashCode() {
    return (1 << col) + row;
  }

  public String toString() {
    return "(" + col + "," + row + ")";
  }

} // end class ColMajorCell
