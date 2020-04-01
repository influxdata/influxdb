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

package io.warp10.quasar.trl;

import java.util.Arrays;

public class ChunkedArray {

  private int chunkSize = 5000000;
  private  float loadFactor = 0.75f;
  private int arrayIndex = 0;
  private long[] array = null;

  public ChunkedArray(int chunkSize, float loadFactor) {
    this.chunkSize = chunkSize;
    this.loadFactor = loadFactor;
    array = new long[chunkSize];
  }

  public ChunkedArray(int intitialAllocation, int chunkSize, float loadFactor) {
    this.chunkSize = chunkSize;
    this.loadFactor = loadFactor;
    array = new long[getChunkedSize(intitialAllocation)];
  }

  public void addLong(long data) {
    // check if the current array must be extended or not
    int sizeFuture = arrayIndex + 1;

    array = getArray(sizeFuture);

    array[arrayIndex]=data;

    arrayIndex++;
  }

  synchronized  private long[] getArray(int needs) {
    int newSize = getChunkedSize(needs);

    // allocate a new
    if (newSize > array.length){
      return Arrays.copyOfRange(array, 0, newSize);
    }
    return array;
  }

  public int getChunkedSize(int needs) {
    // Compute the number of the full chunks needed
    double chunksRatio = ((double)needs) / ((double)chunkSize);
    int chunkNumber = (int) Math.ceil(chunksRatio);

    // Compute load
    double load = ((double)(needs)) / ((double)(chunkNumber * chunkSize));

    // needs a new chunk even if memory is available
    if (load > loadFactor) {
      chunkNumber++;
    }

    return chunkNumber * chunkSize;
  }

  public boolean contains(long data) {
    int result = Arrays.binarySearch(array, 0, arrayIndex, data);

    if (result >= 0) {
      return true;
    }
    return false;
  }

  public void sort() {
    // sort the new array
    Arrays.sort(array, 0, arrayIndex);
  }

  public int size() {
    return arrayIndex;
  }
}
