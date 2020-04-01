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

package io.warp10.script.functions;

/**
 * Decode a binary representation as a String to a byte array.
 */
public class BINTO extends BINTOx {

  public BINTO(String name) {
    super(name);
  }

  @Override
  public Object initData(int numberOfBits) {
    // 1 byte for 8 bits.
    // Structure is an array of byte which will be returned at the end, without any transformation.
    return new byte[numberOfBits / 8];
  }

  @Override
  public void updateData(Object data, int byteIndex, byte currentByte) {
    // Put the decoded byte in the byte array, at the given byte index.
    ((byte[]) data)[byteIndex] = currentByte;
  }

  @Override
  public Object generateResult(Object data) {
    // The result is simply the data structure.
    return data;
  }

}
