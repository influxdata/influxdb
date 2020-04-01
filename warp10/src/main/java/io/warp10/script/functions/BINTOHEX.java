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
 * Decode a binary representation as a String and convert it to an hexadecimal representation as a String.
 */
public class BINTOHEX extends BINTOx {

  private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

  public BINTOHEX(String name) {
    super(name);
  }

  @Override
  public Object initData(int numberOfBits) {
    // 2 characters for each byte, so 1 character for 4 bits.
    // Structure is an array of chars which will be converted to a String at the end.
    return new char[numberOfBits / 4];
  }

  @Override
  public void updateData(Object data, int byteIndex, byte currentByte) {
    // Bit-mask the first 4 bits and the last 4 bits and find the corresponding character in hexArray.
    ((char[]) data)[byteIndex * 2] = hexArray[(currentByte & 0xF0) >>> 4]; // first 4 bits, & 0xF0 is necessary
    ((char[]) data)[byteIndex * 2 + 1] = hexArray[currentByte & 0x0F]; // last 4 bits
  }

  @Override
  public Object generateResult(Object data) {
    // Convert the char array to a String.
    return new String((char[]) data);
  }

}
