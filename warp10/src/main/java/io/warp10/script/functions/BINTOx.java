//
//   Copyright 2019  SenX S.A.S.
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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Parent class to decode a binary representation as a String into a sequence of bytes.
 * What to do with each byte is left to the subclass.
 * For instance: 0000100001100001 is split into 00001000 01100001 which will generate the byte sequence 8, 97.
 * If the representation is not a multiple of 8, the representation is left-padded with zeros.
 * For instance: 10011110000 will be interpreted as 0000010011110000, generating 4, 240
 */
public abstract class BINTOx extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  public BINTOx(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    // Check and get String on stack
    Object o = stack.pop();

    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a String.");
    }

    String bin = (String) o;

    // Compute the number of missing bits to have a string length multiple of 8.
    // Faster than padding with zeros but equivalent when shifting the index in the String by this value (i+missingBitsLength).
    int missingBitsLength = (8 - bin.length() % 8) % 8;

    // Initialize the data structure which will be updated for each decoded byte.
    Object data = initData(missingBitsLength + bin.length());

    int curByte = 0; // current decoded byte
    char bit; // current decoded bit
    // For all the characters in the given string
    for (int i = 0; i < bin.length(); i++) {
      // We're reading the String left to right so shift all bits in the current decoded byte to the left
      curByte = curByte << 1;

      // Get the current bit
      bit = bin.charAt(i);

      if ('1' == bit) {
        // Add the bit to the current byte only if it is a 1. The bit shift operation already put a 0, so there is no
        // need to set the last bit to 0 if '0' == bit
        curByte = curByte | 1;
      } else if ('0' != bit) {
        // Make sure there is only 1s and 0s in the string.
        throw new WarpScriptException(getName() + " expects a valid binary representation. \"" + bin + "\" is invalid.");
      }

      // When at the end of a byte, update the data structure and reset the current byte value
      // We're using the shifted index by missingBitsLength to simulate left-padding with zeros.
      // As a result, we're sure that at the last decoded bit, the last decoded byte will be use for updating the data.
      if (7 == ((i + missingBitsLength) % 8)) {
        updateData(data, (i + missingBitsLength) / 8, (byte) curByte);
        curByte = 0;
      }
    }

    // Generate the final result using the updated data and push it to the stack.
    stack.push(generateResult(data));

    return stack;
  }

  /**
   * Initialize the data structure which will be updated for each decoded byte.
   * It can be any Object as the responsibility of updating this Object also falls on the subclass.
   * @param numberOfBits The number of bits in the binary representation which is always a multiple of 8.
   * @return A data structure.
   */
  public abstract Object initData(int numberOfBits);

  /**
   * Update the data structure for a given byte at given byte index.
   * @param data The data structure to be updated.
   * @param byteIndex The index of the byte to update.
   * @param currentByte The value of the decoded byte.
   */
  public abstract void updateData(Object data, int byteIndex, byte currentByte);

  /**
   * Generate the final result given data structure, updated for each byte.
   * @param data The initialized and updated data structure.
   * @return Any Object to be put on the stack as a result of a call to this function.
   */
  public abstract Object generateResult(Object data);
}
