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
 * TypeConversions.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

public class TypeConversions
{


   public static byte[] doubleToByteArray(double number)
   {
      // double to long representation
      long longNum = Double.doubleToLongBits(number);

      // long to 8 bytes
      return new byte[] {(byte)((longNum >>> 56) & 0xFF),
                         (byte)((longNum >>> 48) & 0xFF),
                         (byte)((longNum >>> 40) & 0xFF),
                         (byte)((longNum >>> 32) & 0xFF),
                         (byte)((longNum >>> 24) & 0xFF),
                         (byte)((longNum >>> 16) & 0xFF),
                         (byte)((longNum >>>  8) & 0xFF),
                         (byte)((longNum >>>  0) & 0xFF)};
   }  // end doubleToByte(.)



   public static byte[] doubleArrayToByteArray(double[] numbers)
   {
      final int doubleSize = 8;  // 8 byes in a double
      final byte[] byteArray = new byte[numbers.length*doubleSize];

      for (int x=0; x<numbers.length; x++)
         System.arraycopy(doubleToByteArray(numbers[x]), 0, byteArray, x*doubleSize, doubleSize);

      return byteArray;
   }  // end doubleArrayToByteArray(.)


}  // end class Typeconversions
