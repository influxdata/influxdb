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

package io.warp10.continuum.gts;

import java.lang.reflect.Field;
import java.nio.CharBuffer;
import java.util.ArrayList;

import java.util.Arrays;
import sun.misc.Unsafe;

/**
 * The following class is inspired by:
 * 
 * @see https://raw.githubusercontent.com/nitsanw/javanetperf/psylobsaw/src/psy/lob/saw/UnsafeString.java
 * @see http://psy-lob-saw.blogspot.fr/2012/12/encode-utf-8-string-to-bytebuffer-faster.html
 * 
 */

public class UnsafeString {
  private static final Unsafe unsafe;
  private static final long valueOffset;
  private static final long offsetOffset;
  private static final long countOffset;
  private static final long hashOffset;
  
  static {
    try {
      // This is a bit of voodoo to force the unsafe object into
      // visibility and acquire it.
      // This is not playing nice, but as an established back door it is
      // not likely to be
      // taken away.
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
      valueOffset = unsafe.objectFieldOffset(String.class.getDeclaredField("value"));
      Field declaredField;
      try {
        declaredField = String.class.getDeclaredField("count");
      } catch (NoSuchFieldException e) {
        // this will happen for jdk7 as these fields have been removed
        declaredField = null;
      }
      
      if (null != declaredField) {
        countOffset = unsafe.objectFieldOffset(declaredField);
      } else {
        countOffset = -1L;
      }
      
      declaredField = null;

      try {
        declaredField = String.class.getDeclaredField("offset");
      } catch (NoSuchFieldException e) {
        // this will happen for jdk7 as these fields have been removed
        declaredField = null;
      }
      
      if (null != declaredField) {
        offsetOffset = unsafe.objectFieldOffset(declaredField);
      } else {
        offsetOffset = -1L;
      }
      
      declaredField = null;

      try {
        declaredField = String.class.getDeclaredField("hash");
      } catch (NoSuchFieldException e) {
        // this will happen for jdk7 as these fields have been removed
        declaredField = null;
      }
      
      if (null != declaredField) {
        hashOffset = unsafe.objectFieldOffset(declaredField);
      } else {
        hashOffset = -1L;
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public final static char[] getChars(String s) {
    return (char[]) unsafe.getObject(s, valueOffset);
  }
  
  public final static char charAt(String s, int idx) {
    char[] chars = (char[]) unsafe.getObject(s, valueOffset);
    return chars[idx];
  }

  public final static int getOffset(String s) {
    if (-1L == offsetOffset) {
      return 0;
    } else {
      return unsafe.getInt(s, offsetOffset);
    }
  }
  
  public final static String[] split(String s, char ch) {
        
    //ArrayList<String> tokens = new ArrayList<String>();
    
    int off = 0;
    int next = 0;
    
    //
    // Count the number of separators
    //
    
    int n = 1;
    
    while((next = indexOf(s,ch,off)) != -1) {
      off = next + 1;
      n++;
    }
    
    next = 0;
    off = 0;

    String[] tokens = new String[n];
    int idx = 0;
    
    while ((next = indexOf(s, ch, off)) != -1) {
      //tokens.add(substring(s, off, next));
      //tokens.add(s.substring(off,next));
      tokens[idx++] = s.substring(off,next);
      off = next + 1;
    }
    
    // If no match was found, return this
    if (off == 0) {
      return new String[]{s};
    }
    
    //tokens.add(substring(s, off, s.length()));
    //tokens.add(s.substring(off));
    tokens[idx] = s.substring(off);

    //return tokens.toArray(new String[0]);
    return tokens;
  }

  public final static String substring(String s, int start, int end) {
    return buildUnsafe(getChars(s), start, (end - start));
  }
  
  private final static String buildUnsafe(char[] chars, int offset, int count) {    
    if (-1 != countOffset) {
      // This is JDK7+
      String mutable = new String();// an empty string to hack
      unsafe.putObject(mutable, valueOffset, chars);
      unsafe.putInt(mutable, countOffset, count);
      unsafe.putIntVolatile(mutable, offsetOffset, offset);
      return mutable;
    } else {
      return new String(chars, offset, count);
    }
  }
  
  public static boolean isLong(String s) {
    char[] c = getChars(s);

    //
    // Skip leading whitespaces
    //
    
    int i = 0;
    
    while(i < c.length && ' ' == c[i]) {
      i++;
    }
    
    if (i == c.length) {
      return false;
    }
    
    //
    // Check sign
    //
    
    if ('+' != c[i] && '-' != c[i] && (c[i] < '0' || c[i] > '9')) {
      return false;
    } else if ('-' == c[i] || '+' == c[i]) {
      i++;
    }
    
    boolean hasDigits = false;
    
    while(i < c.length) {
      if (c[i] < '0' || c[i] > '9') {
        return false;
      }
      hasDigits = true;
      i++;
    }

    if (!hasDigits) {
      return false;
    }
  
    return true;
  }
  
  public static boolean isDouble_LIGHT(String s) {
    char[] c = getChars(s);

    //
    // Skip leading whitespaces
    //
    
    int i = 0;
    
    while(i < c.length && ' ' == c[i]) {
      i++;
    }
    
    if (i == c.length) {
      return false;
    }

    //
    // Check sign
    //

    if ('-' == c[i] || '+' == c[i]) {
      i++;
      if (i >= c.length) {
        return false;
      }
    }

    //
    // Handle NaN
    //
    
    if (3 == c.length - i) {
      if ('N' == c[i] && 'a' == c[i+1] && 'N' == c[i+2]) {
        return true;
      }
    }

    //
    // Handle Infinity
    //
    
    if (8 == c.length - i) {
      if ('I' == c[i] && 'n' == c[i+1] && 'f' == c[i+2] && 'i' == c[i+3] && 'n' == c[i+4] && 'i' == c[i+5] && 't' == c[i+6] && 'y' == c[i+7]) {
        return true;
      }
    }

    //
    // Check if there is a '.'
    //
    
    while (i < c.length) {
      if('.' == c[i]) {
        return true;
      }
      i++;
    }
    
    return false;
  }
  
  public static boolean isDouble(String s) {
    char[] c = getChars(s);

    //
    // Skip leading whitespaces
    //
    
    int i = 0;
    
    while(i < c.length && ' ' == c[i]) {
      i++;
    }
    
    if (i == c.length) {
      return false;
    }
    
    //
    // Check sign
    //

    if ('-' == c[i] || '+' == c[i]) {
      i++;
      if (i >= c.length) {
        return false;
      }
    }
        
    //
    // Handle NaN
    //
    
    if (3 == c.length - i) {
      if ('N' == c[i] && 'a' == c[i+1] && 'N' == c[i+2]) {
        return true;
      }
    }

    //
    // Handle Infinity
    //
    
    if (8 == c.length - i) {
      if ('I' == c[i] && 'n' == c[i+1] && 'f' == c[i+2] && 'i' == c[i+3] && 'n' == c[i+4] && 'i' == c[i+5] && 't' == c[i+6] && 'y' == c[i+7]) {
        return true;
      }
    }
    
    //
    // If the next char is not a digit, exit
    //
    
    if (c[i] < '0' || c[i] > '9') {
      return false;
    }

    int ne = 0;
    int nsign = 0;
    int ndot = 0;
    
    while(i < c.length) {
      if (c[i] >= '0' && c[i] <= '9') {
        i++;
        continue;
      }
      
      if ('e' == c[i] || 'E' == c[i]) {
        if (0 != nsign) {
          // 'e' MUST appear before the sign
          return false;
        }
        ne++;
        i++;
        continue;
      }
      
      if ('-' == c[i] || '+' == c[i]) {
        if (0 == ne) {
          // We MUST have encountered an 'e' prior to a sign
          return false;
        }
        nsign++;
        i++;
        continue;
      }
      
      if ('.' == c[i]) {
        // The dot cannot appear after 'e' or the exponent sign
        if (0 != ne || 0 != nsign) {
          return false;
        }
        ndot++;
        i++;
        continue;
      }      
      return false;
    }
    
    //
    // If we encountered more than one 'e', sign or dot, return false
    //
    
    if (ne > 1 || nsign > 1 || ndot > 1) {
      return false;
    }
    
    //
    // If we encountered neither dot nor 'e', return false
    //
    
    if (0 == ne && 0 == ndot) {
      return false;
    }
    
    return true;    
  }
  
  /**
   * Checks if the given string could be a decimal double, i.e. only contains '.', '-', '+', '0'-'9'.
   * It does not check if the string is actually a valid double or not, just what kind of double it
   * would be.
   * @param s
   * @return
   */
  public static boolean mayBeDecimalDouble(String s) {
    char[] c = getChars(s);
    
    for (int i = 0; i < c.length; i++) {
      if ((c[i] < '0' || c[i] > '9') && '.' != c[i] && '+' != c[i] && '-' != c[i]) {
        return false;
      }
    }
    
    return true;
  }
  
  public static int indexOf(String str, char ch) {
    return indexOf(str, ch, 0);
  }
  
  public static int indexOf(String str, char ch, int idx) {
    char[] c = getChars(str);
    
    for (int i = idx; i < c.length; i++) {
      if (ch == c[i]) {
        return i;
      }
    }
    
    return -1;
  }
  
  /**
   * Replace whitespaces in Strings enclosed in single quotes
   * with '%20'
   * 
   * @param str
   * @return
   */
  public static String sanitizeStrings(String str) {
    char[] c = getChars(str);
    
    int idx = 0;
    
    String newstr = str;
    StringBuilder sb = null;
    boolean instring = false;
    char stringsep = '\0';
    
    int lastidx = 0;
    
    while (idx < c.length) {
      
      if (instring && stringsep == c[idx]) {
        instring = false;
        stringsep = '\0';
      } else if (!instring) {
        if ('\'' == c[idx]) {
          instring = true; 
          stringsep = '\'';
        } else if ('\"' == c[idx]) {
          instring = true;
          stringsep = '\"';
        }
      }
            
      if (instring) {
        if (' ' == c[idx]) {
          if (null == sb) {
            // This is the first space in a string we encounter, allocate a StringBuilder
            // and copy the prefix into it
            sb = new StringBuilder();
          }
          // We encountered a whitespace, copy everything since lastidx
          sb.append(c, lastidx, idx - lastidx);
          sb.append("%20");
          lastidx = idx + 1;
        }
      }
      
      idx++;
    }
    
    if (null != sb) {
      if (lastidx < c.length) {
        sb.append(c, lastidx, c.length - lastidx);
      }
      newstr = sb.toString();
    }
    
    return newstr;
  }
  
  /**
   * Reset the hash by setting it to 0 so hashcode computation is performed
   * again when the internal char array has changed.
   * 
   * @param s
   */
  public static void resetHash(String s) {
    unsafe.getAndSetInt(s, hashOffset,  0);
  }
}
