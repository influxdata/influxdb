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

package io.warp10.script;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.warp10.WarpURLDecoder;

public class WebAccessController {

  private final List<Pattern> patterns = new ArrayList<Pattern>();
  private final BitSet exclusion = new BitSet();

  public WebAccessController(String patternConf) {
    //
    // Extract list of forbidden/allowed patterns
    //

    if (null != patternConf) {
      //
      // Split patterns on ','
      //

      String[] subpatterns = patternConf.split(",");

      int idx = 0;

      for (String pattern: subpatterns) {
        try {
          pattern = WarpURLDecoder.decode(pattern, StandardCharsets.UTF_8);
        } catch (UnsupportedEncodingException uee) {
          throw new RuntimeException(uee);
        }

        boolean exclude = false;

        if (pattern.startsWith("!")) {
          exclude = true;
          pattern = pattern.substring(1);
        }

        //
        // Compile pattern
        //

        Pattern p = Pattern.compile(pattern);

        patterns.add(p);
        exclusion.set(idx, exclude);
        idx++;
      }

      //
      // If no inclusions were specified, add a pass all .* as first pattern
      //

      if (exclusion.cardinality() == idx) {
        int n = exclusion.length();

        for (int i = n; i >= 1; i--) {
          exclusion.set(i, exclusion.get(i - 1));
        }

        exclusion.set(0, false);
        patterns.add(0, Pattern.compile(".*"));
      }

    } else {
      //
      // Permit all hosts by default
      //
      patterns.add(Pattern.compile(".*"));
      exclusion.set(0, false);
    }
  }

  public boolean checkURL(URL url) {

    String protocol = url.getProtocol();

    //
    // Only honor http/https
    //

    if (!("http".equals(protocol)) && !("https".equals(protocol))) {
      return false;
    }

    //
    // Check host patterns in order, consider the final value of 'accept'
    //

    String host = url.getHost();

    boolean accept = false;

    for (int i = 0; i < patterns.size(); i++) {
      Matcher m = patterns.get(i).matcher(host);

      if (m.matches()) {
        accept = !exclusion.get(i);
      }
    }

    return accept;
  }
}
