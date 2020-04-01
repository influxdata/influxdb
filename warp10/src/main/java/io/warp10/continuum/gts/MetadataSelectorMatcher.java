//
//   Copyright 2020  SenX S.A.S.
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


import io.warp10.WarpURLDecoder;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.WarpScriptException;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MetadataSelectorMatcher compile all the regexp contained in a standard or extended selector once for performance.
 * standard selector: class{labelOrAttribute=xxx}
 * if there is no label with this name, look at the attributes.
 * extended selector: class{label=xxx}{attribute=yyy}
 * check separately labels and attributes, both must match.
 */
public class MetadataSelectorMatcher {

  private final Matcher classnamePattern;
  private final Map<String, Matcher> labelsPatterns;
  private final Map<String, Matcher> attributesPatterns;
  private final List<String> missingLabels = new ArrayList<String>();
  private final List<String> missingAttributes = new ArrayList<String>();

  private static final Pattern EXPR_RE = Pattern.compile("^(?<classname>[^{]+)\\{(?<labels>[^}]*)\\}(\\{(?<attributes>[^}]*)\\})?$");

  public MetadataSelectorMatcher(String selector) throws WarpScriptException {

    Matcher m = EXPR_RE.matcher(selector);
    if (!m.matches()) {
      throw new WarpScriptException("invalid syntax for selector.");
    }

    // read class selector
    String classSelector = null;
    try {
      classSelector = WarpURLDecoder.decode(m.group("classname"), StandardCharsets.UTF_8);
    } catch (UnsupportedEncodingException uee) {
      // Can't happen, we're using UTF-8
    }

    // build class selector pattern
    if ("~.*".equals(classSelector)) {
      this.classnamePattern = null;
    } else if ("=".equals(classSelector) || "~".equals(classSelector)) {
      this.classnamePattern = Pattern.compile(Pattern.quote("")).matcher("");
    } else if (classSelector.startsWith("~")) {
      this.classnamePattern = Pattern.compile(classSelector.substring(1)).matcher("");
    } else if (classSelector.startsWith("=")) {
      this.classnamePattern = Pattern.compile(Pattern.quote(classSelector.substring(1))).matcher("");
    } else {
      this.classnamePattern = Pattern.compile(Pattern.quote(classSelector)).matcher("");
    }

    // read label selector
    String labelsSelection = m.group("labels");
    Map<String, String> labelsSelectors = null;
    try {
      labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
    } catch (ParseException pe) {
      throw new WarpScriptException(pe);
    }

    if (0 == labelsSelectors.size()) {
      this.labelsPatterns = null;
    } else {
      this.labelsPatterns = new HashMap<String, Matcher>(labelsSelectors.size());
      // build label patterns map
      for (Entry<String, String> l: labelsSelectors.entrySet()) {
        
        if (Constants.ABSENT_LABEL_SUPPORT) {
          if ("".equals(l.getValue()) || "=".equals(l.getValue())) {
            this.missingLabels.add(l.getKey());
            continue;
          }
        }
        
        if (l.getValue().startsWith("=")) {
          this.labelsPatterns.put(l.getKey(), Pattern.compile(Pattern.quote(l.getValue().substring(1))).matcher(""));
        } else { //starts with ~ , otherwise Parse Exception in GTSHelper.parseLabelsSelectors
          this.labelsPatterns.put(l.getKey(), Pattern.compile(l.getValue().substring(1)).matcher(""));
        }
      }
    }

    // read attribute selector, if any
    Map<String, String> attributesSelectors = null;
    String attributesSelection = m.group("attributes");
    if (attributesSelection != null) {
      try {
        attributesSelectors = GTSHelper.parseLabelsSelectors(attributesSelection);
      } catch (ParseException pe) {
        throw new WarpScriptException(pe);
      }
      this.attributesPatterns = new HashMap<String, Matcher>(attributesSelectors.size());
      // build label patterns map
      for (Entry<String, String> a: attributesSelectors.entrySet()) {
        if (Constants.ABSENT_LABEL_SUPPORT) {
          if ("".equals(a.getValue()) || "=".equals(a.getValue())) {
            this.missingAttributes.add(a.getKey());
            continue;
          }
        }

        if (a.getValue().startsWith("=")) {
          this.attributesPatterns.put(a.getKey(), Pattern.compile(Pattern.quote(a.getValue().substring(1))).matcher(""));
        } else { //starts with ~ , otherwise Parse Exception in GTSHelper.parseLabelsSelectors
          this.attributesPatterns.put(a.getKey(), Pattern.compile(a.getValue().substring(1)).matcher(""));
        }
      }
    } else {
      this.attributesPatterns = null;
    }
  }

  public boolean matches(Metadata metadata) {

    // if metadata is not set, or classname is not set, do not match
    if (null == metadata || null == metadata.getName()) {
      return false;
    }

    // first, check classname
    boolean classnameMatch = (null == this.classnamePattern) || (this.classnamePattern.reset(metadata.getName()).matches());

    // do not check further if classname do not match
    if (!classnameMatch) {
      return false;
    }

    // Check labels.
    // ###### Standard selector: `classname{labelOrAttribute=x}`
    //  - If classname match, `filter.byselector` looks into input labels to check if labelOrAttribute exists and equals x.
    //  If labelOrAttribute is not found among input labels, it looks into input attributes if the label exists and equals x.
    // ###### Extended selector: `classname{labelname=x}{attributename=y}` matches if:
    //  - classname matches
    //  - input have labelname in its labels, and label value matches
    //  - input have attributename in its labels, and attribute value matches
    //
    // ###### Selectors example:
    //  - `~.*{}` matches everything.
    //  - `={}` matches only emtpy classnames, whatever the labels and attributes.
    //  - `~.*{label=value}{} filter.byselector` is equivalent to `{ 'label' 'value' } filter.bylabels`.
    //  - `~.*{}{attribute~value} filter.byselector` is equivalent to `{ 'attribute' '~value' } filter.byattr`.

    Map<String, String> inputLabels = metadata.getLabels();
    Map<String, String> inputAttributes = metadata.getAttributes();
    boolean labelAndAttributeMatch = true;


    if (null != this.attributesPatterns) {
      // extended selector
      if (null != this.labelsPatterns && null != inputLabels) {
        for (String label: this.missingLabels) {
          if (null != inputLabels.get(label)) {
            labelAndAttributeMatch = false;
            break;
          }
        }
        for (Entry<String, Matcher> ls: this.labelsPatterns.entrySet()) {
          if (!labelAndAttributeMatch) {
            break;
          }
          String inputLabel = inputLabels.get(ls.getKey());
          labelAndAttributeMatch = (null != inputLabel) && ls.getValue().reset(inputLabel).matches();
          if (!labelAndAttributeMatch) {
            break;
          }
        }
      }
      if (null != inputAttributes) {
        for (String attr: this.missingAttributes) {
          if (null != inputAttributes.get(attr)) {
            labelAndAttributeMatch = false;
            break;
          }
        }
        for (Entry<String, Matcher> ls: this.attributesPatterns.entrySet()) {
          if (!labelAndAttributeMatch) {
            break;
          }
          String inputAttribute = inputAttributes.get(ls.getKey());
          labelAndAttributeMatch = (null != inputAttribute) && ls.getValue().reset(inputAttribute).matches();
        }
      }
    } else {
      // standard selector
      if (null != this.labelsPatterns) {
        for (String label: this.missingLabels) {
          if ((null != inputLabels && null != inputLabels.get(label))
              || (null != inputAttributes && null != inputAttributes.get(label))) {
            labelAndAttributeMatch = false;
            break;
          }
        }
        for (Entry<String, Matcher> ls: this.labelsPatterns.entrySet()) {
          if (!labelAndAttributeMatch) {
            break;
          }
          if (null != inputLabels) {
            String inputLabel = inputLabels.get(ls.getKey());
            if (null != inputLabel) {
              // label exists in the input, try to match.
              labelAndAttributeMatch = ls.getValue().reset(inputLabel).matches();
            } else if (null != inputAttributes) {
              // label does not exist, look for attribute existence and match.
              String inputAttribute = inputAttributes.get(ls.getKey());
              labelAndAttributeMatch = (null != inputAttribute) && ls.getValue().reset(inputAttribute).matches();
            } else {
              // no label with this name, and no attributes at all
              labelAndAttributeMatch = false;
            }
          }
        }
      }
    }

    return labelAndAttributeMatch;
  }
}
