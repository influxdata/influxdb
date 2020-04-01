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

package io.warp10.script.formatted;

import com.google.common.collect.Lists;
import io.warp10.script.WarpScriptException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.warp10.script.WarpScriptStack.MACRO_END;
import static io.warp10.script.WarpScriptStack.MACRO_START;
import io.warp10.script.functions.SNAPSHOT;

public class DocumentationGenerator {

  //
  // Automatically generate doc of a formatted WarpScript function.
  //

  public static Map<String, Object> generateInfo(FormattedWarpScriptFunction function, List<ArgumentSpecification> outputs) {
    return generateInfo(function,"","","","", new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), outputs);
  }

  public static Map<String, Object> generateInfo(FormattedWarpScriptFunction function,
                                                 String since, String deprecated, String deleted, String version,
                                                 List<String> tags, List<String> related, List<String> examples, List<String> conf,
                                                 List<ArgumentSpecification> outputs) {

    LinkedHashMap<String, Object> info = new LinkedHashMap<>();

    info.put("name", function.getName());
    info.put("since", since);
    info.put("deprecated", deprecated);
    info.put("deleted", deleted);
    info.put("version", version);
    info.put("tags", tags);
    info.put("desc", function.getDocstring().toString());

    List<ArgumentSpecification> args = function.getArguments().getArgsCopy();
    List<ArgumentSpecification> optArgs = function.getArguments().getOptArgsCopy();

    //
    // Signature generation
    //

    List<List<List<Object>>> sig = new ArrayList<>();
    List<Object> output = new ArrayList<>();
    for (ArgumentSpecification arg: Lists.reverse(outputs)) {
      output.add(arg.getName() + ":" + arg.WarpScriptType());
    }

    //
    // Sig without opt args on the stack
    //

    List<List<Object>> sig1 = new ArrayList<>();
    List<Object> input1 = new ArrayList<>();

    if (0 == args.size() && 0 != optArgs.size()) {
      input1.add(new LinkedHashMap<>());
    }

    for (ArgumentSpecification arg: Lists.reverse(args)) {
      input1.add(arg.getName() + ":" + arg.WarpScriptType());
    }

    sig1.add(input1);
    sig1.add(output);
    sig.add(sig1);

    //
    // Sig with opt args on the stack (in a map)
    //

    List<List<Object>> sig2 = new ArrayList<>();
    List<Object> input2 = new ArrayList<>();
    LinkedHashMap<String, String> optMap = new LinkedHashMap<>();

    for (ArgumentSpecification arg: Lists.reverse(optArgs)) {
      optMap.put(arg.getName(), arg.getName() + ":" + arg.WarpScriptType());
    }

    for (ArgumentSpecification arg: Lists.reverse(args)) {
      optMap.put(arg.getName(), arg.getName() + ":" + arg.WarpScriptType());
    }

    input2.add(optMap);
    sig2.add(input2);
    sig2.add(output);
    sig.add(sig2);

    info.put("sig", sig);

    //
    // Params generation
    //

    LinkedHashMap<String, String> params = new LinkedHashMap<>();
    for (ArgumentSpecification arg: Lists.reverse(optArgs)) {
      params.put(arg.getName(), arg.getDoc());
    }
    for (ArgumentSpecification arg: Lists.reverse(args)) {
      params.put(arg.getName(), arg.getDoc());
    }
    for (ArgumentSpecification arg: Lists.reverse(outputs)) {
      params.put(arg.getName(), arg.getDoc());
    }

    info.put("params", params);

    //
    // Remaining fields
    //

    info.put("related", related);
    info.put("examples", examples);
    info.put("conf", conf);

    return info;
  }

  public static String generateWarpScriptDoc(FormattedWarpScriptFunction function, List<ArgumentSpecification> outputs) throws WarpScriptException {
    return generateWarpScriptDoc(function,"","","","", new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), outputs);
  }

  /**
   * Generate readable WarpScript .mc2 documentation file
   *
   * @param function
   * @param since
   * @param deprecated
   * @param deleted
   * @param version
   * @param tags
   * @param related
   * @param examples
   * @param conf
   * @param outputs
   * @return warpscript_code
   * @throws WarpScriptException
   */
  public static String generateWarpScriptDoc(FormattedWarpScriptFunction function,
                                             String since, String deprecated, String deleted, String version,
                                             List<String> tags, List<String> related, List<String> examples, List<String> conf,
                                             List<ArgumentSpecification> outputs) throws WarpScriptException {

    StringBuilder mc2 = new StringBuilder();
    SNAPSHOT.addElement(mc2, generateInfo(function, since, deprecated, deleted, version, tags, related, examples, conf, outputs), true);

    mc2.append("'infomap' STORE" + System.lineSeparator());
    mc2.append(MACRO_START + System.lineSeparator());
    mc2.append("!$infomap INFO" + System.lineSeparator());
    mc2.append(MACRO_START + System.lineSeparator());
    mc2.append("'" + function.getName() + "' EVAL" + System.lineSeparator());
    mc2.append(MACRO_END + System.lineSeparator());
    mc2.append(MACRO_START + " " + MACRO_END + " " + MACRO_START + " " + MACRO_END + " TRY" + System.lineSeparator());
    mc2.append(MACRO_END + System.lineSeparator());
    mc2.append("'macro' STORE" + System.lineSeparator());
    mc2.append("// Unit tests" + System.lineSeparator());

    for (String unitTest: function.getUnitTests()) {
      mc2.append(unitTest + System.lineSeparator());
    }

    mc2.append("$macro" + System.lineSeparator());

    return mc2.toString();
  }
}
