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

import io.warp10.WarpConfig;
import io.warp10.script.*;
import io.warp10.script.formatted.FormattedWarpScriptFunction.Arguments;
import io.warp10.script.functions.TYPEOF;
import org.junit.BeforeClass;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Generate initial .mc2 doc files of instances of FormattedWarpScriptFunction
 * Execute this file to perform its Unit tests
 * Assert that a map is returned in INFOMODE
 *
 * Can be extended by WarpScript extensions
 */
public class RunAndGenerateDocumentationWithUnitTests {

  protected static final String OUTPUT_FOLDER_KEY = "generated.documentation.output.folder";

  //
  // Set WRITE to false if only for unit tests
  //

  private static final boolean WRITE = false;
  private static final String OUTPUT_FOLDER = "my/output/folder";
  private static final boolean OVERWRITE = false;

  static {
    //
    // Here register the extensions you want to include, for example:
    //

    //WarpScriptLib.register(new ForecastExtension());
  }

  private static String VERSION = "all";
  private static List<String> TAGS = new ArrayList<>();
  static {
    //
    // Tags that are common to all functions for which the documentation is being written, for example:
    //

    //TAGS.add("lists");
    //TAGS.add("tensors");
  }
  private static boolean MAKE_FUNCTIONS_RELATED_WITHIN_SAME_PACKAGE = true;
  private static List<String> RELATED = new ArrayList<>();
  private static String SINCE = "2.1";
  private static String DEPRECATED = "";
  private static String DELETED = "";
  private static List<String> CONF = new ArrayList<>();

  //
  // outputs, examples, tags and related are retrieved in each instance of FormattedWarpScriptFunction if they were set
  //

  //
  // Getters that can be overridden by subclasses
  //

  protected boolean WRITE() {return WRITE;}
  protected String OUTPUT_FOLDER() {return WarpConfig.getProperty(OUTPUT_FOLDER_KEY);}
  protected boolean OVERWRITE() {return OVERWRITE;}
  protected String VERSION() {return VERSION;}
  protected List<String> TAGS() {return TAGS;}
  protected boolean MAKE_FUNCTIONS_RELATED_WITHIN_SAME_PACKAGE() {return MAKE_FUNCTIONS_RELATED_WITHIN_SAME_PACKAGE;}
  protected List<String> RELATED() {return RELATED;}
  protected String SINCE() {return SINCE;}
  protected String DEPRECATED() {return DEPRECATED;}
  protected String DELETED() {return DELETED;}
  protected List<String> CONF() {return CONF;}

  //
  // Tests
  //

  @BeforeClass
  final public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.safeSetProperties(new StringReader(props.toString()));
  }

  final protected void generate(List<String> functionNames) throws Exception {

    if (null == OUTPUT_FOLDER()) {
      WarpConfig.setProperty(OUTPUT_FOLDER_KEY, OUTPUT_FOLDER);
    }

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();
    stack.exec(WarpScriptLib.INFOMODE);

    Collections.sort(functionNames);

    for (String name : functionNames) {
      Object function = WarpScriptLib.getFunction(name);

      if (function instanceof FormattedWarpScriptFunction) {
        String doc = "";
        String mc2 = "";

        System.out.println("Generate and assert doc for " + function.getClass().getName());

        // outputs
        List<ArgumentSpecification> output =  new ArrayList<>();
        Method m = searchMethod(function, "getOutput");
        if (null != m) {
          m.setAccessible(true);
          Object out = m.invoke(function);
          if (out instanceof Arguments) {
            output = ((Arguments) out).getArgsCopy();
          }
        }
        if (0 == output.size()) {
          output.add(new ArgumentSpecification(Object.class, "result", "No documentation provided."));
        }

        // examples
        List<String> examples = new ArrayList<>();
        m = searchMethod(function, "getExamples");
        if (null != m) {
          m.setAccessible(true);
          Object exs = m.invoke(function);
          if (exs instanceof List) {
            examples.addAll((List) exs);
          }
        }

        // tags
        List<String> tags = new ArrayList<>(TAGS());
        m = searchMethod(function, "getTags");
        if (null != m) {
          m.setAccessible(true);
          Object tag = m.invoke(function);
          if (tag instanceof List) {
            tags.addAll((List) tag);
          }
        }

        // related
        List<String> related;
        if (MAKE_FUNCTIONS_RELATED_WITHIN_SAME_PACKAGE()) {
          related = getRelatedClasses(function.getClass().getClassLoader(), function.getClass().getPackage().getName());
        } else {
          related = RELATED();
        }
        related.remove(name);
        m = searchMethod(function, "getRelated");
        if (null != m) {
          m.setAccessible(true);
          Object rel = m.invoke(function);
          if (rel instanceof List) {
            for (String r: (List<String>) rel) {
              if (!related.contains(r)) {
                related.add(r);
              }
            }
          }
        }

        try {

          mc2 = DocumentationGenerator.generateWarpScriptDoc((FormattedWarpScriptFunction) function, SINCE(), DEPRECATED(), DELETED(), VERSION(),
            tags, related, examples, CONF(), output);

          boolean caught = false;
          try {

            //
            // Pass warpscript unit tests and push info map
            //

            stack.execMulti(mc2);
            stack.exec(WarpScriptLib.EVAL);
          }
          catch (WarpScriptStopException wse) {

            //
            // Assert info map is pushed
            //

            caught = true;

            stack.exec(WarpScriptLib.DEPTH);
            stack.push(1L);
            stack.exec(WarpScriptLib.EQ);
            stack.exec(WarpScriptLib.ASSERT);
            
            stack.exec(WarpScriptLib.TYPEOF);
            stack.push(TYPEOF.typeof(Map.class));
            stack.exec(WarpScriptLib.EQ);
            stack.exec(WarpScriptLib.ASSERT);
          }
          if (!caught) {
            throw new WarpScriptException("Infomode failed to stop.");
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

        if (WRITE()) {
          String path = OUTPUT_FOLDER() + WarpScriptLib.getFunction(name).getClass().getSimpleName().toUpperCase() + ".mc2";
          File file = new File(path);
          if (OVERWRITE() || !file.exists()) {
            try {
              Files.write(Paths.get(path), mc2.getBytes());
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

        }
      }
    }
  }

  public static List<String> getRelatedClasses(ClassLoader cl, String pack) throws Exception {

    String dottedPackage = pack.replaceAll("[/]", ".");
    List<String> classNames = new ArrayList<>();
    pack = pack.replaceAll("[.]", "/");
    URL upackage = cl.getResource(pack);

    DataInputStream dis = new DataInputStream((InputStream) upackage.getContent());
    String line = null;
    while ((line = dis.readLine()) != null) {
      if(line.endsWith(".class") && !line.contains("$")) {
        classNames.add(Class.forName(dottedPackage+"."+line.substring(0,line.lastIndexOf('.'))).getSimpleName());
      }
    }
    return classNames;
  }

  /**
   * Search a method in a class and in parent's hierarchy
   *
   * @param function
   * @param simpleName
   * @return method if found, or null
   * @throws Exception
   */
  public static Method searchMethod(Object function, String simpleName) throws Exception {
    Class<?> clazz = function.getClass();

    while (clazz != null) {
      for (Method m : clazz.getDeclaredMethods()) {
        if (m.getName().equals(simpleName)) {
          return m;
        }
      }

      clazz = clazz.getSuperclass();
    }

    return null;
  }
}
