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

package io.warp10.worf;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;

import io.warp10.WarpURLDecoder;

public class Worf {


  public static void main(String[] args) throws Exception {
    try {
      WorfCLI worfCLI = new WorfCLI();
      System.exit(worfCLI.execute(args));
    } catch (Exception exp) {
      System.err.println("Worf error: " + exp.getMessage());
      exp.printStackTrace();
      System.exit(-1);
    }
  }

  public static Properties readConfig(String file, PrintWriter out) throws WorfException {
    try {
      //
      // Extract the name of the config file
      //

      File config = new File(file);

      //
      // Read the properties in the config file
      //

      Properties properties = new Properties();
      Properties templatedProperties = new Properties();
      templatedProperties.setProperty("worf.template", "true");

      BufferedReader br = new BufferedReader(new FileReader(config));

      int lineno = 0;

      int errorcount = 0;

      while (true) {
        String line = br.readLine();

        if (null == line) {
          break;
        }

        line = line.trim();
        lineno++;

        // Skip comments and blank lines
        if ("".equals(line) || line.startsWith("//") || line.startsWith("#") || line.startsWith("--")) {
          continue;
        }

        // Lines not containing an '=' will emit warnings

        if (!line.contains("=")) {
          out.println("Line " + lineno + " is missing an '=' sign, skipping.");
          continue;
        }

        String[] tokens = line.split("=");

        if (tokens.length > 2) {
          out.println("Invalid syntax on line " + lineno + ", will force an abort.");
          errorcount++;
          continue;
        }

        if (tokens.length < 2) {
          out.println("Empty value for property '" + tokens[0] + "', ignoring.");
          continue;
        }

        // Remove URL encoding if a '%' sign is present in the token
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = WarpURLDecoder.decode(tokens[i], StandardCharsets.UTF_8);
          tokens[i] = tokens[i].trim();
        }

        //
        // Ignore empty properties
        //

        if ("".equals(tokens[1])) {
          continue;
        }

        Matcher templateMatcher = WorfTemplate.templatePattern.matcher(tokens[1]);
        if (templateMatcher.matches()) {
          templatedProperties.setProperty(tokens[0], tokens[1]);
        } else {
          //
          // Set property
          //
          properties.setProperty(tokens[0], tokens[1]);
        }
      }

      br.close();

      if (errorcount > 0) {
        out.println("Aborting due to " + errorcount + " error" + (errorcount > 1 ? "s" : "") + ".");
        System.exit(-1);
      }

      //
      // Now override properties with system properties
      //

      Properties sysprops = System.getProperties();

      for (Map.Entry<Object, Object> entry : sysprops.entrySet()) {
        String name = entry.getKey().toString();
        String value = entry.getKey().toString();

        // URL Decode name/value if needed
        name = WarpURLDecoder.decode(name, StandardCharsets.UTF_8);
        value = WarpURLDecoder.decode(value, StandardCharsets.UTF_8);

        // Override property
        properties.setProperty(name, value);
      }

      //
      // Now expand ${xxx} constructs
      //

      Pattern VAR = Pattern.compile(".*\\$\\{([^}]+)\\}.*");

      Set<String> emptyProperties = new HashSet<>();

      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        String name = entry.getKey().toString();
        String value = entry.getValue().toString();

        //
        // Replace '' with the empty string
        //

        if ("''".equals(value)) {
          value = "";
        }

        int loopcount = 0;

        while (true) {
          Matcher m = VAR.matcher(value);

          if (m.matches()) {
            String var = m.group(1);

            if (properties.containsKey(var)) {
              value = value.replace("${" + var + "}", properties.getProperty(var));
            } else {
              out.println("Property '" + var + "' referenced in property '" + name + "' is unset, unsetting '" + name + "'");
              value = null;
            }
          } else {
            break;
          }

          if (null == value) {
            break;
          }

          loopcount++;

          if (loopcount > 100) {
            out.println("Hmmm, that's embarassing, but I've been dereferencing variables " + loopcount + " times trying to set a value for '" + name + "'.");
            System.exit(-1);
          }
        }

        if (null == value) {
          emptyProperties.add(name);
        } else {
          properties.setProperty(name, value);
        }
      }

      //
      // Remove empty properties
      //
      for (String property : emptyProperties) {
        properties.remove(property);
      }

      // there is templated properties in the file
      if (templatedProperties.size() > 1) {
        return templatedProperties;
      }

      return properties;
    } catch (Exception exp) {
      throw new WorfException("Configuration file read error cause=" + exp.getMessage());
    }
  }

  private static String getDefaultFilename(String configFile) {
    Path inputConfigurationPath = Paths.get(configFile);
    String configPath = ".";
    Path parent = inputConfigurationPath.getParent();
    if (null != parent) {
      configPath = parent.toString();
    }
    String configFileName = inputConfigurationPath.getFileName().toString();

    // configuration filename
    StringBuilder sb = new StringBuilder(configPath);
    if (!configPath.endsWith(File.separator)) {
      sb.append(File.separator);
    }
    sb.append(".");
    sb.append(configFileName);
    sb.append(".worf");

    return sb.toString();
  }

  public static Properties readDefault(String configFile, PrintWriter out) {
    // read the file config_path/.configFile.worf
    InputStream input = null;
    try {
      Properties prop = new Properties();
      String file = getDefaultFilename(configFile);
      input = new FileInputStream(file);

      // load a properties file
      prop.load(input);

      WorfCLI.consolePrintln("default options loaded from file:" + file, out);

      return prop;
    } catch (IOException ex) {
      // no file
      return null;
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static String getDefault(Properties defaultProperties, PrintWriter out, String field, String optionName) throws WorfException {
    String value = null;

    if (defaultProperties != null && Strings.isNullOrEmpty(field)) {
      value = defaultProperties.getProperty("option.default." + optionName);
    } else {
      value = field;
    }

    if (Strings.isNullOrEmpty(value)) {
      throw new WorfException("The option '" + optionName + "' is missing ");
    }

    return value;
  }

  public static void saveDefault(String fileName, String appName, String producerUUID, String ownerUUID) {
    Properties prop = new Properties();
    OutputStream output = null;

    try {

      output = new FileOutputStream(getDefaultFilename(fileName));

      // set the properties value
      prop.setProperty("option.default." + WorfCLI.APPNAME, appName);
      prop.setProperty("option.default." + WorfCLI.P_UUID, producerUUID);
      prop.setProperty("option.default." + WorfCLI.O_UUID, ownerUUID);

      // save properties to project root folder
      prop.store(output, null);

    } catch (IOException io) {
      io.printStackTrace();
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}