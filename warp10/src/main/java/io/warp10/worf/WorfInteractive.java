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

package io.warp10.worf;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.quasar.token.thrift.data.TokenType;

import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.util.Pair;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.UUID;

public class WorfInteractive {

  private ConsoleReader reader;
  private PrintWriter out;
  private Properties worfConfig = null;


  public WorfInteractive() throws WorfException {
    try {
      reader = new ConsoleReader();
      out = new PrintWriter(reader.getOutput());
      out.println("Welcome to warp10 token command line");
      out.println("I am Worf, security chief of the USS Enterprise (NCC-1701-D)");

    } catch (IOException e) {
      throw new WorfException("Unexpected Worf error:" + e.getMessage());
    }
  }

  private static String readInputPath(ConsoleReader reader, PrintWriter out, String prompt, String defaultPath) {
    try {
      // save file
      StringBuilder sb = new StringBuilder();
      sb.append("warp10:");
      sb.append(prompt);
      if (!Strings.isNullOrEmpty(defaultPath)) {
        sb.append(", default(");
        sb.append(defaultPath);
        sb.append(")");
      }
      sb.append(">");

      reader.setPrompt(sb.toString());
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (Strings.isNullOrEmpty(line) && Strings.isNullOrEmpty(defaultPath)) {
          continue;
        }

        if (Strings.isNullOrEmpty(line) && !Strings.isNullOrEmpty(defaultPath)) {
          return defaultPath;
        }

        if (line.equalsIgnoreCase("cancel")) {
          break;
        }

        Path outputPath = Paths.get(line);
        if (Files.notExists(outputPath)) {
          out.println("The path " + line + " does not exists");
          continue;
        }
        return line;
      }
    } catch (Exception exp) {
      if (WorfCLI.verbose) {
        exp.printStackTrace();
      }
      out.println("Error, unable to read the path. error=" + exp.getMessage());
    }
    return null;
  }

  private static String readInputPath(ConsoleReader reader, PrintWriter out, String prompt) {
    return readInputPath(reader, out, prompt, null);
  }

  private static String readHost(ConsoleReader reader, PrintWriter out, String prompt) {
    try {
      String inputString = readInputString(reader, out, prompt);

      if (InetAddresses.isInetAddress(inputString)) {
        return inputString;
      }
      out.println("Error, " + inputString + " is not a valid inet address");
    } catch (Exception exp) {
      if (WorfCLI.verbose) {
        exp.printStackTrace();
      }
      out.println("Error, unable to read the host. error=" + exp.getMessage());
    }
    return null;
  }

  private static String readInteger(ConsoleReader reader, PrintWriter out, String prompt) {
    try {
      String inputString = readInputString(reader, out, prompt);

      if (NumberUtils.isNumber(inputString)) {
        return inputString;
      }

      if (InetAddresses.isInetAddress(inputString)) {
        return inputString;
      }
      out.println("Error, " + inputString + " is not a number");
    } catch (Exception exp) {
      if (WorfCLI.verbose) {
        exp.printStackTrace();
      }
      out.println("Error, unable to read the host. error=" + exp.getMessage());
    }
    return null;
  }

  private static String readInputString(ConsoleReader reader, PrintWriter out, String prompt, String defaultString) {
    try {
      // save file
      StringBuilder sb = new StringBuilder("warp10:");
      sb.append(prompt);
      if (!Strings.isNullOrEmpty(defaultString)) {
        sb.append(", default(");
        sb.append(defaultString);
        sb.append(")");
      }
      sb.append(">");
      reader.setPrompt(sb.toString());
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (Strings.isNullOrEmpty(line) && Strings.isNullOrEmpty(defaultString)) {
          continue;
        }
        if (Strings.isNullOrEmpty(line) && !Strings.isNullOrEmpty(defaultString)) {
          return defaultString;
        }
        if (line.equalsIgnoreCase("cancel")) {
          break;
        }
        return line;
      }
    } catch (Exception exp) {
      if (WorfCLI.verbose) {
        exp.printStackTrace();
      }
      out.println("Error, unable to read the string. error=" + exp.getMessage());
    }
    return null;

  }

  private static String readInputString(ConsoleReader reader, PrintWriter out, String prompt) {
    return readInputString(reader, out, prompt, null);
  }

  private static TokenType getTokenType(String line, PrintWriter out) {
    try {
      if (Strings.isNullOrEmpty(line)) {
        return null;
      }

      return TokenType.valueOf(line.toUpperCase());
    } catch (Exception exp) {
      out.println("token type " + line + " unknown. (read|write) expected");
      return null;
    }
  }

  private static long getTTL(String line, PrintWriter out) {
    try {
      long ttl = Long.valueOf(line);

      long ttlMax = Long.MAX_VALUE - System.currentTimeMillis();

      if (ttl >= ttlMax) {
        out.println("TTL can not be upper than " + ttlMax);
        return 0L;
      }

      return ttl;
    } catch (NumberFormatException exp) {
      out.println(line + " is not a long");
      return 0L;
    }
  }

  private static Map<String, String> getLabels(String line, PrintWriter out) {
    try {
      if (Strings.isNullOrEmpty(line)) {
        return new HashMap<String, String>();
      }

      return GTSHelper.parseLabels(line);
    } catch (Exception exp) {
      out.println(line + " is not a label selector");
      return null;
    }
  }

  public PrintWriter getPrintWriter() {
    return out;
  }

  public String runTemplate(Properties config, String warp10Configuration) throws WorfException {
    try {
      out.println("The configuration file is a template");
      WorfTemplate template = new WorfTemplate(config, warp10Configuration);

      out.println("Generating crypto keys...");
      for (String cryptoKey : template.getCryptoKeys()) {
        String keySize = template.generateCryptoKey(cryptoKey);
        if (keySize != null) {
          out.println(keySize + " bits secured key for " + cryptoKey + "  generated");
        } else {
          out.println("Unable to generate " + cryptoKey + ", template error");
        }
      }

      out.println("Crypto keys generated");


      Stack<Pair<String, String[]>> fieldsStack = template.getFieldsStack();

      if (fieldsStack.size() > 0) {
        out.println("Update configuration...");
      }

      while (!fieldsStack.isEmpty()) {
        Pair<String, String[]> templateValues = fieldsStack.peek();
        String replaceValue = null;
        // get user input
        switch (templateValues.getValue()[0]) {
          case "path":
            replaceValue = readInputPath(reader, out, templateValues.getValue()[2]);
            break;
          case "host":
            replaceValue = readHost(reader, out, templateValues.getValue()[2]);
            break;
          case "int":
            replaceValue = readInteger(reader, out, templateValues.getValue()[2]);
            break;
        }

        if (replaceValue == null) {
          out.println("Unable to update " + templateValues.getValue()[1] + " key, enter a valid " + templateValues.getValue()[0]);
          continue;
        }

        // replace template value
        template.updateField(templateValues.getKey(), replaceValue);

        // field updated pop
        fieldsStack.pop();
      }

      out.println("Configuration updated.");

      // save file
      Path warp10ConfigurationPath = Paths.get(warp10Configuration);
      String outputFileName = warp10ConfigurationPath.getFileName().toString();
      outputFileName = outputFileName.replace("template", "conf");

      String configPath = ".";
      Path parent = warp10ConfigurationPath.getParent();
      if (null != parent) {
        configPath = parent.toString();
      }
      String outputPath = readInputPath(reader, out, "save config:output path", configPath);
      String outputFilename = readInputString(reader, out, "save config:output filename", outputFileName);

      if (Strings.isNullOrEmpty(outputPath) || Strings.isNullOrEmpty(outputFilename)) {
        throw new Exception("Path or filename empty, unable to save configuration file!");
      }

      StringBuilder sb = new StringBuilder();
      sb.append(outputPath);
      if (!outputPath.endsWith(File.separator)) {
        sb.append(File.separator);
      }
      sb.append(outputFilename);

      warp10Configuration = sb.toString();
      template.saveConfig(warp10Configuration);

      out.println("Configuration saved. filepath=" + warp10Configuration);
      out.println("Reading warp10 configuration " + warp10Configuration);
      return warp10Configuration;
    } catch (Exception exp) {
      throw new WorfException("Unexpected Worf error:" + exp.getMessage());
    }
  }

  public int run(Properties config, Properties worfConfig) throws WorfException {
    this.worfConfig = worfConfig;
    try {
      String line = null;
      TokenCommand currentCommand = null;
      Completer defaultCompleter = null;

      // extract token + oss keys
      // create keystore
      WorfKeyMaster worfKeyMaster = new WorfKeyMaster(config);

      if (!worfKeyMaster.loadKeyStore()) {
        out.println("Unable to load warp10 keystore.");
        out.flush();
        return -1;
      }

      out.println("Warp10 keystore initialized.");
      // completers
      defaultCompleter = new StringsCompleter("quit", "exit", "cancel", "read", "write", "encodeToken", "decodeToken", "uuidgen");

      reader.addCompleter(defaultCompleter);
      reader.setPrompt("warp10> ");

      while ((line = reader.readLine()) != null) {
        line = line.trim();

        // Select command
        // ---------------------------------------------------------
        if (line.equalsIgnoreCase("encodeToken")) {
          currentCommand = new EncodeTokenCommand();
          reader.setPrompt(getPromptMessage(currentCommand));
          // loop nothing more to do
          continue;
        } else if (line.equalsIgnoreCase("decodeToken")) {
          currentCommand = new DecodeTokenCommand();
          reader.setPrompt(getPromptMessage(currentCommand));
          // loop nothing more to do
          continue;
        } else if (line.equalsIgnoreCase("cancel")) {
          currentCommand = null;
          reader.setPrompt(getPromptMessage(currentCommand));
          // loop nothing more to do
          continue;
        }

        // execute command
        // ----------------------------------------------------------
        if (currentCommand != null && currentCommand.isReady()) {
          if (currentCommand.execute(line.toLowerCase(), worfKeyMaster, out)) {
            // reset current command
            currentCommand = null;
          }
        }

        // get input command
        // ----------------------------------------------------------
        getInputField(currentCommand, line, out);
        reader.setPrompt(getPromptMessage(currentCommand));

        if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
          break;
        }
      }

      System.out.println("Bye!");
      return 0;
    } catch (Exception exp) {
      throw new WorfException("Unexpected Worf error:" + exp.getMessage(), exp);
    }
  }

  private void getInputField(TokenCommand command, String input, PrintWriter out) {
    if (command == null) {
      return;
    }

    switch (command.commandName) {
      case "encodeToken":
        EncodeTokenCommand encodeTokenCommand = (EncodeTokenCommand) command;

        if (null == encodeTokenCommand.tokenType) {
          encodeTokenCommand.tokenType = getTokenType(input, out);
        } else if (null == encodeTokenCommand.application) {
          encodeTokenCommand.application = getApplicationName(input, out);
        } else if (null == encodeTokenCommand.applications && TokenType.READ.equals(encodeTokenCommand.tokenType)) {
          encodeTokenCommand.applications = getApplicationsName(input, out);
          if (encodeTokenCommand.applications == null || encodeTokenCommand.applications.isEmpty()) {
            encodeTokenCommand.applications = Arrays.asList(encodeTokenCommand.application);
          }
        } else if (null == encodeTokenCommand.producer) {
          encodeTokenCommand.producer = getUUID(input, out, WorfCLI.P_UUID, null);
        } else if (null == encodeTokenCommand.producers && TokenType.READ.equals(encodeTokenCommand.tokenType)) {
          encodeTokenCommand.producers = getUUIDs(input, out, true, null);
        } else if (Strings.isNullOrEmpty(encodeTokenCommand.owner) && (null == encodeTokenCommand.owners || encodeTokenCommand.owners.isEmpty())) {
          if (TokenType.READ.equals(encodeTokenCommand.tokenType)) {
            encodeTokenCommand.owners = getUUIDs(input, out, false, encodeTokenCommand.producer);
          } else {
            encodeTokenCommand.owner = getUUID(input, out, WorfCLI.O_UUID, encodeTokenCommand.producer);
          }
        } else if (encodeTokenCommand.ttl == 0L) {
          encodeTokenCommand.ttl = getTTL(input, out);
        } else if (null == encodeTokenCommand.labels) {
          encodeTokenCommand.labels = getLabels(input, out);
        }
        break;

      case "decodeToken":
        DecodeTokenCommand decodeTokenCommand = (DecodeTokenCommand) command;
        if (null == decodeTokenCommand.token) {
          decodeTokenCommand.token = getString(input, out);
        }
        break;
    }
  }

  private String getPromptMessage(TokenCommand command) {
    if (command == null) {
      return "warp10>";
    }

    StringBuilder sb = new StringBuilder();
    String defaultValue = null;

    sb.append(command.commandName);

    if (command instanceof EncodeTokenCommand) {
      EncodeTokenCommand encodeTokenCommand = (EncodeTokenCommand) command;

      // updates prompt
      if (null == encodeTokenCommand.tokenType) {
        sb.append("/token type (read|write)");
      } else if (null == encodeTokenCommand.application) {
        sb.append("/application name");
        defaultValue = getApplicationName(null, null);
      } else if (null == encodeTokenCommand.applications && TokenType.READ.equals(encodeTokenCommand.tokenType)) {
        sb.append("/application names - optional (app1,app2)");
        defaultValue = null;
      } else if (null == encodeTokenCommand.producer) {
        sb.append("/data producer UUID (you can create a new one by typing 'uuidgen') ");
        defaultValue = getUUID(null, null, WorfCLI.P_UUID, null);
      } else if (null == encodeTokenCommand.producers && TokenType.READ.equals(encodeTokenCommand.tokenType)) {
        sb.append("/data producers - optional (UUID1,UUID2)");
        defaultValue = null;
      } else if (Strings.isNullOrEmpty(encodeTokenCommand.owner) && (null == encodeTokenCommand.owners || encodeTokenCommand.owners.isEmpty())) {
        if (TokenType.READ.equals(encodeTokenCommand.tokenType)) {
          sb.append("/data owners UUID");
        } else {
          sb.append("/data owner UUID (you can create a new one by typing 'uuidgen') ");
        }
        defaultValue = getUUID(null, null, WorfCLI.O_UUID, encodeTokenCommand.producer);
      } else if (0L == encodeTokenCommand.ttl) {
        sb.append("/token time to live (TTL) in ms ");
      } else if (null == encodeTokenCommand.labels) {
        sb.append("/OPTIONAL fixed labels (key1=value1,key2=value2) ");
      } else {
        sb.append("(generate | cancel)");
      }
    }

    if (command instanceof DecodeTokenCommand) {
      DecodeTokenCommand decodeTokenCommand = (DecodeTokenCommand) command;

      if (decodeTokenCommand.token == null) {
        sb.append("/token");
      } else if (decodeTokenCommand.writeToken != null) {
        sb.append("convert to read token ? (yes)");
      } else {
        sb.append("(decode | cancel)");
      }
    }

    // ads default value to the prompt
    if (!Strings.isNullOrEmpty(defaultValue)) {
      sb.append(", default (");
      sb.append(defaultValue);
      sb.append(")");
    }

    sb.append(">");

    return sb.toString();
  }

  private String getString(String line, PrintWriter out) {
    try {
      if (Strings.isNullOrEmpty(line)) {
        return null;
      } else {
        return line.trim();
      }
    } catch (Exception exp) {
      if (out != null) {
        out.println("Unable to get application name cause=" + exp.getMessage());
      }
      return null;
    }
  }

  private String getApplicationName(String line, PrintWriter out) {
    try {
      String appName = Worf.getDefault(worfConfig, out, line, WorfCLI.APPNAME);

      if (Strings.isNullOrEmpty(appName)) {
        return null;
      }
      return appName;
    } catch (Exception exp) {
      if (out != null) {
        out.println("Unable to get application name cause=" + exp.getMessage());
      }
      return null;
    }
  }

  private List<String> getApplicationsName(String line, PrintWriter out) {
    try {
      List<String> output = new ArrayList<String>();

      //
      // Noting to do, return empty list
      //

      if (Strings.isNullOrEmpty(line)) {
        return output;
      }

      //
      // Split application name on ',' boundaries
      //

      String[] apps = line.split(",");
      output.addAll(Arrays.asList(apps));

      return output;
    } catch (Exception exp) {
      if (out != null) {
        out.println("Unable to get application name cause=" + exp.getMessage());
      }
      return null;
    }
  }

  private String getUUID(String line, PrintWriter out, String worfDefault, String uuidDefault) {
    try {
      String strUuid = Worf.getDefault(worfConfig, out, line, worfDefault);
      UUID uuid = null;

      if (Strings.isNullOrEmpty(strUuid) && Strings.isNullOrEmpty(uuidDefault)) {
        return null;
      }

      // no input and default available
      if (Strings.isNullOrEmpty(line) && !Strings.isNullOrEmpty(uuidDefault)) {
        return uuidDefault;
      }

      if ("uuidgen".equals(strUuid)) {
        // generate uuid from cmd line
        uuid = UUID.randomUUID();
        if (out != null) {
          out.println("uuid generated=" + uuid.toString());
        }
      } else {
        // take default
        uuid = UUID.fromString(strUuid);
      }
      return uuid.toString();
    } catch (Exception exp) {
      if (out != null) {
        out.println("UUID not valid cause=" + exp.getMessage());
      }
      return null;
    }
  }

  private List<String> getUUIDs(String line, PrintWriter out, boolean optional, String uuidDefault) {
    try {
      if (Strings.isNullOrEmpty(line) && Strings.isNullOrEmpty(uuidDefault)) {
        if (optional) {
          return new ArrayList<>();
        }
        return null;
      }

      //
      // no input and default available
      //

      if (Strings.isNullOrEmpty(line) && !Strings.isNullOrEmpty(uuidDefault)) {
        return Arrays.asList(uuidDefault);
      }

      //
      // Split
      //
      String[] uuids = line.split(",");

      //
      // Check uuid integrity
      //
      ArrayList<String> output = new ArrayList<String>();
      for (String strUuid : uuids) {
        UUID.fromString(strUuid);
        output.add(strUuid);
      }
      return output;
    } catch (Exception exp) {
      if (null != out) {
        out.println("UUID not valid cause=" + exp.getMessage());
      }
      return null;
    }
  }
}
