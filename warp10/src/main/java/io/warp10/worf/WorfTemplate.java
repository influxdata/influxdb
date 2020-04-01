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

import org.apache.commons.math3.util.Pair;
import org.bouncycastle.util.encoders.Hex;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WorfTemplate {

  public final static String TEMPLATE_PATTERN = "@(.*)@";
  private static final SecureRandom sr = new SecureRandom();
  public final static Pattern templatePattern = Pattern.compile(TEMPLATE_PATTERN);

  Properties config = null;
  String content = null;

  public WorfTemplate(Properties config, String templateFilePath) throws WorfException {
    try {
      this.config = config;

      if (isTemplate(config)) {
        config.remove("worf.template");
      }

      // load template file
      Path templatePath = Paths.get(templateFilePath);
      Charset charset = StandardCharsets.UTF_8;

      content = new String(Files.readAllBytes(templatePath), charset);
    } catch (Exception exp) {
      throw new WorfException("Unable to load template cause=" + exp.getMessage());
    }
  }

  public List<String> getCryptoKeys() throws WorfException {
    List<String> keys = new ArrayList<>();

    for (Map.Entry<Object,Object> configEntry : config.entrySet()) {
      String key = (String) configEntry.getKey();
      String value = (String) configEntry.getValue();

      //
      // extract template value
      //
      Matcher templateMatcher = templatePattern.matcher(value);

      if (templateMatcher.matches()) {
        String template = templateMatcher.group(1);
        String[] templateValues = template.split(":");

        // generate crypto keys first
        if ("key".equals(templateValues[0])) {
          if (templateValues.length != 3) {
            throw new WorfException("Read template error key=" + key + " t=" + value);
          }
          keys.add(key);
        }
      }
    }

    return keys;
  }

  public List<String> getTokenKeys() throws WorfException {
    List<String> keys = new ArrayList<>();
    for (Map.Entry<Object,Object> configEntry : config.entrySet()) {
      String key = (String) configEntry.getKey();
      String value = (String) configEntry.getValue();

      //
      // extract template value
      //
      Matcher templateMatcher = templatePattern.matcher(value);

      if (templateMatcher.matches()) {
        String template = templateMatcher.group(1);
        String[] templateValues = template.split(":");

        // generate crypto keys first
        if ("warp".equals(templateValues[0])) {
          if (templateValues.length != 2) {
            throw new WorfException("Read template error key=" + key + " t=" + value);
          }
          keys.add(key);
        }
      }
    }
    return keys;
  }

  public Stack<Pair<String, String[]>> getFieldsStack() throws WorfException {
    Stack<Pair<String, String[]>> stack = new Stack<>();

    for (Map.Entry<Object,Object> configEntry : config.entrySet()) {
      String key = (String) configEntry.getKey();
      String value = (String) configEntry.getValue();


      //
      // extract template value
      //
      Matcher templateMatcher = templatePattern.matcher(value);

      if (templateMatcher.matches()) {
        String template = templateMatcher.group(1);
        String[] templateValues = template.split(":");

        if (templateValues.length != 3) {
          throw new WorfException("Read template error key=" + key + " t=" + value);
        }
        stack.push(new Pair<>(value, templateValues));
      }
    }

    return stack;
  }

  public String generateCryptoKey(String configKey) {
    String value = config.getProperty(configKey);

    Matcher templateMatcher = templatePattern.matcher(value);

    if (templateMatcher.matches()) {
      String template = templateMatcher.group(1);
      String[] templateValues = template.split(":");

      StringBuilder sb = new StringBuilder();
      int size = Integer.parseInt(templateValues[1]);
      sb.append("hex:");
      byte[] key = new byte[size / 8];
      sr.nextBytes(key);
      sb.append(new String(Hex.encode(key)));

      // replace template value
      updateField(value, sb.toString());

      // key is processed
      config.remove(configKey);
      return templateValues[1];
    }

    return null;
  }

  public String generateTokenKey(String tokenKey, String name, String owner, String producer, long ttl, WorfKeyMaster templateKeyMaster) throws WorfException {
    String value = config.getProperty(tokenKey);
    String tokenIdent = null;
    Matcher templateMatcher = templatePattern.matcher(value);

    if (templateMatcher.matches()) {
      String template = templateMatcher.group(1);
      String[] templateValues = template.split(":");

      String token = null;
      if ("WriteToken".equals(templateValues[1])) {
        token = templateKeyMaster.deliverWriteToken(name, producer, owner, ttl);

      } else if ("ReadToken".equals(templateValues[1])) {
        token = templateKeyMaster.deliverReadToken(name, Arrays.asList(name), producer, Arrays.asList(owner), ttl);
      }

      tokenIdent =  templateKeyMaster.getTokenIdent(token);

      // replace template value
      updateField(value,token);

      // key is processed
      config.remove(tokenKey);
    }
    return tokenIdent;
  }

  public static boolean isTemplate(Properties warp10Config) {
    return "true".equals(warp10Config.getProperty("worf.template"));
  }

  public void saveConfig(String configPath) throws WorfException {
    try {
      File f = new File(configPath);
      if (f.exists() && !f.isDirectory()) {
        throw new WorfException("File already exists file - " + configPath);
      }
      Path path = Paths.get(configPath);

      Files.write(path, content.getBytes(StandardCharsets.UTF_8));
    } catch (Exception exp) {
      throw new WorfException("Unable to save configuration. cause=" + exp.getMessage());
    }
  }

  public void updateField(String key, String replaceValue) {
    content = content.replaceAll(key, replaceValue);
  }

}
