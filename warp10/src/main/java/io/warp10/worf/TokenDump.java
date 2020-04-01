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

import io.warp10.WarpConfig;
import io.warp10.script.MemoryWarpScriptStack;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class TokenDump extends TokenGen {

  public static void main(String[] args) throws Exception {

    TokenDump instance = new TokenDump();

    instance.usage(args);

    instance.parse(args);

    instance.process(args);
  }

  @Override
  public void usage(String[] args) {
    if (args.length < 3) {
      System.err.println("Usage: TokenDump config ... in out");
      System.exit(-1);
    }
  }

  @Override
  public void process(String[] args) throws Exception {
    PrintWriter pw = new PrintWriter(System.out);

    if (!"-".equals(args[args.length - 1])) {
      pw = new PrintWriter(new FileWriter(args[args.length - 1]));
    }

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, WarpConfig.getProperties());
    stack.maxLimits();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();


    InputStream in = null;

    if ("-".equals(args[args.length - 2])) {
      in = System.in;
    } else {
      in = new FileInputStream(args[args.length - 2]);
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(in));

    boolean json = null != System.getProperty("json");

    while (true) {
      String line = br.readLine();

      if ("".equals(line)) {
        break;
      }

      stack.clear();
      stack.push(line);
      stack.exec("TOKENDUMP");

      if (json) {
        stack.exec("->JSON");
      } else {
        stack.exec("SNAPSHOT");
      }

      pw.println(stack.pop().toString());
      pw.flush();
    }

    br.close();

    pw.flush();
    pw.close();
  }
}
