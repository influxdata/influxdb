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

package io.warp10.continuum.gts;

import io.warp10.WarpConfig;
import io.warp10.script.MemoryWarpScriptStack;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.Properties;

public class mapV2Tests {

  @BeforeClass
  public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.safeSetProperties(new StringReader(props.toString()));
  }

  @Test
  public void testB() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = 0, post = 0
    //

    stack.exec("NEWGTS 1 100 <% 10 * 0 0 0 1 ADDVALUE %> FOR " +
      "{ 'mapper' 0 mapper.replace 'ticks' [ 10 100 <% %> FOR ] } MAP 0 GET " +
      "DUP SIZE 0 == ASSERT");
    stack.exec("NEWGTS 1 100 <% 10 * 0 0 0 1 ADDVALUE %> FOR " +
      "{ 'mapper' 0 mapper.replace 'occurences' -10 'ticks' [ 10 100 <% %> FOR ] } MAP 0 GET " +
      "DUP SIZE 0 == ASSERT");
    stack.clear();
  }

  @Test
  public void testC() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = 0, post = 5
    //

    stack.exec("NEWGTS 1 100 <% 2 * DUP 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "{ 'mapper' mapper.max 'post' 5 'ticks' [ 1 100 <% 2 * 1 + %> FOR ] } MAP 0 GET " +

      // output ticks must match
      " RSORT DUP TICKLIST LIST-> 1 SWAP <% 2 * 1 + == ASSERT %> FOR " +

      // values must match
      " VALUES LIST-> 6 SWAP DUP 99 == ASSERT <% 2 * == ASSERT %> FOR " +

      // 5 values are bounded by 200, and the last one had 0 values in the sliding window
      " 1 5 <% DROP 200 == ASSERT %> FOR " +
      " DEPTH 0 == ASSERT");
    stack.clear();
  }

  @Test
  public void testD() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = 0, post = 5 with reverse order (ie occurences < 0)
    //

    stack.exec("NEWGTS 1 100 <% 2 * DUP 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "{ 'mapper' mapper.max 'post' 5 'occurences' -200 'ticks' [ 1 100 <% 2 * 1 + %> FOR ] } MAP 0 GET " +

      // output ticks must match
      " DUP TICKLIST LIST-> 1 SWAP <% 2 * 1 + == ASSERT %> FOR " +

      // values must match
      " VALUES LIST-> 6 SWAP DUP 99 == ASSERT <% 2 * == ASSERT %> FOR " +

      // 5 values are bounded by 200, and the last one had 0 value in the sliding window
      " 1 5 <% DROP 200 == ASSERT %> FOR " +
      " DEPTH 0 == ASSERT");
    stack.clear();
  }

  @Test
  public void testE() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = 4, post = 5
    //

    // max
    stack.exec("NEWGTS 1 100 <% 2 * DUP 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "{ 'mapper' mapper.max 'pre' 4 'post' 5 'ticks' [ 1 100 <% 2 * 1 + %> FOR ] } MAP 0 GET " +

      // output ticks must match
      " RSORT DUP TICKLIST LIST-> 1 SWAP <% 2 * 1 + == ASSERT %> FOR " +

      // values must match
      " VALUES LIST-> 6 SWAP DUP 100 == ASSERT <% 2 * == ASSERT %> FOR " +

      // 5 values are bounded by 200, and the last one had 0 values in the sliding window
      " 1 5 <% DROP 200 == ASSERT %> FOR " +
      " DEPTH 0 == ASSERT");
    stack.clear();

    // min
    stack.exec("NEWGTS 1 100 <% 2 * DUP 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "{ 'mapper' mapper.min 'pre' 4 'post' 5 'ticks' [ 1 100 <% 2 * 1 + %> FOR ] } MAP 0 GET " +

      // output ticks must match
      " RSORT DUP TICKLIST LIST-> 1 SWAP <% 2 * 1 + == ASSERT %> FOR " +

      // values must match
      "VALUES LIST-> DROP " +

      // 4 values are bounded by 2
      "1 4 <% DROP 2 == ASSERT %> FOR " +

      // 96 values left to check
      "2 97 <% 2 * == ASSERT %> FOR " +
      "DEPTH 0 == ASSERT");
    stack.clear();
  }

  @Test
  public void testF() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = -85, post = -105
    //

    // max
    stack.exec("NEWGTS 1 100 <% 2 * DUP 10 * 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "{ 'mapper' mapper.max 'pre' -85 'post' -105 'ticks' [ 1 100 <% 2 * 1 + 10 * %> FOR ] } MAP 0 GET " +

      // output ticks must match
      " RSORT DUP TICKLIST LIST-> 1 SWAP <% 2 * 1 + 10 * == ASSERT %> FOR " +

      // values must match
      " VALUES LIST-> 6 SWAP <% 2 * == ASSERT %> FOR " +

      // 5 values are bounded by 200, and the last one had 0 values in the sliding window
      " 1 5 <% DROP 200 == ASSERT %> FOR " +
      " DEPTH 0 == ASSERT");
    stack.clear();

    // min
    stack.exec("NEWGTS 1 100 <% 2 * DUP 10 * 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "{ 'mapper' mapper.min 'pre' -85 'post' -105 'ticks' [ 1 100 <% 2 * 1 + 10 * %> FOR ] } MAP 0 GET " +

      // output ticks must match
      " RSORT DUP TICKLIST LIST-> 1 SWAP <% 2 * 1 + 10 * == ASSERT %> FOR " +

      // values must match
      "VALUES LIST-> DROP " +

      // 4 values are bounded by 2
      "1 4 <% DROP 2 == ASSERT %> FOR " +

      // 96 values left to check
      "2 97 <% 2 * == ASSERT %> FOR " +
      "DEPTH 0 == ASSERT");
    stack.clear();
  }

  @Test
  public void testG() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = 0, post = 5 with some input and output ticks that match together
    //

    stack.exec("NEWGTS 1 100 <% 2 * DUP 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "{ 'mapper' <% 7 GET ->JSON 0 NaN NaN NaN 5 ROLL %> MACROMAPPER " +
      " 'post' 5 'ticks' [ 51 150 <% 2 * %> FOR ] } MAP 0 GET " +

      // output ticks must match
      "DUP TICKLIST REVERSE LIST-> 1 SWAP <% 50 + 2 * == ASSERT %> FOR " +

      // sliding windows must match
      "VALUES REVERSE LIST-> DROP 1 46 <% 50 + [ 0 4 <% DUP 3 + PICK + 2 * %> FOR ] SWAP DROP ->JSON == ASSERT %> FOR " +

      // the 4 next sliding windows have 1 less values each time
      "[ 194 196 198 200 ] ->JSON == ASSERT " +
      "[ 196 198 200 ] ->JSON == ASSERT " +
      "[ 198 200 ] ->JSON == ASSERT " +
      "[ 200 ] ->JSON == ASSERT " +

      // the remaining 50 sliding windows are empty
      " 1 50 <% DROP [] ->JSON == ASSERT %> FOR " +
      " DEPTH 0 == ASSERT");
    stack.clear();
  }

  @Test
  public void testH() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = 5, post = 0 with some input and output ticks that match together
    //

    stack.exec("NEWGTS 1 100 <% 2 * DUP 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "{ 'mapper' <% 7 GET ->JSON 0 NaN NaN NaN 5 ROLL %> MACROMAPPER " +
      " 'pre' 5 'ticks' [ 51 150 <% 2 * %> FOR ] } MAP 0 GET " +

      // output ticks must match
      "DUP TICKLIST REVERSE LIST-> 1 SWAP <% 50 + 2 * == ASSERT %> FOR " +

      // sliding windows must match
      "VALUES REVERSE LIST-> DROP 1 51 <% 50 + [ 0 4 <% DUP 3 + PICK + 5 - 2 * %> FOR ] SWAP DROP ->JSON == ASSERT %> FOR " +

      // the remaining 49 sliding windows can not slide further
      " 1 49 <% DROP [ 192 194 196 198 200 ] ->JSON == ASSERT %> FOR " +
      " DEPTH 0 == ASSERT");
    stack.clear();
  }

  @Test
  public void testI() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = 0, post = 5 with some input and output ticks that match together
    // with input GTS being bucketized
    //

    stack.exec("[ NEWGTS 1 100 <% 2 * DUP 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "bucketizer.last 0 2 0 ] BUCKETIZE " +
      "{ 'mapper' <% 7 GET ->JSON 0 NaN NaN NaN 5 ROLL %> MACROMAPPER " +
      "'post' 5 'ticks' [ 51 150 <% 2 * %> FOR ] } MAP 0 GET " +

      // output not supposed to be bucketized
      "DUP BUCKETCOUNT  0 == ASSERT " +
      "DUP BUCKETSPAN  0 == ASSERT " +
      "DUP LASTBUCKET  0 == ASSERT " +

      // output ticks must match
      "DUP TICKLIST REVERSE LIST-> 1 SWAP <% 50 + 2 * == ASSERT %> FOR " +

      // sliding windows must match
      "VALUES REVERSE LIST-> DROP 1 46 <% 50 + [ 0 4 <% DUP 3 + PICK + 2 * %> FOR ] SWAP DROP ->JSON == ASSERT %> FOR " +

      // the 4 next sliding windows have 1 less values each time
      "[ 194 196 198 200 ] ->JSON == ASSERT " +
      "[ 196 198 200 ] ->JSON == ASSERT " +
      "[ 198 200 ] ->JSON == ASSERT " +
      "[ 200 ] ->JSON == ASSERT " +

      // the remaining 50 sliding windows are empty
      " 1 50 <% DROP [] ->JSON == ASSERT %> FOR " +
      " DEPTH 0 == ASSERT");
    stack.clear();
  }

  @Test
  public void testJ() throws Exception {
    Properties conf = WarpConfig.getProperties();
    conf.setProperty("warpscript.maxops", "100000");
    conf.setProperty("warpscript.maxops.hard", "100000");
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, conf);

    //
    // Testing pre = 5, post = 0 with some input and output ticks that match together
    // with input GTS being bucketized
    //

    stack.exec("[ NEWGTS 1 100 <% 2 * DUP 0 0 0 5 ROLL ADDVALUE %> FOR " +
      "bucketizer.last 0 2 0 ] BUCKETIZE " +
      "{ 'mapper' <% 7 GET ->JSON 0 NaN NaN NaN 5 ROLL %> MACROMAPPER " +
      "'pre' 5 'ticks' [ 51 150 <% 2 * %> FOR ] } MAP 0 GET " +

      // output not supposed to be bucketized
      "DUP BUCKETCOUNT  0 == ASSERT " +
      "DUP BUCKETSPAN  0 == ASSERT " +
      "DUP LASTBUCKET  0 == ASSERT " +

      // output ticks must match
      "DUP TICKLIST REVERSE LIST-> 1 SWAP <% 50 + 2 * == ASSERT %> FOR " +

      // sliding windows must match
      "VALUES REVERSE LIST-> DROP 1 51 <% 50 + [ 0 4 <% DUP 3 + PICK + 5 - 2 * %> FOR ] SWAP DROP ->JSON == ASSERT %> FOR " +

      // in the remaining 49 sliding windows, empty values are found
      "[ 194 196 198 200 ] ->JSON == ASSERT " +
      "[ 196 198 200 ] ->JSON == ASSERT " +
      "[ 198 200 ] ->JSON == ASSERT " +
      "[ 200 ] ->JSON == ASSERT " +
      "1 45 <% DROP [] ->JSON == ASSERT %> FOR " +
      "DEPTH 0 == ASSERT");
    stack.clear();
  }


}
