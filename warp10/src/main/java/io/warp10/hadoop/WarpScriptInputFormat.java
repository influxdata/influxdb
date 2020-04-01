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
package io.warp10.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptExecutor;
import io.warp10.script.WarpScriptExecutor.StackSemantics;

/**
 * This InputFormat wraps another InputFormat and creates a RecordReader
 * which returns K/V processed by some WarpScript™ code.
 */
public class WarpScriptInputFormat extends InputFormat<Object, Object> {

  /**
   * Name of symbol under which the Hadoop Configuration will be made available
   * to the executing script.
   */
  private static final String CONFIG_SYMBOL = ".conf";
  
  /**
   * Configuration key we will set to the current path if the split if a FileSplit
   */
  public static final String PATH_CONFIG_KEY = ".path";
  
  /**
   * Suffix to use for the configuration
   */
  public static final String WARPSCRIPT_INPUTFORMAT_SUFFIX = "warpscript.inputformat.suffix";
  
  /**
   * Class of the wrapped InputFormat
   */
  public static final String WARPSCRIPT_INPUTFORMAT_CLASS = "warpscript.inputformat.class";
  
  /**
   * WarpScript™ code fragment to apply
   */
  public static final String WARPSCRIPT_INPUTFORMAT_SCRIPT = "warpscript.inputformat.script";

  /**
   * Suffix to remove from configuration keys to override or create specific configuration entries
   */
  public static final String WARPSCRIPT_INPUTFORMAT_CONF_SUFFIX = "warpscript.inputformat.conf.suffix";
  
  private InputFormat wrappedInputFormat;
  private RecordReader wrappedRecordReader;
    
  private String suffix = "";
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,InterruptedException {
    String sfx = Warp10InputFormat.getProperty(context.getConfiguration(), this.suffix, WARPSCRIPT_INPUTFORMAT_SUFFIX, "");
    if (null != sfx) {
      if (!"".equals(sfx)) {
        this.suffix = "." + sfx;
      } else {
        this.suffix = "";
      }
    }

    ensureInnerFormat(context.getConfiguration());

    return this.wrappedInputFormat.getSplits(context); 
  }
  
  private void ensureInnerFormat(Configuration conf) throws IOException {
    if (null == this.wrappedInputFormat) {
      try {
        String cls = Warp10InputFormat.getProperty(conf, this.suffix, WARPSCRIPT_INPUTFORMAT_CLASS, null);
        
        //
        // Tweak the configuration if a conf suffix was specified
        //
        
        String confsfx = Warp10InputFormat.getProperty(conf, this.suffix, WARPSCRIPT_INPUTFORMAT_CONF_SUFFIX, "");
        
        if (!"".equals(confsfx)) {
          confsfx = "." + confsfx;
          List<Entry<String,String>> keys = new ArrayList<Entry<String,String>>();
          Iterator<Entry<String,String>> iter = conf.iterator();
          while(iter.hasNext()) {
            Entry<String,String> entry = iter.next();
            
            if (entry.getKey().endsWith(confsfx)) {
              keys.add(entry);
            }
          }
          
          // Override or create the unsuffixed configuration parameters
          for (Entry<String,String> entry: keys) {
            String key = entry.getKey().substring(0, entry.getKey().length() - confsfx.length());
            conf.set(key, entry.getValue());
          }
        }

        Class innerClass = Class.forName(cls);
        this.wrappedInputFormat = (InputFormat) innerClass.newInstance();        
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }
  }
  
  @Override
  public RecordReader<Object, Object> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    if (null == this.wrappedRecordReader) {
      ensureInnerFormat(context.getConfiguration());
      this.wrappedRecordReader = this.wrappedInputFormat.createRecordReader(split, context);
    }

    return new WarpScriptRecordReader(this);
  }
  
  /**
   * Return the actual WarpScript code executor given the script
   * which was passed as parameter.
   */
  public WarpScriptExecutor getWarpScriptExecutor(Configuration conf, String code) throws IOException,WarpScriptException {
    if (code.startsWith("@") || code.startsWith("%")) {

      //
      // delete the @/% character
      //

      String originalfilePath = code.substring(1);

      String mc2 = parseWarpScript(originalfilePath);
      
      Map<String,Object> symbols = new HashMap<String,Object>();
      Map<String, List<String>> config = new HashMap<String,List<String>>();
      
      Iterator<Entry<String,String>> iter = conf.iterator();
      
      while(iter.hasNext()) {
        Entry<String,String> entry = iter.next();
        List<String> target = config.get(entry.getKey());
        if (null == target) {
          target = new ArrayList<String>();
          config.put(entry.getKey(), target);
        }
        target.add(entry.getValue());
      }
      
      symbols.put(CONFIG_SYMBOL, config);
      
      WarpScriptExecutor executor = new WarpScriptExecutor(StackSemantics.PERTHREAD, mc2, symbols, null, code.startsWith("@"));
      return executor;
    } else {

      //
      // String with Warpscript commands
      //

      //
      // Compute the hash against String content to identify this run
      //

      WarpScriptExecutor executor = new WarpScriptExecutor(StackSemantics.PERTHREAD, code, null, null);
      return executor;
    }

  }
  
  public String parseWarpScript(String filepath) throws IOException {
    //
    // Load the WarpsScript file
    // Warning: provide target directory when file has been copied on each node
    //
    StringBuffer scriptSB = new StringBuffer();
    InputStream fis = null;
    BufferedReader br = null;
    try {      
      fis = getWarpScriptInputStream(filepath);
      
      br = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

      while (true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }
        scriptSB.append(line).append("\n");
      }
    } catch (IOException ioe) {
      throw new IOException("WarpScript file could not be loaded", ioe);
    } finally {
      if (null == br) { try { br.close(); } catch (Exception e) {} }
      if (null == fis) { try { fis.close(); } catch (Exception e) {} }
    }

    return scriptSB.toString();
  }
  
  /**
   * Create an InputStream from a file path.
   * 
   * This method can be overridden if custom loading is needed. In Spark for
   * example SparkFiles#get could be called.
   */
  public InputStream getWarpScriptInputStream(String originalFilePath) throws IOException {
    String filepath = Paths.get(originalFilePath).toString();

    InputStream fis = WarpScriptInputFormat.class.getClassLoader().getResourceAsStream(filepath);

    if (null == fis) {
      fis = new FileInputStream(filepath);
    }
    
    if (null == fis) {
      throw new IOException("WarpScript file '" + filepath + "' could not be found.");
    }
    
    return fis;
  }
  
  public String getSuffix() {
    return this.suffix;
  }
  
  public RecordReader getWrappedRecordReader() {
    return this.wrappedRecordReader;
  }
}
