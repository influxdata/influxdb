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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;

/**
 * Utility class to convert to/from Writables
 */
public class WritableUtils {
  
  private static final boolean strictWritables;
  private static final boolean rawWritables;
  
  static {
    if ("true".equals(WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_HADOOP_STRICTWRITABLES))) {
      strictWritables = true;
    } else {
      strictWritables = false;
    }
    
    if ("true".equals(WarpConfig.getProperty(Configuration.CONFIG_WARPSCRIPT_HADOOP_RAWWRITABLES))) {
      rawWritables = true;
    } else {
      rawWritables = false;
    }  
  }
  
  public static Object fromWritable(Object w) throws IOException {
    
    if (!(w instanceof Writable) || rawWritables) {
      return w;
    }
  
    if (w instanceof Text) {
      return ((Text) w).toString();
    } else if (w instanceof BytesWritable) {
      return ((BytesWritable) w).copyBytes();
    } else if (w instanceof NullWritable) {
      return null;
    } else if (w instanceof LongWritable) {
      return ((LongWritable) w).get();
    } else if (w instanceof IntWritable) {
      return (long) ((IntWritable) w).get();
    } else if (w instanceof ByteWritable) {
      return (long) ((ByteWritable) w).get();
    } else if (w instanceof ShortWritable) {
      return (long) ((ShortWritable) w).get();
    } else if (w instanceof ArrayWritable) {
      Writable[] a = ((ArrayWritable) w).get();
      List<Object> l = new ArrayList<Object>();
      for (Writable ww: a) {
        l.add(fromWritable(ww));
      }
      return l;
    } else if (w instanceof BooleanWritable) {
      return ((BooleanWritable) w).get();
    } else if (w instanceof DoubleWritable) {
      return ((DoubleWritable) w).get();
    } else if (w instanceof FloatWritable) {
      return (float) ((FloatWritable) w).get();
    } else if (w instanceof MapWritable) {
      Map<Object,Object> map = new HashMap<Object,Object>();
      for (Entry<Writable,Writable> entry: ((MapWritable) w).entrySet()) {
        map.put(fromWritable(entry.getKey()), fromWritable(entry.getValue()));
      }
      return map;
    } else if (w instanceof ObjectWritable) {
      return ((ObjectWritable) w).get();
    } else if (w instanceof GenericWritable) {
      return fromWritable(((GenericWritable) w).get());
    } else if (w instanceof SortedMapWritable) {
      Map<Object,Object> map = new LinkedHashMap<Object,Object>();
      for (Entry<WritableComparable,Writable> entry: ((SortedMapWritable) w).entrySet()) {
        map.put(fromWritable(entry.getKey()), fromWritable(entry.getValue()));        
      }
      return map;
    } else if (w instanceof VIntWritable) {
      return (long) ((VIntWritable) w).get();
    } else if (w instanceof VLongWritable) {
      return ((VLongWritable) w).get();
    } else {
      if (strictWritables) {
        throw new IOException("Unsupported Writable implementation " + w.getClass());
      } else {
        return w;
      }
    }  
  }
  
  public static Writable toWritable(Object o) throws IOException {
    if (o instanceof Long) {
      return new LongWritable(((Long) o).longValue());
    } else if (o instanceof String) {
      return new Text(o.toString());
    } else if (o instanceof byte[]) {
      return new BytesWritable((byte[]) o);
    } else if (o instanceof Integer) {
      return new IntWritable(((Integer) o).intValue());
    } else if (o instanceof Short) {
      return new ShortWritable(((Short) o).shortValue());
    } else if (o instanceof Byte) {
      return new ByteWritable(((Byte) o).byteValue());
    } else if (o instanceof Double) {
      return new DoubleWritable(((Double) o).doubleValue());
    } else if (o instanceof Float) {
      return new FloatWritable(((Float) o).floatValue());
    } else if (o instanceof Boolean) {
      return new BooleanWritable(((Boolean) o).booleanValue());
    } else if (o instanceof List) {
      Writable[] a = new Writable[((List) o).size()];
      for (int i = 0; i < a.length; i++) {
        a[i] = new ObjectWritable(toWritable(((List) o).get(i)));
      }
      return new ArrayWritable(ObjectWritable.class, a);
    } else if (o instanceof Map) {
      MapWritable map = new MapWritable();
      for (Entry<Object,Object> entry: ((Map<Object,Object>) o).entrySet()) {
        map.put(toWritable(entry.getKey()), toWritable(entry.getValue()));
      }
      return map;
    } else if (null == o) {
      return NullWritable.get();
    } else {
      ObjectWritable ow = new ObjectWritable();
      ow.set(o);
      return ow;
    }// else {
    //  throw new IOException("Unsupported type " + o.getClass());
    //}
  }
}
