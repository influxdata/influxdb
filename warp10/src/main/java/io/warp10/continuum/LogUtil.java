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

package io.warp10.continuum;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import io.warp10.json.JsonUtils;
import io.warp10.continuum.thrift.data.LoggingEvent;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

public class LogUtil {
  
  public static final String EVENT_CREATION = "event.creation";
  public static final String WARPSCRIPT_SCRIPT = "warpscript.script";
  public static final String WARPSCRIPT_TIMES = "warpscript.times";
  public static final String WARPSCRIPT_UUID = "warpscript.uuid";
  public static final String WARPSCRIPT_SEQNO = "warpscript.seqno";
  
  public static final String STACK_TRACE = "stack.trace";
  public static final String HTTP_HEADERS = "http.headers";
  
  public static final String DELETION_TOKEN = "deletion.token";
  public static final String DELETION_SELECTOR = "deletion.selector";
  public static final String DELETION_START = "deletion.start";
  public static final String DELETION_END = "deletion.end";
  public static final String DELETION_METADATA = "deletion.metadata";
  public static final String DELETION_GTS = "deletion.gts";
  public static final String DELETION_COUNT = "deletion.count";
  
  public static final String DIRECTORY_SELECTOR = "directory.selector";
  public static final String DIRECTORY_REQUEST_TIMESTAMP = "directory.request.timestamp";
  public static final String DIRECTORY_RESULTS = "directory.results";
  public static final String DIRECTORY_NANOS = "directory.nanos";
  public static final String DIRECTORY_METADATA_INSPECTED = "directory.metadata.inspected";
  public static final String DIRECTORY_CLASSES_INSPECTED = "directory.classes.inspected";
  public static final String DIRECTORY_CLASSES_MATCHED = "directory.classes.matched";
  public static final String DIRECTORY_LABELS_COMPARISONS = "directory.labels.comparisons";
  
  private static boolean checkedAESKey = false;
  private static byte[] loggingAESKey = null;
  
  /**
   * Set the attribute of a logging event
   */
  public static final LoggingEvent setLoggingEventAttribute(LoggingEvent event, String name, Object value) {

    event = ensureLoggingEvent(event);
    
    if (null != name && null != value) {
      try {
        event.putToAttributes(name, JsonUtils.objectToJson(value));
      } catch (IOException e) {
        event.putToAttributes(name, "Could not JSONify: " + value);
      }
    }
    
    return event;
  }

  public static final String serializeLoggingEvent(KeyStore keystore, LoggingEvent event) {
    if (null == event) {
      return null;
    }
    
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    
    byte[] serialized = null;
    
    try {
      serialized = serializer.serialize(event);
    } catch (TException te) {
      return null;
    }
    
    if (!checkedAESKey) {
      checkedAESKey = true;
      loggingAESKey = keystore.getKey(KeyStore.AES_LOGGING);      
    }
    if (null != loggingAESKey) {
      serialized = CryptoUtils.wrap(loggingAESKey, serialized);
    }
    
    return new String(OrderPreservingBase64.encode(serialized), StandardCharsets.US_ASCII);
  }
  
  public static final LoggingEvent setLoggingEventStackTrace(LoggingEvent event, String name, Throwable t) {
    
    event = ensureLoggingEvent(event);
    
    if (null == t) {
      return event;
    }
    
    // Fill the stack trace
    
    Object[][] stacktrace = null;
    int offset = 0;
    
    while(null != t) {
      if (null == t.getStackTrace()) {
        t.fillInStackTrace();
      }
      
      StackTraceElement[] ste = t.getStackTrace();

      if (null == stacktrace) {
        stacktrace = new Object[ste.length + 1][];
      } else {
        Object[][] oldtrace = stacktrace;
        
        // Resize stacktrace
        stacktrace = new Object[stacktrace.length + ste.length + 1][];
        
        System.arraycopy(oldtrace, 0, stacktrace, 0, oldtrace.length);
        
        offset = oldtrace.length;
      }
      
      // Fill message
      stacktrace[offset] = new Object[4];
      stacktrace[offset][0] = "";
      stacktrace[offset][1] = 0;
      stacktrace[offset][2] = t.getClass().getName();
      stacktrace[offset][3] = null != t.getMessage() ? t.getMessage() : "";
      offset++;        

      for (int i = 0; i < ste.length; i++) {
        stacktrace[offset+i] = new Object[4];
        stacktrace[offset+i][0] = ste[i].getFileName();
        stacktrace[offset+i][1] = ste[i].getLineNumber();
        stacktrace[offset+i][2] = ste[i].getClassName();
        stacktrace[offset+i][3] = ste[i].getMethodName();
      }

      t = t.getCause();
    }
    
    if (null == event) {
      event = new LoggingEvent();
    }

    try{
      event.putToAttributes(name, JsonUtils.objectToJson(stacktrace));
    } catch (IOException e) {
      event.putToAttributes(name, "Could not JSONify: " + stacktrace);
    }
    
    return event;
  }
  
  private static final LoggingEvent ensureLoggingEvent(LoggingEvent event) {
    if (null == event) {
      event = new LoggingEvent();
    }

    if (0 == event.getAttributesSize() || !event.getAttributes().containsKey(EVENT_CREATION)) {
      event.putToAttributes(EVENT_CREATION, Long.toString(System.currentTimeMillis()));
    }
    
    return event;
  }
  
  public static final LoggingEvent unwrapLog(byte[] key, String logmsg) {    
    try {
      byte[] data = OrderPreservingBase64.decode(logmsg.getBytes(StandardCharsets.US_ASCII));
      
      if (null == data) {
        return null;      
      }
      
      data = CryptoUtils.unwrap(key, data);
      
      if (null == data) {
        return null;
      }
      
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      LoggingEvent event = new LoggingEvent();
      try {
        deserializer.deserialize(event, data);
      } catch (TException te) {
        return null;
      }
      
      return event;      
    } catch (Exception e) {
      return null;
    }
  }
  
  public static final LoggingEvent addHttpHeaders(LoggingEvent event, HttpServletRequest req) {
    
    event = ensureLoggingEvent(event);
    
    //
    // Add request headers
    //
    
    Map<String,Object> headerMap = new HashMap<String,Object>();
    
    Enumeration<String> headerNames = req.getHeaderNames();

    while(headerNames.hasMoreElements()) {
      String name = headerNames.nextElement();
      Enumeration<String> values = req.getHeaders(name);
      List<String> hdrs = new ArrayList<String>();
      while(values.hasMoreElements()) {
        hdrs.add(values.nextElement());
      }
      if (!hdrs.isEmpty()) {
        headerMap.put(name, hdrs);
      }
    }
    
    if (!headerMap.isEmpty()) {
      try {
      event.putToAttributes(LogUtil.HTTP_HEADERS, JsonUtils.objectToJson(headerMap));
      } catch (IOException e) {
        event.putToAttributes(LogUtil.HTTP_HEADERS, "Could not JSONify: " + headerMap);
      }
    }
    
    return event;
  }
}
