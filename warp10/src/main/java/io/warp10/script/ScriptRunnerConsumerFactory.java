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

package io.warp10.script;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.KafkaOffsetCounters;
import io.warp10.continuum.KafkaSynchronizedConsumerPool;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.thrift.data.RunRequest;
import io.warp10.crypto.CryptoUtils;
import io.warp10.sensision.Sensision;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.zip.GZIPInputStream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;

public class ScriptRunnerConsumerFactory implements ConsumerFactory {
  
  private final ScriptRunner runner;
  
  
  public ScriptRunnerConsumerFactory(ScriptRunner runner) {
    this.runner = runner;        
  }
  
  @Override
  public Runnable getConsumer(final KafkaSynchronizedConsumerPool pool, final KafkaStream<byte[], byte[]> stream) {
    
    return new Runnable() {          
      @Override
      public void run() {
        ConsumerIterator<byte[],byte[]> iter = stream.iterator();

        // Iterate on the messages
        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

        KafkaOffsetCounters counters = pool.getCounters();
        
        try {
          while (iter.hasNext()) {
            //
            // Since the call to 'next' may block, we need to first
            // check that there is a message available
            //
            
            boolean nonEmpty = iter.nonEmpty();
            
            if (nonEmpty) {
              MessageAndMetadata<byte[], byte[]> msg = iter.next();
              counters.count(msg.partition(), msg.offset());
              
              byte[] data = msg.message();

              Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_RUNNER_KAFKA_IN_MESSAGES, Sensision.EMPTY_LABELS, 1);
              Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_RUNNER_KAFKA_IN_BYTES, Sensision.EMPTY_LABELS, data.length);
              
              if (null != runner.KAFKA_MAC) {
                data = CryptoUtils.removeMAC(runner.KAFKA_MAC, data);
              }
              
              // Skip data whose MAC was not verified successfully
              if (null == data) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_RUNNER_KAFKA_IN_INVALIDMACS, Sensision.EMPTY_LABELS, 1);
                continue;
              }
              
              // Unwrap data if need be
              if (null != runner.KAFKA_AES) {
                data = CryptoUtils.unwrap(runner.KAFKA_AES, data);
              }
              
              // Skip data that was not unwrapped successfully
              if (null == data) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_RUNNER_KAFKA_IN_INVALIDCIPHERS, Sensision.EMPTY_LABELS, 1);
                continue;
              }

              final RunRequest request = new RunRequest();

              deserializer.deserialize(request, data);

              //
              // Check if running is overdue
              //
              
              long now = System.currentTimeMillis();
              
              if (request.getScheduledAt() + request.getPeriodicity() >= now) {
                continue;
              }
              
              //
              // Decompress script if it is compressed
              //
              
              if (request.isCompressed()) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(request.getContent()));
                
                byte[] buf = new byte[8192];
                
                while(true) {
                  int len = in.read(buf);
                  
                  if (len <= 0) {
                    break;
                  }
                  
                  out.write(buf, 0, len);
                }
                
                in.close();
                out.close();
                
                request.setContent(out.toByteArray());
              }
              
              //
              // Submit script for execution, do up to 3 attempts
              //
              
              int attempts = 3;
              
              while(attempts > 0) {
                try {
                  runner.executor.submit(new Runnable() {            
                    @Override
                    public void run() {
                      
                      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_CURRENT, Sensision.EMPTY_LABELS, 1);
                      
                      Map<String,String> labels = new HashMap<String,String>();
                      labels.put(SensisionConstants.SENSISION_LABEL_PATH, request.getPath());
                      
                      Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_COUNT, labels, 1);

                      long nano = System.nanoTime();
                      
                      HttpURLConnection conn = null;
                      
                      long ops = 0;
                      long fetched = 0;
                      long elapsed = 0;
                      
                      try {
                        conn = (HttpURLConnection) new URL(runner.endpoint).openConnection();
                        
                        conn.setDoOutput(true);
                        conn.setChunkedStreamingMode(8192);
                        conn.setDoInput(true);
                        conn.setRequestMethod("POST");
                        
                        conn.connect();
                        
                        OutputStream out = conn.getOutputStream();
                        
                        //
                        // Push the script parameters
                        //
                        
                        out.write(Long.toString(request.getPeriodicity()).getBytes(StandardCharsets.UTF_8));
                        out.write(' ');
                        out.write('\'');
                        out.write(URLEncoder.encode(Constants.RUNNER_PERIODICITY, StandardCharsets.UTF_8.name()).replaceAll("\\+","%20").getBytes(StandardCharsets.US_ASCII));
                        out.write('\'');
                        out.write(' ');
                        out.write(WarpScriptLib.STORE.getBytes(StandardCharsets.UTF_8));
                        out.write('\n');
                        
                        out.write('\'');
                        out.write(URLEncoder.encode(request.getPath(), StandardCharsets.UTF_8.name()).replaceAll("\\+","%20").getBytes(StandardCharsets.US_ASCII));
                        out.write('\'');
                        out.write(' ');
                        out.write('\'');
                        out.write(URLEncoder.encode(Constants.RUNNER_PATH, StandardCharsets.UTF_8.name()).replaceAll("\\+","%20").getBytes(StandardCharsets.US_ASCII));
                        out.write('\'');
                        out.write(' ');
                        out.write(WarpScriptLib.STORE.getBytes(StandardCharsets.UTF_8));
                        out.write('\n');

                        out.write(Long.toString(request.getScheduledAt()).getBytes(StandardCharsets.UTF_8));
                        out.write(' ');
                        out.write('\'');
                        out.write(URLEncoder.encode(Constants.RUNNER_SCHEDULEDAT, StandardCharsets.UTF_8.name()).replaceAll("\\+","%20").getBytes(StandardCharsets.US_ASCII));
                        out.write('\'');
                        out.write(' ');
                        out.write(WarpScriptLib.STORE.getBytes(StandardCharsets.UTF_8));
                        out.write('\n');

                        byte[] data = request.getContent();
                        
                        if (request.isCompressed()) {
                          ByteArrayInputStream bais = new ByteArrayInputStream(data);
                          GZIPInputStream gzis = new GZIPInputStream(bais);
                          byte[] buf = new byte[1024];
                          
                          while(true) {
                            int len = bais.read(buf);
                            
                            if (len < 0) {
                              break;
                            }
                            
                            out.write(buf, 0, len);
                          }
                          
                          gzis.close();
                        } else {
                          out.write(data, 0, data.length);
                        }
                        
                        // Add a 'CLEAR' at the end of the script so we don't return anything
                        out.write(runner.CLEAR);
                        
                        out.close();
                        
                        if (200 != conn.getResponseCode()) {
                          Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FAILURES, labels, 1);
                        }
                        
                        String header = conn.getRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_ELAPSEDX));
                        if (null != header) {
                          try {
                            elapsed = Long.parseLong(header);
                          } catch (Exception e) {                            
                          }
                        }
                        header = conn.getRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_OPSX));
                        if (null != header) {
                          try {
                            ops = Long.parseLong(header);
                          } catch (Exception e) {                            
                          }
                        }
                        header = conn.getRequestProperty(Constants.getHeader(Configuration.HTTP_HEADER_FETCHEDX));
                        if (null != header) {
                          try {
                            fetched = Long.parseLong(header);
                          } catch (Exception e) {                            
                          }
                        }
                        
                      } catch (Exception e) {                
                        Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FAILURES, labels, 1);
                      } finally {
                        nano = System.nanoTime() - nano;
                        Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_TIME_US, labels, (long) (nano / 1000L));
                        Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_ELAPSED, labels, elapsed);
                        Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_FETCHED, labels, fetched);
                        Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_OPS, labels, ops);
                        Sensision.update(SensisionConstants.SENSISION_CLASS_WARPSCRIPT_RUN_CURRENT, Sensision.EMPTY_LABELS, -1);
                        if (null != conn) { conn.disconnect(); }                                
                      }              
                    }
                  });                  
                  break;
                } catch (RejectedExecutionException ree) {
                  // Reschedule script immediately
                  Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_RUNNER_REJECTIONS, Sensision.EMPTY_LABELS, 1);
                  attempts--;
                }                
              }
              
              if (0 == attempts) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_RUNNER_FAILURES, Sensision.EMPTY_LABELS, 1);
              }
            }
          }        
        } catch (Throwable t) {
          t.printStackTrace(System.err);
        } finally {
          // Set abort to true in case we exit the 'run' method
          pool.getAbort().set(true);
        }
      }
    };
  }
}
