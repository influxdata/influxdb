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
package io.warp10.plugins.udp;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptKillException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackRegistry;
import io.warp10.script.WarpScriptStopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class UDPConsumer extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(UDPConsumer.class);

  private static final int DEFAULT_QSIZE = 1024;
  private static final int DEFAULT_MAXMESSAGES = 1;

  private static final String PARAM_MACRO = "macro";
  private static final String PARAM_PARALLELISM = "parallelism";
  private static final String PARAM_PARTITIONER = "partitioner";
  private static final String PARAM_QSIZE = "qsize";
  private static final String PARAM_HOST = "host";
  private static final String PARAM_PORT = "port";
  private static final String PARAM_TIMEOUT = "timeout";
  private static final String PARAM_MAXMESSAGES = "maxMessages";

  private final MemoryWarpScriptStack stack;
  private final Macro macro;
  private final Macro partitioner;
  private final String host;
  private final long timeout;
  private final int maxMessages;

  private final int parallelism;
  private final int port;

  private boolean done;

  private final String warpscript;

  private final LinkedBlockingQueue<List<Object>>[] queues;

  private Thread[] executors;

  private DatagramSocket socket;

  public UDPConsumer(Path p) throws Exception {
    //
    // Read content of mc2 file
    //

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    InputStream in = new FileInputStream(p.toFile());
    byte[] buf = new byte[8192];

    while (true) {
      int len = in.read(buf);
      if (len < 0) {
        break;
      }
      baos.write(buf, 0, len);
    }

    in.close();

    this.warpscript = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    this.stack = new MemoryWarpScriptStack(null, null, new Properties());
    stack.maxLimits();

    try {
      stack.execMulti(this.warpscript);
    } catch (Throwable t) {
      t.printStackTrace();
      LOG.error("Caught exception while loading '" + p.getFileName() + "'.", t);
    }

    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new RuntimeException("UDP consumer spec must leave a configuration map on top of the stack.");
    }

    Map<Object, Object> config = (Map<Object, Object>) top;

    //
    // Extract parameters
    //

    this.macro = (Macro) config.get(PARAM_MACRO);
    this.partitioner = (Macro) config.get(PARAM_PARTITIONER);
    this.host = String.valueOf(config.get(PARAM_HOST));
    this.port = ((Number) config.get(PARAM_PORT)).intValue();
    this.parallelism = ((Number) config.getOrDefault(PARAM_PARALLELISM, 1)).intValue();
    this.timeout = ((Number) config.getOrDefault(PARAM_TIMEOUT, 0L)).longValue();
    this.maxMessages = ((Number) config.getOrDefault(PARAM_MAXMESSAGES, DEFAULT_MAXMESSAGES)).intValue();

    int qsize = ((Number) config.getOrDefault(PARAM_QSIZE, DEFAULT_QSIZE)).intValue();

    if (null == this.partitioner) {
      this.queues = new LinkedBlockingQueue[1];
      this.queues[0] = new LinkedBlockingQueue<List<Object>>(qsize);
    } else {
      this.queues = new LinkedBlockingQueue[this.parallelism];
      for (int i = 0; i < this.parallelism; i++) {
        this.queues[i] = new LinkedBlockingQueue<List<Object>>(qsize);
      }
    }

    //
    // Create UDP socket
    //
    this.socket = new DatagramSocket(this.port, InetAddress.getByName(this.host));
    this.socket.setReceiveBufferSize(Integer.MAX_VALUE); // Set SO_RCVBUF to /proc/sys/net/core/rmem_max

    this.setDaemon(true);
    this.setName("[UDP Endpoint port " + this.port + "]");
    this.start();
  }

  @Override
  public void run() {

    this.executors = new Thread[this.parallelism];

    for (int i = 0; i < this.parallelism; i++) {

      final MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());
      stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[Warp10UDPPlugin " + socket.getLocalPort() + " #" + i + "]");
      stack.maxLimits();

      final LinkedBlockingQueue<List<Object>> queue = this.queues[Math.min(i, this.queues.length - 1)];

      executors[i] = new Thread() {
        @Override
        public void run() {
          try {
            while (true) {
              try {
                List<List<Object>> msgs = new ArrayList<List<Object>>();

                if (timeout > 0) {
                  List<Object> msg = queue.poll(timeout, TimeUnit.MILLISECONDS);
                  if (null != msg) {
                    msgs.add(msg);
                    queue.drainTo(msgs, maxMessages - 1);
                  }
                } else {
                  List<Object> msg = queue.take();
                  msgs.add(msg);
                  queue.drainTo(msgs, maxMessages - 1);
                }

                stack.clear();

                if (0 < msgs.size()) {
                  stack.push(msgs);
                } else {
                  stack.push(null);
                }

                stack.exec(macro);
              } catch (InterruptedException e) {
                return;
              } catch (WarpScriptKillException wske) {
                return;
              } catch (WarpScriptStopException wsse) {
              } catch (Exception e) {
                e.printStackTrace();
              }
            }            
          } finally {
            WarpScriptStackRegistry.unregister(stack);
          }
        }
      };

      executors[i].setName("[UDP Executor on port " + this.port + " #" + i + "]");
      executors[i].setDaemon(true);
      executors[i].start();
    }

    while (!done) {
      try {

        byte[] buf = new byte[65507];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        this.socket.receive(packet);

        try {
          int queueIndex = 0;
          
          byte[] payload = Arrays.copyOfRange(packet.getData(), packet.getOffset(), packet.getOffset() + packet.getLength());
          
          // Apply the partitioning macro if it is defined
          if (null != this.partitioner) {
            this.stack.clear();
            this.stack.push(packet.getAddress().getHostAddress());
            this.stack.push((long) packet.getPort());
            this.stack.push(payload);
            this.stack.exec(this.partitioner);
            int seq = ((Number) this.stack.pop()).intValue();
            queueIndex = seq % this.parallelism;
          }

          ArrayList<Object> msg = new ArrayList<Object>();
          msg.add(packet.getAddress().getHostAddress());
          msg.add(packet.getPort());
          msg.add(payload);

          this.queues[queueIndex].put(msg);

        } catch (Exception e) {
          // Ignore exceptions
        }
      } catch (SocketException se) {
        // Closed socket
      } catch (Exception e) {
        LOG.error("Caught exception while receiving message", e);
      }
    }

  }

  public void end() {
    this.done = true;
    try {
      this.socket.close();
    } catch (Exception e) {
    }

    for (Thread t: this.executors) {
      t.interrupt();
    }
  }

  public String getWarpScript() {
    return this.warpscript;
  }
}
