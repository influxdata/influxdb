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
package io.warp10.plugins.tcp;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPClient implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(TCPClient.class);

  private final Socket socket;
  private final Macro partitioner;

  private final BufferedReader reader;
  private final LinkedBlockingQueue<List<Object>>[] queues;

  private final String remoteHost;
  private final int remotePort;

  private final MemoryWarpScriptStack stack;

  TCPClient(Socket socket, Macro partitioner, LinkedBlockingQueue<List<Object>>[] queues, String charset) throws IOException {
    this.socket = socket;
    this.partitioner = partitioner;
    this.queues = queues;

    // TODO(tce): set socket timeout?

    remoteHost = this.socket.getInetAddress().getHostAddress();
    remotePort = this.socket.getPort();

    this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), charset));
    this.stack = new MemoryWarpScriptStack(null, null, new Properties());
    this.stack.setAttribute(WarpScriptStack.ATTRIBUTE_NAME, "[Warp10TCPPlugin " + socket.getLocalPort() + "]");
    stack.maxLimits();
  }

  @Override
  public void run() {
    String line = null;
    try {
      while (!Thread.currentThread().isInterrupted() && null != (line = this.reader.readLine())) {
        try {
          int queueIndex = 0;
          // Apply the partitioning macro if it is defined
          if (null != this.partitioner) {
            this.stack.clear();
            this.stack.push(socket.getInetAddress().getHostAddress());
            this.stack.push((long) socket.getPort());
            this.stack.push(line);
            this.stack.exec(this.partitioner);
            int seq = ((Number) this.stack.pop()).intValue();
            queueIndex = seq % queues.length;
          }

          ArrayList<Object> msg = new ArrayList<Object>();
          msg.add(remoteHost);
          msg.add(remotePort);
          msg.add(line);

          this.queues[queueIndex].put(msg);
        } catch (Exception e) {
          LOG.error("Partitioner failed.", e);
        }
      }
    } catch (IOException e) {
      LOG.error("Problem when receiving text line from tcp on port " + socket.getPort(), e);
    } finally {
      WarpScriptStackRegistry.unregister(stack);
    }
    try {
      this.socket.close();
    } catch (Exception e) {
    }
  }

}
