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
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TCPManager extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(TCPManager.class);

  private static final int DEFAULT_RETRY = 30000;
  private static final int DEFAULT_QSIZE = 1024;
  private static final int DEFAULT_MAXMESSAGES = 1;
  private static final int DEFAULT_MAXCONNECTIONS = 1;
  private static final int DEFAULT_TCP_BACKLOG = 0;
  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private static final String PARAM_MODE = "mode";
  private static final String PARAM_RETRY = "retry";
  private static final String PARAM_MACRO = "macro";
  private static final String PARAM_PARALLELISM = "parallelism";
  private static final String PARAM_PARTITIONER = "partitioner";
  private static final String PARAM_QSIZE = "qsize";
  private static final String PARAM_HOST = "host";
  private static final String PARAM_PORT = "port";
  private static final String PARAM_TCP_BACKLOG = "tcpBacklog";
  private static final String PARAM_TIMEOUT = "timeout";
  private static final String PARAM_MAXMESSAGES = "maxMessages";
  private static final String PARAM_MAXCONNECTIONS = "maxConnections";
  private static final String PARAM_CHARSET = "charset";

  private final MemoryWarpScriptStack stack;
  private final String mode;
  private final long retry;
  private final Macro macro;
  private final Macro partitioner;
  private final String host;
  private final long timeout;
  private final int maxMessages;
  private final int maxConnections;
  private final String charset;

  private final int parallelism;
  private final int port;
  private final int tcpBacklog;

  private Thread[] executors;

  private boolean done;

  private final String warpscript;

  private final LinkedBlockingQueue<List<Object>>[] queues;

  private ServerSocket serverSocket;

  private ThreadPoolExecutor clientsExecutor;

  private Socket clientSocket;

  public TCPManager(Path p) throws Exception {
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

    warpscript = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    stack = new MemoryWarpScriptStack(null, null, new Properties());
    stack.maxLimits();

    try {
      stack.execMulti(warpscript);
    } catch (Throwable t) {
      t.printStackTrace();
      LOG.error("Caught exception while loading '" + p.getFileName() + "'.", t);
    }

    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new RuntimeException("TCP consumer spec must leave a configuration map on top of the stack.");
    }

    Map<Object, Object> config = (Map<Object, Object>) top;

    //
    // Extract parameters
    //

    mode = (String) config.get(PARAM_MODE);
    retry = ((Number) config.getOrDefault(PARAM_RETRY, DEFAULT_RETRY)).longValue();
    macro = (Macro) config.get(PARAM_MACRO);
    partitioner = (Macro) config.get(PARAM_PARTITIONER);
    host = String.valueOf(config.get(PARAM_HOST));
    port = ((Number) config.get(PARAM_PORT)).intValue();
    tcpBacklog = ((Number) config.getOrDefault(PARAM_TCP_BACKLOG, DEFAULT_TCP_BACKLOG)).intValue();
    parallelism = ((Number) config.getOrDefault(PARAM_PARALLELISM, 1)).intValue();
    timeout = ((Number) config.getOrDefault(PARAM_TIMEOUT, 0L)).longValue();
    maxMessages = ((Number) config.getOrDefault(PARAM_MAXMESSAGES, DEFAULT_MAXMESSAGES)).intValue();
    maxConnections = ((Number) config.getOrDefault(PARAM_MAXCONNECTIONS, DEFAULT_MAXCONNECTIONS)).intValue();
    charset = String.valueOf(config.getOrDefault(PARAM_CHARSET, DEFAULT_CHARSET.name()));

    int qsize = ((Number) config.getOrDefault(PARAM_QSIZE, DEFAULT_QSIZE)).intValue();

    if (null == partitioner) {
      queues = new LinkedBlockingQueue[1];
      queues[0] = new LinkedBlockingQueue<List<Object>>(qsize);
    } else {
      queues = new LinkedBlockingQueue[parallelism];
      for (int i = 0; i < parallelism; i++) {
        queues[i] = new LinkedBlockingQueue<List<Object>>(qsize);
      }
    }

    initExecutors();

    // Create server or client socket
    if ("server".equals(mode)) {
      serverSocket = new ServerSocket(port, tcpBacklog, InetAddress.getByName(host));
    } else if (!"client".equals(mode)) {
      throw new RuntimeException("Mode must be either server or client.");
    }

    done = false;

    clientsExecutor = new ThreadPoolExecutor(maxConnections, maxConnections, 30000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(maxConnections), new NamedThreadFactory("Warp TCP Client for port " + port));
    clientsExecutor.allowCoreThreadTimeOut(true);

    setDaemon(true);
    setName("[TCP Server for port " + port + "]");
    start();
  }

  private void initExecutors() {

    executors = new Thread[parallelism];

    for (int i = 0; i < parallelism; i++) {

      final MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());
      stack.maxLimits();

      final LinkedBlockingQueue<List<Object>> queue = queues[Math.min(i, queues.length - 1)];

      executors[i] = new Thread() {
        @Override
        public void run() {
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
            } catch (WarpScriptStopException wsse) {
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      };

      executors[i].setName("[TCP Executor on port " + port + " #" + i + "]");
      executors[i].setDaemon(true);
      executors[i].start();
    }
  }

  @Override
  public void run() {
    while (!done) {
      try {
        if (null != serverSocket) {
          clientSocket = serverSocket.accept();
        } else if (null == clientSocket || clientSocket.isClosed()) {
          try {
            clientSocket = new Socket(host, port);
          } catch (SocketException | UnknownHostException e) {
            // Retry later
            LockSupport.parkNanos(retry * 1000000);
            continue;
          }
        } else {
          // Everything is fine, check later if it still is
          LockSupport.parkNanos(retry * 1000000);
          continue;
        }

        // Execute a new TCPClient with the new Socket
        try {
          clientsExecutor.execute(new TCPClient(clientSocket, partitioner, queues, charset));
        } catch (RejectedExecutionException ree) {
          // If there are too many connections, immediately close this one.
          clientSocket.close();
        }
      } catch (SocketException se) {
        // Closed socket
      } catch (IOException e) {
        LOG.error("Caught exception while receiving message", e);
      }
    }
    clientsExecutor.shutdownNow();
    try {
      serverSocket.close();
    } catch (Exception e) {
    }
  }

  public void end() {
    done = true;

    if (null != serverSocket) {
      try {
        serverSocket.close();
      } catch (IOException e) {
      }
    }

    if (null != clientSocket) {
      try {
        clientSocket.close();
      } catch (IOException e) {
      }
    }

    for (Thread t: executors) {
      t.interrupt();
    }
  }

  public String getWarpScript() {
    return warpscript;
  }
}
