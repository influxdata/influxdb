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

import io.warp10.warp.sdk.AbstractWarp10Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

public class UDPWarp10Plugin extends AbstractWarp10Plugin implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(UDPWarp10Plugin.class);

  /**
   * Directory where spec files are located
   */
  private static final String CONF_UDP_DIR = "udp.dir";

  /**
   * Period at which to scan the spec directory
   */
  private static final String CONF_UDP_PERIOD = "udp.period";

  /**
   * Default scanning period in ms
   */
  private static final long DEFAULT_PERIOD = 60000L;

  private String dir;
  private long period;

  /**
   * Map of spec file to UDPConsumer instance
   */
  private Map<String, UDPConsumer> consumers = new HashMap<String, UDPConsumer>();

  private boolean done = false;

  public UDPWarp10Plugin() {
    super();
  }

  @Override
  public void run() {
    while (!done) {
      try {
        Iterator<Path> iter = null;
        try {
          iter = Files.walk(new File(dir).toPath(), FileVisitOption.FOLLOW_LINKS)
              //.filter(path -> path.toString().endsWith(".mc2"))
              .filter(new Predicate<Path>() {
                @Override
                public boolean test(Path t) {
                  return t.toString().endsWith(".mc2");
                }
              })
              .iterator();
        } catch (NoSuchFileException nsfe) {
          LOG.warn("UDP plugin could not find directory " + dir);
        }

        Set<String> specs = new HashSet<String>();

        while (null != iter && iter.hasNext()) {
          Path p = iter.next();

          boolean load = false;

          if (this.consumers.containsKey(p.toString())) {
            if (this.consumers.get(p.toString()).getWarpScript().length() != p.toFile().length()) {
              load = true;
            }
          } else {
            // This is a new spec
            load = true;
          }

          if (load) {
            load(p);
          }
          specs.add(p.toString());
        }

        //
        // Clean the specs which disappeared
        //

        Set<String> removed = new HashSet<String>(this.consumers.keySet());
        removed.removeAll(specs);

        for (String spec: removed) {
          try {
            consumers.remove(spec).end();
          } catch (Exception e) {
          }
        }
      } catch (Throwable t) {
        t.printStackTrace();
      }

      LockSupport.parkNanos(this.period * 1000000L);
    }
  }

  /**
   * Load a spec file
   *
   * @param p
   */
  private boolean load(Path p) {

    //
    // Stop the current UDPConsumer if it exists
    //

    UDPConsumer consumer = consumers.get(p.toString());

    if (null != consumer) {
      consumer.end();
    }

    try {
      consumer = new UDPConsumer(p);
    } catch (Exception e) {
      return false;
    }

    consumers.put(p.toString(), consumer);

    return true;
  }

  @Override
  public void init(Properties properties) {
    this.dir = properties.getProperty(CONF_UDP_DIR);

    if (null == this.dir) {
      throw new RuntimeException("Missing '" + CONF_UDP_DIR + "' configuration.");
    }

    this.period = Long.parseLong(properties.getProperty(CONF_UDP_PERIOD, Long.toString(DEFAULT_PERIOD)));

    //
    // Register shutdown hook
    //

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        done = true;
        System.out.println("UDP Plugin shutting down all consumers.");
        this.interrupt();
        for (UDPConsumer consumer: consumers.values()) {
          try {
            consumer.end();
          } catch (Exception e) {
          }
        }
      }
    });

    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("[Warp 10 UDP Plugin " + this.dir + "]");
    t.start();
  }
}
