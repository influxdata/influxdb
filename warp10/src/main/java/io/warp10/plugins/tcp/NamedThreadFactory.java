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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class NamedThreadFactory implements ThreadFactory {
  private final ThreadGroup group;
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String name;

  public NamedThreadFactory(String name) {
    this.name = name;

    SecurityManager s = System.getSecurityManager();
    if (null == s) {
      group = Thread.currentThread().getThreadGroup();
    } else {
      group = s.getThreadGroup();
    }
  }

  public Thread newThread(Runnable r) {
    Thread t = new Thread(group, r, "[" + this.name + " #" + threadNumber.getAndIncrement() + "]", 0);

    if (t.isDaemon()) {
      t.setDaemon(false);
    }

    if (Thread.NORM_PRIORITY != t.getPriority()) {
      t.setPriority(Thread.NORM_PRIORITY);
    }

    return t;
  }
}