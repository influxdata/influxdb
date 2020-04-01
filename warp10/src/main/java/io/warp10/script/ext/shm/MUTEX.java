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

package io.warp10.script.ext.shm;

import java.util.concurrent.locks.ReentrantLock;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStackFunction;

public class MUTEX extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
  static final String MUTEX_ATTRIBUTE = "ext.shm.mutex";
  
  public MUTEX(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    if(null != stack.getAttribute(MUTEX_ATTRIBUTE + stack.getUUID())) {
      throw new WarpScriptException(getName() + " calls cannot be nested.");
    }
    
    Object top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the mutex name on top of the stack.");
    }
    
    String mutex = String.valueOf(top);
    
    top = stack.pop();
    
    if (!(top instanceof Macro)) {
      throw new WarpScriptException(getName() + " expects the macro to run below the mutex name.");
    }
    
    Macro macro = (Macro) top;
    
    ReentrantLock lock = SharedMemoryWarpScriptExtension.getLock(mutex);
        
    try {
      lock.lockInterruptibly();
      stack.setAttribute(MUTEX_ATTRIBUTE + stack.getUUID(), mutex);
      stack.exec(macro);
    } catch (WarpScriptException wse) {
      throw wse;
    } catch (Throwable t) {
      throw new WarpScriptException("Error while running mutex macro.", t);
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
        stack.setAttribute(MUTEX_ATTRIBUTE + stack.getUUID(), null);
      }
    }
    
    return stack;
  }
}
