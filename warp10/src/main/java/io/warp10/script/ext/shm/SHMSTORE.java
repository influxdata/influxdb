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
import io.warp10.script.WarpScriptStackFunction;

public class SHMSTORE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  public SHMSTORE(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {    
    Object mutexo = stack.getAttribute(MUTEX.MUTEX_ATTRIBUTE + stack.getUUID());
    
    if (null == mutexo) {
      throw new WarpScriptException(getName() + " can only be called when in a MUTEX section.");
    }
    
    String mutex = String.valueOf(mutexo);
    
    Object top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a symbol name on top of the stack.");
    }
    
    String symbol = String.valueOf(top);
    
    top = stack.pop();
    
    ReentrantLock lock = SharedMemoryWarpScriptExtension.getLock(mutex);
    
    if (!lock.isHeldByCurrentThread()) {
      throw new WarpScriptException(getName() + " expects the mutex '" + mutex + "' to be held when calling " + getName());
    }
    
    SharedMemoryWarpScriptExtension.store(symbol, mutex, top);

    return stack;
  }
}
