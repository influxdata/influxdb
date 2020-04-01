//
//   Copyright 2020  SenX S.A.S.
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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.warp10.script.WarpScriptStack.Signal;
import io.warp10.script.ext.stackps.StackPSWarpScriptExtension;

public class WarpScriptStackRegistry {
  
  private static final Map<String,WeakReference<WarpScriptStack>> stacks = new HashMap<String,WeakReference<WarpScriptStack>>();
  
  private static boolean enabled = false;
  
  public static void register(WarpScriptStack stack) {
    if (!enabled || null == stack) {
      return;
    }
    stacks.put(stack.getUUID(), new WeakReference<WarpScriptStack>(stack));
  }
  
  public static boolean unregister(WarpScriptStack stack) {
    if (!enabled || null == stack) {
      return false;
    }
    return null != stacks.remove(stack.getUUID());
  }
  
  public static boolean unregister(String uuid) {
    return null != stacks.remove(uuid);
  }
  
  public static boolean signalByUuid(String uuid, Signal signal) {
    if (!enabled) {
      return false;
    }
    
    WeakReference<WarpScriptStack> stackref = stacks.get(uuid);
    
    if (null == stackref) {
      return false;
    }
    
    WarpScriptStack stack = stackref.get();
    
    if (null == stack) {
      return false;
    }
    stack.signal(signal);
    return true;
  }
  
  public static int signalBySession(String session, Signal signal) {
    if (null == session) {
      return 0;
    }
    
    List<WeakReference<WarpScriptStack>> refs = new ArrayList<WeakReference<WarpScriptStack>>(stacks.values());
    
    int aborted = 0;
    
    for (WeakReference<WarpScriptStack> ref: refs) {
      WarpScriptStack stack = ref.get();
      
      if (null != stack) {
        if (session.equals(stack.getAttribute(StackPSWarpScriptExtension.ATTRIBUTE_SESSION))) {
          stack.signal(signal);
          aborted++;
        }
      }
    }
    
    return aborted;
  }
  
  public static void enable() {
    enabled = true;
  }
  
  public static void disable() {
    // Clear the registered stacks
    stacks.clear();
    enabled = false;
  }
  
  public static List<WarpScriptStack> stacks() {
    List<WarpScriptStack> stacks = new ArrayList<WarpScriptStack>(WarpScriptStackRegistry.stacks.size());
    
    for (WeakReference<WarpScriptStack> ref: WarpScriptStackRegistry.stacks.values()) {
      WarpScriptStack stack = ref.get();
      if (null != stack) {
        stacks.add(stack);
      }
    }
    
    return stacks;
  }  
}
