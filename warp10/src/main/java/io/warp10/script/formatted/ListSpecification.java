//
//   Copyright 2019  SenX S.A.S.
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

package io.warp10.script.formatted;

import io.warp10.script.functions.TYPEOF;

import java.util.List;

public class ListSpecification extends ArgumentSpecification {

  private Class<?> subClazz;
  public Class<?> getSubClazz() { return subClazz; }

  public ListSpecification(Class clazz, String name, Object default_value, String doc) {
    super(List.class, name, default_value, doc);
    subClazz = clazz;
  }

  public ListSpecification(Class clazz, String name, String doc) {
    super(List.class, name, doc);
    subClazz = clazz;
  }

  public ListSpecification(Class clazz, String name, Object default_value) {
    super(List.class, name, default_value);
    subClazz = clazz;
  }

  public ListSpecification(Class clazz, String name) {
    super(List.class, name);
    subClazz = clazz;
  }

  public String WarpScriptSubType() {
    return TYPEOF.typeof(subClazz);
  }
}
