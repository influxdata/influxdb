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

import java.util.Map;

public class MapSpecification extends ArgumentSpecification {

  private Class<?> classOfKey;
  private Class<?> classOfValue;

  public Class<?> getClassOfKey() { return classOfKey; }
  public Class<?> getClassOfValue() { return classOfValue; }

  public MapSpecification(Class classOfKey, Class classOfValue, String name, Object default_value, String doc) {
    super(Map.class, name, default_value, doc);
    this.classOfKey = classOfKey;
    this.classOfValue = classOfValue;
  }

  public MapSpecification(Class classOfKey, Class classOfValue, String name, String doc) {
    super(Map.class, name, doc);
    this.classOfKey = classOfKey;
    this.classOfValue = classOfValue;
  }

  public MapSpecification(Class classOfKey, Class classOfValue, String name, Object default_value) {
    super(Map.class, name, default_value);
    this.classOfKey = classOfKey;
    this.classOfValue = classOfValue;
  }

  public MapSpecification(Class classOfKey, Class classOfValue, String name) {
    super(Map.class, name);
    this.classOfKey = classOfKey;
    this.classOfValue = classOfValue;
  }
}
