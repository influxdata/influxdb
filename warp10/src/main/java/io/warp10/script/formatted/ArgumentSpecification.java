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

public class ArgumentSpecification {

  private Class<?> clazz;
  private String name;
  private Object default_value; // optional
  private String doc; // used for auto-generated doc

  public Class<?> getClazz() { return clazz; }
  public String getName() { return name; }
  public Object getDefaultValue() { return default_value; }
  public String getDoc() { return doc; }

  public boolean isOptional() { return null != default_value; }
  public boolean isDocumented() {
    return  null != doc;
  }

  public ArgumentSpecification(Class clazz, String name, Object default_value, String doc) {
    if (!(clazz.isInstance(default_value))) {
      throw new ExceptionInInitializerError("Default value is not of the correct class");
    }

    this.clazz = clazz;
    this.name = name;
    this.default_value = default_value;
    this.doc = doc;
  }

  public ArgumentSpecification(Class clazz, String name, String doc) {
    this.clazz = clazz;
    this.name = name;
    this.default_value = null;
    this.doc = doc;
  }

  public ArgumentSpecification(Class clazz, String name, Object default_value) {
    if (!(clazz.isInstance(default_value))) {
      throw new ExceptionInInitializerError("Default value is not of the correct class");
    }

    this.clazz = clazz;
    this.name = name;
    this.default_value = default_value;
    this.doc = null;
  }

  public ArgumentSpecification(Class clazz, String name) {
    this.clazz = clazz;
    this.name = name;
    this.default_value = null;
    this.doc = null;
  }

  public String WarpScriptType() {
    return TYPEOF.typeof(clazz);
  }
}
