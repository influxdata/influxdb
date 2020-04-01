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

package io.warp10.plasma;

public class Dumper {

  private final String fromurl;
  private final String tourl;
  
  private final String fromtoken;
  private final String totoken;
  
  private final String fromselector;
  
  public Dumper(String fromurl, String fromtoken, String fromselector, String tourl, String totoken) {
    this.fromurl = fromurl;
    this.fromtoken = fromtoken;
    this.fromselector = fromselector;
    this.tourl = tourl;
    this.totoken = totoken;
  }
  
  public static void main(String[] args) {
    String fromurl = System.getProperty("from.url");
    String tourl = System.getProperty("to.url");
    String fromtoken = System.getProperty("from.token");
    String totoken = System.getProperty("to.token");
    String fromselector = System.getProperty("from.selector");
    
    Dumper d = new Dumper(fromurl, fromtoken, fromselector, tourl, totoken);
  }

}
