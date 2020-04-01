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
package io.warp10.standalone;

import java.io.File;
import java.io.IOException;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import io.warp10.continuum.Configuration;

public class WarpRepair {
  
  public static final String DISABLE_CHECKSUMS = "disable.checksums";
  public static final String DISABLE_PARANOIDCHECKS = "disable.paranoidchecks";
  
  public static void main(String[] args) throws IOException {
    String path = args[0];

    Options options = new Options();
    options.createIfMissing(false);
    options.maxOpenFiles(200);
    options.verifyChecksums(!("true".equals(System.getProperty(DISABLE_CHECKSUMS))));
    options.paranoidChecks(!("true".equals(System.getProperty(DISABLE_PARANOIDCHECKS))));
    
    boolean nativedisabled = "true".equals(System.getProperty(Configuration.LEVELDB_NATIVE_DISABLE));
    boolean javadisabled = "true".equals(System.getProperty(Configuration.LEVELDB_JAVA_DISABLE));

    repair(path, options, javadisabled, nativedisabled);
  }
  
  public static void repair(String path, Options options, boolean javadisabled, boolean nativedisabled) throws IOException {    
    try {
      if (!nativedisabled) {
        JniDBFactory.factory.repair(new File(path), options);
      } else {
        throw new UnsatisfiedLinkError("Native LevelDB implementation disabled.");
      }
    } catch (UnsatisfiedLinkError ule) {
      ule.printStackTrace();
      if (!javadisabled) {
        LevelDBRepair.repair(new File(path), options);
      } else {
        throw new RuntimeException("No usable LevelDB implementation, aborting.");
      }
    }      
  }
}
