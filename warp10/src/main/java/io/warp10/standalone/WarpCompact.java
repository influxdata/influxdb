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

import org.bouncycastle.util.encoders.Hex;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import io.warp10.continuum.Configuration;

public class WarpCompact {
  public static void main(String[] args) throws IOException {
    
    if (args.length != 3) {
      System.err.println("Usage: WarpCompact /path/to/leveldb/dir STARTKEY(hex) ENDKEY(hex)");
      System.exit(-1);
    }
    
    String path = args[0];
    
    Options options = new Options();
    
    if (null != System.getProperty(Configuration.LEVELDB_BLOCK_SIZE)) {
      options.blockSize(Integer.parseInt(System.getProperty(Configuration.LEVELDB_BLOCK_SIZE)));
    }    

    DB db = null;

    boolean nativedisabled = "true".equals(System.getProperty(Configuration.LEVELDB_NATIVE_DISABLE));
    boolean javadisabled = "true".equals(System.getProperty(Configuration.LEVELDB_JAVA_DISABLE));
    
    try {
      if (!nativedisabled) {
        db = JniDBFactory.factory.open(new File(path), options);
      } else {
        throw new UnsatisfiedLinkError("Native LevelDB implementation disabled.");
      }
    } catch (UnsatisfiedLinkError ule) {
      ule.printStackTrace();
      if (!javadisabled) {
        db = Iq80DBFactory.factory.open(new File(path), options);
      } else {
        throw new RuntimeException("No usable LevelDB implementation, aborting.");
      }
    }      
    
    final DB fdb = db;
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          fdb.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    
    byte[] begin = null;
    byte[] end = null;
    
    if (!"".equals(args[1])) {
      begin = Hex.decode(args[1]);
    }
    
    if (!"".equals(args[2])) {
      end = Hex.decode(args[2]);
    }
    
    db.compactRange(begin, end);    
  }
}
