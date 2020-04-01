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
package io.warp10;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarpClassLoader extends ClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(WarpClassLoader.class);
  
  private final String jarpath;
  
  private final Map<String,Class> knownClasses;
  
  public WarpClassLoader(String jarpath, ClassLoader parent) {
    super(parent);
    this.jarpath = jarpath;
    this.knownClasses = new HashMap<String,Class>();
    registerAsParallelCapable();
  }
  
  
  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return loadClass(name, false);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    try {
      Class c = findClass(name);
      if (resolve) {
        super.resolveClass(c);
      }
      return c;
    } catch (ClassNotFoundException cnfe) {
      return super.loadClass(name, resolve);
    } finally {
    }
  }
  
  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    
    Class knownClass = this.knownClasses.get(name);
    
    if (null != knownClass) {
      return knownClass;
    }
    
    String clsFile = name.replace('.', '/') + ".class";
    
    JarFile jf = null;
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InputStream in = null;
    
    try {
    
      jf = new JarFile(this.jarpath);
      ZipEntry entry = jf.getEntry(clsFile);
      
      if (null == entry) {
        throw new ClassNotFoundException();
      }
      
      in = jf.getInputStream(entry);
      
      if (null == in) {
        return null;
      }
      
      byte[] buffer = new byte[1024];

      while (true) {
        int len = in.read(buffer, 0, buffer.length);
        
        if (-1 == len) {
          break;
        }
        
        out.write(buffer, 0, len);
      }
    } catch (IOException ioe) {
      throw new ClassNotFoundException("",ioe);
    } finally {
      if (null != in) {
        try { in.close(); } catch (IOException ioe) {}
      }
      if (null != jf) {        
        try { jf.close(); } catch (IOException ioe) {}
      }
    }

    byte[] data = out.toByteArray();

    //
    // Return class
    //
    
    try {
      Class c = null;
      
      // Synchronize on the interned class name so we tolerate parallel executions but prevent
      // the same class from being loaded multiple times.
      synchronized(name.intern()) {
        // Check again if the class is known now that we
        // are in the critical section
        knownClass = this.knownClasses.get(name);
        
        if (null != knownClass) {
          return knownClass;
        }

        c = defineClass(name, data, 0, data.length);
        //
        // Store the class in the cache
        //      
        this.knownClasses.put(name, c);
      }
    
      
      return c;
    } catch (Throwable t) {
      LOG.error("Error calling defineClass(" + name + ")", t);
      throw new ClassNotFoundException("Error calling defineClass(" + name + ")", t);
    }
  }
}
