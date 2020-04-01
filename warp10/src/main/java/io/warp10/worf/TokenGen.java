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
package io.warp10.worf;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OSSKeyStore;
import io.warp10.crypto.UnsecureKeyStore;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.StackUtils;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.ext.token.TokenWarpScriptExtension;
import io.warp10.standalone.Warp;

import com.google.common.base.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Properties;

public class TokenGen {
  public static void main(String[] args) throws Exception {

    TokenGen instance = new TokenGen();

    instance.usage(args);

    instance.parse(args);

    instance.process(args);
  }

  public void parse(String[] args) throws Exception {
    WarpConfig.setProperties(Arrays.copyOf(args, args.length - 2));

    Properties properties = WarpConfig.getProperties();

    KeyStore keystore;

    if (properties.containsKey(Configuration.OSS_MASTER_KEY)) {
      keystore = new OSSKeyStore(properties.getProperty(Configuration.OSS_MASTER_KEY));
    } else {
      keystore = new UnsecureKeyStore();
    }

    //
    // Decode generic keys
    // We do that first so those keys do not have precedence over the specific
    // keys.
    //

    for (Entry<Object, Object> entry : properties.entrySet()) {
      if (entry.getKey().toString().startsWith(Configuration.WARP_KEY_PREFIX)) {
        byte[] key = keystore.decodeKey(entry.getValue().toString());
        if (null == key) {
          throw new RuntimeException("Unable to decode key '" + entry.getKey() + "'.");
        }
        keystore.setKey(entry.getKey().toString().substring(Configuration.WARP_KEY_PREFIX.length()), key);
      }
    }

    Warp.extractKeys(keystore, properties);

    keystore.setKey(KeyStore.SIPHASH_CLASS, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_CLASS)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_CLASS).length, Configuration.WARP_HASH_CLASS + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.SIPHASH_LABELS, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_LABELS)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_LABELS).length, Configuration.WARP_HASH_LABELS + " MUST be 128 bits long.");

    //
    // Generate secondary keys. We use the ones' complement of the primary keys
    //

    keystore.setKey(KeyStore.SIPHASH_CLASS_SECONDARY, CryptoUtils.invert(keystore.getKey(KeyStore.SIPHASH_CLASS)));
    keystore.setKey(KeyStore.SIPHASH_LABELS_SECONDARY, CryptoUtils.invert(keystore.getKey(KeyStore.SIPHASH_LABELS)));        
    keystore.setKey(KeyStore.SIPHASH_TOKEN, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_TOKEN)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_TOKEN).length, Configuration.WARP_HASH_TOKEN + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.SIPHASH_APPID, keystore.decodeKey(properties.getProperty(Configuration.WARP_HASH_APP)));
    Preconditions.checkArgument(16 == keystore.getKey(KeyStore.SIPHASH_APPID).length, Configuration.WARP_HASH_APP + " MUST be 128 bits long.");
    keystore.setKey(KeyStore.AES_TOKEN, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_TOKEN)));
    Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_TOKEN).length) || (24 == keystore.getKey(KeyStore.AES_TOKEN).length) || (32 == keystore.getKey(KeyStore.AES_TOKEN).length), Configuration.WARP_AES_TOKEN + " MUST be 128, 192 or 256 bits long.");
    keystore.setKey(KeyStore.AES_SECURESCRIPTS, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_SCRIPTS)));
    Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length) || (24 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length) || (32 == keystore.getKey(KeyStore.AES_SECURESCRIPTS).length), Configuration.WARP_AES_SCRIPTS + " MUST be 128, 192 or 256 bits long.");

    if (properties.containsKey(Configuration.WARP_AES_METASETS)) {
      keystore.setKey(KeyStore.AES_METASETS, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_METASETS)));
      Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_METASETS).length) || (24 == keystore.getKey(KeyStore.AES_METASETS).length) || (32 == keystore.getKey(KeyStore.AES_METASETS).length), Configuration.WARP_AES_METASETS + " MUST be 128, 192 or 256 bits long.");
    }

    if (null != properties.getProperty(Configuration.WARP_AES_LOGGING, Configuration.WARP_DEFAULT_AES_LOGGING)) {
      keystore.setKey(KeyStore.AES_LOGGING, keystore.decodeKey(properties.getProperty(Configuration.WARP_AES_LOGGING, Configuration.WARP_DEFAULT_AES_LOGGING)));
      Preconditions.checkArgument((16 == keystore.getKey(KeyStore.AES_LOGGING).length) || (24 == keystore.getKey(KeyStore.AES_LOGGING).length) || (32 == keystore.getKey(KeyStore.AES_LOGGING).length), Configuration.WARP_AES_LOGGING + " MUST be 128, 192 or 256 bits long.");
    }

    keystore.forget();

    TokenWarpScriptExtension ext = new TokenWarpScriptExtension(keystore);

    WarpScriptLib.register(ext);
  }

  public void usage(String[] args) {
    if (args.length < 3) {
      System.err.println("Usage: TokenGen config ... in out");
      System.exit(-1);
    }
  }

  public void process(String[] args) throws Exception {
    PrintWriter pw = new PrintWriter(System.out);

    if (!"-".equals(args[args.length - 1])) {
      pw = new PrintWriter(new FileWriter(args[args.length - 1]));
    }

    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, WarpConfig.getProperties());
    stack.maxLimits();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] buf = new byte[8192];

    InputStream in = null;

    if ("-".equals(args[args.length - 2])) {
      in = System.in;
    } else {
      in = new FileInputStream(args[args.length - 2]);
    }

    while (true) {
      int len = in.read(buf);

      if (len <= 0) {
        break;
      }

      baos.write(buf, 0, len);
    }

    in.close();

    String script = new String(baos.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);

    stack.execMulti(script);

    StackUtils.toJSON(pw, stack);

    pw.flush();
    pw.close();
  }
}
