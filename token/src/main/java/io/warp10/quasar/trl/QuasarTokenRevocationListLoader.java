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

package io.warp10.quasar.trl;

import io.warp10.crypto.SipHashInline;
import io.warp10.quasar.filter.QuasarConfiguration;
import io.warp10.quasar.filter.sensision.QuasarTokenFilterSensisionConstants;
import io.warp10.sensision.Sensision;

import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QuasarTokenRevocationListLoader {

  Properties config = null;

  private static AtomicBoolean initialized = new AtomicBoolean(false);
  private static long delay = 0L;
  private static String path = null;
  private static QuasarTRL currentTrl = null;

  private static QuasarTokenRevocationListLoader quasarTokenRevocationListLoader = null;
  private static AtomicBoolean singleton = new AtomicBoolean(false);

  private static long appIdSipHashKeyK0 = 0L;
  private static long appIdSipHashKeyK1 = 0L;

  private List<QuasarTRLLoadedHandler> quasarTRLLoadedHandler = new ArrayList<>();

  private String trlPattern = "^([a-zA-Z0-9_-]*)\\.(read|write|full)\\.([0-9]*)-([a-f0-9]{32})\\.trl$";

  // Set of files already read
  private Map<String, JavaTRLLoaded> read = new HashMap<String, JavaTRLLoaded>();
  private Map<String, String> labels = new HashMap<String, String>();


  public static QuasarTokenRevocationListLoader getInstance(Properties config, byte[] appSipHashKey) {
    if (singleton.compareAndSet(false, true)) {
      ByteBuffer bb = ByteBuffer.wrap(appSipHashKey);
      bb.order(ByteOrder.BIG_ENDIAN);
      appIdSipHashKeyK0 = bb.getLong();
      appIdSipHashKeyK1 = bb.getLong();
      quasarTokenRevocationListLoader = new QuasarTokenRevocationListLoader(config);
    }
    return quasarTokenRevocationListLoader;
  }

  private QuasarTokenRevocationListLoader(Properties props) {
    this.config = props;

    delay = Long.parseLong(config.getProperty(QuasarConfiguration.WARP_TRL_PERIOD, QuasarConfiguration.WARP_TRL_PERIOD_DEFAULT));
    path = config.getProperty(QuasarConfiguration.WARP_TRL_PATH);
  }

  public static long getApplicationHash(String appName) {
    if (appName != null && appName.length() > 0) {
      byte[] appNameByteArray = appName.getBytes();
      return SipHashInline.hash24(appIdSipHashKeyK0, appIdSipHashKeyK1, appNameByteArray, 0, appNameByteArray.length);
    }
    return 0L;
  }

  public void loadTrl() {
    try {
      QuasarTRL quasarTRL = null;
      //
      // Sensision metrics thread heart beat
      //
      Sensision.event(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TRL_COUNT, labels, 1);

      //
      // get all files in the directory
      //
      String[] files = getFolderFiles(path);

      //
      // extract the most recent files per warp.type
      //
      Map<String, JavaTRLLoaded> latest = latestFilesToRead(files);

      boolean update = updateTRL(read, latest);

      if (update) {
        long now = System.currentTimeMillis();

        // sum files size
        int size = getSipHashesSize(latest.values());

        // load the selected files
        for (Map.Entry<String, JavaTRLLoaded> entry: latest.entrySet()) {
          if (null == quasarTRL) {
            quasarTRL = new QuasarTRL(size);
          }

          //
          // Read the token revocation list
          //
          BufferedReader br = null;
          try {
            br = new BufferedReader(new FileReader(new File(path, entry.getValue().fileName)));

            while (true) {
              String line = br.readLine();
              if (null == line) {
                break;
              }
              line = line.trim();

              // Skip empty lines
              if ("".equals(line)) {
                continue;
              }
              
              // Skip comments
              if (line.startsWith("#")) {
                continue;
              }

              // application
              if (line.startsWith(QuasarConfiguration.WARP_APPLICATION_PREFIX)) {
                // compute the sip hash with the app name
                long appSipHash = getApplicationHash(line.substring(1));
                quasarTRL.revokeApplication(appSipHash);
              } else {
                // token sip hash hex encoded convert it into long
                byte[] bytes = Hex.decodeHex(line.toCharArray());
                long tokenRevoked = ByteBuffer.wrap(bytes, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong();
                // add it to the future trl list
                quasarTRL.revokeToken(tokenRevoked);
              }
            }

            // mark as read
            read.put(entry.getKey(), entry.getValue());

          } catch (Exception exp) {
            exp.printStackTrace();
          } finally {
            if (null != br) {
              try {
                br.close();
              } catch (IOException e) {
              }
            }
          }
        }   // end for all files


        if (0 != quasarTRLLoadedHandler.size() && null != quasarTRL) {
          //
          // sort and switch the new trl
          //
          quasarTRL.sortTokens();

          //
          // call all the handlers
          //
          for (QuasarTRLLoadedHandler handler: quasarTRLLoadedHandler) {
            handler.onQuasarTRL(quasarTRL);
          }
          currentTrl = quasarTRL;

          //
          // Sensision trl loaded
          //
          long timeElapsed = System.currentTimeMillis() - now;
          Sensision.event(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TRL_LOAD_TIME, labels, timeElapsed);
          Sensision.event(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TRL_TOKENS_COUNT, labels, quasarTRL.getTrlSize());
        }
      } // end if update
    } catch (Exception exp) {
      // thread error
      Sensision.update(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TRL_ERROR_COUNT, labels, 1);
    }
  }

  public void init() {
    // initialize only once per JVM
    if (initialized.get()) {
      return;
    }

    Thread t = new Thread() {

      @Override
      public void run() {
        while (true) {
          loadTrl();
          // time to sleep
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
          }
        } // while(true)
      } // run()
    };

    if (null != path && initialized.compareAndSet(false, true)) {
      t.setName("[TokenRevocationListLoader]");
      t.setDaemon(true);
      t.start();
    }
  }

  private boolean updateTRL(Map<String, JavaTRLLoaded> read, Map<String, JavaTRLLoaded> latest) {
    boolean update = false;
    for (Map.Entry<String, JavaTRLLoaded> keyAndNewTrl: latest.entrySet()) {
      JavaTRLLoaded actualTrl = read.get(keyAndNewTrl.getKey());
      JavaTRLLoaded newTrl = keyAndNewTrl.getValue();

      // not current trl -> load it
      if (null == actualTrl) {
        update = true;
        break;
      }

      // md5 not equals -> load it
      if (!actualTrl.md5.equals(newTrl.md5)) {
        update = true;
        break;
      }
    }
    return update;
  }

  private Map<String, JavaTRLLoaded> latestFilesToRead(String[] files) {

    // key = warp.type
    Map<String, JavaTRLLoaded> filesToRead = new HashMap<String, JavaTRLLoaded>();

    Pattern pattern = Pattern.compile(trlPattern);
    for (String file: files) {
      Matcher matcher = pattern.matcher(file);
      if (matcher.matches()) {
        // get the key warp.type
        String warp = matcher.group(1);
        String type = matcher.group(2);
        long ts = Long.valueOf(matcher.group(3));
        String md5 = matcher.group(4);

        String key = warp + "." + type;

        JavaTRLLoaded current = filesToRead.get(key);

        if (null == current || (null != current && ts > current.timestamp)) {
          JavaTRLLoaded next = new JavaTRLLoaded();
          next.fileName = file;
          next.timestamp = ts;
          next.warp = warp;
          next.type = type;
          next.md5 = md5;

          filesToRead.put(key, next);
        }
      }
    }
    return filesToRead;
  }

  private String[] getFolderFiles(String path) {
    final File root = new File(path);

    String[] files = root.list(new FilenameFilter() {
      @Override
      public boolean accept(File d, String name) {
        if (!d.equals(root)) {
          return false;
        }
        return name.matches(trlPattern);
      }
    });

    // Sort files in lexicographic order
    if (null == files) {
      files = new String[0];
    }

    Arrays.sort(files);

    return files;
  }

  /**
   * Estimation if the number of SIPhashes in the files according to the file size
   * @param files
   * @return
   */
  private int getSipHashesSize(Collection<JavaTRLLoaded> files) {
    // sum files size
    int size = 0;
    for (JavaTRLLoaded file: files) {
      File filename = new File(path, file.fileName);
      size += filename.length();
    }
    // each line = long hexa encoded (16 bytes) + CR
    return size / 17;
  }

  public void addTrlUpdatedHandler(QuasarTRLLoadedHandler handler) {
    quasarTRLLoadedHandler.add(handler);

    // notify if a trl is already available
    if (null != currentTrl) {
      handler.onQuasarTRL(currentTrl);
    }
  }
}
