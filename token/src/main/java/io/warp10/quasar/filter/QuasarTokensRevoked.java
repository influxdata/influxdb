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

package io.warp10.quasar.filter;

import com.google.common.base.Strings;
import io.warp10.quasar.filter.exception.QuasarApplicationRevoked;
import io.warp10.quasar.filter.exception.QuasarFilterUnavailable;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.filter.sensision.QuasarTokenFilterSensisionConstants;
import io.warp10.quasar.trl.QuasarTRL;
import io.warp10.quasar.trl.QuasarTRLLoadedHandler;
import io.warp10.quasar.trl.QuasarTokenRevocationListLoader;
import io.warp10.sensision.Sensision;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class QuasarTokensRevoked implements QuasarTRLLoadedHandler {

  /**
   * If all tokens of an registered application are compromised
   * We can force the refresh of all tokens delivered before the given timestamp
   * key=client_id:value=timestamp
   */
  private static Map<Long, Long> refreshTokenAfter = new ConcurrentHashMap<Long, Long>(256, 0.75f, 2);

  /**
   * List of all the revoked token SIP HASH
   */
  private QuasarTRL quasarTRL = null;

  private QuasarTokenRevocationListLoader quasarTRLLoader = null;

  private static AtomicBoolean loaded = new AtomicBoolean(false);

  private static AtomicBoolean unavailable = new AtomicBoolean(false);

  private static CountDownLatch updateRunning = new CountDownLatch(0);

  /**
   * TS (millis )of the initialisation startup time
   */
  private static long startupTimestamp;

  /**
   * duration (in millis) of the startup time.
   * All is available during this time
   * after if no trl is loaded QuasarFilterUnavailable is thrown
   */
  private static long trlStartupDelay = 0L;


  public QuasarTokensRevoked( Properties config, byte[] appSipHashKey) {
    // initialize the QuasarTRL
    quasarTRLLoader = QuasarTokenRevocationListLoader.getInstance(config, appSipHashKey);
    // add this has handler in all cases
    quasarTRLLoader.addTrlUpdatedHandler(this);

    // only one time per JVM
    if (false == loaded.get()) {
      startupTimestamp = System.currentTimeMillis();

      String startupDelay = config.getProperty(QuasarConfiguration.WARP_TRL_STARTUP_DELAY);

      // startup delay is set
      if (!Strings.isNullOrEmpty(startupDelay)) {

        trlStartupDelay = Long.parseLong(startupDelay);

        // if trlStartupDelay == 0 loads TRL immediately
        if (0L ==trlStartupDelay) {
          // synchronous TRL load
          quasarTRLLoader.loadTrl();

          // trl not loaded mark the filter as unavailable
          if (!loaded.get()) {
            unavailable.set(true);
          }
        }
      } else {
        // set  TRL as loaded
        loaded.set(true);
      }

      quasarTRLLoader.init();
    }
  }

  public boolean loaded() {
    return loaded.get();
  }

  public void available() throws QuasarFilterUnavailable {
    // no trl loaded test the hard startup time
    if (loaded.get() == false) {
      long now = System.currentTimeMillis();
      // timeout, mark the QuasarTokenFilter as unavailable
      if ((now - startupTimestamp) > trlStartupDelay) {
        unavailable.set(true);
      }
    }

    // hard startup delay expired
    if (unavailable.get()) {
      throw new QuasarFilterUnavailable("No TRL available past the startup delay");
    }
  }

  @Override
  public void onQuasarTRL(QuasarTRL trl) {
    // switch the array
    // critical section this must be atomic
    updateRunning = new CountDownLatch(1);

    quasarTRL = trl;

    // Release the critical section
    updateRunning.countDown();

    // set as loaded
    loaded.set(true);
  }


  public void isRegisteredAppAuthorized(long appId) throws QuasarTokenException {
    try {
      // if an update is running, wait...
      updateRunning.await();

      if (quasarTRL == null) {
        Sensision.update(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TRL_UNAVAILABLE_COUNT, 1);
        return;
      }

      if (quasarTRL.isAppAuthorized(appId)) {
        throw new QuasarApplicationRevoked("Application suspended or closed.");
      }
    } catch( io.warp10.quasar.filter.exception.QuasarApplicationRevoked qexp ) {
      throw qexp;
    } catch( Exception exp){
      throw new QuasarTokenException("Application unexpected exception", exp);
    }
  }

  public void isTokenRevoked(long sipHash) throws QuasarTokenException {
    try {
      // if an update is running, wait...
      updateRunning.await();

      if (quasarTRL == null || quasarTRL.getTrlSize() == 0) {
        Sensision.update(QuasarTokenFilterSensisionConstants.SENSISION_CLASS_QUASAR_FILTER_TRL_UNAVAILABLE_COUNT, 1);
        return;
      }

      if (quasarTRL.isTokenRevoked(sipHash)) {
        throw new io.warp10.quasar.filter.exception.QuasarTokenRevoked("Token revoked.");
      }
    } catch( io.warp10.quasar.filter.exception.QuasarTokenRevoked qexp ) {
      throw qexp;
    } catch( Exception exp){
      throw new QuasarTokenException("Token unexpected exception.", exp);
    }
  }

  public Long getClientIdRefreshTimeStamp(long clientId) {
    Long endOfTokenValidity = refreshTokenAfter.get(clientId);

    return endOfTokenValidity;
  }
}
