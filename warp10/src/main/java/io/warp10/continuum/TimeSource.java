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

package io.warp10.continuum;

import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.sensision.Sensision;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * The role of this class is to implement a time source with
 * the required precision for the platform instance.
 * 
 * It can return times up to ns precision.
 * 
 * WARNING: This code can only run for up to 292 years without reset. Passed that delay it will return incoherent values
 * due to System.nanoTime() overflowing.
 */
public class TimeSource {
  
  static long baseMillis = 0L;
  static long baseNanos = 0L;
  static long baseTimeunits = 0L;
  static long lastCalibration = 0L;

  static AtomicLong calibrations = new AtomicLong(0L);
  static AtomicBoolean mustRecalibrate = new AtomicBoolean(true);
  
  static {
    // Start the recalibration thread
    
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        while(true) {
          if (!mustRecalibrate.getAndSet(false)) {
            LockSupport.parkNanos(100000000L);
            continue;
          }
          calibrate();
          // Sleep 100ms
          LockSupport.parkNanos(100000000L);
        }
      }
    });
    
    t.setDaemon(true);
    t.setName("[TimeSource Calibrator]");
    t.start();
  }
  
  private static final void calibrate() {
    
    //
    // Do not recalibrate more often than every 1s
    //
    
    if (System.currentTimeMillis() - lastCalibration < 1000) {
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_TIMESOURCE_CALIBRATIONS_SKIPPED, Sensision.EMPTY_LABELS, 1);      
      return;
    }
     
    //
    // Initialize references for milliseconds and nanoseconds
    //
    
    long myBaseMillis = System.currentTimeMillis();    
    long myBaseNanos = System.nanoTime();

    lastCalibration = myBaseMillis;
    
    //
    // We need to calibrate the reference for nanoseconds so we don't go back in time
    // when generating microsecond timestamps from nanosecond ones.
    //
    
    boolean first = true;
    long lastms = 0L;
    
    int count = 0;
    
    //
    // Calibrate on that many samples, valid samples will need to be 1ms apart, so calibration
    // will roughly take SAMPLES ms
    //
    
    int SAMPLES = 1000;
    
    //
    // Array to keep track of the sequence of 'baseNanos' values
    //
    
    long[] bases = new long[SAMPLES];
    
    while (count < SAMPLES) {
      //
      // Read ms and ns
      //
      
      long ms = System.currentTimeMillis();
      long ns = System.nanoTime();
      
      //
      // Compute ms and ns deltas from the current references
      //
      
      long msdelta = ms - myBaseMillis;
      long nsdelta = ns - myBaseNanos;

      //
      // If this is the first time in the loop, initialize 'lastms' and continue
      //
      
      if (first) {
        lastms = ms;
        first = false;
        continue;
      }
      
      //
      // If the 'ms' value changed, adjust 'baseNanos' so nsdelta == msdelta * 1000000.
      // Store the adjusted 'baseNanos' value and increase 'count'
      //
      
      if (lastms != ms) {
        myBaseNanos += nsdelta - (msdelta * 1000000L);
        bases[count] = myBaseNanos;
        count++;
      }

      lastms = ms;
    }

    //
    // Sort the 'baseNanos' values we kept track of
    //
    
    Arrays.sort(bases);
    
    //
    // Determine the most common 'baseNanos' value in 'bases'
    // This value will be the final 'baseNanos' value we use.
    // We do this step because it might happen that due to GC activity for example,
    // we have readings of ns and ms which are far apart on some of the samples, this
    // step is used to make sure we don't take into consideration the baseNanos value
    // resulting from these weird readings
    //
        
    int mostocc = 0;
    int curocc = 1;
    int mostoccidx = -1;
    int curidx = 0;
    
    long lastval = bases[0];
    
    int idx = 1;
    
    while(idx < SAMPLES) {
      if (bases[curidx] != lastval) {
        if (curocc > mostocc) {
          mostocc = curocc;
          mostoccidx = curidx;
        }
        curidx = idx;
        curocc = 1;
      } else {
        curocc++;
      }
      idx++;
    }
    
    if (curocc > mostocc) {
      mostocc = curocc;
      mostoccidx = curidx;
    }
    
    synchronized(mustRecalibrate) {
      baseNanos = bases[mostoccidx];
      baseMillis = myBaseMillis;
      baseTimeunits = baseMillis * Constants.TIME_UNITS_PER_MS;
    }
    
    calibrations.addAndGet(1L);
    
    //
    // Keep track of calibrations
    //

    Sensision.update(SensisionConstants.CLASS_WARP_TIMESOURCE_CALIBRATIONS, Sensision.EMPTY_LABELS, 1);    
  }
  
  /**
   * Return the current time in the platform's native time units.
   * @return
   */
  public static synchronized long getTime() {    
    //
    // TODO(hbs): add periodic re-adjustment of bases so we can cope with
    //            clock adjustment due to either NTP or PTP
    //
    
    while(0 == calibrations.get()) {      
    }
    
    //
    // Extract nanoseconds
    //
    
    long nano;
        
    //
    // Compute the ns delta
    //
    
    long ts;
    
    synchronized(mustRecalibrate) {
      nano = System.nanoTime();
      nano -= baseNanos;    
      ts = baseTimeunits + (long) (nano / Constants.NS_PER_TIME_UNIT);
    }
    
    //
    // Determine if we need to re-calibrate
    //
    
    long ms = System.currentTimeMillis();
    long delta = (ts / Constants.TIME_UNITS_PER_MS) - ms;

    //
    // If the number of milliseconds of 'ts' is greater than currentTimeMillis then we
    // will recalibrate since this should NOT happen given we call currentTimeMillis AFTER
    // computing ts.
    // If the number of milliseconds of 'ts' is less than 'currentTimeMillis' then we check
    // the number of time units to see if it was more than 100 microseconds away from the next
    // millisecond, we don't want to trigger a calibration only because ts was xxx1901 and the
    // call to currentTimeMillis was done 99 us after thus leading to a number of milliseconds
    // ending in '2'
    //
    
    if (delta > 0) {
      // System.currentTimeMillis went back in time, return this value instead of our calibrated timestamp    
      mustRecalibrate.set(true);
      return ms * Constants.TIME_UNITS_PER_MS;
    } else if (delta <= -1L && (0.9 * Constants.TIME_UNITS_PER_MS > ts % Constants.TIME_UNITS_PER_MS)) {
      // The calibrated timestamp has a ms component which is behind System.currentTimeMillis AND
      // the number of microseconds was less than 900, so we probably had a shift in ms, therefore we
      // will return the ms timestamp and hint for recalibration
      mustRecalibrate.set(true);
      return ms * Constants.TIME_UNITS_PER_MS;
    } else {
      return ts;      
    }
  }

  /**
   * Return the current time in nanoseconds
   * @return
   */
  public static long getNanoTime() {
    while(0 == calibrations.get()) {      
    }
    synchronized(mustRecalibrate) {
      long nano = System.nanoTime();
      nano -= baseNanos;   
      return (baseMillis * 1000000L) + nano;
    }
  }
}
