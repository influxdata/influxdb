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
package io.warp10.continuum.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.Warp;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

public class ParallelGTSDecoderIteratorWrapper extends GTSDecoderIterator {
  
  private static final boolean standalone;
  
  //
  // Have a semaphore which gets picked up by a thread having
  // GTSDecoders to provide until it changes GTS, wat which time it
  // releases the sem so another thread can pick it up
  //
  
  // Have a fixed pool of threads and allow a maximum number of threads per request
  // Divide the metadatas according to this limit
  
  private static class GTSDecoderIteratorRunnable implements Runnable, AutoCloseable {

    private final AtomicInteger pendingCounter;
    private final AtomicInteger inflightCounter;
    private final AtomicBoolean errorFlag;
    private final AtomicReference<Throwable> errorThrowable;
    private final GTSDecoderIterator iterator;
    private final LinkedBlockingQueue<GTSDecoder> queue;
    private final Semaphore sem;
    private boolean done = false;
    private Thread thread = null;    
    private final long creation;
    
    private static volatile boolean foo = false;
    
    public GTSDecoderIteratorRunnable(GTSDecoderIterator iterator, LinkedBlockingQueue<GTSDecoder> queue, Semaphore sem, AtomicInteger pendingCounter, AtomicInteger inflightCounter, AtomicBoolean errorFlag, AtomicReference<Throwable> errorThrowable) {
      this.pendingCounter = pendingCounter;
      this.inflightCounter = inflightCounter;
      this.errorFlag = errorFlag;
      this.errorThrowable = errorThrowable;
      this.iterator = iterator;
      this.queue = queue;
      this.sem = sem;
      this.creation = System.nanoTime();
    }
    
    @Override
    public void run() {
      
      long waitnanos = System.nanoTime() - this.creation;
      
      if (standalone) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_CLIENT_PARALLEL_SCANNERS_WAITNANOS, Sensision.EMPTY_LABELS, waitnanos);
      } else {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_PARALLEL_SCANNERS_WAITNANOS, Sensision.EMPTY_LABELS, waitnanos);
      }

      GTSDecoder lastdecoder = null;

      int count = 0;
      
      /**
       * Number of currently held permits
       */
      int held = 0;
      String name = null;
      
      try {
        this.thread = Thread.currentThread();
        name = this.thread.getName();
        this.thread.setName("GTSDecoderIteratorRunnable");
        if (standalone) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_CLIENT_PARALLEL_SCANNERS, Sensision.EMPTY_LABELS, 1);
        } else {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_PARALLEL_SCANNERS, Sensision.EMPTY_LABELS, 1);
        }

        // Increment inflight BEFORE decrementing pending        
        this.inflightCounter.addAndGet(1);
        this.pendingCounter.addAndGet(-1);

        //
        // Iterate over the GTSDecoders
        //
        
        GTSDecoder decoder = null;
        
        while(!done && !Thread.currentThread().isInterrupted() && !this.errorFlag.get() && iterator.hasNext()) {
          decoder = iterator.next();
          
          //
          // If this is the first decoder, save it for later
          //

          if (null == lastdecoder) {
            lastdecoder = decoder;
            decoder = null;
            continue;
          }
          
          //
          // If the current decoder differs from the previous one, we can safely put it in the queue
          // We attempt to acquire 1 permit from the semaphore. If we manage to do so it means that
          // no other thread has encountered a block of GTS and is holding the queue for itself
          //
          
          if (!decoder.getMetadata().getName().equals(lastdecoder.getMetadata().getName()) || !decoder.getMetadata().getLabels().equals(lastdecoder.getMetadata().getLabels())) {
            if (0 == held) {
              this.sem.acquire(1);
              held = 1;
            }
                        
            queue.put(lastdecoder);
            lastdecoder = decoder;
            decoder = null;
            
            //
            // Release the currently held permits
            //
            this.sem.release(held);
            held = 0;
          } else {
            //
            // Decoder is identical to the previous one, we need to acquire all the permits to
            // be the sole thread accessing the queue while the decoders we read belong to the same GTS
            //
            
            if (0 == held) {
              this.sem.acquire(POOLSIZE);
              held = POOLSIZE;
              if (standalone) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_CLIENT_PARALLEL_SCANNERS_MUTEX, Sensision.EMPTY_LABELS, 1);                
              } else {
                Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_PARALLEL_SCANNERS_MUTEX, Sensision.EMPTY_LABELS, 1);
              }
            }
            
            queue.put(lastdecoder);
            lastdecoder = decoder;
            decoder = null;
          }
        }
        
        if (null == decoder && null != lastdecoder) {
          if (0 == held) {
            this.sem.acquire(1);
            held = 1;
          }
          queue.put(lastdecoder);
        }
        
      } catch (Throwable t) {
        // Only set the error if we are not done, this is to prevent
        // the call to close to trigger an error if the thread is
        // interrupted while in a call to acquire for example
        if (!done) {
          this.errorFlag.set(true);
          this.errorThrowable.set(t);
        }
      } finally {
        if (0 != held) {
          this.sem.release(held);
          held = 0;
        }
        
        try { this.iterator.close(); } catch (Exception e) {}
        
        this.inflightCounter.addAndGet(-1);
        
        if (null != name) {
          this.thread.setName(name);
        }
        
        this.thread = null;
      }
    }
    
    @Override
    public void close() throws Exception {
      this.done = true;
      // Interrupt the thread
      if (null != this.thread) {        
        this.thread.interrupt();
      }
    }
  }
  
  private static final ExecutorService executor;
  
  private static final int MAX_INFLIGHT;
  private static final int POOLSIZE;
  
  static {
    standalone = Warp.isStandaloneMode();
    
    if (standalone) {
      MAX_INFLIGHT = Integer.parseInt(WarpConfig.getProperty(Configuration.STANDALONE_PARALLELSCANNERS_MAXINFLIGHTPERREQUEST, "0"));
      POOLSIZE = Integer.parseInt(WarpConfig.getProperty(Configuration.STANDALONE_PARALLELSCANNERS_POOLSIZE, "0"));
            
      MIN_GTS_PERSCANNER = Integer.parseInt(WarpConfig.getProperty(Configuration.STANDALONE_PARALLELSCANNERS_MIN_GTS_PERSCANNER, "4"));
      MAX_PARALLEL_SCANNERS = Integer.parseInt(WarpConfig.getProperty(Configuration.STANDALONE_PARALLELSCANNERS_MAX_PARALLEL_SCANNERS, "16"));
    } else {
      MAX_INFLIGHT = Integer.parseInt(WarpConfig.getProperty(Configuration.EGRESS_HBASE_PARALLELSCANNERS_MAXINFLIGHTPERREQUEST, "0"));
      POOLSIZE = Integer.parseInt(WarpConfig.getProperty(Configuration.EGRESS_HBASE_PARALLELSCANNERS_POOLSIZE, "0"));
            
      MIN_GTS_PERSCANNER = Integer.parseInt(WarpConfig.getProperty(Configuration.EGRESS_HBASE_PARALLELSCANNERS_MIN_GTS_PERSCANNER, "4"));
      MAX_PARALLEL_SCANNERS = Integer.parseInt(WarpConfig.getProperty(Configuration.EGRESS_HBASE_PARALLELSCANNERS_MAX_PARALLEL_SCANNERS, "16"));
    }
    
    if (MAX_INFLIGHT> 0 && POOLSIZE > 0) {
      executor = new ThreadPoolExecutor(POOLSIZE, POOLSIZE, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(POOLSIZE));
    } else {
      executor = null;
    }
  }
  
  private LinkedList<GTSDecoderIteratorRunnable> runnables = new LinkedList<GTSDecoderIteratorRunnable>();
  
  private final AtomicInteger pending = new AtomicInteger(0);
  private final AtomicInteger inflight = new AtomicInteger(0);
  private final AtomicBoolean errorFlag = new AtomicBoolean(false);
  private final AtomicReference<Throwable> errorThrowable = new AtomicReference<Throwable>();
  
  /**
   * Semaphore for synchronizing the various runnables. Fairness should be enforced.
   */
  private final Semaphore sem = new Semaphore(POOLSIZE, true);
  
  private final LinkedBlockingQueue<GTSDecoder> queue;
  
  private int idx = 0;
  
  private static final int MIN_GTS_PERSCANNER;
  private static final int MAX_PARALLEL_SCANNERS;
  
  public ParallelGTSDecoderIteratorWrapper(boolean optimized, ReadToken token, long now, long then, long count, long skip, double sample, List<Metadata> metadatas, KeyStore keystore, Connection conn, TableName tableName, byte[] colfam, boolean writeTimestamp, boolean useBlockCache, int preBoundary, int postBoundary) throws IOException {
    if (standalone) {
      throw new IOException("Incompatible parallel scanner instantiated.");
    }

    //
    // Allocate a queue for the GTSDecoders
    //

    this.queue = new LinkedBlockingQueue<GTSDecoder>(MAX_INFLIGHT * 4);
        
    //
    // Split the Metadata list in chunks which will be retrieved separately
    // according to 'mingts', the minimum number of GTS per parallel scanner
    // and 'maxscanners', the maximum number of parallel scanners to create
    //
    
    int gtsPerScanner = (int) Math.max(MIN_GTS_PERSCANNER, Math.floor(metadatas.size() / MAX_PARALLEL_SCANNERS));
    
    int metaidx = 0;
    
    List<Metadata> metas = null;
    
    while (metaidx < metadatas.size()) {
      if (null == metas) {
        metas = new ArrayList<Metadata>();
      }
      
      metas.add(metadatas.get(metaidx++));
      
      if (gtsPerScanner == metas.size()) {
        GTSDecoderIterator iterator = null;
        
        if (optimized) {
          long timespan = count > 0 ? -count : now - then + 1;
          iterator = new OptimizedSlicedRowFilterGTSDecoderIterator(now, timespan, metas, conn, tableName, colfam, writeTimestamp, keystore, useBlockCache);
        } else {
          iterator = new MultiScanGTSDecoderIterator(token, now, then, count, skip, sample, metas, conn, tableName, colfam, writeTimestamp, keystore, useBlockCache, preBoundary, postBoundary);      
        }      

        GTSDecoderIteratorRunnable runnable = new GTSDecoderIteratorRunnable(iterator, queue, sem, this.pending, this.inflight, this.errorFlag, this.errorThrowable);
        runnables.add(runnable);
        metas = null;
      }      
    }
    
    if (null != metas) {
      GTSDecoderIterator iterator = null;
      
      if (optimized) {
        long timespan = count > 0 ? -count : now - then + 1;
        iterator = new OptimizedSlicedRowFilterGTSDecoderIterator(now, timespan, metas, conn, tableName, colfam, writeTimestamp, keystore, useBlockCache);
      } else {
        iterator = new MultiScanGTSDecoderIterator(token, now, then, count, skip, sample, metas, conn, tableName, colfam, writeTimestamp, keystore, useBlockCache, preBoundary, postBoundary);      
      }      

      GTSDecoderIteratorRunnable runnable = new GTSDecoderIteratorRunnable(iterator, queue, sem, this.pending, this.inflight, this.errorFlag, this.errorThrowable);
      runnables.add(runnable);
    }
    
    //
    // Now shuffle the runnables so we reduce the probability of accessing the same region from parallel scanners
    //
    
    Collections.shuffle(runnables);

    this.pending.set(runnables.size());
  }
  
  public ParallelGTSDecoderIteratorWrapper(StoreClient client, ReadToken token, long now, long then, long count, long skip, double sample, List<Metadata> metadatas, int preBoundary, int postBoundary) throws IOException {
    
    if (!standalone) {
      throw new IOException("Incompatible parallel scanner instantiated.");
    }
    
    //
    // Allocate a queue for the GTSDecoders
    //

    this.queue = new LinkedBlockingQueue<GTSDecoder>(MAX_INFLIGHT * 4);
        
    //
    // Split the Metadata list in chunks which will be retrieved separately
    // according to 'mingts', the minimum number of GTS per parallel scanner
    // and 'maxscanners', the maximum number of parallel scanners to create
    //
    
    int gtsPerScanner = (int) Math.max(MIN_GTS_PERSCANNER, Math.floor(metadatas.size() / MAX_PARALLEL_SCANNERS));
    
    int metaidx = 0;
    
    List<Metadata> metas = null;
    
    while (metaidx < metadatas.size()) {
      if (null == metas) {
        metas = new ArrayList<Metadata>();
      }
      
      metas.add(metadatas.get(metaidx++));
      
      if (gtsPerScanner == metas.size()) {
        GTSDecoderIterator iterator = null;
    
        iterator = client.fetch(token, metas, now, then, count, skip, sample, false, preBoundary, postBoundary);

        GTSDecoderIteratorRunnable runnable = new GTSDecoderIteratorRunnable(iterator, queue, sem, this.pending, this.inflight, this.errorFlag, this.errorThrowable);
        runnables.add(runnable);
        metas = null;
      }      
    }
    
    if (null != metas) {
      GTSDecoderIterator iterator = null;
      
      iterator = client.fetch(token, metas, now, then, count, skip, sample, false, preBoundary, postBoundary);

      GTSDecoderIteratorRunnable runnable = new GTSDecoderIteratorRunnable(iterator, queue, sem, this.pending, this.inflight, this.errorFlag, this.errorThrowable);
      runnables.add(runnable);
    }
    
    this.pending.set(runnables.size());
  }
  
  public static int getMinGTSPerScanner() {
    return MIN_GTS_PERSCANNER;
  }
  
  @Override
  public void close() throws Exception {
    //
    // Close all known iterators, starting from the end since they are not yet scheduled
    //
    
    for (int i = runnables.size() - 1; i >= 0; i--) {
      runnables.get(i).close();
    }    
  }
  
  @Override
  public boolean hasNext() {
    
    //
    // Wait until the queue has something to offer or there are no more runnables to
    // run or running.
    //
    
    while(!this.errorFlag.get() && this.queue.isEmpty() && !(0 == this.pending.get() &&  0 == this.inflight.get())) {
      schedule();
      LockSupport.parkNanos(50000L);
    }
    
    if (this.errorFlag.get()) {
      throw new RuntimeException("Error in an underlying parallel scanner.", (Throwable) this.errorThrowable.get());
    }
    
    return !this.queue.isEmpty();
  }
  
  @Override
  public GTSDecoder next() {
    
    if (this.errorFlag.get()) {
      throw new RuntimeException("Error in an underlying parallel scanner.", (Throwable) this.errorThrowable.get());
    }

    try {
      return this.queue.take();
    } catch (InterruptedException ie) {
      return null;
    }
  }
  
  /**
   * Attempt to schedule one of the runnables
   */  
  private synchronized void schedule() {
    //
    // If we have already too many inflight requests
    // or if we don't have any left, return
    //
    
    if (this.inflight.get() >= MAX_INFLIGHT) {
      return;
    }
    
    if (0 == this.pending.get()) {
      return;
    }
    
    if (idx >= runnables.size()) {
      return;
    }
    
    GTSDecoderIteratorRunnable runnable = runnables.get(idx);
    
    try {
      executor.execute(runnable);
      idx++;
    } catch (RejectedExecutionException ree) {
      if (standalone) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_CLIENT_PARALLEL_SCANNERS_REJECTIONS, Sensision.EMPTY_LABELS, 1);
      } else {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_HBASE_CLIENT_PARALLEL_SCANNERS_REJECTIONS, Sensision.EMPTY_LABELS, 1);
      }
    }
  }
  
  public static boolean useParallelScanners() {
    return null != executor;
  }
}
