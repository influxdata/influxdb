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
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.script.WarpScriptException;

public class WarpDB extends Thread implements DB {
  
  private static final Logger LOG = LoggerFactory.getLogger(WarpDB.class);
  
  private DB db;

  private AtomicInteger pendingOps = new AtomicInteger(0);
  private AtomicBoolean compactionsSuspended = new AtomicBoolean(false);

  private static final class WarpDBReentrantLock extends ReentrantLock {
    public Thread getOwner() {
      return super.getOwner();
    }    
    public Collection<Thread> getQueuedThreads() {
      return super.getQueuedThreads();
    }
  }
  
  private WarpDBReentrantLock mutex = new WarpDBReentrantLock();
  
  private final boolean nativedisabled;
  private final boolean javadisabled;
  private final String home;
  private final Options options;
  
  public static enum WarpDBCommand {
    CLOSE,
    OPEN,
  }
  
  private final LinkedBlockingQueue<WarpDBCommand> commandQ = new LinkedBlockingQueue<WarpDBCommand>(16);
  
  private static final class WarpIterator implements DBIterator {

    private final DBIterator iter;
    private final AtomicInteger count;
    
    public WarpIterator(AtomicInteger count, DBIterator iter) {
      this.count = count;
      this.iter = iter;
    }
    
    @Override
    public boolean hasNext() {
      return this.iter.hasNext();
    }
    
    @Override
    public void close() throws IOException {
      try {
        this.iter.close();
      } finally {
        count.decrementAndGet();
      }
    }
    
    @Override
    public boolean hasPrev() {
      return this.iter.hasPrev();
    }
    
    @Override
    public Entry<byte[], byte[]> peekNext() {
      return this.iter.peekNext();
    }
    
    @Override
    public Entry<byte[], byte[]> peekPrev() {
      return this.iter.peekPrev();
    }
    
    @Override
    public Entry<byte[], byte[]> next() {
      return this.iter.next();
    }
    
    @Override
    public Entry<byte[], byte[]> prev() {
      return this.iter.prev();
    }
    
    @Override
    public void seek(byte[] key) {
      this.iter.seek(key);
    }
    
    @Override
    public void seekToFirst() {
      this.iter.seekToFirst();
    }
    
    @Override
    public void seekToLast() {
      this.iter.seekToLast();
    }    
  }
  
  public WarpDB(boolean nativedisabled, boolean javadisabled, String home, Options options) throws IOException {
    this.nativedisabled = nativedisabled;
    this.javadisabled = javadisabled;
    this.options = options;
    this.home = home;

    this.setName("[WarpDB Command Thread]");
    this.setDaemon(true);
    this.start();

    this.open(nativedisabled, javadisabled, home, options);
    
  }
  
  private synchronized void open(boolean nativedisabled, boolean javadisabled, String home, Options options) throws IOException {
    
    try {
      mutex.lockInterruptibly();
      
      // Wait for iterators and other ops to finish
      while(pendingOps.get() > 0 || compactionsSuspended.get()) {
        LockSupport.parkNanos(100000000L);
      }
      
      if (null != db) {
        this.db.close();
        this.db = null;
      }
      
      try {
        if (!nativedisabled) {
          db = JniDBFactory.factory.open(new File(home), options);
        } else {
          throw new UnsatisfiedLinkError("Native LevelDB implementation disabled.");
        }
      } catch (UnsatisfiedLinkError ule) {
        ule.printStackTrace();
        if (!javadisabled) {
          System.out.println("WARNING: falling back to pure java implementation of LevelDB.");
          db = Iq80DBFactory.factory.open(new File(home), options);
        } else {
          throw new RuntimeException("No usable LevelDB implementation, aborting.");
        }
      }                
    } catch (InterruptedException ie) {
      throw new RuntimeException("Interrupted while opending LevelDB.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
  }
  
  @Override
  public void close() throws IOException {
    if (null != this.db) {
      this.db.close();
      this.db = null;
      compactionsSuspended.set(false);
    }
  }
  
  @Override
  public void compactRange(byte[] begin, byte[] end) throws DBException {
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
    try {
      this.db.compactRange(begin, end);
    } finally {
      this.pendingOps.decrementAndGet();
    }
  }
  
  @Override
  public WriteBatch createWriteBatch() {
    try {
      mutex.lockInterruptibly();
      return this.db.createWriteBatch();
    } catch (InterruptedException ie) {
      throw new RuntimeException("Interrupted while creating write batch.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }      
    }
  }
  
  @Override
  public void delete(byte[] key) throws DBException {
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
    try {
      this.db.delete(key);
    } finally {
      this.pendingOps.decrementAndGet();
    }
  }
  
  @Override
  public Snapshot delete(byte[] key, WriteOptions options) throws DBException {
    if (options.snapshot()) {
      throw new RuntimeException("Snapshots are unsupported.");      
    }
    
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
    try {
      return this.db.delete(key, options);
    } finally {
      this.pendingOps.decrementAndGet();
    }
  }
  
  @Override
  public byte[] get(byte[] key) throws DBException {
    throw new RuntimeException("Unsupported operation get.");
    //return this.db.get(key);
  }
  
  @Override
  public byte[] get(byte[] key, ReadOptions options) throws DBException {
    throw new RuntimeException("Unsupported operation get.");
    //return this.db.get(key, options);
  }
  
  @Override
  public long[] getApproximateSizes(Range... ranges) {
    throw new RuntimeException("Unsupported operation getApproximateSizes.");
    //return this.db.getApproximateSizes(ranges);
  }
  
  @Override
  public String getProperty(String name) {
    return this.db.getProperty(name);
  }
  
  @Override
  public Snapshot getSnapshot() {
    throw new RuntimeException("Snapshots are unsupported.");
    //return this.db.getSnapshot();
  }
  
  @Override
  public DBIterator iterator() {
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
    return new WarpIterator(pendingOps, this.db.iterator());
  }
  
  @Override
  public DBIterator iterator(ReadOptions options) {
    if (null == options) {
      return iterator();
    }
    if (null != options.snapshot()) {
      throw new RuntimeException("Snapshots are unsupported.");
    }
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }    
    return new WarpIterator(pendingOps, this.db.iterator(options));    
  }
  
  @Override
  public void put(byte[] key, byte[] value) throws DBException {
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
    try {
      this.db.put(key, value);
    } finally {
      this.pendingOps.decrementAndGet();
    }
  }
  
  @Override
  public Snapshot put(byte[] key, byte[] value, WriteOptions options) throws DBException {
    if (options.snapshot()) {
      throw new RuntimeException("Snapshots are unsupported.");      
    }
    
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
    try {
      return this.db.put(key, value, options);
    } finally {
      this.pendingOps.decrementAndGet();
    }
  }
  
  @Override
  public void resumeCompactions() {
    throw new DBException("Unsupported 'resumeCompactions' operation.");
  }
  
  @Override
  public void suspendCompactions() throws InterruptedException {
    throw new DBException("Unsupported 'suspendCompactions' operation.");
  }
  
  @Override
  public void write(WriteBatch updates) throws DBException {
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
    try {
      this.db.write(updates);
    } finally {
      this.pendingOps.decrementAndGet();
    }
  }
  
  @Override
  public Snapshot write(WriteBatch updates, WriteOptions options) throws DBException {
    if (options.snapshot()) {
      throw new RuntimeException("Snapshots are unsupported.");      
    }
    
    try {
      mutex.lockInterruptibly();
      pendingOps.incrementAndGet();
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
    try {
      return this.db.write(updates, options);
    } finally {
      this.pendingOps.decrementAndGet();
    }    
  }
  
  public Object doOffline(Callable callable) throws IOException {
    try {
      mutex.lockInterruptibly();
      
      while(pendingOps.get() > 0 || compactionsSuspended.get()) {
        LockSupport.parkNanos(100000000L);
      }
      
      try {
        // Close the db
        this.db.close();
        this.db = null;

        Object result = callable.call();
        
        // Reopen LevelDB
        open(nativedisabled, javadisabled, home, options);
        
        return result;
      } catch (Throwable t) {
        LOG.error("Exception when attempting offline operation, DB will NOT be reopen.", t);
        throw new RuntimeException("Exception when attempting offline operation, DB will NOT be reopen.", t);
      }
    } catch (InterruptedException ie) {
      throw new DBException("Interrupted while acquiring DB mutex.", ie);
    } finally {
      if (mutex.isHeldByCurrentThread()) {
        mutex.unlock();
      }
    }
  }
  
  public String getHome() {
    return this.home;
  }
  
  @Override
  public void run() {
    while(true) {
      WarpDBCommand command = null;
      try {
         command = this.commandQ.poll(10L, TimeUnit.SECONDS);
         
         // Check if the mutex is held by a dead thread
         // This is bad but could potentially happen if a thread is killed
         // while in a try block in one of the methods of WarpDB, the finally
         // clause would then not be executed and the mutex not released
         // TODO(hbs): think about possible mitigation of this case. One possible strategy:
         //              Allocate a new mutex
         //              Iterate over the threads waiting on the previous mutex, interrupting them all
         //              until there are no more waiters?
                  
         Thread owner = mutex.getOwner();
         if (null != owner && !owner.isAlive()) {
           LOG.warn("WarpDB mutex holder is no longer alive, " + mutex.getHoldCount() + " holds, " + mutex.getQueueLength() + " threads waiting WarpDB.");
         }
         
         if (null == command) {
           continue;
         }         
      } catch (InterruptedException ie) {
        continue;
      }
      
      switch (command) {
        case CLOSE:
          Throwable error = null;
          try {
            mutex.lockInterruptibly();

            while(pendingOps.get() > 0 || compactionsSuspended.get()) {
              LockSupport.parkNanos(100000000L);
            }

            this.close();
          } catch (Throwable t) {
            error = t;
          } finally {
            // Unlock only if we are holding the lock more than once or we failed to close the DB
            if (mutex.isHeldByCurrentThread() && (mutex.getHoldCount() > 1 || null != this.db || null != error)) {
              // We end up holding the lock more than once when we processed multiple CLOSE
              // commands, in this case the unlock below will decrement the hold count and
              // make it fall back to 1
              mutex.unlock();
            }
          }
          break;
        case OPEN:
          // If we are not currently holding the lock or if the DB is not closed, do not attempt
          // to re-open it
          if (mutex.isHeldByCurrentThread() && null == this.db) {
            boolean ok = false;
            try {            
              this.open(this.nativedisabled, javadisabled, home, options);
              ok = true;
            } catch (Throwable t) {
              LOG.error("Error while processing OPEN command, WarpDB will not be re-open automatically.", t);
            } finally {
              if (ok) {
                mutex.unlock();
              }
            }
          }
          break;
      }
    }
  }
  
  public void doClose() throws WarpScriptException {
    try {
      this.commandQ.put(WarpDBCommand.CLOSE);
    } catch (InterruptedException ie) {
      throw new WarpScriptException("Interrupted while attempting to close WarpDB.", ie);
    }
  }

  public void doOpen() throws WarpScriptException {
    try {
      this.commandQ.put(WarpDBCommand.OPEN);
    } catch (InterruptedException ie) {
      throw new WarpScriptException("Interrupted while attempting to open WarpDB.", ie);
    }
  }
  
  public boolean isJavaDisabled() {
    return javadisabled;
  }
  
  public boolean isNativeDisabled() {
    return nativedisabled;
  }
  
  public Options getOptions() {
    //
    // Clone the current options
    //
    
    Options opt = new Options();
    opt.blockRestartInterval(options.blockRestartInterval());
    opt.blockSize(options.blockSize());
    opt.cacheSize(options.cacheSize());
    opt.comparator(options.comparator());
    opt.compressionType(options.compressionType());
    opt.createIfMissing(options.createIfMissing());
    opt.errorIfExists(options.errorIfExists());
    opt.logger(options.logger());
    opt.maxOpenFiles(options.maxOpenFiles());
    opt.paranoidChecks(options.paranoidChecks());
    opt.verifyChecksums(options.verifyChecksums());
    opt.writeBufferSize(options.writeBufferSize());
    
    return opt;
  }
}
