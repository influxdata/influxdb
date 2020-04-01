//
//   Copyright 2020  SenX S.A.S.
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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbLock;
import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.Filename;
import org.iq80.leveldb.impl.Filename.FileInfo;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.InternalUserComparator;
import org.iq80.leveldb.impl.LogMonitor;
import org.iq80.leveldb.impl.LogMonitors;
import org.iq80.leveldb.impl.LogReader;
import org.iq80.leveldb.impl.LogWriter;
import org.iq80.leveldb.impl.Logs;
import org.iq80.leveldb.impl.MemTable;
import org.iq80.leveldb.impl.SeekingIterable;
import org.iq80.leveldb.impl.TableCache;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.impl.VersionEdit;
import org.iq80.leveldb.impl.VersionSet;
import org.iq80.leveldb.impl.WriteBatchImpl;
import org.iq80.leveldb.impl.WriteBatchImpl.Handler;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.TableBuilder;
import org.iq80.leveldb.util.InternalIterator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceInput;
import org.iq80.leveldb.util.Slices;

public class LevelDBRepair {
  
  private static final InternalKeyComparator internalKeyComparator = new InternalKeyComparator(new BytewiseComparator());

  private static final AtomicLong nextFileNumber = new AtomicLong(0L);
  
  public static synchronized void repair(File path, Options options) throws IOException {
    //
    // Lock the DB
    //
    
    DbLock dbLock = new DbLock(new File(path, Filename.lockFileName()));
    
    List<File> files = Filename.listFiles(path);
    
    nextFileNumber.set(Long.MIN_VALUE);

    List<Long> logs = new ArrayList<Long>();
    
    for (File file: files) {
      FileInfo fileInfo = Filename.parseFileName(file);
      
      if (null == fileInfo) {
        continue;
      }
      
      switch (fileInfo.getFileType()) {
        case DESCRIPTOR:
        case TABLE:
          if (fileInfo.getFileNumber() > nextFileNumber.get()) {
            nextFileNumber.set(fileInfo.getFileNumber());
          }
          break;
        case LOG:
          if (fileInfo.getFileNumber() > nextFileNumber.get()) {
            nextFileNumber.set(fileInfo.getFileNumber());
          }
          logs.add(fileInfo.getFileNumber());
          break;
        default:
      }
    }
    
    //
    // Convert log files to SST
    //

    // Sort log files per increasing number
    Collections.sort(logs);
    
    long maxSequence = Long.MIN_VALUE;
    
    for (long fileno: logs) {
      System.out.println("LOG #" + fileno + " - SCAN");
      long maxSeq = recoverLogFile(path, fileno, options);
      
      if (maxSeq > maxSequence) {
        maxSequence = maxSeq;
      }
    }
    
    //
    // Scan each SST to identify smallest/largest keys and largest sequence number
    //
    files = Filename.listFiles(path);
    
    TableCache tcache = new TableCache(path, 1, new InternalUserComparator(internalKeyComparator), true);

    List<FileMetaData> meta = new ArrayList<FileMetaData>();

    long totalSize = 0;
    long totalCount = 0;
    long totalFiles = 0;
    
    for (File file: files) {
      FileInfo fileInfo = Filename.parseFileName(file);
      
      if (null == fileInfo) {
        continue;
      }
      
      switch (fileInfo.getFileType()) {
        case TABLE:
          if (fileInfo.getFileNumber() > nextFileNumber.get()) {
            nextFileNumber.set(fileInfo.getFileNumber());
          }

          long number = fileInfo.getFileNumber();
          long fileSize = file.length();
          
          InternalKey smallest = null;
          InternalKey largest = null;

          try {
            InternalIterator iter = tcache.newIterator(number);
            
            long count = 0;
            
            while(iter.hasNext()) {
              Entry<InternalKey, Slice> entry = iter.next();
            
              if (null == smallest) {
                smallest = entry.getKey();
              }
              largest = entry.getKey();
              count++;
            }
            
            meta.add(new FileMetaData(number, fileSize, smallest, largest));
            
            totalSize += fileSize;
            totalCount += count;
            totalFiles++;
            System.out.println("SST #" + number + " - " + count + " entries, " + fileSize + " bytes.");            
          } catch (Exception ioe) {
            //
            // Ignore the current SST file
            //
            file.renameTo(new File(file.getAbsolutePath() + ".corrupt"));
            System.out.println("SST #" + number + " - CORRUPTED [" + (null != smallest ? smallest.getUserKey() : "-") + "]-[" + (null != largest ? largest.getUserKey() : "-") + "]");
          }
          break;
        default:
      }
    }
    System.out.println("------------------------");
    System.out.println("Scanned " + totalFiles + " SST files - " + totalCount + " entries, " + totalSize + " bytes.");
    
    //
    // Sort the files by increasing sequence number
    //
    
    Collections.sort(meta, new Comparator<FileMetaData>() {
      @Override
      public int compare(FileMetaData o1, FileMetaData o2) {
        return Long.compare(o1.getNumber(), o2.getNumber());
      }
    });

    //
    // Now generate the MANIFEST file, putting all files at level 0
    //
    
    // Remove CURRENT file
    
    File currentFile = new File(path, Filename.currentFileName());
    currentFile.delete();
    
    //
    // Initialize a MANIFEST (from VersionSet constructor)
    //
    
    long manifestFileNumber = nextFileNumber.addAndGet(1);
    VersionEdit edit = new VersionEdit();
    edit.setComparatorName(internalKeyComparator.name());
    edit.setLogNumber(nextFileNumber.addAndGet(1));
    edit.setNextFileNumber(nextFileNumber.addAndGet(1));
    edit.setLastSequenceNumber(maxSequence);

    LogWriter log = Logs.createLogWriter(new File(path, Filename.descriptorFileName(manifestFileNumber)), manifestFileNumber);
    
    try {
      log.addRecord(edit.encode(), false);
    } finally {
      log.close();
    }

    Filename.setCurrentFile(path, log.getFileNumber());

    VersionSet set = new VersionSet(path, tcache, internalKeyComparator);
    set.recover();
    
    edit = new VersionEdit();
    
    for (FileMetaData f: meta) {
      edit.addFile(0, f);
    }

    set.logAndApply(edit);
    set.destroy();
    
    tcache.close();
    
    dbLock.release();
  }
  
  private static class InsertIntoHandler implements Handler {
    private long sequence;
    private final MemTable memTable;

    public InsertIntoHandler(MemTable memTable, long sequenceBegin) {
      this.memTable = memTable;
      this.sequence = sequenceBegin;
    }

    @Override
    public void put(Slice key, Slice value) {
      memTable.add(sequence++, ValueType.VALUE, key, value);
    }

    @Override
    public void delete(Slice key) {
      memTable.add(sequence++, ValueType.DELETION, key, Slices.EMPTY_SLICE);
    }
  }

  /**
   * Convert a log file to a table
   * This is adapted from org.iq80.leveldb.impl.DbImpl#recoverLogFile
   */
  private static long recoverLogFile(File databaseDir, long fileNumber, Options opt) throws IOException {
    Options options = null != opt ? opt : new Options();
    File file = new File(databaseDir, Filename.logFileName(fileNumber));
    try (FileInputStream fis = new FileInputStream(file); FileChannel channel = fis.getChannel()) {
      LogMonitor logMonitor = LogMonitors.logMonitor();
      LogReader logReader = new LogReader(channel, logMonitor, true, 0);

      // Read all the records and add to a memtable
      long maxSequence = 0;
      MemTable memTable = null;
      for (Slice record = logReader.readRecord(); record != null; record = logReader.readRecord()) {
        SliceInput sliceInput = record.input();
        // read header
        if (sliceInput.available() < 12) {
          logMonitor.corruption(sliceInput.available(), "log record too small");
          continue;
        }
        
        long sequenceBegin = sliceInput.readLong();
        int updateSize = sliceInput.readInt();

        // read entries
        WriteBatchImpl writeBatch = readWriteBatch(sliceInput, updateSize);

        // apply entries to memTable
        if (memTable == null) {
          memTable = new MemTable(internalKeyComparator);
        }
        
        writeBatch.forEach(new InsertIntoHandler(memTable, sequenceBegin));

        // update the maxSequence
        long lastSequence = sequenceBegin + updateSize - 1;
        if (lastSequence > maxSequence) {
          maxSequence = lastSequence;
        }

        // flush mem table if necessary
        if (memTable.approximateMemoryUsage() > options.writeBufferSize()) {
          FileMetaData sst = writeLevel0Table(databaseDir, memTable);
          if (null != sst) {
            System.out.println("LOG #" + fileNumber + " >>> SST #" + sst.getNumber());
          } else {
            System.out.println("LOG #" + fileNumber + " >>> NO SST");
          }
          memTable = null;
        }
      }
      
      // flush mem table
      if (memTable != null && !memTable.isEmpty()) {
        FileMetaData sst = writeLevel0Table(databaseDir, memTable);
        if (null != sst) {
          System.out.println("LOG #" + fileNumber + " >>> SST #" + sst.getNumber());
        } else {
          System.out.println("LOG #" + fileNumber + " >>> NO SST");
        }
      }
      
      file.renameTo(new File(file.getAbsolutePath() + ".converted"));

      return maxSequence;
    }
  }
  
  private static WriteBatchImpl readWriteBatch(SliceInput record, int updateSize) throws IOException {
    WriteBatchImpl writeBatch = new WriteBatchImpl();
    int entries = 0;
    while (record.isReadable()) {
      entries++;
      ValueType valueType = ValueType.getValueTypeByPersistentId(record.readByte());
      if (valueType == ValueType.VALUE) {
        Slice key = Slices.readLengthPrefixedBytes(record);
        Slice value = Slices.readLengthPrefixedBytes(record);
        writeBatch.put(key, value);
      } else if (valueType == ValueType.DELETION) {
        Slice key = Slices.readLengthPrefixedBytes(record);
        writeBatch.delete(key);
      } else {
        throw new IllegalStateException("Unexpected value type " + valueType);
      }
    }

    if (entries != updateSize) {
      throw new IOException(String.format("Expected %d entries in log record but found %s entries", updateSize, entries));
    }

    return writeBatch;
  }

  private static FileMetaData writeLevel0Table(File databaseDir, MemTable mem) throws IOException {
    // skip empty mem table
    if (mem.isEmpty()) {
      return null;
    }

    // write the memtable to a new sstable
    long fileNumber = nextFileNumber.addAndGet(1L);

    return buildTable(databaseDir, mem, fileNumber);
  }

  private static FileMetaData buildTable(File databaseDir, SeekingIterable<InternalKey, Slice> data, long fileNumber) throws IOException {
    Options options = new Options();
    
    File file = new File(databaseDir, Filename.tableFileName(fileNumber));
    try {
      InternalKey smallest = null;
      InternalKey largest = null;
      FileChannel channel = new FileOutputStream(file).getChannel();
      try {
        TableBuilder tableBuilder = new TableBuilder(options, channel, new InternalUserComparator(internalKeyComparator));

        for (Entry<InternalKey, Slice> entry : data) {
          // update keys
          InternalKey key = entry.getKey();
          if (smallest == null) {
            smallest = key;
          }
          largest = key;

          tableBuilder.add(key.encode(), entry.getValue());
        }

        tableBuilder.finish();
      } finally {
        try {
          channel.force(true);
        } finally {
          channel.close();
        }
      }

      if (smallest == null) {
        return null;
      }
      
      FileMetaData fileMetaData = new FileMetaData(fileNumber, file.length(), smallest, largest);

      return fileMetaData;
    } catch (IOException e) {
      file.delete();
      throw e;
    }
  }
  
  public static void main(String[] args) throws Exception {
    repair(new File(args[0]), new Options());       
  }
}
