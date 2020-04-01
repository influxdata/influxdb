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
import io.warp10.sensision.Sensision;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class used to keep track of Kafka offsets
 */
public class KafkaOffsetCounters {
  private static final int GROWBY = 128;
  
  private final String topic;
  private final String groupid;
  
  private AtomicLong[] counters = new AtomicLong[128];
  
  private final long ttl;
  
  private long[] committedOffsets = null;

  /**
   * Map of labels for Sensision, meant to be accessed ONLY from synchronized methods
   */
  private final Map<String,String> labels = new HashMap<String,String>();    

  public KafkaOffsetCounters(String topic, String groupid, long ttl) {
    this.topic = topic;
    this.groupid = groupid;
    this.ttl = ttl;
  }
  
  /**
   * Remove per partition counters and associated Sensision metrics
   */
  public synchronized void reset() {
    labels.clear();
    labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, this.topic);
    labels.put(SensisionConstants.SENSISION_LABEL_GROUPID, this.groupid);
    
    for (int i = 0; i < this.counters.length; i++) {
      if (null == this.counters[i]) {
        continue;
      }
      
      labels.put(SensisionConstants.SENSISION_LABEL_PARTITION, Integer.toString(i));
      Sensision.clear(SensisionConstants.SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET, labels);
      
      this.counters[i] = null;
    }
  }
  
  public synchronized void count(int partition, long offset) {    
    AtomicLong counter = null;
    
    if (partition >= this.counters.length) {
      this.counters = Arrays.copyOf(this.counters, this.counters.length + GROWBY);
    }
    counter = counters[partition];
    if (null == counter) {
      counter = new AtomicLong(0L);
      counters[partition] = counter;
    }
    
    counter.set(offset);    
  }
  
  /**
   * Will account for the new offset, checking several things:
   * 
   * 1.- the offset is not below the previously committed offset
   * 2.- the offset does not differ of more than 1 from the previously known counter
   * 
   * If 1. happens, safeCount will return 'false'.
   * If 2. happens, safeCount will throw an IOException.
   * 
   * @param partition
   * @param offset
   * @return
   * @throws IOException
   */
  public synchronized boolean safeCount(int partition, long offset) throws IOException {
    AtomicLong counter = null;
    
    if (partition >= this.counters.length) {
      this.counters = Arrays.copyOf(this.counters, this.counters.length + GROWBY);
    }
    
    counter = counters[partition];
    
    if (null == counter) {
      counter = new AtomicLong(0L);
      counters[partition] = counter;
      counter.set(offset);
      
      //
      // Note that we check if the difference is greater than 2 because the offset returned by the message is the one AFTER the given
      // message but the committed offset is the one BEFORE the last consumed message
      //
      
      if (null != committedOffsets && committedOffsets.length > partition && committedOffsets[partition] >= 0 && 2 < (offset - committedOffsets[partition])) {
        labels.clear();
        labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, this.topic);
        labels.put(SensisionConstants.SENSISION_LABEL_GROUPID, this.groupid);
        labels.put(SensisionConstants.SENSISION_LABEL_PARTITION, Integer.toString(partition));
        Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET_FORWARD_LEAPS, labels, 1);
        throw new IOException("Kafka offset " + offset + " leapt forward relative to previously committed value " + committedOffsets[partition]);
      }
    } else {
      //
      // Check if we leapt forward
      //
      
      long previousOffset = counter.getAndSet(offset);
      
      if (1 < (offset - previousOffset)) {
        labels.clear();
        labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, this.topic);
        labels.put(SensisionConstants.SENSISION_LABEL_GROUPID, this.groupid);
        labels.put(SensisionConstants.SENSISION_LABEL_PARTITION, Integer.toString(partition));
        Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET_FORWARD_LEAPS, labels, 1);
        throw new IOException("Kafka offset " + offset + " leapt forward relative to previous offset " + previousOffset);
      }
    }
    

    //
    // Check if we leapt backward relative to the previous committed offset
    //
    
    if (null != committedOffsets && committedOffsets.length > partition && offset <= committedOffsets[partition]) {
      labels.clear();
      labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, this.topic);
      labels.put(SensisionConstants.SENSISION_LABEL_GROUPID, this.groupid);
      labels.put(SensisionConstants.SENSISION_LABEL_PARTITION, Integer.toString(partition));
      Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET_BACKWARD_LEAPS, labels, 1);
      return false;
    }
  
    return true;
  }
  
  /**
   * Notify the counter that offsets were committed to Kafka, this is
   * only used when checkRatchet is intended to be called
   */
  public synchronized void commit() {    
    if (null == this.committedOffsets || this.committedOffsets.length < this.counters.length) {
      this.committedOffsets = new long[this.counters.length];
    }
    
    for (int i = 0; i < this.committedOffsets.length; i++) {
      this.committedOffsets[i] = null != this.counters[i] ? this.counters[i].get() : -1L;
    }
  }
  
  public synchronized void sensisionPublish() {
    labels.clear();
    labels.put(SensisionConstants.SENSISION_LABEL_TOPIC, this.topic);
    labels.put(SensisionConstants.SENSISION_LABEL_GROUPID, this.groupid);
    
    for (int i = 0; i < this.counters.length; i++) {
      if (null == this.counters[i]) {
        continue;
      }
      
      labels.put(SensisionConstants.SENSISION_LABEL_PARTITION, Integer.toString(i));
      Sensision.set(SensisionConstants.SENSISION_CLASS_WARP_KAFKA_CONSUMER_OFFSET, labels, this.counters[i].get(), ttl);
      // Reset counter so we do not continue publishing an old value
      this.counters[i] = null;
    }
  }  
}
