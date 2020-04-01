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

import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.ingress.Ingress;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.HyperLogLogPlus;
import io.warp10.sensision.Sensision;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.primitives.Longs;

public class ThrottlingManagerEstimatorConsumerFactory implements ConsumerFactory {
  
  private final byte[] macKey;
  
  public ThrottlingManagerEstimatorConsumerFactory(byte[] macKey) {
    this.macKey = macKey;
  }
  
  @Override
  public Runnable getConsumer(final KafkaSynchronizedConsumerPool pool, final KafkaStream<byte[], byte[]> stream) {
    
    return new Runnable() {          
      @Override
      public void run() {
        ConsumerIterator<byte[],byte[]> iter = stream.iterator();

        // Iterate on the messages
        TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

        KafkaOffsetCounters counters = pool.getCounters();
        
        try {
          while (iter.hasNext()) {
            //
            // Since the call to 'next' may block, we need to first
            // check that there is a message available
            //
            
            boolean nonEmpty = iter.nonEmpty();
            
            if (nonEmpty) {
              MessageAndMetadata<byte[], byte[]> msg = iter.next();
              counters.count(msg.partition(), msg.offset());
              
              byte[] data = msg.message();

              Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_KAFKA_THROTTLING_IN_MESSAGES, Sensision.EMPTY_LABELS, 1);
              Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_KAFKA_THROTTLING_IN_BYTES, Sensision.EMPTY_LABELS, data.length);
              
              if (null != macKey) {
                data = CryptoUtils.removeMAC(macKey, data);
              }
              
              // Skip data whose MAC was not verified successfully
              if (null == data) {
                Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_KAFKA_THROTTLING_IN_INVALIDMACS, Sensision.EMPTY_LABELS, 1);
                continue;
              }

              //
              // Update throttling manager
              //
              
              try {
                ThrottlingManager.fuse(HyperLogLogPlus.fromBytes(data));
                Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_THROTLLING_FUSIONS, Sensision.EMPTY_LABELS, 1);
              } catch (Exception e) {
                Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_THROTLLING_FUSIONS_FAILED, Sensision.EMPTY_LABELS, 1);
              }
            }
          }        
        } catch (Throwable t) {
          t.printStackTrace(System.err);
        } finally {
          // Set abort to true in case we exit the 'run' method
          pool.getAbort().set(true);
        }
      }
    };
  }
}
