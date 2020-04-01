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

package io.warp10.continuum.ingress;

import io.warp10.continuum.Configuration;
import io.warp10.continuum.KafkaOffsetCounters;
import io.warp10.continuum.KafkaSynchronizedConsumerPool;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.quasar.token.thrift.data.ReadToken;
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

public class IngressMetadataConsumerFactory implements ConsumerFactory {
  
  private final Ingress ingress;
  
  public IngressMetadataConsumerFactory(Ingress ingress) {
    this.ingress = ingress;
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

              Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_INGRESS_KAFKA_META_IN_MESSAGES, Sensision.EMPTY_LABELS, 1);
              Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_INGRESS_KAFKA_META_IN_BYTES, Sensision.EMPTY_LABELS, data.length);
              
              if (null != ingress.SIPHASH_KAFKA_META) {
                data = CryptoUtils.removeMAC(ingress.SIPHASH_KAFKA_META, data);
              }
              
              // Skip data whose MAC was not verified successfully
              if (null == data) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_INGRESS_KAFKA_META_IN_INVALIDMACS, Sensision.EMPTY_LABELS, 1);
                continue;
              }
              
              // Unwrap data if need be
              if (null != ingress.AES_KAFKA_META) {
                data = CryptoUtils.unwrap(ingress.AES_KAFKA_META, data);
              }
              
              // Skip data that was not unwrapped successfully
              if (null == data) {
                Sensision.update(SensisionConstants.SENSISION_CLASS_WARP_INGRESS_KAFKA_META_IN_INVALIDCIPHERS, Sensision.EMPTY_LABELS, 1);
                continue;
              }
              
              //
              // Extract Metadata
              //
              
              //
              // TODO(hbs): We could check that metadata class/labels Id match those of the key, but
              // since it was wrapped/authenticated, we suppose it's ok.
              //
                          
              byte[] clslblsBytes = Arrays.copyOf(data, 16);
              BigInteger clslblsId = new BigInteger(clslblsBytes);
              
              byte[] metadataBytes = Arrays.copyOfRange(data, 16, data.length);

              Metadata metadata = new Metadata();
              deserializer.deserialize(metadata, metadataBytes);
              
              //
              // Only handle DELETE and METADATA sources
              // We treat those two types of updates the same way, by removing the cache entry
              // for the corresponding Metadata. By doing so we simplify handling
              //
              
              if (Configuration.INGRESS_METADATA_DELETE_SOURCE.equals(metadata.getSource())) {
                //
                // Remove entry from Metadata cache
                //
                
                synchronized(ingress.metadataCache) {
                  ingress.metadataCache.remove(clslblsId);
                }
                continue;
              } else if (Configuration.INGRESS_METADATA_UPDATE_ENDPOINT.equals(metadata.getSource())) {
                //
                // Remove entry from Metadata cache
                //
                
                synchronized(ingress.metadataCache) {
                  ingress.metadataCache.remove(clslblsId);
                }
                continue;
              } else {
                continue;
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
