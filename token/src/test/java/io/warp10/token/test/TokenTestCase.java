package io.warp10.token.test;

import io.warp10.crypto.KeyStore;
import io.warp10.crypto.UnsecureKeyStore;
import io.warp10.quasar.filter.QuasarConfiguration;
import org.junit.Ignore;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Ignore
public class TokenTestCase {
  KeyStore getKeyStore() {
    // Create an unsecured  keystore
    UnsecureKeyStore keystore = new UnsecureKeyStore();

    // SET THE KEYS NEEDED BY THE TOKEN FILTER
    keystore.setKey(KeyStore.SIPHASH_TOKEN, keystore.decodeKey("hex:00000000000000000000000000000000"));
    keystore.setKey(KeyStore.SIPHASH_APPID, keystore.decodeKey("hex:00000000000000000000000000000001"));
    keystore.setKey(KeyStore.AES_TOKEN, keystore.decodeKey("hex:00000000000000000000000000000002"));

    return ((KeyStore) (keystore));
  }

  Properties getConfig() {
    Properties conf = new Properties();

    conf.setProperty(QuasarConfiguration.WARP_TRL_PATH, "./trl");
    conf.setProperty(QuasarConfiguration.WARP_TRL_PERIOD, Integer.toString(500));
    conf.setProperty(QuasarConfiguration.WARP_TRL_STARTUP_DELAY, Integer.toString(5000));
    return conf;
  }

  ByteBuffer uuidToByteBuffer(String strUUID) {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.order(ByteOrder.BIG_ENDIAN);

    UUID uuid = UUID.fromString(strUUID);

    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());

    buffer.position(0);
    return buffer;
  }

  List<ByteBuffer> uuidToByteBuffer(List<String> uuids) {
    final List<ByteBuffer> out = new ArrayList<ByteBuffer>();

    for (String uuid: uuids) {
      out.add(uuidToByteBuffer((String) uuid));
    }

    return out;
  }

  byte[] uuidToBinaryByteArray(String strUUID) {
    return uuidToByteBuffer(strUUID).array();
  }

}
