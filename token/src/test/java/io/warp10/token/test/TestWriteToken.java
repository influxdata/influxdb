package io.warp10.token.test;

import io.warp10.quasar.encoder.QuasarTokenEncoder;
import io.warp10.quasar.filter.QuasarTokenFilter;
import io.warp10.quasar.filter.exception.QuasarNoToken;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.filter.exception.QuasarTokenExpired;
import io.warp10.quasar.filter.exception.QuasarTokenInvalid;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import io.warp10.quasar.token.thrift.data.WriteToken;

public class TestWriteToken extends TokenTestCase {
  @Test
  public void testGenerateAndDecodeStandard() throws TException, QuasarTokenException {
    // generate a valid token with
    // owner = producer
    String uuid = UUID.randomUUID().toString();
    String app = "warp10.test";
    long ttl = 32468;

    String writeToken = tokenEncoder.deliverWriteToken(app, uuid, ttl, getKeyStore());

    // decode it with the token filter
    QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());
    WriteToken tokenDecoded = tokenFilter.getWriteToken(writeToken);

    // asserts token values
    assert ttl == ((long) tokenDecoded.getExpiryTimestamp() - (long) tokenDecoded.getIssuanceTimestamp());

    // this token belongs to this app and producer
    tokenDecoded.getAppName().equals(app);
    assert Arrays.equals(tokenDecoded.getOwnerId(), uuidToBinaryByteArray(uuid));
    assert Arrays.equals(tokenDecoded.getProducerId(), uuidToBinaryByteArray(uuid));

    // no fixes labels, no indices
    assert tokenDecoded.getLabels() == null;
    assert tokenDecoded.getIndices() == null;
  }

  @Test
  public void testGenerateAndDecode() throws TException, QuasarTokenException {
    // generate a valid token with
    // owner = producer
    String owner = UUID.randomUUID().toString();
    String producer = UUID.randomUUID().toString();
    String app = "warp10.test";
    long ttl = 32468;

    String writeToken = tokenEncoder.deliverWriteToken(app, producer, owner, ttl, getKeyStore());

    // decode it with the token filter
    QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());
    WriteToken tokenDecoded = tokenFilter.getWriteToken(writeToken);

    // asserts token values
    assert ttl == ((long) tokenDecoded.getExpiryTimestamp() - (long) tokenDecoded.getIssuanceTimestamp());

    // this token belongs to this app and producer
    tokenDecoded.getAppName().equals(app);
    assert Arrays.equals(tokenDecoded.getOwnerId(), uuidToBinaryByteArray(owner));
    assert Arrays.equals(tokenDecoded.getProducerId(), uuidToBinaryByteArray(producer));

    // no fixes labels, no indices
    assert tokenDecoded.getLabels() == null;
    assert tokenDecoded.getIndices() == null;
  }

  @Test
  public void testGenerateAndDecodeWithLabels() throws TException, QuasarTokenException {
    // generate a valid token with
    // owner = producer
    String owner = UUID.randomUUID().toString();
    String producer = UUID.randomUUID().toString();
    String app = "warp10.test";
    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>(1);
    map.put("labelKey", "labelValue");
    Map<String, String> labels = map;
    long ttl = 32468;

    String writeToken = tokenEncoder.deliverWriteToken(app, producer, owner, labels, ttl, getKeyStore());

    // decode it with the token filter
    QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());
    WriteToken tokenDecoded = tokenFilter.getWriteToken(writeToken);

    // asserts token values
    assert ttl == ((long) tokenDecoded.getExpiryTimestamp() - (long) tokenDecoded.getIssuanceTimestamp());

    // this token belongs to this app and producer
    tokenDecoded.getAppName().equals(app);
    assert Arrays.equals(tokenDecoded.getOwnerId(), uuidToBinaryByteArray(owner));
    assert Arrays.equals(tokenDecoded.getProducerId(), uuidToBinaryByteArray(producer));

    // fixed labels on write
    assert tokenDecoded.getLabels().equals(labels);

    // no indices
    assert tokenDecoded.getIndices() == null;
  }

  @Test
  public void testTokenExpired() throws TException, InterruptedException {
    // token with a validity of 1ms, its quite short
    String uuid = UUID.randomUUID().toString();
    final String writeToken = tokenEncoder.deliverWriteToken("app", uuid, 1, getKeyStore());

    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    Thread.sleep(1);

    try {
      tokenFilter.getWriteToken(writeToken);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenExpired;
    }
  }

  @Test
  public void testNoToken() {
    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    try {
      tokenFilter.getWriteToken(null);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarNoToken;
    }

    try {
      tokenFilter.getWriteToken("");
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarNoToken;
    }
  }

  @Test
  public void testTokenAESCorrupted() throws TException {
    // generate a token
    String uuid = UUID.randomUUID().toString();
    String writeToken = tokenEncoder.deliverWriteToken("app", uuid, 32478, getKeyStore());

    // corrupt the token (pick a random character and decrement it)
    int corruptedIndex = new Random().nextInt(60);
    writeToken = writeToken.substring(0, corruptedIndex) + (writeToken.charAt(corruptedIndex) - 1) + writeToken.substring(corruptedIndex + 1);

    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    try {
      tokenFilter.getWriteToken(writeToken);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenInvalid;
    }
  }

  @Test
  public void testTokenB64Corrupted() throws TException {
    // generate a token
    String uuid = UUID.randomUUID().toString();
    String writeToken = tokenEncoder.deliverWriteToken("app", uuid, 32478, getKeyStore());

    // corrupt the token (replace a by A)
    writeToken = "&~#{[]}".concat(writeToken);

    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    try {
      tokenFilter.getWriteToken(writeToken);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenInvalid;
    }
  }

  @Test
  public void testBadTokenType() throws TException {
    String uuid = UUID.randomUUID().toString();
    final String readToken = tokenEncoder.deliverReadToken("app", uuid, uuid, new ArrayList<String>(Arrays.asList("app")), 32468, getKeyStore());

    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    try {
      tokenFilter.getWriteToken(readToken);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenInvalid;
    }
  }

  private QuasarTokenEncoder tokenEncoder = new QuasarTokenEncoder();
}
