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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import io.warp10.quasar.token.thrift.data.ReadToken;

public class TestReadToken extends TokenTestCase {
  @Test
  public void testGenerateAndDecodeStandard() throws TException, QuasarTokenException {
    // generate a valid token with
    // owner = producer
    String producer = UUID.randomUUID().toString();
    String app = "warp10.test";
    long ttl = 32468;

    String readToken = tokenEncoder.deliverReadToken(app, producer, producer, new ArrayList<String>(Arrays.asList(app)), ttl, getKeyStore());

    // decode it with the token filter
    QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());
    ReadToken tokenDecoded = tokenFilter.getReadToken(readToken);

    // asserts token values
    assert ttl == ((long) tokenDecoded.getExpiryTimestamp() - (long) tokenDecoded.getIssuanceTimestamp());

    // this token belongs to this app and producer
    tokenDecoded.getAppName().equals(app);
    assert Arrays.equals(tokenDecoded.getBilledId(), uuidToBinaryByteArray(producer));

    // matrix' token authorisation
    assert tokenDecoded.getApps().size() == 1;
    assert tokenDecoded.getOwners().size() == 1;
    assert tokenDecoded.getProducers().size() == 0;
    assert tokenDecoded.getApps().contains(app);
    assert tokenDecoded.getOwners().contains(uuidToByteBuffer(producer));

    // no hooks
    assert tokenDecoded.getHooks() == null;
  }

  @Test
  public void testGenerateAndDecodeCustom() throws TException, QuasarTokenException {
    // generate a valid token with a exotic authorization matrix
    //
    // this token belongs to
    String tokenOwner = UUID.randomUUID().toString();
    String tokenApp = "warp10.test";

    // this token reads data from 2 other apps
    List<String> apps = new ArrayList<String>(Arrays.asList("warp10.app1", "warp10.app2"));
    // from 2 data owners
    List<String> owners = new ArrayList<String>(Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>(1);
    map.put("PRIVATE_HOOK", "NOW");
    Map<String, String> hooks = map;

    long ttl = 32468;

    String readToken = tokenEncoder.deliverReadToken(tokenApp, tokenOwner, owners, apps, hooks, ttl, getKeyStore());

    // decode it with the token filter
    QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());
    ReadToken tokenDecoded = tokenFilter.getReadToken(readToken);

    // asserts token values
    assert ttl == ((long) tokenDecoded.getExpiryTimestamp() - (long) tokenDecoded.getIssuanceTimestamp());

    // this token belongs to this app and producer
    tokenDecoded.getAppName().equals(tokenApp);
    assert Arrays.equals(tokenDecoded.getBilledId(), uuidToBinaryByteArray(tokenOwner));

    // matrix' token authorisation
    assert tokenDecoded.getApps().equals(apps);
    assert tokenDecoded.getOwners().equals(uuidToByteBuffer(owners));
    assert tokenDecoded.getProducers().size() == 0;

    // hooks
    assert tokenDecoded.getHooks().equals(hooks);
  }

  @Test
  public void testTokenExpired() throws InterruptedException, TException {
    // token with a validity of 20ms, its quite short
    String uuid = UUID.randomUUID().toString();
    final String readToken = tokenEncoder.deliverReadToken("app", uuid, uuid, new ArrayList<String>(Arrays.asList("app")), 20, getKeyStore());

    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    Thread.sleep(40);

    try {
      tokenFilter.getReadToken(readToken);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenExpired;
    }
  }

  @Test
  public void testNoToken() {
    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    try {
      tokenFilter.getReadToken(null);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarNoToken;
    }

    try {
      tokenFilter.getReadToken("");
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarNoToken;
    }
  }

  @Test
  public void testTokenAESCorrupted() throws TException {
    // generate a token
    String uuid = UUID.randomUUID().toString();
    String readToken = tokenEncoder.deliverReadToken("app", uuid, uuid, new ArrayList<String>(Arrays.asList("app")), 32468, getKeyStore());

    // corrupt the token (pick a random character and decrement it)
    int corruptedIndex = new Random().nextInt(60);
    readToken = readToken.substring(0, corruptedIndex) + (readToken.charAt(corruptedIndex) - 1) + readToken.substring(corruptedIndex + 1);

    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    try {
      tokenFilter.getReadToken(readToken);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenInvalid;
    }
  }

  @Test
  public void testTokenB64Corrupted() throws TException {
    // generate a token
    String uuid = UUID.randomUUID().toString();
    String readToken = tokenEncoder.deliverReadToken("app", uuid, uuid, new ArrayList<String>(Arrays.asList("app")), 32468, getKeyStore());

    // corrupt the token
    readToken = "&~#{[]}".concat(readToken);

    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    try {
      tokenFilter.getReadToken(readToken);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenInvalid;
    }
  }

  @Test
  public void testBadTokenType() throws TException {
    String uuid = UUID.randomUUID().toString();
    final String writeToken = tokenEncoder.deliverWriteToken("app", uuid, uuid, null, 32468, getKeyStore());

    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());

    try {
      tokenFilter.getReadToken(writeToken);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenInvalid;
    }
  }

  private QuasarTokenEncoder tokenEncoder = new QuasarTokenEncoder();
}
