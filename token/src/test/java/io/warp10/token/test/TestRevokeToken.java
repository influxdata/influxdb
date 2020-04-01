package io.warp10.token.test;

import io.warp10.quasar.encoder.QuasarTokenEncoder;
import io.warp10.quasar.filter.QuasarTokenFilter;
import io.warp10.quasar.filter.QuasarTokensRevoked;
import io.warp10.quasar.filter.exception.QuasarTokenException;
import io.warp10.quasar.filter.exception.QuasarTokenRevoked;
import io.warp10.quasar.trl.QuasarTRL;
import org.apache.thrift.TException;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

public class TestRevokeToken extends TokenTestCase {
  @Test
  public void testRevokeToken() throws IllegalAccessException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException, TException, QuasarTokenException {
    // Generate 2 tokens
    String producer = UUID.randomUUID().toString();
    String app = "warp10.test";
    long ttl = 32468;

    String readTokenValid = tokenEncoder.deliverReadToken(app, producer, producer, new ArrayList<String>(Arrays.asList(app)), ttl, getKeyStore());
    final String readTokenRevoked = tokenEncoder.deliverReadToken(app, producer, producer, new ArrayList<String>(Arrays.asList(app)), ttl, getKeyStore());

    //instanciate a token filter
    final QuasarTokenFilter tokenFilter = new QuasarTokenFilter(getConfig(), getKeyStore());
    // should work
    tokenFilter.getReadToken(readTokenValid);

    // get the token ident by accessing private method getTokenSipHash
    Method m = tokenFilter.getClass().getDeclaredMethod("getTokenSipHash", byte[].class);
    m.setAccessible(true);
    long tokenIdent = (long)m.invoke(tokenFilter, readTokenRevoked.getBytes());

    // revoke it manually
    QuasarTRL trl = new QuasarTRL();
    trl.revokeToken(tokenIdent);

    // Get access to private field quasarTokenRevoked
    Field f = tokenFilter.getClass().getDeclaredField("quasarTokenRevoked");
    f.setAccessible(true);
    QuasarTokensRevoked revoked = (QuasarTokensRevoked) f.get(tokenFilter);

    revoked.onQuasarTRL(trl);

    try {
      tokenFilter.getReadToken(readTokenRevoked);
      assert false;
    } catch (QuasarTokenException qte) {
      assert qte instanceof QuasarTokenRevoked;
    }
  }

  private QuasarTokenEncoder tokenEncoder = new QuasarTokenEncoder();
}
