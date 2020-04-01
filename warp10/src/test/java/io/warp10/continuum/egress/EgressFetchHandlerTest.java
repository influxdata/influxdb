package io.warp10.continuum.egress;

import io.warp10.WarpConfig;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.Metadata;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class EgressFetchHandlerTest {

  private static final ClassLoader CL = EgressFetchHandlerTest.class.getClassLoader();

  @BeforeClass
  public static void beforeClass() throws Exception {
    WarpConfig.safeSetProperties(new StringReader("warp.timeunits=us"));
  }

  @Test
  public void testJsonDumpWithoutExpose() throws Exception {
    String json = jsonDumpWithExpose(false);
    String expect = IOUtils.toString(CL.getResourceAsStream("EgressFetchHandlerTest_1.json"), StandardCharsets.UTF_8);

    assertEquals(expect.trim(), json);
  }

  @Test
  public void testJsonDumpWithExpose() throws Exception {
    String json = jsonDumpWithExpose(true);
    String expect = IOUtils.toString(CL.getResourceAsStream("EgressFetchHandlerExposeTest.json"), StandardCharsets.UTF_8);

    assertEquals(expect.trim(), json);
  }

  private String jsonDumpWithExpose(boolean expose) throws Exception {
    Map<String, String> labels = new HashMap<String, String>();
    labels.put("foo", "bar");
    labels.put(".producer", "33000ee4-3bfc-4940-9aef-4fd27975f6f9");
    labels.put(".owner", "6d9bbc0b-2af8-4053-a287-5d6b95405af0");

    GTSEncoder gts = new GTSEncoder();
    gts.getMetadata()
        .setName("my.class")
        .setLabels(labels)
        .setAttributes(singletonMap("foo", "baz"))
        .setLastActivity(1234);
    gts.addValue(5678, 0, 0, 3.14);
    Iterator<GTSDecoder> iter = singleton(gts.getDecoder()).iterator();

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (PrintWriter pw = new PrintWriter(buf)) { // auto flush
      EgressFetchHandler.jsonDump(pw, iter, 10000, -1, false, false,
          new AtomicReference<Metadata>(null), new AtomicLong(0), expose);
    }

    return buf.toString(StandardCharsets.UTF_8.name());
  }
}
