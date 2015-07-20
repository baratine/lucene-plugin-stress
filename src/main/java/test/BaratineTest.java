
package test;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class BaratineTest extends BaseSearchClient
{
  ContentType _defaultContentType
    = ContentType.create("x-application/jamp-rpc", StandardCharsets.UTF_8);

  static byte[] contentTypeHeader
    = "Content-Type: ".getBytes();

  static byte[] CRLF = {'\r', '\n'};

  CloseableHttpClient _client = HttpClients.createDefault();
  JsonFactory _jsonFactory = new JsonFactory();
  private AtomicLong _counter = new AtomicLong(0);

  private AtomicReference<String> _jampChannel = new AtomicReference<>();

  String _baseUrl;

  List<String> _matches = new ArrayList<>(8000);
  ObjectMapper _mapper = new ObjectMapper();

  public BaratineTest(DataProvider dataProvider,
                     int n,
                     int searchRate,
                     String host,
                     int port)
  {
    super(dataProvider, n, searchRate);
    _baseUrl = "http://" + host + ':' + port;
  }

  public void update(InputStream in, String id) throws IOException
  {
  }

  private String getCookie(HttpResponse response)
  {
    Header header = response.getFirstHeader("Set-Cookie");
    if (header == null)
      return null;

    String cookie = header.getValue();

    if (cookie.startsWith("Jamp_Channel="))
      return cookie;

    return null;
  }

  @Override
  public void search(String luceneQuery, String expectedDocId)
    throws IOException
  {
    String url = _baseUrl + "/s/lucene";

    HttpPost post = new HttpPost(url);

    String template
      = "[[\"query\",{},\"/test\",%1$s,\"/test\",\"test\"]]";

    if (_jampChannel != null)
      post.setHeader("Cookie", _jampChannel.get());

    String messageId = Long.toString(_counter.getAndIncrement());

    String data = String.format(template, messageId);

    StringEntity e = new StringEntity(data, _defaultContentType);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    if (_jampChannel == null)
      _jampChannel.set(getCookie(response));

    TestRpcResponse tesResponse = parseResponse(response, messageId);

    //tesResponse.validate();
  }

  private TestRpcResponse parseResponse(CloseableHttpResponse response,
                                        String expectedMessageId)
    throws IOException
  {
    byte[] bytes;

    try (InputStream in = response.getEntity().getContent();
         ByteArrayOutputStream out = new ByteArrayOutputStream(0xFFFF)) {
      byte[] buffer = new byte[0xFFFF];

      int l;

      while ((l = in.read(buffer)) > 0)
        out.write(buffer, 0, l);

      bytes = out.toByteArray();
    }

    if (true)
      return null;

    try (InputStream in = new ByteArrayInputStream(bytes)) {

      JsonParser parser = _jsonFactory.createJsonParser(in);

      JsonNode tree = _mapper.readTree(parser);

      if (tree == null || tree.size() == 0) {
        throw new IllegalStateException(String.format(
          "received no reply for message %1$s",
          expectedMessageId));
      }
      else if (tree.size() > 1) {
        throw new IllegalStateException(String.format(
          "too many replies for message %1$s %2$s",
          expectedMessageId, new String(bytes)));
      }

      JsonNode node = tree.get(0);

      if (!"reply".equals(node.get(0).asText())) {
        System.out.println(" error: " + tree);
      }

      String messageId = node.get(3).asText();

      if (!expectedMessageId.equals(messageId)) {
        throw new IllegalStateException(String.format(
          "unexpected messageId in reply to %1$s '%2$s'",
          expectedMessageId,
          new String(bytes)));
      }

      _matches.add(messageId);

      TestRpcResponse query;

      query = new TestRpcResponse(messageId);
      query.setUpdateResult(node.get(4).asBoolean());

      return query;
    } catch (JsonParseException e) {
      System.err.println(new String(bytes));

      e.printStackTrace();

      throw e;
    }
  }

  @Override
  public List<String> getMatches()
  {
    return _matches;
  }

  class TestRpcResponse
  {
    final private String _messageId;
    private Boolean _updateResult;

    public TestRpcResponse(String messageId)
    {
      _messageId = messageId;
    }

    public void setUpdateResult(Boolean result)
    {
      _updateResult = result;
    }

    public void validate()
    {
      if (!Boolean.TRUE.equals(_updateResult))
        throw new IllegalStateException(String.format(
          "unexpected update result %1$s",
          this.toString()));
    }

    @Override
    public String toString()
    {
      return "BaratineRpcQuery["
             + _messageId + ", "
             + _updateResult + ']';
    }
  }

  private static void update(BaratineTest driver)
  {
    try {
      driver.update(new FileInputStream(
        "/Users/alex/data/wiki/40002/4000225.txt"), "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void search(BaratineTest driver)
  {
    try {
      driver.search("2e6ddb8e-d235-4286-94d0-fc8029f0114a", "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void maina(String[] args)
    throws IOException, InterruptedException, ExecutionException
  {
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    BaratineTest driver = new BaratineTest(new NullDataProvider(1),
                                           1,
                                           1,
                                           "localhost",
                                           8085);

    Future future = executorService.submit(() -> update(driver));
    Thread.sleep(100);
    System.out.println("BaratineDriverRpc.update " + future.get());

    future = executorService.submit(() -> search(driver));
    Thread.sleep(100);
    System.out.println("BaratineDriverRpc.search " + future.get());

    executorService.shutdown();
  }

  public static void testPost(String host,
                              int port,
                              AtomicLong start,
                              int n) throws IOException
  {
    HttpClient client = new HttpClient(host, port);

    int requests = n + 100;

    byte[] data = "[[\"query\",{},\"/test\",0,\"/test\",\"test\"]]".getBytes();

    for (int i = 0; i < requests; i++) {
      if (i == 100 && start.get() == -1)
        start.compareAndSet(-1, System.currentTimeMillis());

      HttpClient.ClientResponseStream response
        = client.post("/s/lucene", "x-application/jamp-rpc", data);

      String reply = response.getAsString();

      //System.out.println(reply);
    }
  }

  public static void main(String[] args)
    throws IOException, InterruptedException
  {

    AtomicLong start = new AtomicLong(-1);

    final int n = 10_000 * 20;

    Thread[] threads = new Thread[2];

    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        try {
          testPost("localhost", 8085, start, n);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }

    for (Thread t : threads) {
      t.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    long end = System.currentTimeMillis();

    System.out.println("BaratineTest.main " + (end - start.get()));

    System.out.println((float) (n * 2) / (end - start.get()) * 1000);
  }
}
