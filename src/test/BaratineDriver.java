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
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class BaratineDriver implements SearchEngineDriver
{
  //[["reply",{},"/update",3085,true]]
  //[["reply",{},"/search",3023,[{"_searchResult":"3926930","_id":766,"_score":1.9077651500701904}]]]

  Map<String,BaratineQuery> _queries
    = Collections.synchronizedMap(new HashMap<>());

  ContentType _defaultContentType
    = ContentType.create("x-application/jamp-poll",
                         StandardCharsets.UTF_8);

  ContentType _pollContentType
    = ContentType.create("x-application/jamp-poll",
                         StandardCharsets.UTF_8);

  CloseableHttpClient _client = HttpClients.createDefault();
  JsonFactory _jsonFactory = new JsonFactory();
  private AtomicLong _counter = new AtomicLong(0);

  String _jampChannel;

  public void update(InputStream in, String id) throws IOException
  {
    String url = "http://localhost:8085/s/lucene";

    HttpPost post = new HttpPost(url);

    String template
      = "[[\"query\",{},\"/update\",%1$s,\"/lucene\",\"indexText\", \"%2$s\", \"%3$s\", \"%4$s\"]]";

    StringWriter writer = new StringWriter();

    try (Reader reader = new InputStreamReader(in, "UTF-8")) {
      char[] buffer = new char[0x1024];
      int l;

      while ((l = reader.read(buffer)) > 0) {
        writer.write(buffer, 0, l);
      }

      writer.flush();
      writer.close();
    }

    if (_jampChannel != null)
      post.setHeader("Cookie", _jampChannel);

    String data = writer.getBuffer().toString();

    String messageId = Long.toString(_counter.getAndIncrement());

    _queries.put(messageId, new BaratineQuery(messageId));

    data = String.format(template, messageId, "foo", id, data);

    StringEntity e = new StringEntity(data, _defaultContentType);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    if (_jampChannel == null)
      _jampChannel = getCookie(response);

    BaratineQuery baratineQuery = parseResponse(response);

/*
    System.out.println("BaratineDriver.update["
                       + messageId
                       + "]: "
                       + baratineQuery);
*/

    processQuery(baratineQuery);

    if (!baratineQuery.getUpdateResult())
      throw new IllegalStateException("expected true received "
                                      + baratineQuery);
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
  public void search(String query, String expectedId) throws IOException
  {
    String url = "http://localhost:8085/s/lucene";

    HttpPost post = new HttpPost(url);

    if (_jampChannel != null)
      post.setHeader("Cookie", _jampChannel);

    String messageId = Long.toString(_counter.getAndIncrement());

    _queries.put(messageId, new BaratineQuery(messageId));

    String template
      = "[[\"query\",{},\"/search\",%1$s,\"/lucene\",\"search\", \"%2$s\", \"%3$s\", \"%4$d\"]]";

    String data = String.format(template, messageId, "foo", query, 1);

    StringEntity e = new StringEntity(data, _defaultContentType);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    if (_jampChannel == null)
      _jampChannel = getCookie(response);

    BaratineQuery baratineQuery = parseResponse(response);

/*
    System.out.println("BaratineDriver.search["
                       + messageId
                       + ", " + Thread.currentThread() + " ]: "
                       + baratineQuery);
*/

    processQuery(baratineQuery);

    if (!expectedId.equals(baratineQuery.getSearchResult()))
      throw new IllegalStateException(String.format(
        "expected %1$s recieved %2$s",
        expectedId,
        baratineQuery.toString()));
  }

  @Override
  public void poll() throws IOException
  {
    String url = "http://localhost:8085/s/lucene/lucene";

    HttpPost post = new HttpPost(url);

    if (_jampChannel != null)
      post.setHeader("Cookie", _jampChannel);

    StringEntity e = new StringEntity("[]", _pollContentType);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    BaratineQuery baratineQuery = parseResponse(response);

//    System.out.println("BaratineDriver.poll:  " + baratineQuery);

    processQuery(baratineQuery);
  }

  private BaratineQuery parseResponse(CloseableHttpResponse response)
    throws IOException
  {
    try (InputStream in = response.getEntity().getContent()) {
      ObjectMapper mapper = new ObjectMapper();

      JsonParser parser = _jsonFactory.createJsonParser(in);

      JsonNode tree = mapper.readTree(parser);

      if (tree == null || tree.size() == 0) {
        return null;
      }

      if (!"reply".equals(tree.get(0).get(0).asText())) {
        System.out.println(" error: " + tree);
      }

      String messageId = tree.get(0).get(3).asText();

      String type = tree.get(0).get(2).asText();

      BaratineQuery query = _queries.get(messageId);

      synchronized (query) {
        if ("/update".equals(type)) {
          query.setUpdateResult(tree.get(0).get(4).asBoolean());
        }
        else if ("/search".equals(type)) {
          String extId = tree.get(0).get(4).get(0).get("_externalId").asText();

          query.setSearchResult(extId);
        }
        else {
          throw new IllegalStateException(tree.toString());
        }
      }

      return query;
    }
  }

  private void processQuery(BaratineQuery query)
  {
    if (query == null)
      return;

    synchronized (query) {
      if (query.getUpdateResult() == null && query.getSearchResult() == null) {
        try {
          query.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      else {
        query.notify();

        _queries.remove(query.getMessageId());
      }
    }
  }

  class BaratineQuery
  {
    final private String _messageId;
    private Boolean _updateResult;
    private String _searchResult;

    public BaratineQuery(String messageId)
    {
      _messageId = messageId;
    }

    public String getMessageId()
    {
      return _messageId;
    }

    public Boolean getUpdateResult()
    {
      return _updateResult;
    }

    public void setUpdateResult(Boolean result)
    {
      _updateResult = result;
    }

    public String getSearchResult()
    {
      return _searchResult;
    }

    public void setSearchResult(String externalId)
    {
      _searchResult = externalId;
    }

    @Override public String toString()
    {
      return "BaratineQuery["
             + _messageId
             + ", "
             + _updateResult
             + ", "
             + _searchResult
             + ']';
    }
  }

  enum TokenType
  {
    SEARCH,
    UPDATE
  }

  private static void update(BaratineDriver driver)
  {
    try {
      driver.update(new FileInputStream(
        "/Users/alex/data/wiki/40002/4000225.txt"), "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void search(BaratineDriver driver)
  {
    try {
      driver.search("2e6ddb8e-d235-4286-94d0-fc8029f0114a", "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void poll(BaratineDriver driver)
  {
    try {
      driver.poll();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args)
    throws IOException, InterruptedException
  {
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    BaratineDriver driver = new BaratineDriver();

    executorService.submit(() -> update(driver));
    Thread.sleep(100);
    executorService.submit(() -> poll(driver));
    Thread.sleep(100);

    executorService.submit(() -> search(driver));
    Thread.sleep(100);
    executorService.submit(() -> poll(driver));
    Thread.sleep(100);

    executorService.shutdown();
  }
}
