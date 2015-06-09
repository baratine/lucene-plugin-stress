package test;

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
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class BaratineRpc2 extends BaseSearchClient
{
  //[["reply",{},"/update",3085,true]]
  //[["reply",{},"/search",3023,[{"_searchResult":"3926930","_id":766,"_score":1.9077651500701904}]]]

  static int bufferSize = 8 * 1024;

  String _defaultContentType = "x-application/jamp-rpc";

  private JsonFactory _jsonFactory = new JsonFactory();
  private ObjectMapper _jsonMapper = new ObjectMapper();

  private AtomicLong _counter = new AtomicLong(0);

  private HttpClient _client;

  public BaratineRpc2(DataProvider dataProvider,
                      int n,
                      int searchRate,
                      String host,
                      int port)
    throws IOException
  {
    super(dataProvider, n, searchRate);

    _client = new HttpClient(host, port);
  }

  public void update(InputStream in, String id) throws IOException
  {
    String template
      = "[[\"query\",{},\"/update\",%1$s,\"/service\",\"indexText\", \"%2$s\", \"%3$s\", \"%4$s\"]]";

    StringWriter writer = new StringWriter();

    try (Reader reader = new InputStreamReader(in, "UTF-8")) {
      char[] buffer = new char[bufferSize];
      int l;

      while ((l = reader.read(buffer)) > 0) {
        writer.write(buffer, 0, l);
      }

      writer.flush();
      writer.close();
    }

    String data = writer.getBuffer().toString();

    String messageId = Long.toString(_counter.getAndIncrement());

    data = String.format(template, messageId, "foo", id, data);

    HttpClient.ClientResponseStream response
      = _client.execute(HttpClient.HttpMethod.POST,
                        "/s/lucene",
                        _defaultContentType,
                        data.getBytes());

    LuceneRpcResponse rpcResponse = parseResponse(response, messageId);

    rpcResponse.validate();
  }

  @Override
  public void search(String luceneQuery, String expectedDocId)
    throws IOException
  {
    String messageId = Long.toString(_counter.getAndIncrement());

    String template
      = "[[\"query\",{},\"/search\",%1$s,\"/service\",\"search\", \"%2$s\", \"%3$s\", \"%4$d\"]]";

    String data = String.format(template, messageId, "foo", luceneQuery, 255);

    HttpClient.ClientResponseStream response
      = _client.execute(HttpClient.HttpMethod.POST,
                        "/s/lucene",
                        _defaultContentType,
                        data.getBytes());

    LuceneRpcResponse rpcResponse = null;

    try {
      rpcResponse = parseResponse(response, messageId);

      rpcResponse.setExcpectedSearchResult(expectedDocId);

      rpcResponse.validate();
    } catch (IllegalStateException t) {
      String message;

      if (rpcResponse == null) {
        message = String.format(
          "expected %1$s for query %2$s received empty reply",
          expectedDocId,
          luceneQuery);

      }
      else {
        message = String.format(
          "expected %1$s for query %2$s received %3$s",
          expectedDocId,
          luceneQuery,
          rpcResponse.getSearchResult());
      }

      throw new IllegalStateException(message, t);
    }
  }

  private LuceneRpcResponse parseResponse(HttpClient.ClientResponseStream response,
                                          String expectedMessageId)
    throws IOException
  {
    byte[] bytes;

    InputStream in = response.getInputStream();

    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[bufferSize];

      int len = response.getLength();
      int l;

      while (len > 0) {
        l = in.read(buffer);

        out.write(buffer, 0, l);

        len -= l;
      }

      bytes = out.toByteArray();
    }

    try (InputStream buffer = new ByteArrayInputStream(bytes)) {
      JsonParser parser = _jsonFactory.createJsonParser(buffer);

      JsonNode tree = _jsonMapper.readTree(parser);

      if (tree == null || tree.size() == 0) {
        throw new IllegalStateException(
          String.format("received no reply for message %1$s",
                        expectedMessageId));
      }
      else if (tree.size() > 1) {
        throw new IllegalStateException(String.format(
          "too many replies for message %1$s %2$s",
          expectedMessageId, new String(bytes)));
      }

//BaratineDriver.parseResponse: [["reply",{},"/search",193,[{"_externalId":"3925383","_id":116,"_score":0.44276124238967896}]],["reply",{},"/search",192,[{"_externalId":"3925383","_id":116,"_score":0.44276124238967896}]]]

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

      String type = node.get(2).asText();

      LuceneRpcResponse query;

      if ("/update".equals(type)) {
        query = new LuceneRpcResponse(messageId, QueryType.UPDATE);
        query.setUpdateResult(node.get(4).asBoolean());
      }
      else if ("/search".equals(type)) {
        query = new LuceneRpcResponse(messageId, QueryType.SEARCH);

        String extId = node.get(4).get(0).get("_externalId").asText();

        query.setSearchResult(extId);
      }
      else {
        throw new IllegalStateException(tree.toString());
      }

      return query;
    } catch (NullPointerException | JsonParseException e) {
      throw new IllegalStateException(
        String.format("unexpected result %1$s", new String(bytes)), e);
    }
  }

  @Override
  public List<String> getMatches()
  {
    return null;
  }

  class LuceneRpcResponse
  {
    final private String _messageId;
    private Boolean _updateResult;
    private String _searchResult;

    private QueryType _type;

    private String _expectedSearchResult;

    public LuceneRpcResponse(String messageId, QueryType type)
    {
      _messageId = messageId;
      _type = type;
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

    public void setExcpectedSearchResult(String expected)
    {
      _expectedSearchResult = expected;
    }

    public String getSearchResult()
    {
      return _searchResult;
    }

    public void setSearchResult(String externalId)
    {
      _searchResult = externalId;
    }

    public void validate()
    {
      switch (_type) {
      case SEARCH: {
        if (!_expectedSearchResult.equals(_searchResult))
          throw new IllegalStateException(String.format(
            "unexpected search result %1$s",
            this.toString()));
        break;
      }
      case UPDATE: {
        if (!Boolean.TRUE.equals(_updateResult))
          throw new IllegalStateException(String.format(
            "unexpected update result %1$s",
            this.toString()));
        break;
      }
      default: {
        throw new IllegalStateException();
      }
      }
    }

    @Override
    public String toString()
    {
      return "BaratineRpcQuery["
             + _messageId + ", "
             + _type + ", "
             + _expectedSearchResult + ", "
             + _updateResult + ", "
             + _searchResult + ']';
    }
  }

  enum QueryType
  {
    SEARCH,
    UPDATE
  }

  private static void update(BaratineRpc2 driver)
  {
    try {
      driver.update(new FileInputStream(
        "/Users/alex/data/wiki/40002/4000225.txt"), "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void search(BaratineRpc2 driver)
  {
    try {
      driver.search("2e6ddb8e-d235-4286-94d0-fc8029f0114a", "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args)
    throws IOException, InterruptedException, ExecutionException
  {
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    BaratineRpc2 driver = new BaratineRpc2(new NullDataProvider(1),
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

    executorService.shutdownNow();
  }
}
