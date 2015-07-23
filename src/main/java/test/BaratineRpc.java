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
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class BaratineRpc extends BaseSearchClient
{
  //[["reply",{},"/update",3085,true]]
  //[["reply",{},"/search",3023,[{"_searchResult":"3926930","_id":766,"_score":1.9077651500701904}]]]

  ContentType _defaultContentType
    = ContentType.create("x-application/jamp-rpc", StandardCharsets.UTF_8);

  CloseableHttpClient _client = HttpClients.createDefault();
  JsonFactory _jsonFactory = new JsonFactory();
  private AtomicLong _counter = new AtomicLong(0);

  private AtomicReference<String> _jampChannel = new AtomicReference<>();

  String _baseUrl;

  List<String> _matches = new ArrayList<>(8000);

  public BaratineRpc(DataProvider dataProvider,
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
    String url = _baseUrl + "/s/lucene";

    HttpPost post = new HttpPost(url);

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

    if (_jampChannel != null)
      post.setHeader("Cookie", _jampChannel.get());

    String data = writer.getBuffer().toString();

    String messageId = Long.toString(_counter.getAndIncrement());

    data = String.format(template, messageId, "foo", id, data);

    StringEntity e = new StringEntity(data, _defaultContentType);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    if (_jampChannel == null)
      _jampChannel.set(getCookie(response));

    LuceneRpcResponse rpcResponse = parseResponse(response, messageId);

    rpcResponse.validate();
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

    if (_jampChannel != null)
      post.setHeader("Cookie", _jampChannel.get());

    String messageId = Long.toString(_counter.getAndIncrement());

    String template
      = "[[\"query\",{},\"/search\",%1$s,\"/service\",\"search\", \"%2$s\", \"%3$s\", \"%4$d\"]]";

    String data = String.format(template, messageId, "foo", luceneQuery, 255);

    StringEntity e = new StringEntity(data, _defaultContentType);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    if (_jampChannel == null)
      _jampChannel.set(getCookie(response));

    LuceneRpcResponse rpcResponse = parseResponse(response, messageId);

    rpcResponse.setExcpectedSearchResult(expectedDocId);

    rpcResponse.validate();
  }

  private LuceneRpcResponse parseResponse(CloseableHttpResponse response,
                                          String expectedMessageId)
    throws IOException
  {
    byte[] bytes;

    try (InputStream in = response.getEntity().getContent();
         ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[0xFFFF];

      int l;

      while ((l = in.read(buffer)) > 0)
        out.write(buffer, 0, l);

      bytes = out.toByteArray();
    }

    try (InputStream in = new ByteArrayInputStream(bytes)) {
      ObjectMapper mapper = new ObjectMapper();

      JsonParser parser = _jsonFactory.createJsonParser(in);

      JsonNode tree = mapper.readTree(parser);

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

//BaratineDriver.parseResponse: [["reply",{},"/search",193,[{"_externalId":"3925383","_id":116,"_score":0.44276124238967896}]],["reply",{},"/search",192,[{"_externalId":"3925383","_id":116,"_score":0.44276124238967896}]]]

      LuceneRpcResponse[] queries = new LuceneRpcResponse[1];

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

  private static void update(BaratineRpc driver)
  {
    try {
      driver.update(new FileInputStream(
        "/Users/alex/data/wiki/40002/4000225.txt"), "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void search(BaratineRpc driver)
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

    BaratineRpc driver = new BaratineRpc(new NullDataProvider(1),
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
}
