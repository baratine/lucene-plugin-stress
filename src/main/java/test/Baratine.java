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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class Baratine extends BaseSearchClient
{
  //[["reply",{},"/update",3085,true]]
  //[["reply",{},"/search",3023,[{"_searchResult":"3926930","_id":766,"_score":1.9077651500701904}]]]

  Map<String,BaratineQuery> _queries = new ConcurrentHashMap<>();

  ContentType _defaultContentType
    = ContentType.create("x-application/jamp-push",
                         StandardCharsets.UTF_8);

  ContentType _pollContentType
    = ContentType.create("x-application/jamp-pull",
                         StandardCharsets.UTF_8);

  CloseableHttpClient _client = HttpClients.createDefault();
  JsonFactory _jsonFactory = new JsonFactory();
  private AtomicLong _counter = new AtomicLong(0);

  String _jampChannel;
  boolean _isPreload;

  String _baseUrl;
  private Thread _pollThread;

  public Baratine(DataProvider dataProvider,
                  int n,
                  int searchRate,
                  String host,
                  int port)
  {
    super(dataProvider, n, searchRate);
    _baseUrl = "http://" + host + ':' + port;
  }

  public void update(InputStream in, String id)
    throws IOException
  {
    String url = _baseUrl + "/s/lucene";

    HttpPost post = new HttpPost(url);

    String template
      = "[[\"query\",{},\"/update\",%1$s,\"/session\",\"indexText\", \"%2$s\", \"%3$s\", \"%4$s\"]]";

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

    BaratineQuery updateQuery
      = new BaratineQuery(messageId, QueryType.UPDATE);

    _queries.put(messageId, updateQuery);

    data = String.format(template, messageId, "foo", id, data);

    StringEntity e = new StringEntity(data, _defaultContentType);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    if (_jampChannel == null)
      _jampChannel = getCookie(response);

    BaratineQuery[] queries = parseResponse(response);

    boolean isMatch = false;

    for (BaratineQuery query : queries) {
      if (query == updateQuery) {
        isMatch = true;
        _queries.remove(messageId);
      }
      else {
        processQuery(query);
      }
    }

    if (!isMatch)
      processQuery(updateQuery);

    if (!_isPreload)
      updateQuery.validate();
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
  public void search(String luceneQuery, String expectedId) throws IOException
  {
    String url = _baseUrl + "/s/lucene";

    HttpPost post = new HttpPost(url);

    if (_jampChannel != null)
      post.setHeader("Cookie", _jampChannel);

    String messageId = Long.toString(_counter.getAndIncrement());

    final BaratineQuery searchQuery
      = new BaratineQuery(messageId, QueryType.SEARCH);

    searchQuery.setExcpectedSearchResult(expectedId);

    _queries.put(messageId, searchQuery);

    String template
      = "[[\"query\",{},\"/search\",%1$s,\"/session\",\"search\", \"%2$s\", \"%3$s\", \"%4$d\"]]";

    String data = String.format(template, messageId, "foo", luceneQuery, 255);

    StringEntity e = new StringEntity(data, _defaultContentType);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    if (_jampChannel == null)
      _jampChannel = getCookie(response);

    BaratineQuery[] queries = parseResponse(response);

    boolean isMatch = false;

    for (BaratineQuery query : queries) {
      if (query == searchQuery) {
        isMatch = true;
        _queries.remove(messageId);
      }
      else {
        processQuery(query);
      }
    }

    if (!isMatch)
      processQuery(searchQuery);

    if (!_isPreload)
      searchQuery.validate();
  }

  private void pollImpl()
  {
    while (true) {
      try {
        String url = _baseUrl + "/s/lucene/session";

        HttpPost post = new HttpPost(url);

        if (_jampChannel != null)
          post.setHeader("Cookie", _jampChannel);

        StringEntity e = new StringEntity("[]", _pollContentType);

        post.setEntity(e);

        CloseableHttpResponse response = _client.execute(post);

        BaratineQuery[] queries = parseResponse(response);

        //System.out.println("BaratineDriver.pollImpl: " + queries.length);

        for (BaratineQuery query : queries) {
          //System.out.println("  " + query);
          processQuery(query);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private BaratineQuery[] parseResponse(CloseableHttpResponse response)
    throws IOException
  {
    try (InputStream in = response.getEntity().getContent()) {
      ObjectMapper mapper = new ObjectMapper();

      JsonParser parser = _jsonFactory.createJsonParser(in);

      JsonNode tree = mapper.readTree(parser);

      if (tree == null || tree.size() == 0) {
        return new BaratineQuery[0];
      }
//BaratineDriver.parseResponse: [["reply",{},"/search",193,[{"_externalId":"3925383","_id":116,"_score":0.44276124238967896}]],["reply",{},"/search",192,[{"_externalId":"3925383","_id":116,"_score":0.44276124238967896}]]]

      BaratineQuery[] queries = new BaratineQuery[tree.size()];
      for (int i = 0; i < tree.size(); i++) {
        JsonNode node = tree.get(i);

        if (!"reply".equals(node.get(0).asText())) {
          System.out.println(" error: " + tree);
        }

        String messageId = node.get(3).asText();

        String type = node.get(2).asText();

        BaratineQuery query = _queries.get(messageId);

        synchronized (query) {
          if ("/update".equals(type)) {
            query.setUpdateResult(node.get(4).asBoolean());
          }
          else if ("/search".equals(type)) {
            String extId = node.get(4).get(0).get("_externalId").asText();

            query.setSearchResult(extId);
          }
          else {
            throw new IllegalStateException(tree.toString());
          }
        }

        queries[i] = query;
      }

      return queries;
    }
  }

  private void processQuery(BaratineQuery query)
  {
    if (query == null)
      return;

    synchronized (query) {
      if (query.getUpdateResult() == null && query.getSearchResult() == null) {
        try {
          if (!_isPreload)
            query.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      else {
        _queries.remove(query.getMessageId());

        query.notify();
      }
    }
  }

  @Override
  public List<String> getMatches()
  {
    return null;
  }

  class BaratineQuery
  {
    final private String _messageId;
    private Boolean _updateResult;
    private String _searchResult;

    private QueryType _type;

    private String _expectedSearchResult;

    public BaratineQuery(String messageId, QueryType type)
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
      return "BaratineQuery["
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

  private static void update(Baratine driver)
  {
    try {
      driver.update(new FileInputStream(
        "/Users/alex/data/wiki/40002/4000225.txt"), "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void search(Baratine driver)
  {
    try {
      driver.search("2e6ddb8e-d235-4286-94d0-fc8029f0114a", "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void poll(Baratine driver)
  {
    driver.poll(driver);
  }

  public static void main(String[] args)
    throws IOException, InterruptedException
  {
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    Baratine driver = new Baratine(new NullDataProvider(1),
                                   1,
                                   1,
                                   "localhost",
                                   8085);

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