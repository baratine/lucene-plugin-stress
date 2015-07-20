package test;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

public class Solr extends BaseSearchClient
{
  private JsonFactory _jsonFactory = new JsonFactory();
  private ObjectMapper _jsonMapper = new ObjectMapper();

  private CloseableHttpClient _client = HttpClients.createDefault();

  final String _updateUrl;
  final String _searchUrl;

  RequestConfig _config = null;

  public Solr(DataProvider data,
              int n,
              int searchRate,
              String host,
              int port)
  {
    super(data, n, searchRate);
    _updateUrl = "http://"
                 + host
                 + ":"
                 + port
                 + "/solr/foo/update/json/docs?commit=true";

    _searchUrl
      = "http://"
        + host
        + ":"
        + port
        + "/solr/foo/select?q=%1$s&wt=json";

    _config = RequestConfig.copy(RequestConfig.DEFAULT)
                           .setConnectTimeout(100)
                           .setSocketTimeout(1000)
                           .setConnectionRequestTimeout(1000)
                           .build();
  }

  @Override
  public void update(InputStream in, String id)
    throws IOException
  {
    HttpPost post = new HttpPost(_updateUrl);

    post.setConfig(_config);

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

    String template = "{\"id\": \"%1$s\", \"data_tt\":\"%2$s\"}";

    String data = String.format(template, id, writer.getBuffer().toString());

    StringEntity e = new StringEntity(data, ContentType.APPLICATION_JSON);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    try (InputStream respIn = response.getEntity().getContent()) {
      JsonParser parser = _jsonFactory.createJsonParser(respIn);

      Map map = _jsonMapper.readValue(parser, Map.class);

      if (!"0".equals(
        ((Map) map.get("responseHeader")).get("status").toString()))
        throw new AssertionError("expected 0 but received " + map);
    }
  }

  @Override
  public void search(String query, String expectedId) throws IOException
  {
    String url = String.format(_searchUrl, query);

    HttpGet get = new HttpGet(url);

    get.setConfig(_config);

    CloseableHttpResponse response = _client.execute(get);

    try (InputStream in = response.getEntity().getContent()) {
      JsonParser parser = _jsonFactory.createJsonParser(in);

      Map map = _jsonMapper.readValue(parser, Map.class);

      if (!expectedId.equals(((Map) ((List) ((Map) map.get("response")).get(
        "docs")).get(0)).get("id")))
        throw new AssertionError("expected " + expectedId + " received " + map);
    }
  }

  @Override
  public List<String> getMatches()
  {
    return null;
  }

  private static void update(Solr driver)
  {
    try {
      driver.update(new FileInputStream(
        "/Users/alex/projects/data/wiki/40002/4000225.txt"), "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void search(Solr driver)
  {
    try {
      driver.search("11a66e0d0dad9040920abe40c55e226a8700", "4000225");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException
  {
    Solr solr = new Solr(new NullDataProvider(1), 1, 1, "localhost", 8984);
    update(solr);
    search(solr);
  }
}
/*
POST /solr/foo/update?wt=json HTTP/1.1
  Host: localhost:8984
  Origin: http://localhost:8984
  Content-Type: application/json
  Accept-Encoding: gzip, deflate
  Content-Length: 104
  Connection: keep-alive
  Accept: ** /*
  User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/600.5.17 (KHTML, like Gecko) Version/8.0.5 Safari/600.5.17
  Referer: http://localhost:8984/solr/
  Accept-Language: en-us
  X-Requested-With: XMLHttpRequest

{"add":{ "doc":{"id":"change.me","title":"change.me"},"boost":1.0,"overwrite":true,"commitWithin":1000}}c^C
*/
