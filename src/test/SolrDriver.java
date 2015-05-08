package test;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class SolrDriver implements SearchEngineDriver
{
  @Override
  public void update(InputStream in) throws IOException
  {
    //http://localhost:8984/solr/foo/update/json/docs";
    String url = "http://localhost:8983/solr/foo/update/json/docs?commit=true";

    JsonFactory jsonFactory = new JsonFactory();

    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost post = new HttpPost(url);

    RequestConfig config
      = RequestConfig.copy(RequestConfig.DEFAULT)
                     .setConnectTimeout(100)
                     .setSocketTimeout(1000)
                     .setConnectionRequestTimeout(1000)
                     .build();

    post.setConfig(config);

    InputStreamEntity e = new InputStreamEntity(in,
                                                ContentType.APPLICATION_JSON);

    post.setEntity(e);

    CloseableHttpResponse response = client.execute(post);

    try (InputStream respIn = response.getEntity().getContent()) {
      ObjectMapper mapper = new ObjectMapper();

      JsonParser parser = jsonFactory.createJsonParser(respIn);

      Map map = mapper.readValue(parser, Map.class);
    }
  }

  @Override
  public void search(String query) throws IOException
  {
    String url
      = "http://localhost:8983/solr/foo/select?q=%1$s&wt=json&indent=true";
    url = String.format(url, query);

    CloseableHttpClient client = HttpClients.createDefault();
    HttpGet get = new HttpGet(url);

    RequestConfig config
      = RequestConfig.copy(RequestConfig.DEFAULT)
                     .setConnectTimeout(100)
                     .setSocketTimeout(1000)
                     .setConnectionRequestTimeout(1000)
                     .build();

    get.setConfig(config);

    CloseableHttpResponse response = client.execute(get);

    try (InputStream in = response.getEntity().getContent()) {
      JsonFactory jsonFactory = new JsonFactory();

      ObjectMapper mapper = new ObjectMapper();

      JsonParser parser = jsonFactory.createJsonParser(in);

      Map map = mapper.readValue(parser, Map.class);
    }
  }

  public static void main(String[] args) throws IOException
  {
    new SolrDriver().update(null);
    new SolrDriver().search(null);
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
