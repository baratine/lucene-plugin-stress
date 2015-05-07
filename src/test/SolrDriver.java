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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class SolrDriver implements SearchEngineDriver
{
  @Override
  public void update(File file) throws IOException
  {

    //http://localhost:8984/solr/foo/update/json/docs";
    String url = "http://localhost:8984/solr/foo/update/json/docs?commit=true";

    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost post = new HttpPost(url);

    RequestConfig config
      = RequestConfig.copy(RequestConfig.DEFAULT)
                     .setConnectTimeout(100)
                     .setSocketTimeout(1000)
                     .setConnectionRequestTimeout(1000)
                     .build();

    post.setConfig(config);

    StringEntity e
      = new StringEntity("{id:'id', data_t:'mary had a little lamb'}",
                         ContentType.APPLICATION_JSON);
    post.setEntity(e);

    CloseableHttpResponse response = client.execute(post);

    try (InputStream in = response.getEntity().getContent()) {
      byte[] buffer = new byte[0xFFFF];
      int l = in.read(buffer);

      System.out.println(new String(buffer, 0, l));
    }
  }

  @Override
  public void search(String query) throws IOException
  {
    String url
      = "http://localhost:8983/solr/foo/select?q=*%3A*&wt=json&indent=true";

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
    byte[] buffer = new byte[0xFFFF];
    int l;

    try (InputStream in = response.getEntity().getContent()) {
      l = in.read(buffer);
    }

    JsonFactory jsonFactory = new JsonFactory();

    ObjectMapper mapper = new ObjectMapper();

    JsonParser parser = jsonFactory.createJsonParser(buffer, 0, l);

    Map map = mapper.readValue(parser, Map.class);
  }

  public static void main(String[] args) throws IOException
  {
    //new SolrDriver().update(null);
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
