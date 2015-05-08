package test;

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

public class BaratineDriver implements SearchEngineDriver
{
  CloseableHttpClient _client = HttpClients.createDefault();

  public void update(InputStream in, String id) throws IOException
  {
    String url = "http://localhost:8085/s/lucene";

    HttpPost post = new HttpPost(url);

    ContentType ct = ContentType.create("x-application/jamp-poll",
                                        StandardCharsets.UTF_8);

    String template
      = "[[\"query\",{},\"/from\",1,\"/lucene\",\"indexText\", \"%1$s\", \"%2$s\", \"%3$s\"]]";

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

    String data = writer.getBuffer().toString();

    data = String.format(template, "foo", id, data);

    StringEntity e = new StringEntity(data, ct);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    try (InputStream respIn = response.getEntity().getContent()) {
      JsonFactory jsonFactory = new JsonFactory();

      ObjectMapper mapper = new ObjectMapper();

      JsonParser parser = jsonFactory.createJsonParser(respIn);

      JsonNode tree = mapper.readTree(parser);

      //System.out.println("BaratineDriver.update " + tree);
    }
  }

  @Override
  public void search(String query) throws IOException
  {
    String url = "http://localhost:8085/s/lucene";

    HttpPost post = new HttpPost(url);

    ContentType ct = ContentType.create("x-application/jamp-poll",
                                        StandardCharsets.UTF_8);

    String template
      = "[[\"query\",{},\"/from\",1,\"/lucene\",\"search\", \"%1$s\", \"%2$s\", \"%3$d\"]]";

    String data = String.format(template, "foo", query, 255);

    StringEntity e = new StringEntity(data, ct);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    try (InputStream respIn = response.getEntity().getContent()) {
      JsonFactory jsonFactory = new JsonFactory();

      ObjectMapper mapper = new ObjectMapper();

      JsonParser parser = jsonFactory.createJsonParser(respIn);

      JsonNode tree = mapper.readTree(parser);

      //System.out.println("BaratineDriver.search" + tree);
    }
  }

  public static void main(String[] args) throws IOException
  {

    new BaratineDriver().update(new FileInputStream(
      "/Users/alex/data/wiki/40002/4000225.txt"), "4000225");

    new BaratineDriver().search("2e6ddb8e-d235-4286-94d0-fc8029f0114a");

  }
}
