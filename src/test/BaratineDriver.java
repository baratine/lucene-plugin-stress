package test;

import com.sun.nio.sctp.IllegalReceiveException;
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
  JsonFactory _jsonFactory = new JsonFactory();
  private int _counter = 0;

  public void update(InputStream in, String id) throws IOException
  {
    String url = "http://localhost:8085/s/lucene";

    HttpPost post = new HttpPost(url);

    ContentType ct = ContentType.create("x-application/jamp-rpc",
                                        StandardCharsets.UTF_8);

    String template
      = "[[\"query\",{},\"/update\",%1$d,\"/lucene\",\"indexText\", \"%2$s\", \"%3$s\", \"%4$s\"]]";

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

    data = String.format(template, _counter++, "foo", id, data);

    StringEntity e = new StringEntity(data, ct);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    try (InputStream respIn = response.getEntity().getContent()) {
      ObjectMapper mapper = new ObjectMapper();

      JsonParser parser = _jsonFactory.createJsonParser(respIn);

      JsonNode tree = mapper.readTree(parser);

      try {
        if (!tree.get(0).get(4).getBooleanValue()) {
          throw new IllegalReceiveException(tree.toString());
        }
      } catch (NullPointerException npe) {
        System.out.println("BaratineDriver.update " + tree);
        throw npe;
      }
    }
  }

  @Override
  public void search(String query, String expectedId) throws IOException
  {
    String url = "http://localhost:8085/s/lucene";

    HttpPost post = new HttpPost(url);

    ContentType ct = ContentType.create("x-application/jamp-rpc",
                                        StandardCharsets.UTF_8);

    String template
      = "[[\"query\",{},\"/search\",%1$d,\"/lucene\",\"search\", \"%2$s\", \"%3$s\", \"%4$d\"]]";

    String data = String.format(template, _counter++, "foo", query, 255);

    StringEntity e = new StringEntity(data, ct);

    post.setEntity(e);

    CloseableHttpResponse response = _client.execute(post);

    try (InputStream respIn = response.getEntity().getContent()) {

      JsonNode tree = parse(respIn);

      if (!expectedId.equals(tree.get(0)
                                 .get(4)
                                 .get(0)
                                 .get("_externalId")
                                 .asText())) {
        throw new IllegalReceiveException(expectedId + " : " + tree.toString());
      }
    }
  }

  JsonNode parse(InputStream in) throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();

    JsonParser parser = _jsonFactory.createJsonParser(in);

    JsonNode tree = mapper.readTree(parser);

    return tree;
  }

  public static void main(String[] args) throws IOException
  {
    new BaratineDriver().update(new FileInputStream(
      "/Users/alex/data/wiki/40002/4000225.txt"), "4000225");

    new BaratineDriver().search("2e6ddb8e-d235-4286-94d0-fc8029f0114a",
                                "4000225");

  }
}
