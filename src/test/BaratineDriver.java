package test;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class BaratineDriver implements SearchEngineDriver
{

  public void update(InputStream in) throws IOException
  {
    String url = "http://localhost:8085/s/lucene";
    //String url = "http://localhost:8085/s/lucene/lucene/fo?m=indexText&p0=f&p1=foo&p2=foo";

    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost post = new HttpPost(url);

    ContentType ct = ContentType.create("x-application/jamp-poll",
                                        StandardCharsets.UTF_8);
    StringEntity e
      = new StringEntity(
      "[[\"query\",{},\"/from\",1,\"/lucene\",\"indexText\", \"foo\", \"id\", \"mary\"]]",
      ct);
    post.setEntity(e);

    CloseableHttpResponse response = client.execute(post);

    try (InputStream respIn = response.getEntity().getContent()) {
      byte[] buffer = new byte[0xFFFF];
      int l = respIn.read(buffer);

      System.out.println(new String(buffer, 0, l));
    }
  }

  public void submitx() throws IOException
  {
    String url = "http://localhost:8085/s/pod";
    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost post = new HttpPost(url);

    ContentType ct = ContentType.create("x-application/jamp-poll",
                                        StandardCharsets.UTF_8);
    StringEntity e
      = new StringEntity("[[\"query\",{},\"/from\",1,\"/my-service\",\"test\"]]",
                         ct);
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

  }

  public static void main(String[] args) throws IOException
  {

    new BaratineDriver().update(null);
  }
}
