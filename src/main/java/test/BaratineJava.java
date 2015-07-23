package test;

import com.caucho.lucene.LuceneEntry;
import com.caucho.lucene.LuceneFacadeSync;
import io.baratine.core.ServiceClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.List;

public class BaratineJava extends BaseSearchClient
{
  LuceneFacadeSync _lucene;

  public BaratineJava(DataProvider provider,
                      int n,
                      int searchRate,
                      ServiceClient client)
  {
    super(provider, n, searchRate);

    _lucene = client.lookup("remote:///service").as(LuceneFacadeSync.class);
  }

  @Override
  public void update(InputStream in, String id) throws IOException
  {
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

    boolean result = _lucene.indexText("foo", id, data);

    if (!result)
      throw new IllegalStateException(String.format(
        "unexpected update result %1$s",
        this.toString()));
  }

  @Override
  public void search(String query, String expectedId) throws IOException
  {
    List<LuceneEntry> entries = _lucene.search("foo", query, 255);

    if (entries.size() != 1)
      throw new IllegalStateException(String.format(
        "expected 1 entry received %1$d ",
        entries.size()));

    if (!entries.get(0).getExternalId().equals(expectedId))
      throw new IllegalStateException(String.format(
        "unexpected search result %1$s",
        entries.get(0).getExternalId()));
  }

  @Override
  public List<String> getMatches()
  {
    return null;
  }
}
