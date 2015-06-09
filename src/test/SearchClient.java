package test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface SearchClient extends Runnable
{
  void update(InputStream file, String id) throws IOException;

  void search(String query, String expectedId) throws IOException;

  void preload(int preload) throws IOException;

  List<String> getMatches();

  int getUpdateCount();

  int getSearchCount();

  long getUpdateTime();

  long getSearchTime();

  List getErrors();
}
