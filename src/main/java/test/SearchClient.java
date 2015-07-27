package test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface SearchClient extends Runnable
{
  Result update(InputStream file, String id) throws IOException;

  Result search(String query, String expectedId) throws IOException;

  void preload(int preload) throws IOException;

  List<String> getMatches();

  int getUpdateCount();

  int getSearchCount();

  int getNotFoundCount();

  int getUpdateFailedCount();

  long getUpdateTime();

  long getSearchTime();

  List getErrors();

  enum Result
  {
    OK,
    FAILED,
    NOT_FOUND
  }
}
