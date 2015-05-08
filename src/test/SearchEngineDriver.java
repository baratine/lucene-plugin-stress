package test;

import java.io.IOException;
import java.io.InputStream;

public interface SearchEngineDriver
{
  void update(InputStream file) throws IOException;

  void search(String query) throws IOException;
}
