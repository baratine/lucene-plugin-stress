package test;

import java.io.File;
import java.io.IOException;

public interface SearchEngineDriver
{
  void submit(File file) throws IOException;

  void search(String query) throws IOException;
}
