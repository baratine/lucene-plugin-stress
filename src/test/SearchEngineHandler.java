package test;

import java.io.IOException;

public interface SearchEngineHandler
{
  void submit() throws IOException;

  void search() throws IOException;
}
