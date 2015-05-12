package test;

import java.io.IOException;
import java.io.InputStream;

public interface SearchEngineDriver
{
  void update(InputStream file, String id) throws IOException;

  void search(String query, String expectedId) throws IOException;

  void poll() throws IOException;

  void printState();

  void setPreload(boolean preload);
}
