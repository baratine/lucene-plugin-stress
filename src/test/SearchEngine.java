package test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface SearchEngine
{
  void update(InputStream file, String id) throws IOException;

  void search(String query, String expectedId) throws IOException;

  void poll() throws IOException;

  void printState();

  void setPreload(boolean preload);

  List<String> getMatches();
}
