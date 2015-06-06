package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;

public interface DataProvider
{
  boolean hasNext();

  Data next();

  String getQuery(String key);

  void reset();

  interface Data
  {
    String getKey();

    InputStream getInputStream() throws FileNotFoundException;

    File getFile();
  }
}
