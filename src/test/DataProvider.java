package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;

public interface DataProvider
{
  Query getQuery();

  Iterator<Data> iterator();

  interface Data
  {
    String getKey();

    InputStream getInputStream() throws FileNotFoundException;

    File getFile();
  }

  interface Query
  {
    String getKey();

    String getQuery();
  }
}
