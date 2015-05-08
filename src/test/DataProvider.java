package test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DataProvider
{
  List<File> file = new ArrayList<>();

  public DataProvider(File file)
  {

  }

  public Data[] getData(int n)
  {
    return new Data[0];
  }

  static class Data
  {
    public String getQuery()
    {
      return null;
    }

    public File getFile()
    {
      return null;
    }
  }
}
