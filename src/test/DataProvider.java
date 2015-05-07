package test;

import java.io.File;

public class DataProvider
{
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
