package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataProvider implements Iterator<DataProvider.Data>
{
  List<Data> _data = new ArrayList<>();

  private int _current = 0;

  public DataProvider(File file)
  {
    add(file);
  }

  public void add(File file)
  {
    if (file.isDirectory()) {
      File[] files = file.listFiles();

      for (File f : files) {
        add(f);
      }
    }
    else {
      _data.add(new Data(file));
    }
  }

  @Override
  public boolean hasNext()
  {
    return _current < _data.size();
  }

  @Override
  public Data next()
  {
    return _data.get(_current++);
  }

  public void reset()
  {
    _current = 0;
  }

  static class Data
  {
    private File _file;
    private String _query;

    public Data(File file)
    {
      _file = file;
    }

    public String getQuery()
    {
      return "test";
    }

    public InputStream getInputStream() throws FileNotFoundException
    {
      return new FileInputStream(_file);
    }
  }
}
