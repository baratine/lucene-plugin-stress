package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class DataProvider implements Iterator<DataProvider.Data>
{
  List<Data> _data = new ArrayList<>();
  Properties _queries = new Properties();

  private int _current = 0;

  public DataProvider(File file) throws IOException
  {
    add(file);

    try (Reader in = new InputStreamReader(
      new FileInputStream(
        new File(file, "query.properties")), StandardCharsets.UTF_8)) {
      _queries.load(in);
    }
  }

  public void add(File file)
  {
    if (file.isDirectory()) {
      File[] files = file.listFiles(pathname -> {
        if (pathname.isDirectory())
          return true;
        else if (pathname.getName().endsWith(".txt"))
          return true;
        else
          return false;
      });

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

  class Data
  {
    private File _file;
    private String _query;

    public Data(File file)
    {
      _file = file;
    }

    public String getId()
    {
      String id = _file.getName();
      id = id.replace("\\", "/");

      int start = id.lastIndexOf('/');

      if (start == -1)
        start = 0;

      id = id.substring(start, id.lastIndexOf('.'));

      return id;
    }

    public String getQuery()
    {
      return _queries.getProperty(getId());
    }

    public InputStream getInputStream() throws FileNotFoundException
    {
      return new FileInputStream(_file);
    }

    public File getFile()
    {
      return _file;
    }
  }
}
