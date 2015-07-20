package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class WikiDataProvider implements DataProvider, Iterator
{
  List<WikiData> _wikiData = new ArrayList<>();
  Properties _queries = new Properties();

  private AtomicInteger _current = new AtomicInteger(0);
  private long _size;
  private Random _random = new Random();

  private List<String> _uploads = new ArrayList<>();

  public WikiDataProvider(File file, long size) throws IOException
  {
    _size = size;

    add(file);

    try (Reader in = new InputStreamReader(
      new FileInputStream(
        new File(file, "query.properties")), StandardCharsets.UTF_8)) {
      _queries.load(in);
    }

    try (FileWriter writer = new FileWriter("/tmp/data-files.txt")) {
      for (int i = 0; i < _wikiData.size(); i++) {
        WikiData wikiData = _wikiData.get(i);
        writer.write(wikiData.getKey() + ":" + wikiData.getFile() + '\n');
      }
    }
  }

  public void add(File file)
  {
    if (_size != -1 && _wikiData.size() >= _size) {
      return;
    }

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
      _wikiData.add(new WikiData(file));
    }
  }

  @Override
  public boolean hasNext()
  {
    return _current.get() < _wikiData.size();
  }

  @Override
  public WikiData next()
  {
    return _wikiData.get(_current.getAndIncrement());
  }

  @Override
  public Query getQuery()
  {
    int i;
    synchronized (_uploads) {
      i = _random.nextInt(_uploads.size() - 1);
    }

    i = Math.max(0, i);

    WikiData data;

    synchronized (_uploads) {
      data = _wikiData.get(i);
    }

    String key = data.getKey();

    String query = _queries.getProperty(key);

    return new Query()
    {
      @Override
      public String getKey()
      {
        return key;
      }

      @Override
      public String getQuery()
      {
        return query;
      }
    };
  }

  @Override
  public Iterator<Data> iterator()
  {
    return this;
  }

  @Override
  public void updateComplete(Data data)
  {
    synchronized (_uploads) {
      _uploads.add(data.getKey());
    }
  }

  class WikiData implements DataProvider.Data
  {
    private File _file;

    public WikiData(File file)
    {
      _file = file;
    }

    @Override
    public String getKey()
    {
      String id = _file.getName();
      id = id.replace("\\", "/");

      int start = id.lastIndexOf('/');

      if (start == -1)
        start = 0;

      id = id.substring(start, id.lastIndexOf('.'));

      return id;
    }

    @Override
    public InputStream getInputStream() throws FileNotFoundException
    {
      return new FileInputStream(_file);
    }

    @Override
    public File getFile()
    {
      return _file;
    }
  }
}
