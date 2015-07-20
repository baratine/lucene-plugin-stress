package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class NullDataProvider implements DataProvider, Iterator
{
  private long _size;
  private Data _data;

  private AtomicLong _counter = new AtomicLong();

  public NullDataProvider(long size)
  {
    _size = size;
    _data = new NullData();
  }

  @Override
  public boolean hasNext()
  {
    return _counter.getAndIncrement() < _size;
  }

  @Override
  public Data next()
  {
    return _data;
  }

  @Override
  public Query getQuery()
  {
    return new Query()
    {
      @Override
      public String getKey()
      {
        return null;
      }

      @Override
      public String getQuery()
      {
        return null;
      }
    };
  }

  @Override
  public Iterator<Data> iterator()
  {
    return null;
  }

  @Override
  public void updateComplete(Data data)
  {

  }

  class NullData implements Data
  {
    InputStream _inputStream = new InputStream()
    {
      @Override
      public int read() throws IOException
      {
        return -1;
      }
    };

    @Override
    public String getKey()
    {
      return "";
    }

    @Override
    public InputStream getInputStream() throws FileNotFoundException
    {
      return null;
    }

    @Override
    public File getFile()
    {
      return null;
    }
  }
}
