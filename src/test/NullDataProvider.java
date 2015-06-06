package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

public class NullDataProvider implements DataProvider
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
  public String getQuery(String key)
  {
    return null;
  }

  @Override
  public void reset()
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
