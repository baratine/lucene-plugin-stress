package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class BaseSearchClient implements SearchClient
{
  private DataProvider _dataProvider;

  private int _n;

  private int _searchRate;
  private int _updateCount;
  private int _searchCount;
  //
  private long _updateTime;
  private long _searchTime;
  private Iterator<DataProvider.Data> _iterator;
  private List _errors = new ArrayList();

  static int bufferSize = 8 * 1024;

  public BaseSearchClient(DataProvider dataProvider, int n, int searchRate)
  {
    _dataProvider = dataProvider;
    _n = n;
    _searchRate = searchRate;
    _iterator = _dataProvider.iterator();
  }

  @Override
  final public int getUpdateCount()
  {
    return _updateCount;
  }

  @Override
  final public int getSearchCount()
  {
    return _searchCount;
  }

  @Override
  final public long getUpdateTime()
  {
    return _updateTime;
  }

  @Override
  final public long getSearchTime()
  {
    return _searchTime;
  }

  public void addError(Object error)
  {
    _errors.add(error);
  }

  public List getErrors()
  {
    return _errors;
  }

  @Override
  final public void preload(int preload) throws IOException
  {
    for (int i = 0; i < preload; i++) {
      DataProvider.Data data = _iterator.next();

      try {
        update(data.getInputStream(), data.getKey());
        _dataProvider.updateComplete(data);
      } catch (Throwable e) {
        e.printStackTrace();
        System.out.println("preload error: " + data.getFile());
      }
    }
  }

  private float ratio()
  {
    return (float) _updateCount / _searchCount;
  }

  @Override
  final public void run()
  {
    int x = _n / 5;

    while ((_searchCount + _updateCount) < _n) {

      if ((_searchCount + _updateCount) % x == 0)
        System.out.println("run " + (_searchCount + _updateCount));

      if (_searchRate == Integer.MAX_VALUE) {
        search();
      }
      else if ((_searchCount % _searchRate) == 0) {
        search();
        update();
      }
      else {
        search();
      }
    }
  }

  private void search()
  {
    long start = System.currentTimeMillis();
    try {
      DataProvider.Query query = _dataProvider.getQuery();
      start = System.currentTimeMillis();
      search(query.getQuery(), query.getKey());
    } catch (Throwable e) {
      addError(e);
    } finally {
      _searchTime += System.currentTimeMillis() - start;
      _searchCount++;
    }
  }

  private void update()
  {
    long start = System.currentTimeMillis();
    try {
      DataProvider.Data data = _iterator.next();
      start = System.currentTimeMillis();
      update(data.getInputStream(), data.getKey());

      _dataProvider.updateComplete(data);
    } catch (Throwable e) {
      addError(e);
    } finally {
      _updateTime += System.currentTimeMillis() - start;
      _updateCount++;
    }
  }
}
