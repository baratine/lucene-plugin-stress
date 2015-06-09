package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class BaseSearchClient implements SearchClient
{
  DataProvider _data;

  private int _n;

  private int _searchRate;
  private int _updateCount;
  private int _searchCount;
  //
  private long _updateTime;
  private long _searchTime;
  private Iterator<DataProvider.Data> _iterator;
  private List _errors = new ArrayList();

  public BaseSearchClient(DataProvider data, int n, int searchRate)
  {
    _data = data;
    _n = n;
    _searchRate = searchRate;
    _iterator = _data.iterator();
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
      update(data.getInputStream(), data.getKey());
    }
  }

  private float ratio()
  {
    return (float) _updateCount / _searchCount;
  }

  @Override
  final public void run()
  {
    while ((_searchCount + _updateCount) < _n) {
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
    try {
      DataProvider.Query query = _data.getQuery();
      long start = System.currentTimeMillis();
      search(query.getQuery(), query.getKey());
      _searchTime += System.currentTimeMillis() - start;
      _searchCount++;
    } catch (Throwable e) {
      addError(e);
    }
  }

  private void update()
  {
    try {
      DataProvider.Data data = _iterator.next();
      long start = System.currentTimeMillis();
      update(data.getInputStream(), data.getKey());
      _updateTime += System.currentTimeMillis() - start;
      _updateCount++;
    } catch (Throwable e) {
      addError(e);
    }
  }
}
