package test;

import test.DataProvider;
import test.SearchClient;

import java.io.IOException;
import java.util.Iterator;

public abstract class BaseSearchClient implements SearchClient
{
  DataProvider _data;

  private int _n;

  private float _ratioTarget;
  private int _updateCount;
  private int _searchCount;
  //
  private long _updateTime;
  private long _searchTime;
  private Iterator<DataProvider.Data> _iterator;

  public BaseSearchClient(DataProvider data, int n, float targetRatio)
  {
    _data = data;
    _n = n;
    _ratioTarget = targetRatio;
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

  @Override
  final public void preload(int preload) throws IOException
  {
    for (int i = 0; i < preload; i++) {
      DataProvider.Data data = _iterator.next();
      update(data.getInputStream(), data.getKey());
    }
  }

  @Override
  final public void run()
  {
    for (int i = 0; i < _n; i++) {
      float ratio = _updateCount > 0 ? _searchCount / _updateCount : 0;

      if (ratio < _ratioTarget) {
        DataProvider.Query query = _data.getQuery(i);
        search(query);
        _searchCount++;
      }
      else {
        update(_iterator.next());
        _updateCount++;
      }
    }
  }

  private void search(DataProvider.Query query)
  {
    try {
      long start = System.currentTimeMillis();
      search(query.getQuery(), query.getKey());
      _searchTime += System.currentTimeMillis() - start;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void update(DataProvider.Data data)
  {
    try {
      long start = System.currentTimeMillis();
      update(data.getInputStream(), data.getKey());
      _updateTime += System.currentTimeMillis() - start;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
