package test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestDriver
{
  private final ExecutorService _executors;
  private final int _clients;

  private final DataProvider _provider;

  private final SearchEngineDriver _searchEngineDriver;

  private final LinkedList<String> _queryTerms = new LinkedList<>();

  private final LinkedList<Future<Result>> _results = new LinkedList<>();

  private final float _searchSubmitRatio;

  private long _limit;

  public TestDriver(int n,
                    float searchSubmitRatio,
                    long limit,
                    DataProvider provider,
                    SearchEngineDriver driver)
  {
    Objects.requireNonNull(provider);
    Objects.requireNonNull(driver);

    if (!(0f < searchSubmitRatio && searchSubmitRatio < 1f))
      throw new IllegalArgumentException();

    _searchSubmitRatio = searchSubmitRatio;
    _limit = limit;
    _clients = n;
    _provider = provider;
    _searchEngineDriver = driver;

    _executors = Executors.newFixedThreadPool(n);
  }

  public void run()
  {
    long searchCounter = 0;
    long submitCounter = 0;
    float ratio = 1;

    while (!_executors.isShutdown() && _limit-- > 0) {
      if (_results.size() == _clients)
        Thread.yield();

      if (ratio < _searchSubmitRatio) {
        submitSearch();
        searchCounter++;
      }
      else {
        submitUpdate();
        submitCounter++;
      }

      ratio = (float) searchCounter / submitCounter;

    }

    _executors.shutdown();

    System.out.println(String.format(
      "submitted %1$d searched %2$d search-update-ratio %3$f search-update-ratio-target %4$f",
      submitCounter,
      searchCounter,
      ((float) searchCounter / submitCounter),
      _searchSubmitRatio));
  }

  public void testResults()
  {
    synchronized (_results) {
      for (int i = _results.size() - 1; i >= 0; i--) {
        Future<Result> future = _results.get(i);

        if (future.isDone()) {
          _results.remove(i);
        }
      }
    }
  }

  public void submitSearch()
  {
    Callable<Result> callable = new Callable<Result>()
    {
      @Override
      public Result call() throws Exception
      {
        return new Result();
      }
    };

    Future<Result> result = _executors.submit(callable);

    _results.add(result);
  }

  public void submitUpdate()
  {
    Callable<Result> callable = new Callable<Result>()
    {
      @Override
      public Result call() throws Exception
      {
        return new Result();
      }
    };

    Future<Result> result = _executors.submit(callable);

    _results.add(result);
  }

  public void preload(int n) throws IOException
  {
    DataProvider.Data[] data = _provider.getData(n);

    for (DataProvider.Data d : data) {
      _searchEngineDriver.submit(d.getFile());
      _queryTerms.addLast(d.getQuery());
    }
  }

  public static void main(String[] args) throws IOException
  {
    TestDriver driver = new TestDriver(10,
                                       .59999999f,
                                       10000,
                                       new DataProvider(),
                                       new SolrDriver());

    driver.preload(100);

    driver.run();
  }
}

/**
 * 5 clients
 * - 1 update
 * - 1 search
 * in: Reade Write ratio
 * Search Data sorted
 * Search Terms
 * URL
 * Handler (Solr or Baratine)
 * Number of clients
 * <p>
 * <p>
 * out: Rate requests per second (or cycles per second)
 */
