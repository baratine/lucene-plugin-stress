package test;

import java.io.IOException;
import java.util.Iterator;
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

  private final LinkedList<Future<Result>> _futureResults = new LinkedList<>();

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

    _executors = Executors.newFixedThreadPool(n + 1);
  }

  public void run()
  {
    long searchCounter = 0;
    long submitCounter = 0;
    float ratio = 1;

    _executors.submit(() -> testResults());

    while (!_executors.isShutdown()) {
      if (_futureResults.size() == _clients) {
        Thread.yield();
        continue;
      }

      if (ratio < _searchSubmitRatio) {
        submitSearch();
        searchCounter++;
      }
      else {
        submitUpdate();
        submitCounter++;
      }

      ratio = (float) searchCounter / submitCounter;

      if (_limit-- == 0)
        break;
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
    while (!_executors.isShutdown()) {
      synchronized (_futureResults) {
        for (Iterator<Future<Result>> it = _futureResults.iterator();
             it.hasNext(); ) {
          Future<Result> future = it.next();
          if (future.isDone()) {
            it.remove();
          }
        }
      }

      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
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

    submit(callable);
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

    submit(callable);
  }

  private void submit(Callable<Result> callable)
  {
    Future<Result> future = _executors.submit(callable);

    addFutureResult(future);
  }

  private void addFutureResult(Future<Result> result)
  {
    synchronized (_futureResults) {
      _futureResults.add(result);
    }
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
