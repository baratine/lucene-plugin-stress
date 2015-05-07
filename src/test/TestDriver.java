package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

  private final List<String> _queryTerms = new ArrayList<>();

  private final List<Future<RequestResult>> _futureResults
    = new ArrayList<>();

  private final List<RequestResult> _results = new ArrayList<>();

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
        for (Iterator<Future<RequestResult>> it = _futureResults.iterator();
             it.hasNext(); ) {
          Future<RequestResult> future = it.next();
          if (future.isDone()) {
            it.remove();
            try {
              _results.add(future.get());
            } catch (Throwable t) {
              _results.add(RequestResult.createErrorResult(t));
            }
          }
          else if (future.isCancelled()) {
            throw new IllegalStateException();
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
    Callable<RequestResult> callable = () -> search();

    submit(callable);
  }

  private RequestResult search()
  {
    RequestResult result;

    try {
      _searchEngineDriver.search(null);

      result = RequestResult.createSearchResult();
    } catch (IOException e) {
      result = RequestResult.createErrorResult(e);
    }

    return result;
  }

  public void submitUpdate()
  {
    Callable<RequestResult> callable = () -> update();

    submit(callable);
  }

  private RequestResult update()
  {
    RequestResult result;

    try {
      _searchEngineDriver.update(null);

      result = RequestResult.createUpdateResult();
    } catch (IOException e) {
      result = RequestResult.createErrorResult(e);
    }

    return result;
  }

  private void submit(Callable<RequestResult> callable)
  {
    Future<RequestResult> future
      = _executors.submit(new TimedCallable(callable));

    addFutureResult(future);
  }

  private void addFutureResult(Future<RequestResult> result)
  {
    synchronized (_futureResults) {
      _futureResults.add(result);
    }
  }

  public void preload(int n) throws IOException
  {
    DataProvider.Data[] data = _provider.getData(n);

    for (DataProvider.Data d : data) {
      _searchEngineDriver.update(d.getFile());
      _queryTerms.add(d.getQuery());
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

class TimedCallable implements Callable<RequestResult>
{

  private Callable<RequestResult> _delegate;

  public TimedCallable(Callable<RequestResult> delegate)
  {
    _delegate = delegate;
  }

  @Override
  public RequestResult call() throws Exception
  {
    final long start = System.currentTimeMillis();

    RequestResult result = _delegate.call();
    result.setStartTime(start);
    result.setFinishTime(System.currentTimeMillis());

    return result;
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
