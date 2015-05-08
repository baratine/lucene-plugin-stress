package test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

    if (0f > searchSubmitRatio)
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

    while (true) {
      if (_futureResults.size() == _clients) {
        Thread.yield();
        continue;
      }

      if (ratio < _searchSubmitRatio) {
        if (submitSearch())
          searchCounter++;
      }
      else {
        if (submitUpdate())
          submitCounter++;
      }

      ratio = (float) searchCounter / submitCounter;

      if (_limit-- == 0) {
        break;
      }
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
              _results.add(future.get(2000, TimeUnit.MILLISECONDS));
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

  public boolean submitSearch()
  {
    Callable<RequestResult> callable = () -> search();

    submit(callable);

    return true;
  }

  private RequestResult search()
  {
    RequestResult result;

    try {
      _searchEngineDriver.search(getSearchTerm());

      result = RequestResult.createSearchResult();
    } catch (IOException e) {
      result = RequestResult.createErrorResult(e);
    }

    return result;
  }

  private String getSearchTerm()
  {
    return _queryTerms.get(_queryTerms.size() - 1);
  }

  public boolean submitUpdate()
  {
    if (!_provider.hasNext())
      return false;

    DataProvider.Data data = _provider.next();

    Callable<RequestResult> callable = () -> update(data);

    submit(callable);

    return true;
  }

  private RequestResult update(DataProvider.Data data)
  {
    RequestResult result;

    try (InputStream in = data.getInputStream()) {
      _searchEngineDriver.update(in, data.getId());
      _queryTerms.add(data.getQuery());

      result = RequestResult.createUpdateResult();
    } catch (IOException e) {
      System.exit(2);
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
    for (int i = 0; i < n && _provider.hasNext(); i++) {
      DataProvider.Data d = _provider.next();
      _searchEngineDriver.update(d.getInputStream(), d.getId());
      _queryTerms.add(d.getQuery());
    }
  }

  public void printStats()
  {
    class Stat
    {
      String _name;
      double _sum = 0d;
      int _n = 0;
      float _max = 0f;
      float _min = Float.MAX_VALUE;

      public Stat(String name)
      {
        _name = name;
      }

      void add(RequestResult result)
      {
        float time = result.getFinishTime() - result.getStartTime();

        _sum += time;
        _n++;
        if (time > _max)
          _max = time;
        if (time < _min)
          _min = time;
      }

      void print(PrintStream out)
      {
        out.print(_name);
        double avg = _sum / _n;
        out.print(String.format("\n  avg: %1$f\tmin: %2$f\t max: %3$f\t n: %4$d",
                                avg,
                                _min,
                                _max,
                                _n));
      }
    }
    Stat search = new Stat("search");
    Stat update = new Stat("update");

    for (RequestResult result : _results) {
      switch (result.getType()) {
      case SEARCH: {
        search.add(result);
        break;
      }
      case UPDATE: {
        update.add(result);
        break;
      }
      default: {
        System.out.println(result);
        break;
      }
      }
    }

    search.print(System.out);
    System.out.println();
    update.print(System.out);
  }

  public static void main(String[] args) throws IOException
  {
    File file = new File(args[0]);

    SearchEngineDriver driver = new SolrDriver();
    driver = new BaratineDriver();
    TestDriver testDriver = new TestDriver(4,
                                       5f,
                                       4000,
                                       new DataProvider(file),
                                       driver);

    testDriver.preload(100);

    testDriver.run();

    testDriver.printStats();
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
