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

  private final List<String> _queryKeys = new ArrayList<>();

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
    long pollCounter = 0;
    float ratio = 1;

    _executors.submit(() -> testResults());

    while (true) {
      if (_futureResults.size() > _clients) {
        submitPoll();
        pollCounter++;

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

      if (_limit % 100 == 0) {
        System.out.println("limit:  "
                           + _limit
                           + ", polls: " + pollCounter
                           +", futureResults.size: "
                           + _futureResults.size());
      }

      if (_limit-- <= 0) {
        break;
      }
    }

    if (_futureResults.size() > 0) {
      for (int i = 0; i < 3; i++) {

        for (int j = 0; j < _futureResults.size(); j++) {
          submitPoll();
          try {
            Thread.sleep(100 * i);
          } catch (InterruptedException e) {
          }
        }

        try {
          Thread.sleep(1000 * i);
        } catch (InterruptedException e) {
        }
      }
    }

    _executors.shutdown();

    System.out.println(String.format(
      "clients %1$d, submitted %2$d, searched %3$d, search-update-ratio %4$f, search-update-ratio-target %5$f",
      _clients,
      submitCounter,
      searchCounter,
      ((float) searchCounter / submitCounter),
      _searchSubmitRatio));

    _searchEngineDriver.printState();
  }

  public void testResults()
  {
    while (!_executors.isShutdown()) {
      synchronized (_futureResults) {
        for (Iterator<Future<RequestResult>> it = _futureResults.iterator();
             it.hasNext(); ) {
          Future<RequestResult> future = it.next();

          if (future.isCancelled()) {
            throw new IllegalStateException();
          }
          else if (future.isDone()) {
            it.remove();
            try {
              _results.add(future.get(1, TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
              _results.add(RequestResult.createErrorResult(t));
            }
          }
        }
      }

      Thread.yield();
    }
  }

  public boolean submitPoll()
  {
    try {
      _searchEngineDriver.poll();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return true;
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
      String key = getSearchKey();
      String query = _provider.getQuery(key);

      _searchEngineDriver.search(query, key);

      result = RequestResult.createSearchResult();
    } catch (Throwable t) {
      result = RequestResult.createErrorResult(t);
    }

    return result;
  }

  private String getSearchKey()
  {
    return _queryKeys.get(_queryKeys.size() - 1);
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
      _searchEngineDriver.update(in, data.getKey());
      _queryKeys.add(data.getKey());

      result = RequestResult.createUpdateResult();
    } catch (Throwable t) {
      result = RequestResult.createErrorResult(t);
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
    _searchEngineDriver.setPreload(true);
    for (int i = 0; i < n && _provider.hasNext(); i++) {
      DataProvider.Data d = _provider.next();

      try {
        _searchEngineDriver.update(d.getInputStream(), d.getKey());
      } catch (Exception e) {
        System.out.println("TestDriver.preload " + e);
      }
      _queryKeys.add(d.getKey());
    }

    for (int i = 0; i < n && _provider.hasNext(); i++)
      _searchEngineDriver.poll();

    _searchEngineDriver.setPreload(false);
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

        result.printStackTrace();
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

    long start = System.currentTimeMillis();
    testDriver.preload(100);

    testDriver.run();

    testDriver.printStats();

    System.out.println("\ntest run time: " + (System.currentTimeMillis()
                                              - start));
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
