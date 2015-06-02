package test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceTest
{
  private final ExecutorService _executors;
  private final int _clients;

  private final DataProvider _provider;

  private final SearchEngine _searchEngine;

  private final List<String> _queryKeys = new ArrayList<>();

  private final ConcurrentLinkedQueue<RequestResult> _results
    = new ConcurrentLinkedQueue<>();

  private final AtomicInteger _currentRequests = new AtomicInteger(0);

  private final float _searchSubmitTarget;

  private long _limit;

  public PerformanceTest(int n,
                         float searchSubmitTarget,
                         long limit,
                         DataProvider provider,
                         SearchEngine driver)
  {
    Objects.requireNonNull(provider);
    Objects.requireNonNull(driver);

    if (0f > searchSubmitTarget)
      throw new IllegalArgumentException();

    _searchSubmitTarget = searchSubmitTarget;
    _limit = limit;
    _clients = n;
    _provider = provider;
    _searchEngine = driver;

    _executors = Executors.newFixedThreadPool(n);
  }

  public void run()
  {
    long searchCounter = 0;
    long submitCounter = 0;
    long pollCounter = 0;
    float ratio = 1;

    submitPoll();

    while (true) {
      if (_currentRequests.get() > _clients) {
        Thread.yield();
        continue;
      }

      if (ratio < _searchSubmitTarget) {
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
                           + ", futureResults.size: "
                           + _currentRequests.get());
      }

      _currentRequests.incrementAndGet();

      if (_limit-- <= 0) {
        break;
      }
    }

    System.out.println(String.format("waiting for %1$d requests to complete",
                                     _currentRequests.get()));

    for (int i = 0; i < 3; i++) {

      for (int j = 0; j < _currentRequests.get(); j++) {
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

    _executors.shutdown();

    System.out.println(String.format("%1$d requests did not complete",
                                     _currentRequests.get()));

    System.out.println(String.format(
      "clients %1$d, submitted %2$d, searched %3$d, search-update-ratio %4$f, search-update-ratio-target %5$f",
      _clients,
      submitCounter,
      searchCounter,
      ((float) searchCounter / submitCounter),
      _searchSubmitTarget));

    _searchEngine.printState();
  }

  public boolean submitPoll()
  {
    try {
      _searchEngine.poll();
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

      _searchEngine.search(query, key);

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
      _searchEngine.update(in, data.getKey());
      _queryKeys.add(data.getKey());

      result = RequestResult.createUpdateResult();
    } catch (Throwable t) {
      result = RequestResult.createErrorResult(t);
    }

    return result;
  }

  private void submit(Callable<RequestResult> callable)
  {
    _executors.submit(new TimedCallable(callable));
  }

  public void preload(long n) throws IOException
  {
    _searchEngine.setPreload(true);

    for (int i = 0; i < n && _provider.hasNext(); i++) {
      DataProvider.Data d = _provider.next();

      try {
        _searchEngine.update(d.getInputStream(), d.getKey());
      } catch (Exception e) {
        System.out.println("TestDriver.preload " + e);
      }
      _queryKeys.add(d.getKey());
    }

    for (int i = 0; i < n && _provider.hasNext(); i++)
      _searchEngine.poll();

    _searchEngine.setPreload(false);
  }

  public void printStats()
  {
    class Stat
    {
      String _name;
      double _sum = 0d;
      int _n = 0;
      List<Long> _max = new ArrayList<>();

      long _min = Long.MAX_VALUE;

      public Stat(String name)
      {
        _name = name;
      }

      void add(RequestResult result)
      {
        long time = result.getFinishTime() - result.getStartTime();

        _sum += time;
        _n++;

        _max.add(time);

        if (time < _min)
          _min = time;
      }

      void print(PrintStream out)
      {
        out.print(_name);
        double avg = _sum / _n;
        StringBuilder max = new StringBuilder();
        Collections.sort(_max);

        for (int i = _max.size() - 1; i > _max.size() - 8; i--) {
          long v = _max.get(i);
          max.append(v).append(" ");
        }

        out.print(String.format("\n  avg: %1$f\tmin: %2$d\t max: %3$s\t n: %4$d",
                                avg, _min, max, _n));
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
    System.out.println("Start-Time: " + new Date());
    File file = new File(args[0]);

    long preloadSize = 100;
    long loadSize = 4000;

    DataProvider provider = new DataProvider(file, preloadSize + loadSize);

    SearchEngine driver = new Solr();
    //driver = new BaratineDriver("http://localhost:8085");
    driver = new BaratineRpc("http://debosx:8085");
    //driver = new BaratineTest("http://localhost:8085");

    PerformanceTest performanceTest = new PerformanceTest(4,
                                                          5f,
                                                          loadSize,
                                                          provider,
                                                          driver);

    long start = System.currentTimeMillis();
    performanceTest.preload(1);
    performanceTest.submitPoll();
    performanceTest.preload(preloadSize - 1);

    performanceTest.run();

    performanceTest.printStats();

    System.out.println("\ntest run time: " + (System.currentTimeMillis()
                                              - start));

    System.out.println("TestDriver.main " + driver.getMatches());
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

      result.setFinishTime(System.currentTimeMillis());
      result.setStartTime(start);

      _currentRequests.decrementAndGet();

      _results.add(result);

      return result;
    }
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
