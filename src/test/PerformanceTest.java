package test;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PerformanceTest
{
  private final int _c;

  private final DataProvider _provider;

  public PerformanceTest(int c,
                         int n,
                         String host,
                         int port,
                         float targetRatio,
                         DataProvider provider)
  {
    Objects.requireNonNull(provider);

    _c = c;

    ExecutorService _executors = Executors.newFixedThreadPool(c);

    SearchClient[] clients = new SearchClient[c];

    for (int i = 0; i < clients.length; i++) {
      SearchClient client = new BaratineRpc2(provider,
                                             n,
                                             targetRatio,
                                             host,
                                             port);

    }

    _executors.shutdown();
  }

  public void run()
  {

  }

  public void printStats()
  {

  }

  public static void main(String[] args) throws IOException
  {
    System.out.println("Start-Time: " + new Date());
    File file = new File(args[0]);

    long preloadSize = 100;
    long loadSize = 4000;

    DataProvider provider;//= new DataProvider(file, preloadSize + loadSize);

    SearchClient driver;
    if (false) {
      driver = new Solr();
      //driver = new BaratineDriver("http://localhost:8085");
      //driver = new BaratineRpc("http://debosx:8085");
      provider = new WikiDataProvider(file, preloadSize + loadSize);
    }

    {
      driver = new BaratineTest("http://localhost:8085");

      provider = new NullDataProvider(preloadSize + loadSize);
    }

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
