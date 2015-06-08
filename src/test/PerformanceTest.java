package test;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PerformanceTest
{
  private final int _preload;
  private final DataProvider _provider;
  private final float _targetRatio;
  private SearchClient[] _clients;

  public PerformanceTest(int c,
                         int n,
                         int preload,
                         String host,
                         int port,
                         float targetRatio,
                         DataProvider provider,
                         ClientType type)
    throws IOException, ExecutionException, InterruptedException
  {
    Objects.requireNonNull(provider);
    _preload = preload;
    _provider = provider;
    _targetRatio = targetRatio;

    _clients = new SearchClient[c];

    for (int i = 0; i < c; i++) {
      SearchClient client = null;

      switch (type) {
      case BRPC: {
        client = new BaratineRpc(_provider,
                                 n,
                                 targetRatio,
                                 host,
                                 port);
        break;
      }
      case BRPC2: {
        client = new BaratineRpc2(_provider,
                                  n,
                                  targetRatio,
                                  host,
                                  port);
        break;
      }
      case SOLR: {
        client = new Solr(_provider,
                          n,
                          targetRatio,
                          host,
                          port);
        break;
      }
      default: {
        throw new IllegalArgumentException();
      }
      }

      _clients[i] = client;
    }
  }

  public void run() throws IOException, ExecutionException, InterruptedException
  {
    ExecutorService executors = Executors.newFixedThreadPool(_clients.length);

    for (SearchClient client : _clients) {
      client.preload(_preload);
    }

    Future[] futures = new Future[_clients.length];
    for (int i = 0; i < _clients.length; i++) {
      futures[i] = executors.submit(_clients[i]);
    }

    for (Future future : futures) {
      future.get();
    }

    executors.shutdown();
  }

  public void printStats()
  {
    int updates = 0;
    int searches = 0;
    long updateTime = 0;
    long searchTime = 0;
    for (SearchClient client : _clients) {
      updates += client.getUpdateCount();
      searches += client.getSearchCount();

      updateTime = Math.max(updateTime, client.getUpdateTime());
      searchTime = Math.max(searchTime, client.getSearchTime());
    }

    System.out.println(
      String.format(
        "  clients %1$d, submitted %2$d, searched %3$d, search-update-ratio %4$f, search-update-ratio-target %5$f",
        _clients.length,
        updates,
        searches,
        ((float) searches / updates),
        _targetRatio));

    System.out.println(
      String.format("  search avg: %1$f total-time: %2$d ops: %3$f",
                    ((float) searchTime / searches), searchTime,
                    ((float) searches / searchTime * 1000)));
    System.out.println(
      String.format("  update avg: %1$f total-time: %2$d ops: %3$f",
                    ((float) updateTime / updates), updateTime,
                    ((float) updates / updateTime * 1000)));
  }

  public static void main(String[] args)
    throws IOException, ExecutionException, InterruptedException
  {
    System.out.println("Start-Time: " + new Date());
    File file = new File(args[0]);

    int c = 4;
    int n = 1000;
    int preload = 100;
    String host = "localhost";
    int port = 8085;
    float targetRatio = 5;

    if (!true) {
      c = 4;
      n = 400;
      preload = 10;
    }

    int size = c * (n + preload);

    DataProvider provider = new WikiDataProvider(file, size);

    ClientType type = ClientType.BRPC2;
    //type = ClientType.BRPC;
      type = ClientType.SOLR;

    PerformanceTest test = new PerformanceTest(c,
                                               n,
                                               preload,
                                               host,
                                               port,
                                               targetRatio,
                                               provider,
                                               type);

    System.out.println("Start-Time-Run : " + new Date());

    System.out.println("Finish-Time-Run: " + new Date());
    test.run();

    System.out.println(type + ":");

    test.printStats();
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

enum ClientType
{
  BRPC2,
  BRPC,
  SOLR
}