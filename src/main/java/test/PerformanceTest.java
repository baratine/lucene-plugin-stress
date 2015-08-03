package test;

import io.baratine.core.ServiceClient;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PerformanceTest
{
  private final DataProvider _provider;
  private SearchClient[] _clients;

  private Args _args;

  private long _start, _finish;

  //
  private ServiceClient _serviceClient;

  public PerformanceTest(Args args)
    throws IOException, ExecutionException, InterruptedException
  {
    Objects.requireNonNull(args);

    _args = args;

    long size = _args.c() * (_args.n() / args.searchRate()
                             + _args.preload()
                             + 1);

    _provider = new WikiDataProvider(args.getDataDir(), size);

    _clients = new SearchClient[_args.c()];

    for (int i = 0; i < _clients.length; i++) {
      SearchClient client;

      switch (args.type()) {
      case BRPC2: {
        client = new BaratineRpc2(_provider,
                                  _args.n(),
                                  _args.searchRate(),
                                  _args.host(),
                                  _args.port());
        break;
      }
      case BRPJ: {
        client = createBaratineJavaClient();

        break;
      }
      case SOLR: {
        client = new Solr(_provider,
                          _args.n(),
                          _args.searchRate(),
                          _args.host(),
                          _args.port());
        break;
      }
      default: {
        throw new IllegalArgumentException();
      }
      }

      _clients[i] = client;
    }
  }

  private BaratineJava createBaratineJavaClient()
  {
    if (_serviceClient == null) {
      String url
        = String.format("http://%1$s:%2$d/s/lucene",
                        _args.host(),
                        _args.port());

      _serviceClient = ServiceClient.newClient(url).build();
    }

    return new BaratineJava(_provider,
                            _args.n(),
                            _args.searchRate(),
                            _serviceClient);
  }

  public void run() throws IOException, ExecutionException, InterruptedException
  {
    ExecutorService executors = Executors.newFixedThreadPool(_clients.length);

    preload(executors);

    System.out.println("sleeping for 2 seconds before warm up search ...");
    Thread.sleep(2000);

    warmupSearch(executors);
    System.out.println("starting test now ...");

    _start = System.currentTimeMillis();

    Future[] futures = new Future[_clients.length];
    for (int i = 0; i < _clients.length; i++) {
      futures[i] = executors.submit(_clients[i]);
    }

    for (Future future : futures) {
      future.get();
    }

    _finish = System.currentTimeMillis();

    executors.shutdown();
  }

  private void preload(ExecutorService executors)
    throws ExecutionException, InterruptedException
  {
    long preload = System.currentTimeMillis();
    Future[] futures = new Future[_clients.length];
    for (int i = 0; i < _clients.length; i++) {
      SearchClient client = _clients[i];
      futures[i] = executors.submit(() -> {
        try {
          client.preload(_args.preload());
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }

    for (Future future : futures) {
      future.get();
    }

    float elapsed = (float) (System.currentTimeMillis() - preload) / 1000;

    System.out.println(String.format(
      "preload complete %1$f seconds",
      elapsed));

  }

  private void warmupSearch(ExecutorService executors)
    throws ExecutionException, InterruptedException
  {
    long preload = System.currentTimeMillis();
    Future[] futures = new Future[_clients.length];
    for (int i = 0; i < _clients.length; i++) {
      SearchClient client = _clients[i];
      futures[i] = executors.submit(() -> {
        for (int j = 0; j < 100; j++) {
          DataProvider.Query query = _provider.getQuery();
          try {
            client.search(query.getQuery(), query.getKey());
          } catch (Throwable t) {
            t.printStackTrace();
          }
        }
      });
    }

    for (Future future : futures) {
      future.get();
    }

    float elapsed = (float) (System.currentTimeMillis() - preload) / 1000;

    System.out.println(String.format("warmupSearch complete %1$f seconds",
                                     elapsed));

  }

  public void printStats() throws IOException
  {
    List errors = new ArrayList<>();
    try (PrintWriter writer
           = new PrintWriter(new FileWriter(_args.getFile(), true), true)) {

      int updates = 0;
      int searches = 0;
      long updateTime = 0;
      long searchTime = 0;
      long notFoundCount = 0;
      long updateFailedCount = 0;
      for (SearchClient client : _clients) {
        updates += client.getUpdateCount();
        searches += client.getSearchCount();
        notFoundCount += client.getNotFoundCount();
        updateFailedCount += client.getUpdateFailedCount();

        updateTime = Math.max(updateTime, client.getUpdateTime());
        searchTime = Math.max(searchTime, client.getSearchTime());

        errors.addAll(client.getErrors());
      }

      writer.println(_args);
      writer.println(String.format("date: %1$s, run-time: %2$f",
                                   new Date(),
                                   (float) (_finish - _start) / 1000.f));

      writer.println(
        String.format(
          "  submitted %1$d, searched %2$d, search-rate-target: %3$d",
          updates, searches, _args.searchRate()));

      writer.println(
        String.format(
          "  search avg: %1$f total-time: %2$d ops: %3$f not-found: %4$d",
          ((float) searchTime / searches),
          searchTime,
          ((float) searches / searchTime * 1000),
          notFoundCount));
      writer.println(
        String.format(
          "  update avg: %1$f total-time: %2$d ops: %3$f update-failed: %4$d",
          ((float) updateTime / updates),
          updateTime,
          ((float) updates / updateTime * 1000),
          updateFailedCount));

      if (errors != null) {
        Map<String,Integer> map = new HashMap<>();

        for (Object error : errors) {
          String trace = toString((Throwable) error);
          Integer i = map.get(trace);
          if (i == null)
            i = new Integer(0);
          else
            i = new Integer(i.intValue() + 1);

          map.put(trace, i);
        }

        for (Map.Entry<String,Integer> entry : map.entrySet()) {
          writer.println(entry.getValue() + "  " + entry.getKey());
        }
      }

      writer.println();
    }
  }

  private String toString(Throwable t)
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintWriter writer = new PrintWriter(out, true);

    t.printStackTrace(writer);

    return new String(out.toByteArray());
  }

  public static void main(String[] vargs)
    throws IOException, ExecutionException, InterruptedException
  {
    Args args = new Args(vargs);

    System.out.println("PerformanceTest.main " + args);

    PerformanceTest test = new PerformanceTest(args);

    test.run();

    test.printStats();
  }
}

class Args
{
  private int _c = 1;
  private int _n = 1;
  private int _pre = 1;
  private String _host = "localhost";
  private int _port = 8085;
  private int _searchRate = 0;
  private ClientType _type = ClientType.BRPC2;
  private File _dataDir;
  private File _file;

  Args(String[] args)
  {
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];

      if ("-c".equals(arg)) {
        _c = Integer.parseInt(args[++i]);
      }
      else if ("-n".equals(arg)) {
        _n = Integer.parseInt(args[++i]);
      }
      else if ("-pre".equals(arg)) {
        _pre = Integer.parseInt(args[++i]);
      }
      else if ("-host".equals(arg)) {
        _host = args[++i];
      }
      else if ("-port".equals(arg)) {
        _port = Integer.parseInt(args[++i]);
      }
      else if ("-rate".equals(arg)) {
        _searchRate = Integer.parseInt(args[++i]);
      }
      else if ("-type".equals(arg)) {
        _type = ClientType.valueOf(args[++i]);
      }
      else if ("-dir".equals(arg)) {
        _dataDir = new File(args[++i]);
      }
      else if ("-file".equals(arg)) {
        _file = new File(args[++i]);
      }
      else {
        throw new IllegalStateException("unknown argument " + arg);
      }
    }

    Objects.requireNonNull(_dataDir);

    if (!_dataDir.isDirectory()) {
      throw new IllegalStateException(_dataDir + " is not a directory");
    }

  }

  public int c()
  {
    return _c;
  }

  public int n()
  {
    return _n;
  }

  public int preload()
  {
    return _pre;
  }

  public String host()
  {
    return _host;
  }

  public int port()
  {
    return _port;
  }

  public int searchRate()
  {
    return _searchRate;
  }

  public ClientType type()
  {
    return _type;
  }

  public File getDataDir()
  {
    return _dataDir;
  }

  public File getFile()
  {
    return _file;
  }

  @Override
  public String toString()
  {
    return String.format(
      "Args[type: %1$s, c: %2$d, n: %3$d, pre: %4$d, search-rate: %5$d, address: %6$s]",
      _type,
      _c,
      _n,
      _pre,
      _searchRate,
      _host + ':' + _port);
  }
}

/**
 * 5 clients
 * - 1 update
 * - 1 search
 * in: Reade Write searchRate
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
  BRPJ,
  SOLR
}