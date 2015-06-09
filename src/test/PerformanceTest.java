package test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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

  public PerformanceTest(Args args)
    throws IOException, ExecutionException, InterruptedException
  {
    Objects.requireNonNull(args);

    _args = args;

    long size = _args.c() * (_args.n() + _args.preload());

    _provider = new WikiDataProvider(args.getDataDir(), size);

    _clients = new SearchClient[_args.c()];

    for (int i = 0; i < _clients.length; i++) {
      SearchClient client;

      switch (args.type()) {
      case BRPC: {
        client = new BaratineRpc(_provider,
                                 _args.n(),
                                 _args.ratio(),
                                 _args.host(),
                                 _args.port());
        break;
      }
      case BRPC2: {
        client = new BaratineRpc2(_provider,
                                  _args.n(),
                                  _args.ratio(),
                                  _args.host(),
                                  _args.port());
        break;
      }
      case SOLR: {
        client = new Solr(_provider,
                          _args.n(),
                          _args.ratio(),
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

  public void run() throws IOException, ExecutionException, InterruptedException
  {
    ExecutorService executors = Executors.newFixedThreadPool(_clients.length);

    for (SearchClient client : _clients) {
      client.preload(_args.preload());
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

  public void printStats() throws IOException
  {
    try (PrintWriter writer
           = new PrintWriter(new FileWriter(_args.getFile(), true), true)) {

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

      writer.println(_args);
      writer.println(
        String.format(
          "  submitted %1$d, searched %2$d, search-update-ratio %3$f, search-update-target %4$f",
          updates,
          searches,
          ((float) searches / updates),
          _args.ratio()));

      writer.println(
        String.format("  search avg: %1$f total-time: %2$d ops: %3$f",
                      ((float) searchTime / searches), searchTime,
                      ((float) searches / searchTime * 1000)));
      writer.println(
        String.format("  update avg: %1$f total-time: %2$d ops: %3$f",
                      ((float) updateTime / updates), updateTime,
                      ((float) updates / updateTime * 1000)));

      writer.println();
    }
  }

  public static void main(String[] vargs)
    throws IOException, ExecutionException, InterruptedException
  {
    Args args = new Args(vargs);

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
  private float _ratio = 5;
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
      else if ("-ratio".equals(arg)) {
        _ratio = Float.parseFloat(args[++i]);
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

  public float ratio()
  {
    return _ratio;
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
    return String.format("Args[%1$s, %2$d, %3$d, %4$d, %5$s, %6$d]",
                         _type,
                         _c,
                         _n,
                         _pre,
                         _host,
                         _port);
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