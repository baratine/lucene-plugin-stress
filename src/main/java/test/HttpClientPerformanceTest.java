package test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class HttpClientPerformanceTest
{
  private final HttpClient _client;
  private int _requests = 0;
  private HttpClient.HttpMethod _method;
  private String _path;
  private String _contentType;
  private byte[] _data;
  private int _n;

  private long _start;
  private long _end;

  public HttpClientPerformanceTest(String host,
                                   int port,
                                   HttpClient.HttpMethod method,
                                   String path,
                                   String contentType,
                                   byte[] data,
                                   int n)
    throws IOException
  {
    _method = method;
    _path = path;
    _contentType = contentType;
    _data = data;
    _n = n;

    _client = new HttpClient(host, port);
  }

  public void reset()
  {
    _start = 0;
    _end = 0;
    _requests = 0;
  }

  public void execute(int n) throws IOException
  {
    _start = System.currentTimeMillis();

    for (int i = 0; i < n; i++) {
      HttpClient.ClientResponseStream response
        = _client.execute(_method, _path, _contentType, _data);

      if (response.getStatus() != 200) {
        throw new IllegalStateException("bad response " + response.getStatus());
      }

      String reply = response.getAsString();

      _requests++;
    }

    _end = System.currentTimeMillis();
  }

  public int getRequests()
  {
    return _requests;
  }

  public long getTime()
  {
    return _end - _start;
  }

  public static void execute(int c,
                             int n,
                             HttpClient.HttpMethod method,
                             byte[] data)
    throws IOException, ExecutionException, InterruptedException
  {
    ExecutorService executor = Executors.newFixedThreadPool(5);

    HttpClientPerformanceTest[] tests = new HttpClientPerformanceTest[c];

    for (int i = 0; i < tests.length; i++) {
      tests[i] = new HttpClientPerformanceTest("localhost",
                                               8085,
                                               method,
                                               "/s/lucene/test?m=test",
                                               "x-application/json-rpc",
                                               data,
                                               n);
    }

    for (HttpClientPerformanceTest test : tests) {
      test.execute(1);
      test.reset();
    }

    Future[] futures = new Future[tests.length];

    for (int i = 0; i < tests.length; i++) {
      HttpClientPerformanceTest test = tests[i];
      futures[i] = executor.submit(() -> {
        try {
          test.execute(n);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    }

    for (Future future : futures) {
      future.get();
    }

    executor.shutdown();

    long time = 0;
    int requests = 0;
    for (HttpClientPerformanceTest test : tests) {
      requests += test.getRequests();

      time = Math.max(time, test.getTime());
    }

    System.out.println("{");
    System.out.println("  clients  : " + tests.length);
    System.out.println("  requests : " + requests);
    System.out.println("  time     : " + time);
    System.out.println("  ops      : "
                       + (float) requests / time * 1000);
    System.out.println("}");
  }

  public static void main(String[] args)
    throws InterruptedException, ExecutionException, IOException
  {
    int c = Integer.parseInt(args[0]);
    int n = Integer.parseInt(args[1]);
    int r = Integer.parseInt(args[2]);

    String data = "[[\"query\",{},\"/test\",0,\"/test\",\"test\"]]";

    for (int i = 0; i < r; i++) {
      execute(c, n, HttpClient.HttpMethod.GET, data.getBytes());
    }
  }
}
