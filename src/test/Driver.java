package test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Driver
{
  ExecutorService _executors;

  List<Future> _list = new ArrayList<>();

  public Driver(int n, int rwRatio)
  {
    _executors = Executors.newFixedThreadPool(n);

  }

  public void run()
  {
    while (!_executors.isShutdown()) {

    }
  }

  public void stop()
  {

  }

  public static void main(String[] args)
  {
  }
}

/**
 * 5 clients
 * - 1 update
 * - 1 search
 */

/**
 * in: Reade Write ratio
 *     Search Data sorted
 *     Search Terms
 *     URL
 *     Handler (Solr or Baratine)
 *     Number of clients
 *
 *
 * out: Rate requests per second (or cycles per second)
 *
 */
