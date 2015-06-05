
package test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class HttpClient implements AutoCloseable
{
  static byte[] CRLF = {'\r', '\n'};

  private String _host;
  private Socket _socket;
  private OutputStream _out;
  private InputStream _in;

  public HttpClient(String host, int port) throws IOException
  {
    _host = host;
    _socket = new Socket(host, port);
    _socket.setReceiveBufferSize(512);
    _socket.setSendBufferSize(512);
    _socket.setKeepAlive(true);
    _socket.setTcpNoDelay(true);

    _out = new BufferedOutputStream(_socket.getOutputStream());

    _in = new BufferedInputStream(_socket.getInputStream());
  }

  @Override
  public void close() throws Exception
  {
    _socket.close();
  }

  public ClientResponseStream get(String path) throws IOException
  {
    return execute(HttpMethod.GET, path, null, null);
  }

  public ClientResponseStream post(String path, String contentType, byte[] data)
    throws IOException
  {
    return execute(HttpMethod.POST, path, contentType, data);
  }

  public ClientResponseStream execute(HttpMethod method,
                                      String path,
                                      String contentType,
                                      byte[] data)
    throws IOException
  {
    writeMethod(_out, method, path);

    writeHeader(_out, "Host", _host);

    if (HttpMethod.GET == method) {
      _out.write(CRLF);
    }
    else {
      writeHeader(_out, "Content-Type", contentType);

      int l = data == null ? 0 : data.length;

      writeHeader(_out, "Content-Length", l);

      _out.write(CRLF);

      _out.write(data);
    }

    _out.flush();

    int status = parseStatus(_in);

    Map<String,HttpHeader> headers = new HashMap<>();
    HttpHeader header;
    while ((header = parseHeader(_in)) != null)
      headers.put(header.getKey(), header);

    return new ClientResponseStream(status, headers, _in);
  }

  private OutputStream writeMethod(OutputStream out,
                                   HttpMethod method,
                                   String path)
    throws IOException
  {
    out.write(method.method().getBytes());
    out.write(' ');
    out.write(path.getBytes());
    out.write(" HTTP/1.1".getBytes());
    out.write(CRLF);

    return out;
  }

  private OutputStream writeHeader(OutputStream out, String key, String value)
    throws IOException
  {
    return writeHeader(out, key, value.getBytes());
  }

  private OutputStream writeHeader(OutputStream out, String key, int v)
    throws IOException
  {
    return writeHeader(out, key, Integer.toString(v).getBytes());
  }

  private OutputStream writeHeader(OutputStream out, String key, byte[] header)
    throws IOException
  {
    out.write(key.getBytes());
    out.write(':');
    out.write(' ');
    out.write(header);
    out.write(CRLF);

    return out;
  }

  class ClientResponseStream
  {
    private int _status;
    Map<String,HttpHeader> _headers;
    private InputStream _in;

    public ClientResponseStream(int status,
                                Map<String,HttpHeader> headers,
                                InputStream in)
    {
      _status = status;
      _headers = headers;
      _in = in;
    }

    public int getStatus()
    {
      return _status;
    }

    public int getLength()
    {
      HttpHeader header = _headers.get("Content-Length");

      if (header == null)
        return 0;
      else
        return header.getIntValue();
    }

    public Map<String,HttpHeader> getHeaders()
    {
      return _headers;
    }

    public InputStream getInputStream()
    {
      return _in;
    }

    public String getAsString() throws IOException
    {
      return new String(getBytes());
    }

    public byte[] getBytes() throws IOException
    {
      byte[] buffer = new byte[getLength()];

      int off = 0, l = 0;
      int len = getLength();

      while (len > 0) {
        l = _in.read(buffer, off, len);
        off += l;
        len -= l;
      }

      return buffer;
    }
  }

  /*
  HTTP/1.1 200 OK
  Server: Resin/snap
  Content-Length: 29
  Date: Fri, 05 Jun 2015 16:55:11 GMT

  [["reply",{},"/test",0,true]]
  */

  private static int parseStatus(InputStream in) throws IOException
  {
    int i;

    for (i = in.read(); i != ' '; i = in.read()) ;

    int status = 0;

    for (i = in.read(); i != ' '; i = in.read()) {
      status = status * 10 + i - '0';
    }

    for (i = in.read(); i != '\r'; i = in.read()) ;

    if ((i = in.read()) != '\n')
      throw new IllegalStateException(String.format(
        "expected %1$s but recieved %2$s",
        Integer.toHexString('\n'),
        Integer.toHexString(i)));

    return status;
  }

  /*
  HTTP/1.1 200 OK
  Server: Resin/snap
  Content-Length: 29
  Date: Fri, 05 Jun 2015 16:55:11 GMT

  [["reply",{},"/test",0,true]]
  */

  static class HttpHeader
  {
    private String _key;
    private String _value;

    public HttpHeader(StringBuilder key, StringBuilder value)
    {
      _key = key.toString();
      _value = value == null ? null : value.toString();
    }

    public int getIntValue()
    {
      return Integer.parseInt(_value);
    }

    public String getKey()
    {
      return _key;
    }

    @Override
    public String toString()
    {
      return "HttpHeader[" + _key + ": " + _value + ']';
    }
  }

  public static HttpHeader parseHeader(InputStream in) throws IOException
  {
    StringBuilder key = new StringBuilder();
    StringBuilder value = new StringBuilder();

    int i = in.read();

    if (i == '\r')
      if ((i = in.read()) == '\n') {
        return null;
      }
      else {
        throw new IllegalStateException(String.format(
          "expected %1$s but recieved %2$s",
          Integer.toHexString('\n'),
          Integer.toHexString(i)));
      }

    key.append((char) i);

    for (i = in.read(); i != ':'; i = in.read()) {
      key.append((char) i);
    }

    for (i = in.read(); i == ' '; i = in.read()) ;

    if (i == '\r')
      if ((i = in.read()) == '\n') {
        return new HttpHeader(key, null);
      }
      else {
        throw new IllegalStateException(String.format(
          "expected %1$s but recieved %2$s",
          Integer.toHexString('\n'),
          Integer.toHexString(i)));
      }

    value.append((char) i);

    for (i = in.read(); i != '\r'; i = in.read()) {
      value.append((char) i);
    }

    if ((i = in.read()) != '\n')
      throw new IllegalStateException(String.format(
        "expected %1$s but recieved %2$s",
        Integer.toHexString('\n'),
        Integer.toHexString(i)));

    return new HttpHeader(key, value);
  }

  public static void testGet() throws IOException
  {
    HttpClient client = new HttpClient("localhost", 8085);

    ClientResponseStream response = client.get("/s/lucene/test?m=test");

    System.out.println(response.getAsString());
  }

  public static void testPost() throws IOException
  {
    HttpClient client = new HttpClient("localhost", 8085);

    String template
      = "[[\"query\",{},\"/test\",%1$s,\"/test\",\"test\"]]";

    byte[] data = String.format(template, "0").getBytes();

    ClientResponseStream response
      = client.post("/s/lucene", "x-application/jamp-rpc",
                    data);

    System.out.println(response.getAsString());
  }

  public static void main(String[] args) throws IOException
  {
    //testGet();
    testPost();
  }

  enum HttpMethod
  {

    GET {
      @Override
      public String method()
      {
        return "GET";
      }
    },
    POST {
      @Override
      public String method()
      {
        return "POST";
      }
    };

    public abstract String method();
  }
}
