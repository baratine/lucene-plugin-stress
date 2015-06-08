package test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class HttpClient implements AutoCloseable
{
  static byte[] CRLF = {'\r', '\n'};

  private String _host;
  private Socket _socket;
  private OutputStream _out;
  private InputStream _in;
  private ClientResponseStream _clientResponseStream;

  ByteBuffer _outBuffer = ByteBuffer.allocateDirect(1024 * 512);

  public HttpClient(String host, int port) throws IOException
  {
    _host = host;
    _socket = new Socket(host, port);
    _socket.setReceiveBufferSize(1024);
    _socket.setSendBufferSize(1024);
    _socket.setKeepAlive(true);
    _socket.setTcpNoDelay(true);

    _out = _socket.getOutputStream();

    _in = new BufferedInputStream(_socket.getInputStream());

    _clientResponseStream = new ClientResponseStream(_in);
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
    _outBuffer.clear();

    writeMethod(_outBuffer, method, path);

    writeHeader(_outBuffer, "Host", _host);

    if (HttpMethod.GET == method) {
      _outBuffer.put(CRLF);
    }
    else {
      writeHeader(_outBuffer, "Content-Type", contentType);

      int l = data == null ? 0 : data.length;

      writeHeader(_outBuffer, "Content-Length", l);

      _outBuffer.put(CRLF);

      _outBuffer.put(data);
    }

    byte[] dataOut = new byte[_outBuffer.position()];

    _outBuffer.flip();
    _outBuffer.get(dataOut);

    _out.write(dataOut);
    _out.flush();

    _clientResponseStream.reset();
    _clientResponseStream.parse();

    return _clientResponseStream;
  }

  private ByteBuffer writeMethod(ByteBuffer out,
                                 HttpMethod method,
                                 String path)
    throws IOException
  {
    out.put(method.method().getBytes());
    out.put((byte) ' ');
    out.put(path.getBytes());
    out.put(" HTTP/1.1".getBytes());
    out.put(CRLF);

    return out;
  }

  private ByteBuffer writeHeader(ByteBuffer out, String key, String value)
    throws IOException
  {
    return writeHeader(out, key, value.getBytes());
  }

  private ByteBuffer writeHeader(ByteBuffer out, String key, int v)
    throws IOException
  {
    return writeHeader(out, key, Integer.toString(v).getBytes());
  }

  private ByteBuffer writeHeader(ByteBuffer out, String key, byte[] header)
    throws IOException
  {
    out.put(key.getBytes());
    out.put((byte) ':');
    out.put((byte) ' ');
    out.put(header);
    out.put(CRLF);

    return out;
  }

  public static class ClientResponseStream
  {
    private int _status = 0;
    Map<String,HttpHeader> _headers = new HashMap<>();
    private InputStream _in;

    public ClientResponseStream(InputStream in) throws IOException
    {
      _in = in;
    }

    public void reset()
    {
      _headers.clear();
      _status = 0;
    }

    private void parse() throws IOException
    {
      _status = parseStatus(_in);

      HttpHeader header;
      while ((header = parseHeader(_in)) != null)
        _headers.put(header.getKey(), header);
    }

    private int parseStatus(InputStream in) throws IOException
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

    private HttpHeader parseHeader(InputStream in) throws IOException
    {
      StringBuilder key = new StringBuilder(32);
      StringBuilder value = new StringBuilder(32);

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
    testGet();
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
