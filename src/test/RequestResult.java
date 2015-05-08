package test;

public class RequestResult
{
  private long _startTime;
  private long _finishTime;

  private RequestType _type;
  private Throwable _throwable;

  public RequestResult(RequestType type)
  {
    _type = type;
  }

  public RequestResult(RequestType type, Throwable throwable)
  {
    _type = type;
    _throwable = throwable;
  }

  public void setStartTime(long startTime)
  {
    _startTime = startTime;
  }

  public long getStartTime()
  {
    return _startTime;
  }

  public void setFinishTime(long finishTime)
  {
    _finishTime = finishTime;
  }

  public long getFinishTime()
  {
    return _finishTime;
  }

  public RequestType getType()
  {
    return _type;
  }

  @Override
  public String toString()
  {
    switch (_type) {
    case ERROR: {
      return "RequestResult["
             + _type
             + " , "
             + _throwable
             + "]";

    }
    default: {
      return "RequestResult["
             + _type
             + " , "
             + (_finishTime - _startTime)
             + "]";
    }
    }
  }

  public static RequestResult createSearchResult()
  {
    return new RequestResult(RequestType.SEARCH);
  }

  public static RequestResult createUpdateResult()
  {
    return new RequestResult(RequestType.UPDATE);
  }

  public static RequestResult createErrorResult(Throwable t)
  {
    return new RequestResult(RequestType.ERROR, t);
  }

  enum RequestType
  {
    SEARCH,
    UPDATE,
    ERROR
  }
}
