namespace Messaging.Abstractions.Exceptions;

public class RpcTimeoutException : TimeoutException
{
    public string Destination { get; }
    public string RequestType { get; }
    public TimeSpan Timeout { get; }
    public RpcTimeoutException(string destination, string requestType, TimeSpan timeout) : base($"RPC request '{requestType}' to '{destination}' timed out after {timeout}.")
    {
        Destination = destination;
        RequestType = requestType;
        Timeout = timeout;
    }
}
